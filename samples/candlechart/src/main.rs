use std::{env, io::Write, time::Duration};

use cli_candlestick_chart::{Candle, Chart};
use tokio::{signal, sync::mpsc, time::Instant};
use tqsdk_rs::{Client, ClientConfig};
use tracing_subscriber::EnvFilter;

const SYMBOL: &str = "CFFEX.TL2612";
/// 终端渲染远贵于一次行情推送，两帧之间留个下限
const MIN_REDRAW_INTERVAL: Duration = Duration::from_millis(200);

#[tokio::main]
async fn main() {
    // 日志走 stderr：stdout 让给图表，否则一条 error 日志就能把画面撕开。
    // 抢先 init 之后，Client 内部的 init_logger 会因 try_init 失败而变成 no-op。
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(EnvFilter::new("warn"))
        .init();

    let username = env::var("SHINNYTECH_ID").expect("请设置 SHINNYTECH_ID 环境变量");
    let password = env::var("SHINNYTECH_PW").expect("请设置 SHINNYTECH_PW 环境变量");

    let config = ClientConfig {
        view_width: 500,
        ..Default::default()
    };

    let mut client = Client::new(&username, &password, config)
        .await
        .expect("创建客户端失败");
    client.init_market().await.expect("初始化行情功能失败");

    let series_api = client.series().expect("获取 series API 失败");
    let sub = series_api
        .kline(SYMBOL, Duration::from_secs(60), 200)
        .await
        .expect("订阅失败");

    // 容量 1：只关心最新一帧
    let (tx, mut rx) = mpsc::channel::<Vec<Candle>>(1);

    sub.on_update(move |data, _info| {
        let Some(sym) = data.get_symbol_klines(SYMBOL) else {
            return;
        };
        let candles: Vec<Candle> = sym
            .data
            .iter()
            .map(|k| {
                Candle::new(
                    k.open,
                    k.high,
                    k.low,
                    k.close,
                    Some(k.volume as f64),
                    Some(k.datetime / 1_000_000_000),
                )
            })
            .collect();
        // 同步发送保证顺序；满了说明上一帧还在画，丢掉旧帧正是我们要的

        let _ = tx.try_send(candles);
    })
    .await;

    sub.on_error(|e| eprintln!("行情错误: {e}")).await;
    sub.start().await.expect("启动监听失败");

    enter_alt_screen();

    let mut last_draw: Option<Instant> = None;
    loop {
        tokio::select! {
            _ = signal::ctrl_c() => break,
            frame = rx.recv() => {
                let Some(mut candles) = frame else { break };
                // 排空积压，只画最新的
                while let Ok(newer) = rx.try_recv() {
                    candles = newer;
                }

                if let Some(t) = last_draw {
                    let wait = MIN_REDRAW_INTERVAL.saturating_sub(t.elapsed());
                    if !wait.is_zero() {
                        tokio::time::sleep(wait).await;
                    }
                }
                last_draw = Some(Instant::now());

                // Chart 内含 Rc<RefCell<_>>，不是 Send，必须在同一个线程内建好画完。
                // 渲染 + 写 stdout 是阻塞活儿，别占着 runtime worker。
                let _ = tokio::task::spawn_blocking(move || draw(&candles)).await;
            }
        }
    }

    leave_alt_screen();
    if let Err(e) = sub.close().await {
        eprintln!("关闭订阅失败: {e}");
    }
}

fn draw(candles: &[Candle]) {
    let mut chart = Chart::new(candles);
    chart.set_name(SYMBOL.to_string());
    chart.set_bear_color(1, 205, 254);
    chart.set_bull_color(255, 107, 153);
    chart.set_vol_bear_color(1, 205, 254);
    chart.set_vol_bull_color(255, 107, 153);
    chart.set_volume_pane_enabled(true);

    print!("\x1b[H"); // 回左上角覆盖上一帧，而不是向下追加
    chart.draw();
    print!("\x1b[J"); // 擦掉上一帧可能更长的尾巴
    let _ = std::io::stdout().flush();
}

fn enter_alt_screen() {
    print!("\x1b[?1049h\x1b[?25l");
    let _ = std::io::stdout().flush();
}

fn leave_alt_screen() {
    print!("\x1b[?25h\x1b[?1049l");
    let _ = std::io::stdout().flush();
}
