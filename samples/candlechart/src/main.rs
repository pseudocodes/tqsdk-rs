use std::{env, time::Duration};

use cli_candlestick_chart::Candle;
use tokio::sync::mpsc;
use tqsdk_rs::{Client, ClientConfig};
use tracing::info;

#[tokio::main]
async fn main() {
    tqsdk_rs::init_logger("error", false);

    let username = env::var("SHINNYTECH_ID").expect("请设置 SHINNYTECH_ID 环境变量");
    let password = env::var("SHINNYTECH_PW").expect("请设置 SHINNYTECH_PW 环境变量");

    let mut config = ClientConfig::default();
    config.log_level = "info".to_string();
    config.view_width = 500;

    let (tx, mut rx) = mpsc::channel(100);

    let symbol = "SHFE.au2606";

    let mut client = Client::new(&username, &password, config)
        .await
        .expect("创建客户端失败");

    client.init_market().await.expect("初始化行情功能失败");

    // 创建订阅（延迟启动，推荐方式）
    let series_api = client.series().expect("获取 series API 失败");
    let sub = series_api
        .kline(symbol, Duration::from_secs(60), 200)
        .await
        .expect("订阅失败");

    // 使用通用更新回调（包含更新信息）
    let tx_clone = tx.clone();
    sub.on_update(move |data, _info| {
        if let Some(sym_data) = data.get_symbol_klines(symbol) {
            let candles: Vec<Candle> = sym_data
                .data
                .iter()
                .map(|kline| {
                    Candle::new(
                        kline.open,
                        kline.high,
                        kline.low,
                        kline.close,
                        Some(kline.volume as f64),
                        Some(kline.datetime / 1_000_000_000),
                    )
                })
                .collect();

            let tx = tx_clone.clone();
            tokio::spawn(async move {
                let _ = tx.send(candles).await;
            });
        }
    })
    .await;

    sub.start().await.expect("启动监听失败");
    info!("✅ 订阅已启动");

    let candles: Vec<Candle> = Vec::new();
    let mut chart = cli_candlestick_chart::Chart::new(&candles);
    chart.set_name(symbol.to_string());

    while let Some(candles) = rx.recv().await {
        chart = cli_candlestick_chart::Chart::new(&candles);
        chart.set_name(symbol.to_string());
        chart.set_bull_color(1, 205, 254);
        chart.set_bear_color(255, 107, 153);
        chart.set_vol_bull_color(1, 205, 254);
        chart.set_vol_bear_color(255, 107, 153);
        chart.set_volume_pane_enabled(true);
        chart.draw();
    }
}
