//! Quote 行情订阅示例
//!
//! 演示以下功能：
//! - Quote 实时行情订阅（Channel 和 Callback 两种模式）
//! - 单合约 K线订阅（延迟启动模式，推荐）
//! - 多合约 K线订阅（自动对齐）
//! - Tick 订阅

use std::time::Duration;
use std::{env, vec};
use tokio;
use tqsdk_rs::*;
use tracing::info;

/// Quote 订阅示例
async fn quote_subscription_example() {
    info!("==================== Quote 订阅示例 ====================");

    let username = env::var("SHINNYTECH_ID").expect("请设置 SHINNYTECH_ID 环境变量");
    let password = env::var("SHINNYTECH_PW").expect("请设置 SHINNYTECH_PW 环境变量");

    // 创建客户端
    let mut config = ClientConfig::default();
    config.log_level = "info".to_string();
    config.view_width = 10000;
    config.development = true;

    let mut client = Client::new(&username, &password, config)
        .await
        .expect("创建客户端失败");

    // 初始化行情功能
    client.init_market().await.expect("初始化行情功能失败");

    info!("等待客户端初始化完成...");
    tokio::time::sleep(Duration::from_secs(2)).await;

    // 订阅多个合约
    info!("开始订阅合约...");
    let ins_list = vec!["SHFE.au2602", "SHFE.ag2512", "DCE.m2512"];

    // let quote_sub = client
    //     .subscribe_quote(&["SHFE.au2602", "SHFE.ag2512", "DCE.m2512"])
    //     .await
    //     .expect("订阅失败");
    let quote_sub = client.subscribe_quote(&ins_list).await.expect("订阅失败");

    // 方式 1: 使用 Channel 接收（async-channel 支持多个订阅者）
    let quote_rx = quote_sub.quote_channel();
    tokio::spawn(async move {
        let mut count = 0;
        loop {
            match quote_rx.recv().await {
                Ok(quote) => {
                    count += 1;
                    info!("收到 Quote 更新 #{}: {}", count, quote.instrument_id);

                    // 用户自行过滤合约
                    if quote.instrument_id == "SHFE.au2602" {
                        info!(
                            "📊 沪金: 最新价={:.2}, 涨跌={:.2}, 买一={:.2}, 卖一={:.2}",
                            quote.last_price, quote.change, quote.bid_price1, quote.ask_price1
                        );
                    }
                }
                Err(_) => {
                    info!("Quote Channel 已关闭");
                    break;
                }
            }
        }
    });

    // 方式 2: 使用回调接口（参数是 Arc<Quote>）
    quote_sub
        .on_quote(|quote| {
            // quote 是 Arc<Quote>，可以直接使用
            if quote.instrument_id == "SHFE.ag2512" {
                info!(
                    "📊 沪银: 最新价={:.2}, 涨跌={:.2}, 买一={:.2}, 卖一={:.2}",
                    quote.last_price, quote.change, quote.bid_price1, quote.ask_price1
                );
            }
            info!(
                "[回调] {}: 最新价={:.2}",
                quote.instrument_id, quote.last_price
            );
        })
        .await;

    // 运行 30 秒
    tokio::time::sleep(Duration::from_secs(30)).await;
    info!("Quote 订阅示例结束");
}

/// 单合约 K线订阅示例（推荐用法：延迟启动模式）
async fn single_kline_subscription_example() {
    info!("==================== 单合约 K线订阅示例 ====================");

    let username = env::var("SHINNYTECH_ID").expect("请设置 SHINNYTECH_ID 环境变量");
    let password = env::var("SHINNYTECH_PW").expect("请设置 SHINNYTECH_PW 环境变量");

    let mut config = ClientConfig::default();
    config.log_level = "info".to_string();
    config.view_width = 20000;

    // let symbol = "SHFE.au2602";
    let symbol = "CFFEX.IF2604";

    let mut client = Client::new(&username, &password, config)
        .await
        .expect("创建客户端失败");

    client.init_market().await.expect("初始化行情功能失败");

    // 创建订阅（延迟启动，推荐方式）
    let series_api = client.series().expect("获取 series API 失败");
    let sub = series_api
        // .kline("SHFE.au2602", Duration::from_secs(60), 5)
        .kline(&symbol, Duration::from_secs(60), 3010)
        .await
        .expect("订阅失败");

    let symbol2 = symbol;
    // 方式 1: 使用通用更新回调（包含更新信息）
    sub.on_update(|data, info| {
        if let Some(sym_data) = data.get_symbol_klines(symbol2) {
            if info.has_new_bar {
                // 新增了一根 K线
                info!(
                    "🆕 新 K线! ID={:?}, 数据量={}",
                    info.new_bar_ids.get(symbol2),
                    sym_data.data.len()
                );

                if let Some(latest) = sym_data.data.last() {
                    info!(
                        "   时间={} O:{:.2} H:{:.2} L:{:.2} C:{:.2} V:{}",
                        chrono::DateTime::from_timestamp_nanos(latest.datetime).format("%H:%M:%S"),
                        latest.open,
                        latest.high,
                        latest.low,
                        latest.close,
                        latest.volume
                    );
                }
            }

            if info.has_bar_update && !info.has_new_bar {
                // 更新了最后一根 K线
                info!("🔄 K线更新 (LastID={})", sym_data.last_id);

                if let Some(latest) = sym_data.data.last() {
                    info!(
                        "   当前价:{:.2} (L:{:.2} H:{:.2} V:{})",
                        latest.close, latest.low, latest.high, latest.volume
                    );
                }
            }

            if info.chart_range_changed {
                info!(
                    "📊 Chart 范围变化: [{},{}] -> [{},{}]",
                    info.old_left_id, info.old_right_id, info.new_left_id, info.new_right_id
                );
            }

            if info.has_chart_sync {
                if let Some(single) = &data.single {
                    if let Some(chart) = &single.chart {
                        info!(
                            "✅ Chart 同步完成! 范围: [{},{}]",
                            chart.left_id, chart.right_id
                        );
                    }
                }
            }
        }
    })
    .await;

    let symbol3 = symbol;
    // 方式 2: 使用专门的新 K线回调
    sub.on_new_bar(|data| {
        if let Some(sym_data) = data.get_symbol_klines(symbol3) {
            if let Some(latest) = sym_data.data.last() {
                info!(
                    "🎯 新 K线: [{}] C={:.2} V={} (序列长度={})",
                    latest.id,
                    latest.close,
                    latest.volume,
                    sym_data.data.len()
                );

                // 示例：计算最近5根K线的平均价格
                if sym_data.data.len() >= 5 {
                    let sum: f64 = sym_data.data[sym_data.data.len() - 5..]
                        .iter()
                        .map(|k| k.close)
                        .sum();
                    let ma5 = sum / 5.0;
                    info!("   MA5={:.2}", ma5);
                }
            }
        }
    })
    .await;

    sub.on_bar_update(|data| {
        if let Some(sym_data) = data.get_symbol_klines(symbol) {
            if let Some(latest) = sym_data.data.last() {
                info!("⏰ K线更新: [{}] C={:.2} (实时)", latest.id, latest.close);
            }
        }
    })
    .await;

    // 所有回调注册完成后，启动监听（不会错过数据）
    sub.start().await.expect("启动监听失败");
    info!("✅ 订阅已启动");

    // 运行 50 秒
    tokio::time::sleep(Duration::from_secs(50)).await;
    info!("单合约 K线订阅示例结束");
}

/// 多合约 K线订阅示例（自动对齐）
async fn multi_kline_subscription_example() {
    info!("==================== 多合约 K线订阅示例 ====================");

    let username = env::var("SHINNYTECH_ID").expect("请设置 SHINNYTECH_ID 环境变量");
    let password = env::var("SHINNYTECH_PW").expect("请设置 SHINNYTECH_PW 环境变量");

    let mut config = ClientConfig::default();
    config.log_level = "info".to_string();
    config.view_width = 500;

    let mut client = Client::new(&username, &password, config)
        .await
        .expect("创建客户端失败");

    client.init_market().await.expect("初始化行情功能失败");

    // 订阅多个合约的 1分钟 K线
    let series_api = client.series().expect("获取 series API 失败");
    let sub = series_api
        .kline_multi(
            &[
                "SHFE.au2602".to_string(),
                "SHFE.ag2512".to_string(),
                "INE.sc2601".to_string(),
            ],
            Duration::from_secs(60),
            10,
        )
        .await
        .expect("订阅失败");

    sub.on_update(|data, info| {
        if info.has_new_bar {
            info!("\n🆕 新 K线产生!");
            for (symbol, bar_id) in &info.new_bar_ids {
                info!("  - {}: ID={}", symbol, bar_id);
            }

            // 显示对齐的数据
            if let Some(multi) = &data.multi {
                if let Some(latest) = multi.data.last() {
                    info!(
                        "\n时间: {} (MainID={})",
                        latest.timestamp.format("%H:%M:%S"),
                        latest.main_id
                    );

                    for (symbol, kline) in &latest.klines {
                        info!(
                            "  {}: O={:.2} C={:.2} H={:.2} L={:.2} V={}",
                            symbol, kline.open, kline.close, kline.high, kline.low, kline.volume
                        );
                    }
                }
            }
        }

        if info.has_chart_sync {
            if let Some(multi) = &data.multi {
                info!("✅ 多合约 Chart 同步完成!");
                info!(
                    "主合约: {}, 合约数: {}",
                    multi.main_symbol,
                    multi.symbols.len()
                );
                info!(
                    "数据范围: [{}, {}], 总共 {} 根K线",
                    multi.left_id,
                    multi.right_id,
                    multi.data.len()
                );
            }
        }
    })
    .await;

    sub.start().await.expect("启动监听失败");

    // 运行 30 秒
    tokio::time::sleep(Duration::from_secs(30)).await;
    info!("多合约 K线订阅示例结束");
}

/// Tick 订阅示例（推荐用法：延迟启动模式）
async fn tick_subscription_example() {
    info!("==================== Tick 订阅示例 ====================");

    let username = env::var("SHINNYTECH_ID").expect("请设置 SHINNYTECH_ID 环境变量");
    let password = env::var("SHINNYTECH_PW").expect("请设置 SHINNYTECH_PW 环境变量");

    let mut config = ClientConfig::default();
    config.log_level = "info".to_string();
    config.development = true;
    config.view_width = 500;

    let mut client = Client::new(&username, &password, config)
        .await
        .expect("创建客户端失败");

    client.init_market().await.expect("初始化行情功能失败");

    // 创建订阅（延迟启动，推荐方式）
    let series_api = client.series().expect("获取 series API 失败");
    let sub = series_api.tick("SHFE.au2602", 5).await.expect("订阅失败");

    // 先注册所有回调函数
    sub.on_new_bar(|data| {
        if let Some(tick_data) = &data.tick_data {
            if let Some(tick) = tick_data.data.last() {
                info!(
                    "📈 新 Tick: [{}] 最新价={:.2} 买一={:.2}({}) 卖一={:.2}({}) 成交量={} (序列长度={})",
                    tick.id,
                    tick.last_price,
                    tick.bid_price1,
                    tick.bid_volume1,
                    tick.ask_price1,
                    tick.ask_volume1,
                    tick.volume,
                    tick_data.data.len()
                );
            }
        }
    }).await;

    sub.on_bar_update(|data| {
        if let Some(tick_data) = &data.tick_data {
            if let Some(tick) = tick_data.data.last() {
                info!("🔄 Tick 更新: [{}] 最新价={:.2}", tick.id, tick.last_price);
            }
        }
    })
    .await;

    // 所有回调注册完成后，启动监听（不会错过数据）
    sub.start().await.expect("启动监听失败");
    info!("✅ 订阅已启动");

    // 运行 20 秒
    tokio::time::sleep(Duration::from_secs(20)).await;
    info!("Tick 订阅示例结束");
}

#[tokio::main]
async fn main() {
    // 初始化日志：同时输出到终端和文件
    init_logger_with_file("quote=info,tqsdk_rs=debug", false);

    // 运行各个示例（取消注释以运行）
    // quote_subscription_example().await;
    single_kline_subscription_example().await;
    // multi_kline_subscription_example().await;
    // tick_subscription_example().await;

    info!("所有示例运行完成!");
}

/// 初始化日志系统：同时输出到终端和文件
fn init_logger_with_file(level: &str, filter_crate_only: bool) {
    use std::fs;
    use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

    // 创建日志目录
    fs::create_dir_all("logs").expect("无法创建日志目录");

    // 生成日志文件名（带时间戳）
    let log_file = format!(
        "logs/quote_{}.log",
        chrono::Local::now().format("%Y%m%d_%H%M%S")
    );
    let file = fs::File::create(&log_file).expect("无法创建日志文件");

    // 构建过滤器
    let filter = if filter_crate_only {
        EnvFilter::new(format!("tqsdk_rs={}", level))
    } else {
        EnvFilter::new(level)
    };

    // Layer 1: 终端输出（带颜色）
    let console_layer = fmt::layer()
        .with_target(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_line_number(true)
        .with_file(true)
        .with_ansi(true) // 启用颜色
        .with_timer(fmt::time::OffsetTime::local_rfc_3339().expect("无法获取本地时区"))
        .compact();

    // Layer 2: 文件输出（无颜色）
    let file_layer = fmt::layer()
        .with_target(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_line_number(true)
        .with_file(true)
        .with_ansi(false) // 禁用颜色
        .with_timer(fmt::time::OffsetTime::local_rfc_3339().expect("无法获取本地时区"))
        .with_writer(file)
        .compact();

    // 组合两个 Layer
    tracing_subscriber::registry()
        .with(filter)
        .with(console_layer)
        .with(file_layer)
        .init();

    println!("日志已初始化，输出到终端和文件: {}", log_file);
}
