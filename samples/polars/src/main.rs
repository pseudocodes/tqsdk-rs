//! Polars DataFrame 集成示例
//!
//! 演示如何使用 KlineBuffer 和 TickBuffer 进行数据分析
//!
//! 运行方式：
//! ```bash
//! cargo run --example polars_demo --features polars
//! ```

use std::sync::{Arc, RwLock};
use std::time::Duration;
use tqsdk_rs::{Client, ClientConfig, KlineBuffer, SeriesAPI, TickBuffer};

// 导入 Polars 的 trait
use polars::prelude::*;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 初始化日志
    tqsdk_rs::init_logger("debug", false);

    // 从环境变量读取账号信息
    let username = std::env::var("SHINNYTECH_ID").unwrap_or_else(|_| "your_username".to_string());
    let password = std::env::var("SHINNYTECH_PW").unwrap_or_else(|_| "your_password".to_string());

    println!("=== Polars DataFrame 集成示例 ===\n");

    // 创建客户端
    let mut client = Client::new(&username, &password, ClientConfig::default()).await?;

    // 初始化行情
    client.init_market().await?;
    println!("✓ 行情初始化完成\n");

    let series_api = client.series().expect("获取 Series_api 失败");

    example_kline(&series_api).await?;
    example_tick(&series_api).await?;
    example_multi_kline(&series_api).await?;

    println!("\n=== 示例完成 ===");
    Ok(())
}

/// 示例 1: K线数据分析
async fn example_kline(series_api: &SeriesAPI) -> Result<(), Box<dyn std::error::Error>> {
    println!("【示例 1】K线数据分析");
    println!("订阅 SHFE.au2506 的 1分钟 K线...\n");

    let subscription = series_api
        .kline("SHFE.au2506", Duration::from_secs(60), 100)
        .await?;

    let kline_buffer = Arc::new(RwLock::new(KlineBuffer::new()));

    subscription
        .on_update({
            let buffer = Arc::clone(&kline_buffer);
            move |series_data, update_info| {
                println!("收到数据更新:");
                println!("  - 有新K线: {}", update_info.has_new_bar);
                println!("  - K线更新: {}", update_info.has_bar_update);

                if let Some(kline_data) = &series_data.single {
                    let mut buf = buffer.write().unwrap();
                    if update_info.has_chart_sync {
                        // 初始同步完成：用全量历史数据初始化缓冲区
                        *buf = KlineBuffer::from_klines(&kline_data.data);
                        println!("  ✓ 初始化缓冲区，共 {} 根K线", buf.len());
                    } else if update_info.has_new_bar {
                        if let Some(last_kline) = kline_data.data.last() {
                            buf.push(last_kline);
                            println!("  ✓ 追加新K线，ID: {}", last_kline.id);
                        }
                    } else if update_info.has_bar_update {
                        if let Some(last_kline) = kline_data.data.last() {
                            buf.update_last(last_kline);
                            println!("  ✓ 更新最后一根K线");
                        }
                    }

                    if !buf.is_empty() {
                        match buf.to_dataframe() {
                            Ok(df) => {
                                println!("\n  DataFrame 信息:");
                                println!("    - 行数: {}", df.height());
                                println!("    - 列数: {}", df.width());

                                if let Ok(tail_df) = buf.tail(5) {
                                    println!("\n  最后 5 根K线:");
                                    println!("{}", tail_df);
                                }

                                if let Ok(_close_series) = df.column("close") {
                                    println!("\n  统计信息:");

                                    if let Ok(stats) = df
                                        .clone()
                                        .lazy()
                                        .select([
                                            col("close").mean().alias("mean"),
                                            col("close").std(1).alias("std"),
                                            col("close").max().alias("max"),
                                            col("close").min().alias("min"),
                                        ])
                                        .collect()
                                    {
                                        if let Ok(mean_val) =
                                            stats.column("mean").and_then(|s| s.f64())
                                        {
                                            if let Some(mean) = mean_val.get(0) {
                                                println!("    - 收盘价均值: {:.2}", mean);
                                            }
                                        }
                                        if let Ok(std_val) =
                                            stats.column("std").and_then(|s| s.f64())
                                        {
                                            if let Some(std) = std_val.get(0) {
                                                println!("    - 收盘价标准差: {:.2}", std);
                                            }
                                        }
                                        if let Ok(max_val) =
                                            stats.column("max").and_then(|s| s.f64())
                                        {
                                            if let Some(max) = max_val.get(0) {
                                                println!("    - 最高收盘价: {:.2}", max);
                                            }
                                        }
                                        if let Ok(min_val) =
                                            stats.column("min").and_then(|s| s.f64())
                                        {
                                            if let Some(min) = min_val.get(0) {
                                                println!("    - 最低收盘价: {:.2}", min);
                                            }
                                        }
                                    }
                                }
                                drop(buf);
                            }
                            Err(e) => {
                                eprintln!("  ✗ 转换 DataFrame 失败: {}", e);
                            }
                        }
                    }
                }

                println!("{}", "=".repeat(60));
            }
        })
        .await;

    subscription.start().await?;

    println!("等待 5 秒接收数据...\n");
    tokio::time::sleep(Duration::from_secs(5)).await;

    subscription.close().await?;
    println!("\n✓ K线订阅已关闭");
    Ok(())
}

/// 示例 2: Tick 数据分析
async fn example_tick(series_api: &SeriesAPI) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n【示例 2】Tick 数据分析");
    println!("订阅 SHFE.au2506 的 Tick 数据...\n");

    let tick_subscription = series_api.tick("SHFE.au2506", 10).await?;

    let tick_buffer = Arc::new(RwLock::new(TickBuffer::new()));

    tick_subscription
        .on_update({
            let buffer = Arc::clone(&tick_buffer);
            move |series_data, update_info| {
                if let Some(tick_data) = &series_data.tick_data {
                    let mut buf = buffer.write().unwrap();
                    if update_info.has_chart_sync {
                        // 初始同步完成：用全量历史数据初始化缓冲区
                        *buf = TickBuffer::from_ticks(&tick_data.data);
                        println!("  ✓ 初始化缓冲区，共 {} 条Tick", buf.len());
                    } else if update_info.has_new_bar {
                        if let Some(last_tick) = tick_data.data.last() {
                            buf.push(last_tick);
                            println!(
                                "收到新 Tick: 价格={:.2}, 成交量={}",
                                last_tick.last_price, last_tick.volume
                            );
                        }
                    }

                    if buf.len() % 10 == 0 && !buf.is_empty() {
                        if let Ok(df) = buf.to_dataframe() {
                            println!("\nTick 统计 (共 {} 条):", buf.len());

                            if let Ok(stats) = df
                                .clone()
                                .lazy()
                                .select([
                                    col("last_price").mean().alias("mean"),
                                    col("last_price").max().alias("max"),
                                    col("last_price").min().alias("min"),
                                ])
                                .collect()
                            {
                                if let Ok(mean_val) = stats.column("mean").and_then(|s| s.f64()) {
                                    if let Some(mean) = mean_val.get(0) {
                                        println!("  - 最新价均值: {:.2}", mean);
                                    }
                                }
                                if let Ok(max_val) = stats.column("max").and_then(|s| s.f64()) {
                                    if let Some(max) = max_val.get(0) {
                                        println!("  - 最高价: {:.2}", max);
                                    }
                                }
                                if let Ok(min_val) = stats.column("min").and_then(|s| s.f64()) {
                                    if let Some(min) = min_val.get(0) {
                                        println!("  - 最低价: {:.2}", min);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        })
        .await;

    tick_subscription.start().await?;

    println!("等待 10 秒接收 Tick 数据...\n");
    tokio::time::sleep(Duration::from_secs(10)).await;

    tick_subscription.close().await?;
    println!("\n✓ Tick 订阅已关闭");
    Ok(())
}

/// 示例 3: 多合约 K线分析
async fn example_multi_kline(series_api: &SeriesAPI) -> Result<(), Box<dyn std::error::Error>> {
    println!("\n【示例 3】多合约 K线分析");
    println!("订阅多个合约的对齐 K线...\n");

    let symbols = vec!["SHFE.au2506".to_string(), "SHFE.ag2506".to_string()];
    let multi_subscription = series_api
        .kline_multi(&symbols, Duration::from_secs(60), 50)
        .await?;

    multi_subscription
        .on_update(|series_data, _update_info| {
            if let Some(multi_data) = &series_data.multi {
                println!("收到多合约数据更新:");
                println!("  - 合约数: {}", multi_data.symbols.len());
                println!("  - 数据点数: {}", multi_data.data.len());

                if let Ok(df) = series_data.to_dataframe() {
                    println!("\n  长表格式 DataFrame:");
                    println!("    - 行数: {}", df.height());
                    println!("    - 列数: {}", df.width());
                }

                if let Ok(wide_df) = series_data.to_wide_dataframe() {
                    println!("\n  宽表格式 DataFrame:");
                    println!("    - 行数: {}", wide_df.height());
                    println!("    - 列数: {}", wide_df.width());
                    println!("\n{}", wide_df);
                }
            }
        })
        .await;

    multi_subscription.start().await?;

    println!("等待 20 秒接收多合约数据...\n");
    tokio::time::sleep(Duration::from_secs(20)).await;

    multi_subscription.close().await?;
    println!("\n✓ 多合约订阅已关闭");
    Ok(())
}
