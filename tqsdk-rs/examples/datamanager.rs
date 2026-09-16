//! DataManager 高级功能示例
//!
//! 演示以下功能：
//! - 路径监听 (Watch/UnWatch)
//! - 数据访问 (Get/GetByPath)
//! - 多路径同时监听
//! - 数据更新回调

use serde_json::{json, Value};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tqsdk_rs::*;
use tokio;
use tracing::info;

/// DataManager Watch 功能示例
async fn watch_example() {
    info!("==================== DataManager Watch 示例 ====================");

    // 创建 DataManager
    let mut initial_data = HashMap::new();
    initial_data.insert("quotes".to_string(), json!({}));

    let config = DataManagerConfig::default();
    let dm = Arc::new(DataManager::new(initial_data, config));

    // 监听特定路径
    let watch_rx = dm.watch(vec!["quotes".to_string(), "SHFE.au2512".to_string()]);

    // 启动 tokio task 接收数据
    tokio::spawn(async move {
        let rx = watch_rx;
        while let Ok(data) = rx.recv().await {
            if let Value::Object(quote_map) = data {
                info!(
                    "📊 Quote 更新: 最新价={:.2}, 成交量={}",
                    quote_map
                        .get("last_price")
                        .and_then(|v| v.as_f64())
                        .unwrap_or(0.0),
                    quote_map
                        .get("volume")
                        .and_then(|v| v.as_i64())
                        .unwrap_or(0)
                );
            }
        }
    });

    // 模拟数据更新
    info!("模拟数据更新...");
    tokio::time::sleep(Duration::from_millis(500)).await;

    dm.merge_data(
        json!({
            "quotes": {
                "SHFE.au2512": {
                    "last_price": 500.0,
                    "volume": 1000
                }
            }
        }),
        true,
        false,
    );

    tokio::time::sleep(Duration::from_millis(100)).await;

    // 更新数据
    dm.merge_data(
        json!({
            "quotes": {
                "SHFE.au2512": {
                    "last_price": 501.5,
                    "volume": 1200
                }
            }
        }),
        true,
        false,
    );

    tokio::time::sleep(Duration::from_millis(100)).await;

    // 取消监听
    info!("取消监听...");
    dm.unwatch(&[
        "quotes".to_string(),
        "SHFE.au2512".to_string(),
    ])
    .ok();

    info!("Watch 示例结束");
}

/// 数据访问示例
async fn data_access_example() {
    info!("==================== 数据访问示例 ====================");

    let mut initial_data = HashMap::new();
    initial_data.insert(
        "quotes".to_string(),
        json!({
            "SHFE.au2512": {
                "last_price": 500.0,
                "volume": 1000
            }
        }),
    );

    let config = DataManagerConfig::default();
    let dm = DataManager::new(initial_data, config);

    // 使用 get_by_path 方法
    if let Some(data) = dm.get_by_path(&["quotes", "SHFE.au2512"]) {
        info!("GetByPath 成功: {:?}", data);
    }

    // 访问不存在的路径
    if let None = dm.get_by_path(&["quotes", "INVALID"]) {
        info!("预期的行为: 路径不存在返回 None");
    }

    // 检查数据是否在变化
    if dm.is_changing(&["quotes", "SHFE.au2512"]) {
        info!("数据在最近一次更新中发生了变化");
    }

    // 获取当前 epoch
    let epoch = dm.get_epoch();
    info!("当前 epoch: {}", epoch);

    info!("数据访问示例结束");
}

/// 多路径监听示例
async fn multi_watch_example() {
    info!("==================== 多路径监听示例 ====================");

    let mut initial_data = HashMap::new();
    initial_data.insert("quotes".to_string(), json!({}));

    let config = DataManagerConfig::default();
    let dm = Arc::new(DataManager::new(initial_data, config));

    // 监听多个路径
    let symbols = vec!["SHFE.au2512", "SHFE.ag2512", "DCE.m2505"];
    let mut channels = HashMap::new();

    for symbol in &symbols {
        let rx = dm.watch(vec!["quotes".to_string(), symbol.to_string()]);
        channels.insert(symbol.to_string(), rx);
    }

    // 启动多个 tokio task 接收数据
    for (symbol, rx) in channels {
        tokio::spawn(async move {
            while let Ok(data) = rx.recv().await {
                if let Value::Object(quote_map) = data {
                    info!(
                        "📊 {} 更新: {:.2}",
                        symbol,
                        quote_map
                            .get("last_price")
                            .and_then(|v| v.as_f64())
                            .unwrap_or(0.0)
                    );
                }
            }
        });
    }

    // 模拟批量更新
    tokio::time::sleep(Duration::from_millis(500)).await;
    dm.merge_data(
        json!({
            "quotes": {
                "SHFE.au2512": {"last_price": 500.0},
                "SHFE.ag2512": {"last_price": 50.0},
                "DCE.m2505": {"last_price": 3000.0}
            }
        }),
        true,
        false,
    );

    tokio::time::sleep(Duration::from_millis(500)).await;

    // 清理
    for symbol in &symbols {
        dm.unwatch(&["quotes".to_string(), symbol.to_string()])
            .ok();
    }

    info!("多路径监听示例结束");
}

/// 数据更新回调示例
async fn on_data_callback_example() {
    info!("==================== 数据更新回调示例 ====================");

    let mut initial_data = HashMap::new();
    initial_data.insert("quotes".to_string(), json!({}));

    let config = DataManagerConfig::default();
    let dm = Arc::new(DataManager::new(initial_data, config));

    // 注册数据更新回调
    let dm_clone = Arc::clone(&dm);
    dm.on_data(move || {
        info!("🔔 数据更新通知");
        let epoch = dm_clone.get_epoch();
        info!("   当前 epoch: {}", epoch);

        // 可以在这里触发其他操作
        // 例如：检查特定路径的数据
        if let Some(data) = dm_clone.get_by_path(&["quotes", "SHFE.au2512"]) {
            info!("   SHFE.au2512 数据: {:?}", data);
        }
    });

    // 模拟数据更新
    tokio::time::sleep(Duration::from_millis(500)).await;
    info!("触发第一次数据更新...");
    dm.merge_data(
        json!({
            "quotes": {
                "SHFE.au2512": {
                    "last_price": 500.0,
                    "volume": 1000
                }
            }
        }),
        true,
        false,
    );

    tokio::time::sleep(Duration::from_millis(500)).await;
    info!("触发第二次数据更新...");
    dm.merge_data(
        json!({
            "quotes": {
                "SHFE.au2512": {
                    "last_price": 501.5,
                    "volume": 1200
                }
            }
        }),
        true,
        false,
    );

    tokio::time::sleep(Duration::from_millis(500)).await;

    info!("数据更新回调示例结束");
}

#[tokio::main]
async fn main() {
    // 初始化日志
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    // 运行各个示例
    watch_example().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    data_access_example().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    multi_watch_example().await;
    tokio::time::sleep(Duration::from_secs(1)).await;

    on_data_callback_example().await;

    info!("\n所有示例运行完成!");
}

