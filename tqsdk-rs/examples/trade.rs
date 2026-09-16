//! 交易操作示例
//!
//! 演示以下功能：
//! - 实盘交易（回调模式）
//! - 实盘交易（Channel 流式模式）
//! - 混合模式（回调 + Channel）

use std::env;
use std::time::Duration;
use tokio;
use tqsdk_rs::*;
use tracing::info;

/// 使用回调模式的交易示例（实盘交易）
async fn trade_callback_example() {
    info!("==================== 交易回调模式示例（实盘）====================");

    let username = env::var("SHINNYTECH_ID").expect("请设置 SHINNYTECH_ID 环境变量");
    let password = env::var("SHINNYTECH_PW").expect("请设置 SHINNYTECH_PW 环境变量");

    let sim_user_id = env::var("SIMNOW_USER_0").expect("请设置 SIMNOW_USER_0 环境变量");
    let sim_password = env::var("SIMNOW_PASS_0").expect("请设置 SIMNOW_PASS_0 环境变量");

    // let sim_user_id = "68823183".to_string();
    // let sim_password = "zhangyu816".to_string();

    // 创建客户端
    let mut config = ClientConfig::default();
    config.log_level = "info".to_string();

    let client = Client::new(&username, &password, config)
        .await
        .expect("创建客户端失败");

    // 创建交易会话（不自动连接）
    info!("创建交易会话: simnow, {}", sim_user_id);
    let trader = client
        .create_trade_session("simnow", &sim_user_id, &sim_password)
        // .create_trade_session("X兴证期货", &sim_user_id, &sim_password)
        .await
        .expect("创建交易会话失败");

    // 先注册账户更新回调（避免丢失消息）
    trader
        .on_account(|account| {
            info!(
                "💰 账户更新: 权益={:.2}, 可用={:.2}, 风险度={:.2}%",
                account.balance,
                account.available,
                account.risk_ratio * 100.0
            );
        })
        .await;

    // 注册持仓更新回调（单个持仓）
    trader
        .on_position(|symbol, pos| {
            let total_long = pos.volume_long_today + pos.volume_long_his;
            let total_short = pos.volume_short_today + pos.volume_short_his;

            if total_long > 0 || total_short > 0 {
                info!(
                    "📊 {} 持仓更新: 多头={}, 空头={}, 浮动盈亏={:.2}",
                    symbol, total_long, total_short, pos.float_profit
                );
            }
        })
        .await;

    // 注册委托单更新回调
    trader
        .on_order(|order| {
            info!(
                "📝 订单 {}: {}.{} {} {}@{:.2}, 状态={}, 剩余={}",
                order.order_id,
                order.exchange_id,
                order.instrument_id,
                order.direction,
                order.offset,
                order.price(),
                order.status,
                order.volume_left
            );
        })
        .await;

    // 注册成交回调
    trader
        .on_trade(|trade| {
            info!(
                "✅ 成交 {}: {}.{} {} {}@{:.2} x{}, 手续费={:.2}",
                trade.trade_id,
                trade.exchange_id,
                trade.instrument_id,
                trade.direction,
                trade.offset,
                trade.price,
                trade.volume,
                trade.commission
            );
        })
        .await;

    trader
        .on_notification(|notification| {
            info!("🔔 通知: {}", notification.content);
        })
        .await;

    // 所有回调注册完毕后，再连接交易服务器（避免丢失消息）
    info!("连接交易服务器...");
    trader.connect().await.expect("连接失败");

    // 等待登录就绪
    info!("等待登录就绪...");
    while !trader.is_ready() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    info!("✅ 已登录，交易就绪!");

    // 查询账户（同步方式）
    if let Ok(account) = trader.get_account().await {
        info!("\n当前账户信息:");
        info!("  权益: {:.2}", account.balance);
        info!("  可用: {:.2}", account.available);
        info!("  保证金: {:.2}", account.margin);
        info!("  风险度: {:.2}%", account.risk_ratio * 100.0);
    }

    // 查询持仓
    if let Ok(positions) = trader.get_positions().await {
        if !positions.is_empty() {
            info!("\n当前持仓:");
            for (symbol, pos) in positions {
                let total_long = pos.volume_long_today + pos.volume_long_his;
                let total_short = pos.volume_short_today + pos.volume_short_his;
                if total_long > 0 || total_short > 0 {
                    info!(
                        "  {}: 多={} 空={} 浮盈={:.2}",
                        symbol, total_long, total_short, pos.float_profit
                    );
                }
            }
        }
    }

    // 下单示例（注释掉，避免实际下单）
    /*
    info!("\n准备下单...");
    let order = trader.insert_order(&InsertOrderRequest {
        symbol: "SHFE.au2512".to_string(),
        direction: Direction::Buy,
        offset: Offset::Open,
        price_type: PriceType::Limit,
        limit_price: 500.0,
        volume: 1,
        ..Default::default()
    }).await;

    if let Ok(order) = order {
        info!("下单成功: {}", order.order_id);

        // 等待一会儿
        tokio::time::sleep(Duration::from_secs(2)).await;

        // 撤单
        info!("准备撤单 {}...", order.order_id);
        if let Ok(_) = trader.cancel_order(&order.order_id).await {
            info!("撤单成功!");
        } else {
            info!("撤单失败");
        }
    }
    */

    // 运行 30 秒
    info!("\n监听交易数据更新...");
    tokio::time::sleep(Duration::from_secs(300)).await;
    info!("回调模式示例结束\n");

    info!("开始关闭交易会话...");
    trader.close().await.ok();
    info!("交易会话已关闭");

    // 等待一下让所有后台任务完成
    tokio::time::sleep(Duration::from_millis(500)).await;
    info!("清理完成，准备退出");
}

#[tokio::main]
async fn main() {
    // 初始化日志
    tqsdk_rs::init_logger("debug", false);

    // 运行交易示例
    trade_callback_example().await; // 实盘交易 - 回调模式

    info!("\n交易示例运行完成!");
}
