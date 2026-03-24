//! # TQSDK-RS3
//!
//! 天勤 DIFF 协议的 Rust 语言封装
//!
//! 这是一个用于连接天勤量化交易平台的 Rust SDK，支持：
//! - 实时行情订阅（Quote, K线, Tick）
//! - 历史数据获取
//! - 实盘/模拟交易
//! - DIFF 协议数据管理
//!
//! ## 快速开始
//!
//! ```no_run
//! use tqsdk_rs::{Client, ClientConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // 创建客户端
//!     let client = Client::new("username", "password", ClientConfig::default()).await?;
//!     
//!     // 初始化行情
//!     client.init_market().await?;
//!     
//!     // 订阅行情
//!     let quote_sub = client.subscribe_quote(&["SHFE.au2602"]).await?;
//!     
//!     Ok(())
//! }
//! ```

// v1alpha1 实现
pub mod v1alpha1;

// 重新导出常用类型（保持向后兼容）
pub use v1alpha1::auth::Authenticator;
pub use v1alpha1::client::{Client, ClientConfig, ClientOption};
pub use v1alpha1::datamanager::{DataManager, DataManagerConfig};
pub use v1alpha1::errors::{Result, TqError};
pub use v1alpha1::logger::{create_logger_layer, init_logger};
pub use v1alpha1::quote::QuoteSubscription;
pub use v1alpha1::series::{SeriesAPI, SeriesSubscription};
pub use v1alpha1::trade_session::TradeSession;
pub use v1alpha1::types::*;
pub use v1alpha1::websocket::TqWebsocket;

// Polars 扩展
#[cfg(feature = "polars")]
pub use v1alpha1::polars_ext::{KlineBuffer, TickBuffer};
