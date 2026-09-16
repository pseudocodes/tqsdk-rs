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
//!     let mut client = Client::new("username", "password", ClientConfig::default()).await?;
//!
//!     // 初始化行情
//!     client.init_market().await?;
//!
//!     // 订阅行情
//!     let _quote_sub = client.subscribe_quote(&["SHFE.au2602"]).await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## 模块组织
//!
//! 模块按职责划分，公共 API 面在本文件一次性导出：
//! 常用类型可直接从 crate 根导入（如 `tqsdk_rs::Client`），
//! 需要更细的类型时再走模块路径（如 `tqsdk_rs::auth::TqAuth`）。

// 错误类型
pub mod errors;

// 日志系统
pub mod logger;

// 工具函数
pub mod utils;

// 数据结构
pub mod types;

// 数据管理器
pub mod datamanager;

// 认证模块
pub mod auth;

// WebSocket 连接
pub mod websocket;

// Quote 订阅
pub mod quote;

// Series API
pub mod series;

// 交易会话
pub mod trade_session;

// 客户端
pub mod client;

// Polars 扩展（可选功能）
#[cfg(feature = "polars")]
pub mod polars_ext;

// ---------------------------------------------------------------------------
// 公共 API 面：只在这里导出一次，避免多层 re-export 清单互相漂移
// ---------------------------------------------------------------------------

pub use auth::Authenticator;
pub use client::{Client, ClientBuilder, ClientConfig, ClientOption};
pub use datamanager::{DataManager, DataManagerConfig};
pub use errors::{Result, TqError};
pub use logger::{create_logger_layer, init_logger};
pub use quote::QuoteSubscription;
pub use series::{SeriesAPI, SeriesSubscription};
pub use trade_session::TradeSession;
pub use types::*;
pub use websocket::TqWebsocket;

#[cfg(feature = "polars")]
pub use polars_ext::{KlineBuffer, TickBuffer};
