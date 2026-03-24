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

// 重新导出常用类型
pub use auth::Authenticator;
pub use client::{Client, ClientConfig, ClientOption};
pub use datamanager::{DataManager, DataManagerConfig};
pub use errors::{Result, TqError};
pub use logger::{init_logger, create_logger_layer};
pub use quote::QuoteSubscription;
pub use series::{SeriesAPI, SeriesSubscription};
pub use trade_session::TradeSession;
pub use types::*;
pub use websocket::TqWebsocket;

#[cfg(feature = "polars")]
pub use polars_ext::{KlineBuffer, TickBuffer};
