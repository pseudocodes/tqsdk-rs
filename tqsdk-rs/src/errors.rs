//! 错误类型定义
//!
//! 使用 thiserror 定义所有错误类型

/// TQSDK Result 类型
pub type Result<T> = std::result::Result<T, TqError>;

/// TQSDK 错误类型
#[derive(Debug, thiserror::Error)]
pub enum TqError {
    /// 权限不足错误
    #[error("权限不足: {0}")]
    PermissionDenied(String),

    /// WebSocket 连接错误
    #[error("WebSocket 连接错误: {0}")]
    WebSocketError(String),

    /// 认证失败
    #[error("认证失败: {0}")]
    AuthenticationError(String),

    /// 数据解析错误
    #[error("数据解析错误: {0}")]
    ParseError(String),

    /// 网络请求错误
    #[error("网络请求错误: {0}")]
    NetworkError(String),

    /// 配置错误
    #[error("配置错误: {0}")]
    ConfigError(String),

    /// 无效的合约代码
    #[error("无效的合约代码: {0}")]
    InvalidSymbol(String),

    /// 无效的参数
    #[error("无效的参数: {0}")]
    InvalidParameter(String),

    /// 交易错误
    #[error("交易错误: {0}")]
    TradeError(String),

    /// 订单不存在
    #[error("订单不存在: {0}")]
    OrderNotFound(String),

    /// 账户未登录
    #[error("账户未登录")]
    NotLoggedIn,

    /// 数据未找到
    #[error("数据未找到: {0}")]
    DataNotFound(String),

    /// 内部错误
    #[error("内部错误: {0}")]
    InternalError(String),

    /// 超时错误
    #[error("操作超时")]
    Timeout,

    /// IO 错误
    #[error("IO 错误: {0}")]
    IoError(String),

    /// JSON 错误
    #[error("JSON 错误: {0}")]
    JsonError(String),

    /// JWT 错误
    #[error("JWT 错误: {0}")]
    JwtError(String),

    /// 其他错误
    #[error("错误: {0}")]
    Other(String),
}

// 实现 From trait 用于错误转换

impl From<std::io::Error> for TqError {
    fn from(err: std::io::Error) -> Self {
        TqError::IoError(err.to_string())
    }
}

impl From<serde_json::Error> for TqError {
    fn from(err: serde_json::Error) -> Self {
        TqError::JsonError(err.to_string())
    }
}

impl From<reqwest::Error> for TqError {
    fn from(err: reqwest::Error) -> Self {
        TqError::NetworkError(err.to_string())
    }
}

impl From<jsonwebtoken::errors::Error> for TqError {
    fn from(err: jsonwebtoken::errors::Error) -> Self {
        TqError::JwtError(err.to_string())
    }
}

impl From<url::ParseError> for TqError {
    fn from(err: url::ParseError) -> Self {
        TqError::InvalidParameter(err.to_string())
    }
}

// 预定义的错误常量
impl TqError {
    /// 权限不足 - 期货行情
    pub fn permission_denied_futures() -> Self {
        TqError::PermissionDenied(
            "您的账户不支持查看期货行情数据，需要购买后才能使用。\
             升级网址: https://www.shinnytech.com/tqsdk-buy/"
                .to_string(),
        )
    }

    /// 权限不足 - 股票行情
    pub fn permission_denied_stocks() -> Self {
        TqError::PermissionDenied(
            "您的账户不支持查看股票行情数据，需要购买后才能使用。\
             升级网址: https://www.shinnytech.com/tqsdk-buy/"
                .to_string(),
        )
    }

    /// 权限不足 - 历史数据
    pub fn permission_denied_history() -> Self {
        TqError::PermissionDenied(
            "数据获取方式仅限专业版用户使用，如需购买专业版或者申请试用，\
             请访问 https://www.shinnytech.com/tqsdk-buy/"
                .to_string(),
        )
    }

    /// 无效的 left_kline_id
    pub fn invalid_left_kline_id() -> Self {
        TqError::InvalidParameter("left_kline_id 必须大于 0".to_string())
    }

    /// 无效的 focus_position
    pub fn invalid_focus_position() -> Self {
        TqError::InvalidParameter(
            "使用 focus_datetime 时必须提供 focus_position，且 focus_position 必须 >= 0"
                .to_string(),
        )
    }
}
