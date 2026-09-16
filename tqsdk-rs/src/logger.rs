//! 日志系统
//!
//! 使用 tracing 实现日志系统，支持：
//! - 多级别日志（debug, info, warn, error）
//! - 日志过滤（只显示本库日志）
//! - WebSocket 消息详细日志

use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter, Layer};

/// 日志级别
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogLevel {
    /// Trace 级别
    Trace,
    /// Debug 级别
    Debug,
    /// Info 级别
    Info,
    /// Warn 级别
    Warn,
    /// Error 级别
    Error,
}

impl LogLevel {
    /// 转换为字符串
    pub fn as_str(&self) -> &'static str {
        match self {
            LogLevel::Debug => "debug",
            LogLevel::Info => "info",
            LogLevel::Warn => "warn",
            LogLevel::Error => "error",
            LogLevel::Trace => "trace",
        }
    }
}

impl From<&str> for LogLevel {
    fn from(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "trace" => LogLevel::Trace,
            "debug" => LogLevel::Debug,
            "info" => LogLevel::Info,
            "warn" => LogLevel::Warn,
            "error" => LogLevel::Error,
            _ => LogLevel::Info,
        }
    }
}

/// 创建 tqsdk-rs 的日志 Layer
///
/// 返回一个配置好的 Layer，可以与业务层的其他 Layer 组合使用
///
/// # 参数
/// - `level`: 日志级别 ("trace", "debug", "info", "warn", "error")
/// - `filter_crate_only`: 是否只显示本库的日志
///
/// # 返回
/// 返回一个 `impl Layer<S>` 可以与其他 Layer 组合
///
/// # 示例
/// ```no_run
/// use tqsdk_rs::create_logger_layer;
/// use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};
///
/// // 创建 tqsdk-rs 的 Layer
/// let tqsdk_layer = create_logger_layer("debug", false);
///
/// // 与业务层的其他 Layer 组合
/// tracing_subscriber::registry()
///     .with(tqsdk_layer)
///     // .with(your_custom_layer)
///     .init();
/// ```
pub fn create_logger_layer<S>(
    level: &str,
    filter_crate_only: bool,
) -> impl Layer<S> + Send + Sync + 'static
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a>,
{
    let log_level = LogLevel::from(level);

    // 构建过滤器
    let filter = if filter_crate_only {
        // 只显示本库的日志
        EnvFilter::new(format!("tqsdk_rs={}", log_level.as_str()))
    } else {
        // 显示所有日志
        EnvFilter::new(log_level.as_str())
    };

    // 配置格式化输出（使用本地时区）
    fmt::layer()
        .with_target(true)
        .with_thread_ids(false)
        .with_thread_names(false)
        .with_line_number(true)
        .with_file(true)
        .with_ansi(true)
        .with_timer(fmt::time::OffsetTime::local_rfc_3339().expect("无法获取本地时区"))
        .compact()
        .with_filter(filter)
}

/// 初始化日志系统（便捷方法）
///
/// 这是一个便捷方法，内部调用 `create_logger_layer` 并初始化全局订阅者
///
/// # 参数
/// - `level`: 日志级别 ("trace", "debug", "info", "warn", "error")
/// - `filter_crate_only`: 是否只显示本库的日志
///
/// # 示例
/// ```no_run
/// use tqsdk_rs::init_logger;
///
/// // 显示所有 debug 级别日志
/// init_logger("debug", false);
///
/// // 只显示本库的 info 级别日志
/// init_logger("info", true);
/// ```
pub fn init_logger(level: &str, filter_crate_only: bool) {
    let layer = create_logger_layer(level, filter_crate_only);

    // 初始化全局订阅者
    // 使用 try_init 避免重复初始化时 panic
    let _ = tracing_subscriber::registry()
        .with(layer)
        .try_init();
}

/// 初始化日志系统（带默认值）
///
/// 默认为 info 级别，只显示本库日志
pub fn init_default_logger() {
    init_logger("info", true);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_log_level_from_str() {
        assert_eq!(LogLevel::from("Trace"), LogLevel::Trace);
        assert_eq!(LogLevel::from("debug"), LogLevel::Debug);
        assert_eq!(LogLevel::from("DEBUG"), LogLevel::Debug);
        assert_eq!(LogLevel::from("info"), LogLevel::Info);
        assert_eq!(LogLevel::from("warn"), LogLevel::Warn);
        assert_eq!(LogLevel::from("error"), LogLevel::Error);
        assert_eq!(LogLevel::from("unknown"), LogLevel::Info); // 默认
    }

    #[test]
    fn test_log_level_as_str() {
        assert_eq!(LogLevel::Debug.as_str(), "debug");
        assert_eq!(LogLevel::Info.as_str(), "info");
        assert_eq!(LogLevel::Warn.as_str(), "warn");
        assert_eq!(LogLevel::Error.as_str(), "error");
        assert_eq!(LogLevel::Trace.as_str(), "trace");
    }
}
