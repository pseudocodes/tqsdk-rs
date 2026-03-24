//! 工具函数
//!
//! 提供各种工具函数，包括：
//! - 流式 JSON 下载
//! - 时间转换
//! - 字符串处理

use super::errors::{Result, TqError};
use chrono::{DateTime, Utc};
use futures::StreamExt;
use serde_json::Value;
use std::time::Duration;
use tracing::{debug, info};

/// 流式下载 JSON 文件
///
/// 使用 reqwest 的 stream 特性流式下载 JSON 文件，支持 gzip 和 brotli 压缩
///
/// # 参数
///
/// * `url` - 要下载的 URL
///
/// # 返回
///
/// 解析后的 JSON Value
///
/// # 示例
///
/// ```no_run
/// # use tqsdk_rs::utils::fetch_json;
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let json = fetch_json("https://openmd.shinnytech.com/t/md/symbols/latest.json").await?;
/// # Ok(())
/// # }
/// ```
pub async fn fetch_json(url: &str) -> Result<Value> {
    info!("开始下载 JSON: {}", url);

    // 创建支持 gzip 和 brotli 的 HTTP 客户端
    let client = reqwest::Client::builder()
        .gzip(true)
        .brotli(true)
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|e| TqError::NetworkError(format!("创建 HTTP 客户端失败: {}", e)))?;

    // 发送请求
    let response = client
        .get(url)
        .send()
        .await
        .map_err(|e| TqError::NetworkError(format!("请求失败: {}", e)))?;

    // 检查状态码
    if !response.status().is_success() {
        return Err(TqError::NetworkError(format!(
            "HTTP 状态码错误: {}",
            response.status()
        )));
    }

    debug!("HTTP 状态: {}", response.status());

    // 流式下载并累积字节
    let mut stream = response.bytes_stream();
    let mut buffer = Vec::new();

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.map_err(|e| TqError::NetworkError(format!("下载数据失败: {}", e)))?;
        buffer.extend_from_slice(&chunk);
        debug!("已下载: {} 字节", buffer.len());
    }

    info!("下载完成，总大小: {} 字节", buffer.len());

    // 解析 JSON
    let json: Value = serde_json::from_slice(&buffer)
        .map_err(|e| TqError::ParseError(format!("JSON 解析失败: {}", e)))?;

    Ok(json)
}

/// 将纳秒时间戳转换为 DateTime
///
/// # 参数
///
/// * `nanos` - 纳秒时间戳
///
/// # 返回
///
/// DateTime<Utc> 对象
pub fn nanos_to_datetime(nanos: i64) -> DateTime<Utc> {
    let secs = nanos / 1_000_000_000;
    let nsecs = (nanos % 1_000_000_000) as u32;
    DateTime::from_timestamp(secs, nsecs).unwrap_or_else(|| Utc::now())
}

/// 将 DateTime 转换为纳秒时间戳
///
/// # 参数
///
/// * `dt` - DateTime<Utc> 对象
///
/// # 返回
///
/// 纳秒时间戳
pub fn datetime_to_nanos(dt: &DateTime<Utc>) -> i64 {
    dt.timestamp() * 1_000_000_000 + dt.timestamp_subsec_nanos() as i64
}

/// 将合约代码拆分为交易所和合约
///
/// # 参数
///
/// * `symbol` - 合约代码（格式：EXCHANGE.INSTRUMENT）
///
/// # 返回
///
/// (exchange, instrument) 元组
///
/// # 示例
///
/// ```
/// # use tqsdk_rs::utils::split_symbol;
/// let (exchange, instrument) = split_symbol("SHFE.au2602");
/// assert_eq!(exchange, "SHFE");
/// assert_eq!(instrument, "au2602");
/// ```
pub fn split_symbol(symbol: &str) -> (&str, &str) {
    if let Some(pos) = symbol.find('.') {
        let exchange = &symbol[..pos];
        let instrument = &symbol[pos + 1..];
        (exchange, instrument)
    } else {
        ("", symbol)
    }
}

/// 获取交易所前缀
///
/// # 参数
///
/// * `symbol` - 合约代码
///
/// # 返回
///
/// 交易所代码
pub fn get_exchange(symbol: &str) -> &str {
    split_symbol(symbol).0
}

/// 生成唯一的图表 ID
///
/// # 参数
///
/// * `prefix` - 前缀
///
/// # 返回
///
/// 唯一的图表 ID
pub fn generate_chart_id(prefix: &str) -> String {
    let uuid = uuid::Uuid::new_v4();
    format!("{}_{}", prefix, uuid)
}

/// 检查值是否为 NaN 字符串
///
/// # 参数
///
/// * `s` - 字符串
///
/// # 返回
///
/// 是否为 NaN
pub fn is_nan_string(s: &str) -> bool {
    s == "NaN" || s == "-" || s.is_empty()
}

/// 将 JSON Value 转换为 i64
///
/// # 参数
///
/// * `value` - JSON Value
///
/// # 返回
///
/// i64 值
pub fn value_to_i64(value: &Value) -> i64 {
    match value {
        Value::Number(n) => n.as_i64().unwrap_or(0),
        Value::String(s) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

/// 将 JSON Value 转换为 f64
///
/// # 参数
///
/// * `value` - JSON Value
///
/// # 返回
///
/// f64 值
pub fn value_to_f64(value: &Value) -> f64 {
    match value {
        Value::Number(n) => n.as_f64().unwrap_or(0.0),
        Value::String(s) => {
            if is_nan_string(s) {
                f64::NAN
            } else {
                s.parse().unwrap_or(0.0)
            }
        }
        _ => 0.0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_split_symbol() {
        let (exchange, instrument) = split_symbol("SHFE.au2602");
        assert_eq!(exchange, "SHFE");
        assert_eq!(instrument, "au2602");

        let (exchange, instrument) = split_symbol("DCE.m2512");
        assert_eq!(exchange, "DCE");
        assert_eq!(instrument, "m2512");

        let (exchange, instrument) = split_symbol("invalid");
        assert_eq!(exchange, "");
        assert_eq!(instrument, "invalid");
    }

    #[test]
    fn test_get_exchange() {
        assert_eq!(get_exchange("SHFE.au2602"), "SHFE");
        assert_eq!(get_exchange("DCE.m2512"), "DCE");
        assert_eq!(get_exchange("invalid"), "");
    }

    #[test]
    fn test_is_nan_string() {
        assert!(is_nan_string("NaN"));
        assert!(is_nan_string("-"));
        assert!(is_nan_string(""));
        assert!(!is_nan_string("123"));
        assert!(!is_nan_string("0"));
    }

    #[test]
    fn test_datetime_conversion() {
        let now = Utc::now();
        let nanos = datetime_to_nanos(&now);
        let dt = nanos_to_datetime(nanos);
        
        // 允许少量误差（纳秒精度可能有损失）
        let diff = (dt.timestamp() - now.timestamp()).abs();
        assert!(diff <= 1);
    }

    #[test]
    fn test_generate_chart_id() {
        let id1 = generate_chart_id("TQGO_kline");
        let id2 = generate_chart_id("TQGO_kline");
        
        assert!(id1.starts_with("TQGO_kline_"));
        assert!(id2.starts_with("TQGO_kline_"));
        assert_ne!(id1, id2); // 应该是不同的 ID
    }
}

