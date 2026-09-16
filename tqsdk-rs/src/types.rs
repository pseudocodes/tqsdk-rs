//! 数据结构定义
//!
//! 定义所有 TQSDK 使用的数据结构，包括：
//! - Quote: 行情报价
//! - Kline: K线数据
//! - Tick: Tick数据
//! - Account: 账户信息
//! - Position: 持仓信息
//! - Order: 委托单
//! - Trade: 成交记录
//! - Chart: 图表状态
//! - SeriesData: 序列数据

use chrono::{DateTime, Utc};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;

// ==================== 自定义反序列化辅助函数 ====================

/// 返回 NaN 作为默认值
fn default_nan() -> f64 {
    f64::NAN
}

/// 将 null 转换为 NaN
fn deserialize_f64_or_nan<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Number(n) => n.as_f64().ok_or_else(|| Error::custom("invalid number")),
        serde_json::Value::String(s) if s.is_empty() || s == "-" => Ok(f64::NAN),
        serde_json::Value::Null => Ok(f64::NAN),
        _ => Err(Error::custom(
            "expected number, empty string, \"-\", or null",
        )),
    }
}

/// 将 null 转换为 0
fn deserialize_i64_or_zero<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    let opt = Option::<i64>::deserialize(deserializer)?;
    Ok(opt.unwrap_or(0))
}

/// 将 null 或 "-" 转换为 NaN
fn deserialize_f64_or_nan_or_dash<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: Deserializer<'de>,
{
    use serde::de::Error;
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::Number(n) => n.as_f64().ok_or_else(|| Error::custom("invalid number")),
        serde_json::Value::String(s) if s == "-" => Ok(f64::NAN),
        serde_json::Value::Null => Ok(f64::NAN),
        _ => Err(Error::custom("expected number, string \"-\", or null")),
    }
}

// ==================== Quote 行情报价 ====================

/// 行情报价数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Quote {
    /// 合约代码
    pub instrument_id: String,
    /// 行情时间
    pub datetime: String,
    /// 最新价
    #[serde(default = "default_nan")]
    pub last_price: f64,

    // 买卖盘口
    /// 卖一价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price1: f64,
    /// 卖一量
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub ask_volume1: i64,
    /// 卖二价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price2: f64,
    /// 卖二量
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub ask_volume2: i64,
    /// 卖三价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price3: f64,
    /// 卖三量
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub ask_volume3: i64,
    /// 卖四价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price4: f64,
    /// 卖四量
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub ask_volume4: i64,
    /// 卖五价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price5: f64,
    /// 卖五量
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub ask_volume5: i64,

    /// 买一价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price1: f64,
    /// 买一量
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub bid_volume1: i64,
    /// 买二价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price2: f64,
    /// 买二量
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub bid_volume2: i64,
    /// 买三价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price3: f64,
    /// 买三量
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub bid_volume3: i64,
    /// 买四价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price4: f64,
    /// 买四量
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub bid_volume4: i64,
    /// 买五价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price5: f64,
    /// 买五量
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub bid_volume5: i64,

    // 当日统计
    /// 最高价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub highest: f64,
    /// 最低价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub lowest: f64,
    /// 开盘价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub open: f64,
    /// 收盘价
    #[serde(
        default = "default_nan",
        deserialize_with = "deserialize_f64_or_nan_or_dash"
    )]
    pub close: f64,
    /// 均价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub average: f64,
    /// 成交量
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub volume: i64,
    /// 成交额
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub amount: f64,
    /// 持仓量
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub open_interest: i64,

    // 涨跌停
    /// 跌停价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub lower_limit: f64,
    /// 涨停价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub upper_limit: f64,

    // 结算价
    /// 结算价
    #[serde(
        default = "default_nan",
        deserialize_with = "deserialize_f64_or_nan_or_dash"
    )]
    pub settlement: f64,
    /// 昨结算价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub pre_settlement: f64,

    // 涨跌
    /// 涨跌
    #[serde(default = "default_nan")]
    pub change: f64,
    /// 涨跌幅
    #[serde(default = "default_nan")]
    pub change_percent: f64,

    // 期权相关
    /// 行权价
    #[serde(default = "default_nan")]
    pub strike_price: f64,

    // 昨日数据
    /// 昨持仓量
    #[serde(default, deserialize_with = "deserialize_i64_or_zero")]
    pub pre_open_interest: i64,
    /// 昨收盘价
    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub pre_close: f64,
    /// 昨成交量
    #[serde(default)]
    pub pre_volume: i64,

    // 保证金和手续费
    /// 每手保证金
    #[serde(default = "default_nan")]
    pub margin: f64,
    /// 每手手续费
    #[serde(default = "default_nan")]
    pub commission: f64,

    // 合约信息
    /// 合约类型
    #[serde(default)]
    pub class: String,
    /// 交易所代码
    #[serde(default)]
    pub exchange_id: String,
    /// 品种代码
    #[serde(default)]
    pub product_id: String,
    /// 品种简称
    #[serde(default)]
    pub product_short_name: String,
    /// 标的产品
    #[serde(default)]
    pub underlying_product: String,
    /// 标的合约
    #[serde(default)]
    pub underlying_symbol: String,
    /// 交割年份
    #[serde(default)]
    pub delivery_year: i32,
    /// 交割月份
    #[serde(default)]
    pub delivery_month: i32,
    /// 到期时间
    #[serde(default)]
    pub expire_datetime: i64,
    /// 合约乘数
    #[serde(default)]
    pub volume_multiple: i32,
    /// 最小变动价位
    #[serde(default = "default_nan")]
    pub price_tick: f64,
    /// 价格小数位数
    #[serde(default)]
    pub price_decs: i32,
    /// 市价单最大下单量
    #[serde(default)]
    pub max_market_order_vol: i32,
    /// 市价单最小下单量
    #[serde(default)]
    pub min_market_order_vol: i32,
    /// 限价单最大下单量
    #[serde(default)]
    pub max_limit_order_vol: i32,
    /// 限价单最小下单量
    #[serde(default)]
    pub min_limit_order_vol: i32,
    /// 是否已下市
    #[serde(default)]
    pub expired: bool,
    /// 拼音
    #[serde(default)]
    pub py: String,

    // 内部字段（不序列化到 JSON）
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "_epoch")]
    pub epoch: Option<i64>,
}

impl Default for Quote {
    fn default() -> Self {
        Quote {
            instrument_id: String::new(),
            datetime: String::new(),
            last_price: f64::NAN,
            ask_price1: f64::NAN,
            ask_volume1: 0,
            ask_price2: f64::NAN,
            ask_volume2: 0,
            ask_price3: f64::NAN,
            ask_volume3: 0,
            ask_price4: f64::NAN,
            ask_volume4: 0,
            ask_price5: f64::NAN,
            ask_volume5: 0,
            bid_price1: f64::NAN,
            bid_volume1: 0,
            bid_price2: f64::NAN,
            bid_volume2: 0,
            bid_price3: f64::NAN,
            bid_volume3: 0,
            bid_price4: f64::NAN,
            bid_volume4: 0,
            bid_price5: f64::NAN,
            bid_volume5: 0,
            highest: f64::NAN,
            lowest: f64::NAN,
            open: f64::NAN,
            close: f64::NAN,
            average: f64::NAN,
            volume: 0,
            amount: f64::NAN,
            open_interest: 0,
            lower_limit: f64::NAN,
            upper_limit: f64::NAN,
            settlement: f64::NAN,
            pre_settlement: f64::NAN,
            change: f64::NAN,
            change_percent: f64::NAN,
            strike_price: f64::NAN,
            pre_open_interest: 0,
            pre_close: f64::NAN,
            pre_volume: 0,
            margin: f64::NAN,
            commission: f64::NAN,
            class: String::new(),
            exchange_id: String::new(),
            product_id: String::new(),
            product_short_name: String::new(),
            underlying_product: String::new(),
            underlying_symbol: String::new(),
            delivery_year: 0,
            delivery_month: 0,
            expire_datetime: 0,
            volume_multiple: 0,
            price_tick: f64::NAN,
            price_decs: 0,
            max_market_order_vol: 0,
            min_market_order_vol: 0,
            max_limit_order_vol: 0,
            min_limit_order_vol: 0,
            expired: false,
            py: String::new(),
            epoch: None,
        }
    }
}

impl Quote {
    /// 更新涨跌和涨跌幅
    pub fn update_change(&mut self) {
        if !self.last_price.is_nan() && !self.pre_settlement.is_nan() && self.pre_settlement != 0.0
        {
            self.change = self.last_price - self.pre_settlement;
            self.change_percent = self.change / self.pre_settlement * 100.0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quote_deserialize_with_nulls() {
        let json_data = r#"{
            "instrument_id":"DCE.m2512",
            "datetime":"2025-11-24 22:59:59.000001",
            "ask_price1":3005.0,
            "ask_volume1":1,
            "ask_price2":null,
            "ask_volume2":null,
            "bid_price1":2995.0,
            "bid_volume1":2,
            "bid_price2":null,
            "bid_volume2":null,
            "last_price":3000.0,
            "highest":3000.0,
            "lowest":2986.0,
            "open":2998.0,
            "close":"-",
            "average":2995.0,
            "volume":688,
            "amount":20609740.0,
            "open_interest":5278,
            "settlement":"-",
            "upper_limit":3181.0,
            "lower_limit":2821.0,
            "pre_open_interest":5729,
            "pre_settlement":3001.0,
            "pre_close":3001.0
        }"#;

        let result = serde_json::from_str::<Quote>(json_data);
        assert!(result.is_ok(), "Quote 解析失败: {:?}", result.err());

        let quote = result.unwrap();
        assert_eq!(quote.instrument_id, "DCE.m2512");
        assert_eq!(quote.last_price, 3000.0);
        assert_eq!(quote.ask_price1, 3005.0);
        assert!(quote.ask_price2.is_nan(), "null 应该被转换为 NaN");
        assert_eq!(quote.ask_volume2, 0, "null 应该被转换为 0");
        assert!(quote.close.is_nan(), "dash 应该被转换为 NaN");
        assert!(quote.settlement.is_nan(), "dash 应该被转换为 NaN");
    }
}

// ==================== Kline K线数据 ====================

/// K线数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Kline {
    /// K线ID
    #[serde(default)]
    pub id: i64,
    /// K线起点时间(纳秒)
    pub datetime: i64,
    /// 开盘价
    pub open: f64,
    /// 收盘价
    pub close: f64,
    /// 最高价
    pub high: f64,
    /// 最低价
    pub low: f64,
    /// 起始持仓量
    pub open_oi: i64,
    /// 结束持仓量
    pub close_oi: i64,
    /// 成交量
    pub volume: i64,

    // 内部字段
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "_epoch")]
    pub epoch: Option<i64>,
}

impl Default for Kline {
    fn default() -> Self {
        Kline {
            id: 0,
            datetime: 0,
            open: f64::NAN,
            close: f64::NAN,
            high: f64::NAN,
            low: f64::NAN,
            open_oi: 0,
            close_oi: 0,
            volume: 0,
            epoch: None,
        }
    }
}

// ==================== Tick Tick数据 ====================

/// Tick数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Tick {
    /// Tick ID
    #[serde(default)]
    pub id: i64,
    /// tick时间(纳秒)
    pub datetime: i64,
    /// 最新价
    pub last_price: f64,
    /// 均价
    pub average: f64,
    /// 最高价
    pub highest: f64,
    /// 最低价
    pub lowest: f64,

    // 盘口
    pub ask_price1: f64,
    pub ask_volume1: i64,

    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price2: f64,

    pub ask_volume2: i64,

    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price3: f64,
    pub ask_volume3: i64,

    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price4: f64,
    pub ask_volume4: i64,

    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub ask_price5: f64,
    pub ask_volume5: i64,

    pub bid_price1: f64,
    pub bid_volume1: i64,

    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price2: f64,
    pub bid_volume2: i64,

    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price3: f64,
    pub bid_volume3: i64,

    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price4: f64,
    pub bid_volume4: i64,

    #[serde(default = "default_nan", deserialize_with = "deserialize_f64_or_nan")]
    pub bid_price5: f64,
    pub bid_volume5: i64,

    /// 成交量
    pub volume: i64,
    /// 成交额
    pub amount: f64,
    /// 持仓量
    pub open_interest: i64,

    // 内部字段
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "_epoch")]
    pub epoch: Option<i64>,
}

impl Default for Tick {
    fn default() -> Self {
        Tick {
            id: 0,
            datetime: 0,
            last_price: f64::NAN,
            average: f64::NAN,
            highest: f64::NAN,
            lowest: f64::NAN,
            ask_price1: f64::NAN,
            ask_volume1: 0,
            ask_price2: f64::NAN,
            ask_volume2: 0,
            ask_price3: f64::NAN,
            ask_volume3: 0,
            ask_price4: f64::NAN,
            ask_volume4: 0,
            ask_price5: f64::NAN,
            ask_volume5: 0,
            bid_price1: f64::NAN,
            bid_volume1: 0,
            bid_price2: f64::NAN,
            bid_volume2: 0,
            bid_price3: f64::NAN,
            bid_volume3: 0,
            bid_price4: f64::NAN,
            bid_volume4: 0,
            bid_price5: f64::NAN,
            bid_volume5: 0,
            volume: 0,
            amount: f64::NAN,
            open_interest: 0,
            epoch: None,
        }
    }
}

// ==================== Chart 图表状态 ====================

/// 图表状态
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Chart {
    /// 左边界K线ID
    pub left_id: i64,
    /// 右边界K线ID
    pub right_id: i64,
    /// 是否有更多数据
    pub more_data: bool,
    /// 数据是否已准备好（分片传输完成）
    pub ready: bool,
    /// 图表状态
    #[serde(default)]
    pub state: HashMap<String, serde_json::Value>,

    // 内部字段
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "_epoch")]
    pub epoch: Option<i64>,
}

impl Default for Chart {
    fn default() -> Self {
        Chart {
            left_id: -1,
            right_id: -1,
            more_data: true,
            ready: false,
            state: HashMap::new(),
            epoch: None,
        }
    }
}

/// Chart 信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChartInfo {
    #[serde(default)]
    pub chart_id: String,
    pub left_id: i64,
    pub right_id: i64,
    pub more_data: bool,
    pub ready: bool,
    #[serde(default)]
    pub view_width: usize,
}

// ==================== K线序列数据 ====================

/// K线序列数据（带Chart信息）
#[derive(Debug, Clone)]
pub struct KlineSeriesData {
    /// 合约代码
    pub symbol: String,
    /// K线周期（纳秒）
    pub duration: i64,
    /// 关联的Chart ID
    pub chart_id: String,
    /// Chart 信息
    pub chart: Option<ChartInfo>,
    /// 最新K线ID
    pub last_id: i64,
    /// 交易日起始ID
    pub trading_day_start_id: i64,
    /// 交易日结束ID
    pub trading_day_end_id: i64,
    /// K线数组（仅保留 ViewWidth 长度）
    pub data: Vec<Kline>,
    /// 是否有新K线
    pub has_new_bar: bool,
}

/// Tick序列数据
#[derive(Debug, Clone)]
pub struct TickSeriesData {
    /// 合约代码
    pub symbol: String,
    /// 关联的Chart ID
    pub chart_id: String,
    /// Chart 信息
    pub chart: Option<ChartInfo>,
    /// 最新Tick ID
    pub last_id: i64,
    /// Tick数组
    pub data: Vec<Tick>,
    /// 是否有新Tick
    pub has_new_bar: bool,
}

// ==================== 多合约对齐K线 ====================

/// 对齐的K线集合（一个时间点的多个合约）
#[derive(Debug, Clone)]
pub struct AlignedKlineSet {
    /// 主合约的K线ID
    pub main_id: i64,
    /// 时间戳
    pub timestamp: DateTime<Utc>,
    /// symbol -> Kline
    pub klines: HashMap<String, Kline>,
}

/// K线元数据
#[derive(Debug, Clone)]
pub struct KlineMetadata {
    pub symbol: String,
    pub last_id: i64,
    pub trading_day_start_id: i64,
    pub trading_day_end_id: i64,
}

/// 多合约K线序列数据（已对齐）
#[derive(Debug, Clone)]
pub struct MultiKlineSeriesData {
    /// 图表ID
    pub chart_id: String,
    /// K线周期（纳秒）
    pub duration: i64,
    /// 主合约（第一个合约）
    pub main_symbol: String,
    /// 所有合约列表
    pub symbols: Vec<String>,
    /// 左边界ID
    pub left_id: i64,
    /// 右边界ID
    pub right_id: i64,
    /// 视图宽度
    pub view_width: usize,
    /// 对齐的K线数据集
    pub data: Vec<AlignedKlineSet>,
    /// 是否有新K线产生
    pub has_new_bar: bool,
    /// 每个合约的元数据
    pub metadata: HashMap<String, KlineMetadata>,
}

// ==================== 序列数据统一接口 ====================

/// 序列数据（统一接口）
#[derive(Debug, Clone)]
pub struct SeriesData {
    /// 是否为多合约
    pub is_multi: bool,
    /// 是否为Tick数据
    pub is_tick: bool,
    /// 合约列表
    pub symbols: Vec<String>,
    /// 单合约K线数据
    pub single: Option<KlineSeriesData>,
    /// 多合约K线数据
    pub multi: Option<MultiKlineSeriesData>,
    /// Tick数据
    pub tick_data: Option<TickSeriesData>,
}

impl SeriesData {
    /// 获取指定合约的K线数据
    pub fn get_symbol_klines(&self, symbol: &str) -> Option<&KlineSeriesData> {
        if self.is_multi || self.symbols.is_empty() || self.symbols[0] != symbol {
            return None;
        }
        self.single.as_ref()
    }
}

/// 数据更新信息
#[derive(Debug, Clone)]
pub struct UpdateInfo {
    /// 是否有新 K线/Tick
    pub has_new_bar: bool,
    /// 新 K线的 ID（symbol -> id）
    pub new_bar_ids: HashMap<String, i64>,
    /// 是否有 K线更新（最后一根）
    pub has_bar_update: bool,
    /// Chart 范围是否变化
    pub chart_range_changed: bool,
    /// 旧左边界
    pub old_left_id: i64,
    /// 旧右边界
    pub old_right_id: i64,
    /// 新左边界
    pub new_left_id: i64,
    /// 新右边界
    pub new_right_id: i64,
    /// Chart 是否同步完成
    pub has_chart_sync: bool,
    /// Chart数据传输是否完成（分片传输场景）
    pub chart_ready: bool,
}

impl Default for UpdateInfo {
    fn default() -> Self {
        UpdateInfo {
            has_new_bar: false,
            new_bar_ids: HashMap::new(),
            has_bar_update: false,
            chart_range_changed: false,
            old_left_id: -1,
            old_right_id: -1,
            new_left_id: -1,
            new_right_id: -1,
            has_chart_sync: false,
            chart_ready: false,
        }
    }
}

// ==================== 交易相关数据结构 ====================

/// 账户资金信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Account {
    /// 用户ID
    #[serde(default)]
    pub user_id: String,
    /// 货币类型（默认 CNY）
    #[serde(default = "default_currency")]
    pub currency: String,
    /// 可用资金
    #[serde(default)]
    pub available: f64,
    /// 账户权益
    #[serde(default)]
    pub balance: f64,
    /// 本交易日内平仓盈亏
    #[serde(default)]
    pub close_profit: f64,
    /// 手续费 - 本交易日内交纳的手续费
    #[serde(default)]
    pub commission: f64,
    /// CTP可用资金
    #[serde(default)]
    pub ctp_available: f64,
    /// CTP账户权益
    #[serde(default)]
    pub ctp_balance: f64,
    /// 入金金额 - 本交易日内的入金金额
    #[serde(default)]
    pub deposit: f64,
    /// 浮动盈亏
    #[serde(default)]
    pub float_profit: f64,
    /// 冻结手续费
    #[serde(default)]
    pub frozen_commission: f64,
    /// 冻结保证金
    #[serde(default)]
    pub frozen_margin: f64,
    /// 冻结权利金
    #[serde(default)]
    pub frozen_premium: f64,
    /// 保证金占用
    #[serde(default)]
    pub margin: f64,
    /// 期权市值
    #[serde(default)]
    pub market_value: f64,
    /// 持仓盈亏
    #[serde(default)]
    pub position_profit: f64,
    /// 昨日账户权益
    #[serde(default)]
    pub pre_balance: f64,
    /// 权利金 - 本交易日内交纳的权利金
    #[serde(default)]
    pub premium: f64,
    /// 风险度 = 1 - available / balance
    #[serde(default)]
    pub risk_ratio: f64,
    /// 静态权益
    #[serde(default)]
    pub static_balance: f64,
    /// 出金金额 - 本交易日内的出金金额
    #[serde(default)]
    pub withdraw: f64,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "_epoch")]
    pub epoch: Option<i64>,
}

fn default_currency() -> String {
    "CNY".to_string()
}

impl Account {
    /// curr_margin 别名（返回 margin）
    pub fn curr_margin(&self) -> f64 {
        self.margin
    }

    /// _epoch 字段访问
    pub fn _epoch(&self) -> Option<i64> {
        self.epoch
    }
}

/// 持仓信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    /// 用户ID
    #[serde(default)]
    pub user_id: String,
    /// 交易所代码
    #[serde(default)]
    pub exchange_id: String,
    /// 合约代码
    #[serde(default)]
    pub instrument_id: String,
    /// 多头今仓持仓手数
    #[serde(default)]
    pub volume_long_today: i64,
    /// 多头老仓持仓手数
    #[serde(default)]
    pub volume_long_his: i64,
    /// 多头持仓手数
    #[serde(default)]
    pub volume_long: i64,
    /// 多头今仓冻结手数
    #[serde(default)]
    pub volume_long_frozen_today: i64,
    /// 多头老仓冻结手数
    #[serde(default)]
    pub volume_long_frozen_his: i64,
    /// 多头持仓冻结
    #[serde(default)]
    pub volume_long_frozen: i64,
    /// 空头今仓持仓手数
    #[serde(default)]
    pub volume_short_today: i64,
    /// 空头老仓持仓手数
    #[serde(default)]
    pub volume_short_his: i64,
    /// 空头持仓手数
    #[serde(default)]
    pub volume_short: i64,
    /// 空头今仓冻结手数
    #[serde(default)]
    pub volume_short_frozen_today: i64,
    /// 空头老仓冻结手数
    #[serde(default)]
    pub volume_short_frozen_his: i64,
    /// 空头持仓冻结
    #[serde(default)]
    pub volume_short_frozen: i64,
    /// 多头昨仓手数
    #[serde(default)]
    pub volume_long_yd: i64,
    /// 空头昨仓手数
    #[serde(default)]
    pub volume_short_yd: i64,
    /// 多头老仓手数
    #[serde(default)]
    pub pos_long_his: i64,
    /// 多头今仓手数
    #[serde(default)]
    pub pos_long_today: i64,
    /// 空头老仓手数
    #[serde(default)]
    pub pos_short_his: i64,
    /// 空头今仓手数
    #[serde(default)]
    pub pos_short_today: i64,
    /// 多头开仓均价
    #[serde(default)]
    pub open_price_long: f64,
    /// 空头开仓均价
    #[serde(default)]
    pub open_price_short: f64,
    /// 多头开仓市值
    #[serde(default)]
    pub open_cost_long: f64,
    /// 空头开仓市值
    #[serde(default)]
    pub open_cost_short: f64,
    /// 多头持仓均价
    #[serde(default)]
    pub position_price_long: f64,
    /// 空头持仓均价
    #[serde(default)]
    pub position_price_short: f64,
    /// 多头持仓市值
    #[serde(default)]
    pub position_cost_long: f64,
    /// 空头持仓市值
    #[serde(default)]
    pub position_cost_short: f64,
    /// 最新价
    #[serde(default)]
    pub last_price: f64,
    /// 多头浮动盈亏
    #[serde(default)]
    pub float_profit_long: f64,
    /// 空头浮动盈亏
    #[serde(default)]
    pub float_profit_short: f64,
    /// 浮动盈亏 = floatProfitLong + floatProfitShort
    #[serde(default)]
    pub float_profit: f64,
    /// 多头持仓盈亏
    #[serde(default)]
    pub position_profit_long: f64,
    /// 空头持仓盈亏
    #[serde(default)]
    pub position_profit_short: f64,
    /// 持仓盈亏 = positionProfitLong + positionProfitShort
    #[serde(default)]
    pub position_profit: f64,
    /// 多头持仓占用保证金
    #[serde(default)]
    pub margin_long: f64,
    /// 空头持仓占用保证金
    #[serde(default)]
    pub margin_short: f64,
    /// 持仓占用保证金 = marginLong + marginShort
    #[serde(default)]
    pub margin: f64,
    /// 期权权利方市值(始终 >= 0)
    #[serde(default)]
    pub market_value_long: f64,
    /// 期权义务方市值(始终 <= 0)
    #[serde(default)]
    pub market_value_short: f64,
    /// 期权市值
    #[serde(default)]
    pub market_value: f64,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "_epoch")]
    pub epoch: Option<i64>,
}

/// 委托单信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Order {
    /// 内部序号
    #[serde(default)]
    pub seqno: i64,
    /// 用户ID
    #[serde(default)]
    pub user_id: String,
    /// 委托单ID, 对于一个user, orderId 是永远不重复的
    #[serde(default)]
    pub order_id: String,
    /// 交易所代码
    #[serde(default)]
    pub exchange_id: String,
    /// 在交易所中的合约代码
    #[serde(default)]
    pub instrument_id: String,
    /// 下单方向 (buy=买, sell=卖)
    #[serde(default)]
    pub direction: String,
    /// 开平标志 (open=开仓, close=平仓, closetoday=平今)
    #[serde(default)]
    pub offset: String,
    /// 总报单手数
    #[serde(default)]
    pub volume_orign: i64,
    /// 指令类型 (any=市价, limit=限价)
    #[serde(default)]
    pub price_type: String,
    /// 委托价格, 仅当 priceType = limit 时有效
    #[serde(default)]
    pub limit_price: f64,
    /// 时间条件 (ioc=立即完成，否则撤销, gfs=本节有效, *gfd=当日有效, gtc=撤销前有效, gfa=集合竞价有效)
    #[serde(default)]
    pub time_condition: String,
    /// 数量条件 (any=任何数量, min=最小数量, all=全部数量)
    #[serde(default)]
    pub volume_condition: String,
    /// 下单时间(按北京时间)，自unix epoch(1970-01-01 00:00:00 gmt)以来的纳秒数
    #[serde(default)]
    pub insert_date_time: i64,
    /// 交易所单号
    #[serde(default)]
    pub exchange_order_id: String,
    /// 委托单状态, (alive=有效, finished=已完)
    #[serde(default)]
    pub status: String,
    /// 未成交手数
    #[serde(default)]
    pub volume_left: i64,
    /// 冻结保证金
    #[serde(default)]
    pub frozen_margin: f64,
    /// 委托单状态信息
    #[serde(default)]
    pub last_msg: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "_epoch")]
    pub epoch: Option<i64>,
}

impl Order {
    /// volume 别名（返回 volume_orign）
    pub fn volume(&self) -> i64 {
        self.volume_orign
    }

    /// price 别名（返回 limit_price）
    pub fn price(&self) -> f64 {
        self.limit_price
    }
}

/// 成交记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Trade {
    /// 内部序号
    #[serde(default)]
    pub seqno: i64,
    /// 账户号
    #[serde(default)]
    pub user_id: String,
    /// 成交ID, 对于一个用户的所有成交，这个ID都是不重复的
    #[serde(default)]
    pub trade_id: String,
    /// 交易所
    #[serde(default)]
    pub exchange_id: String,
    /// 交易所内的合约代码
    #[serde(default)]
    pub instrument_id: String,
    /// 委托单ID, 对于一个用户的所有委托单，这个ID都是不重复的
    #[serde(default)]
    pub order_id: String,
    /// 交易所成交单号
    #[serde(default)]
    pub exchange_trade_id: String,
    /// 下单方向 (BUY=买, SELL=卖)
    #[serde(default)]
    pub direction: String,
    /// 开平标志 (OPEN=开仓, CLOSE=平仓, CLOSETODAY=平今)
    #[serde(default)]
    pub offset: String,
    /// 成交手数
    #[serde(default)]
    pub volume: i64,
    /// 成交价格
    #[serde(default)]
    pub price: f64,
    /// 成交时间, epoch nano
    #[serde(default)]
    pub trade_date_time: i64,
    /// 成交手续费
    #[serde(default)]
    pub commission: f64,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(rename = "_epoch")]
    pub epoch: Option<i64>,
}

/// 通知事件
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NotifyEvent {
    /// 通知代码
    pub code: String,
    /// 通知级别
    pub level: String,
    /// 通知类型
    pub r#type: String,
    /// 通知内容
    pub content: String,
    /// 期货公司
    pub bid: String,
    /// 用户ID
    pub user_id: String,
}

/// 通知
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Notification {
    /// 通知代码
    pub code: String,
    /// 通知级别
    pub level: String,
    /// 通知类型
    pub r#type: String,
    /// 通知内容
    pub content: String,
    /// 期货公司
    pub bid: String,
    /// 用户ID
    pub user_id: String,
}

/// 持仓更新
#[derive(Debug, Clone)]
pub struct PositionUpdate {
    /// 合约代码
    pub symbol: String,
    /// 持仓信息
    pub position: Position,
}

/// 下单请求
#[derive(Debug, Clone)]
pub struct InsertOrderRequest {
    /// 合约代码（格式：EXCHANGE.INSTRUMENT，如 SHFE.au2512）
    pub symbol: String,
    /// 交易所代码（可选，如果提供 symbol 则自动拆分）
    pub exchange_id: Option<String>,
    /// 合约代码（可选，如果提供 symbol 则自动拆分）
    pub instrument_id: Option<String>,
    /// 下单方向 BUY/SELL
    pub direction: String,
    /// 开平标志 OPEN/CLOSE/CLOSETODAY
    pub offset: String,
    /// 价格类型 LIMIT/ANY
    pub price_type: String,
    /// 委托价格
    pub limit_price: f64,
    /// 下单手数
    pub volume: i64,
}

impl InsertOrderRequest {
    /// 获取交易所代码（从 symbol 拆分或使用 exchange_id）
    pub fn get_exchange_id(&self) -> String {
        if let Some(ref exchange) = self.exchange_id {
            return exchange.clone();
        }
        // 从 symbol 拆分：EXCHANGE.INSTRUMENT
        if let Some(dot_pos) = self.symbol.find('.') {
            self.symbol[..dot_pos].to_string()
        } else {
            String::new()
        }
    }

    /// 获取合约代码（从 symbol 拆分或使用 instrument_id）
    pub fn get_instrument_id(&self) -> String {
        if let Some(ref instrument) = self.instrument_id {
            return instrument.clone();
        }
        // 从 symbol 拆分：EXCHANGE.INSTRUMENT
        if let Some(dot_pos) = self.symbol.find('.') {
            self.symbol[dot_pos + 1..].to_string()
        } else {
            self.symbol.clone()
        }
    }
}

// ==================== 常量定义 ====================

/// 方向 - 买入
pub const DIRECTION_BUY: &str = "BUY";
/// 方向 - 卖出
pub const DIRECTION_SELL: &str = "SELL";

/// 开平 - 开仓
pub const OFFSET_OPEN: &str = "OPEN";
/// 开平 - 平仓
pub const OFFSET_CLOSE: &str = "CLOSE";
/// 开平 - 平今
pub const OFFSET_CLOSETODAY: &str = "CLOSETODAY";

/// 价格类型 - 限价单
pub const PRICE_TYPE_LIMIT: &str = "LIMIT";
/// 价格类型 - 市价单
pub const PRICE_TYPE_ANY: &str = "ANY";

/// 订单状态 - 活动（未成交或部分成交）
pub const ORDER_STATUS_ALIVE: &str = "ALIVE";
/// 订单状态 - 已完成（全部成交或已撤销）
pub const ORDER_STATUS_FINISHED: &str = "FINISHED";

// ==================== 序列订阅选项 ====================

/// 序列订阅选项
#[derive(Debug, Clone)]
pub struct SeriesOptions {
    /// 合约列表
    pub symbols: Vec<String>,
    /// K线周期（纳秒，0表示Tick）
    pub duration: i64,
    /// 视图宽度（最大 10000）
    pub view_width: usize,
    /// 图表ID（可选）
    pub chart_id: Option<String>,
    /// 左边界 K线 ID（可选，优先级最高）
    pub left_kline_id: Option<i64>,
    /// 焦点时间（可选，需配合 focus_position 使用）
    pub focus_datetime: Option<DateTime<Utc>>,
    /// 焦点位置（可选，需配合 focus_datetime 使用，1=右侧，-1=左侧）
    pub focus_position: Option<i32>,
}

impl Default for SeriesOptions {
    fn default() -> Self {
        SeriesOptions {
            symbols: Vec::new(),
            duration: 0,
            view_width: 10000,
            chart_id: None,
            left_kline_id: None,
            focus_datetime: None,
            focus_position: None,
        }
    }
}
