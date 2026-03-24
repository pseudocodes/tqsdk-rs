//! Polars DataFrame 扩展
//!
//! 提供 K线和 Tick 数据的 Polars DataFrame 转换和缓冲功能

#[cfg(feature = "polars")]
use polars::prelude::*;

use super::errors::{Result, TqError};
use super::types::{Kline, Tick, SeriesData, KlineSeriesData, TickSeriesData, MultiKlineSeriesData};

// ==================== K线缓冲区 ====================

/// K线数据缓冲区
/// 
/// 维护可变的列向量，支持高效的追加和更新操作
/// 可按需转换为 Polars DataFrame 进行分析
#[cfg(feature = "polars")]
#[derive(Debug, Clone)]
pub struct KlineBuffer {
    /// K线ID
    pub ids: Vec<i64>,
    /// 时间戳（纳秒）
    pub datetimes: Vec<i64>,
    /// 开盘价
    pub opens: Vec<f64>,
    /// 最高价
    pub highs: Vec<f64>,
    /// 最低价
    pub lows: Vec<f64>,
    /// 收盘价
    pub closes: Vec<f64>,
    /// 成交量
    pub volumes: Vec<i64>,
    /// 起始持仓量
    pub open_ois: Vec<i64>,
    /// 结束持仓量
    pub close_ois: Vec<i64>,
}

#[cfg(feature = "polars")]
impl KlineBuffer {
    /// 创建空缓冲区
    pub fn new() -> Self {
        Self {
            ids: Vec::new(),
            datetimes: Vec::new(),
            opens: Vec::new(),
            highs: Vec::new(),
            lows: Vec::new(),
            closes: Vec::new(),
            volumes: Vec::new(),
            open_ois: Vec::new(),
            close_ois: Vec::new(),
        }
    }

    /// 从 K线数组创建缓冲区
    pub fn from_klines(klines: &[Kline]) -> Self {
        let mut buffer = Self::new();
        for kline in klines {
            buffer.push(kline);
        }
        buffer
    }

    /// 添加新 K线
    pub fn push(&mut self, kline: &Kline) {
        self.ids.push(kline.id);
        self.datetimes.push(kline.datetime);
        self.opens.push(kline.open);
        self.highs.push(kline.high);
        self.lows.push(kline.low);
        self.closes.push(kline.close);
        self.volumes.push(kline.volume);
        self.open_ois.push(kline.open_oi);
        self.close_ois.push(kline.close_oi);
    }

    /// 更新最后一根 K线
    /// 
    /// 如果缓冲区为空，则添加新 K线
    pub fn update_last(&mut self, kline: &Kline) {
        if self.ids.is_empty() {
            self.push(kline);
            return;
        }

        let last_idx = self.ids.len() - 1;
        
        // 更新可能变化的字段
        self.highs[last_idx] = kline.high;
        self.lows[last_idx] = kline.low;
        self.closes[last_idx] = kline.close;
        self.volumes[last_idx] = kline.volume;
        self.close_ois[last_idx] = kline.close_oi;
    }

    /// 获取缓冲区长度
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// 检查缓冲区是否为空
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// 清空缓冲区
    pub fn clear(&mut self) {
        self.ids.clear();
        self.datetimes.clear();
        self.opens.clear();
        self.highs.clear();
        self.lows.clear();
        self.closes.clear();
        self.volumes.clear();
        self.open_ois.clear();
        self.close_ois.clear();
    }

    /// 转换为 Polars DataFrame
    pub fn to_dataframe(&self) -> Result<DataFrame> {
        if self.is_empty() {
            return Err(TqError::Other("K线缓冲区为空".to_string()));
        }

        let df = DataFrame::new(vec![
            Column::new("id".into(), &self.ids),
            Column::new("datetime".into(), &self.datetimes),
            Column::new("open".into(), &self.opens),
            Column::new("high".into(), &self.highs),
            Column::new("low".into(), &self.lows),
            Column::new("close".into(), &self.closes),
            Column::new("volume".into(), &self.volumes),
            Column::new("open_oi".into(), &self.open_ois),
            Column::new("close_oi".into(), &self.close_ois),
        ])
        .map_err(|e| TqError::Other(format!("创建 DataFrame 失败: {}", e)))?;

        Ok(df)
    }

    /// 获取最后 n 行的 DataFrame
    pub fn tail(&self, n: usize) -> Result<DataFrame> {
        if self.is_empty() {
            return Err(TqError::Other("K线缓冲区为空".to_string()));
        }

        let len = self.ids.len();
        let start = if n >= len { 0 } else { len - n };

        let df = DataFrame::new(vec![
            Column::new("id".into(), &self.ids[start..]),
            Column::new("datetime".into(), &self.datetimes[start..]),
            Column::new("open".into(), &self.opens[start..]),
            Column::new("high".into(), &self.highs[start..]),
            Column::new("low".into(), &self.lows[start..]),
            Column::new("close".into(), &self.closes[start..]),
            Column::new("volume".into(), &self.volumes[start..]),
            Column::new("open_oi".into(), &self.open_ois[start..]),
            Column::new("close_oi".into(), &self.close_ois[start..]),
        ])
        .map_err(|e| TqError::Other(format!("创建 DataFrame 失败: {}", e)))?;

        Ok(df)
    }

    /// 获取前 n 行的 DataFrame
    pub fn head(&self, n: usize) -> Result<DataFrame> {
        if self.is_empty() {
            return Err(TqError::Other("K线缓冲区为空".to_string()));
        }

        let len = self.ids.len();
        let end = if n >= len { len } else { n };

        let df = DataFrame::new(vec![
            Column::new("id".into(), &self.ids[..end]),
            Column::new("datetime".into(), &self.datetimes[..end]),
            Column::new("open".into(), &self.opens[..end]),
            Column::new("high".into(), &self.highs[..end]),
            Column::new("low".into(), &self.lows[..end]),
            Column::new("close".into(), &self.closes[..end]),
            Column::new("volume".into(), &self.volumes[..end]),
            Column::new("open_oi".into(), &self.open_ois[..end]),
            Column::new("close_oi".into(), &self.close_ois[..end]),
        ])
        .map_err(|e| TqError::Other(format!("创建 DataFrame 失败: {}", e)))?;

        Ok(df)
    }

    /// 获取指定范围的 DataFrame
    pub fn slice(&self, start: usize, length: usize) -> Result<DataFrame> {
        if self.is_empty() {
            return Err(TqError::Other("K线缓冲区为空".to_string()));
        }

        let len = self.ids.len();
        if start >= len {
            return Err(TqError::Other(format!("起始位置 {} 超出范围 {}", start, len)));
        }

        let end = std::cmp::min(start + length, len);

        let df = DataFrame::new(vec![
            Column::new("id".into(), &self.ids[start..end]),
            Column::new("datetime".into(), &self.datetimes[start..end]),
            Column::new("open".into(), &self.opens[start..end]),
            Column::new("high".into(), &self.highs[start..end]),
            Column::new("low".into(), &self.lows[start..end]),
            Column::new("close".into(), &self.closes[start..end]),
            Column::new("volume".into(), &self.volumes[start..end]),
            Column::new("open_oi".into(), &self.open_ois[start..end]),
            Column::new("close_oi".into(), &self.close_ois[start..end]),
        ])
        .map_err(|e| TqError::Other(format!("创建 DataFrame 失败: {}", e)))?;

        Ok(df)
    }
}

#[cfg(feature = "polars")]
impl Default for KlineBuffer {
    fn default() -> Self {
        Self::new()
    }
}

// ==================== Tick 缓冲区 ====================

/// Tick 数据缓冲区
#[cfg(feature = "polars")]
#[derive(Debug, Clone)]
pub struct TickBuffer {
    /// Tick ID
    pub ids: Vec<i64>,
    /// 时间戳（纳秒）
    pub datetimes: Vec<i64>,
    /// 最新价
    pub last_prices: Vec<f64>,
    /// 均价
    pub averages: Vec<f64>,
    /// 最高价
    pub highests: Vec<f64>,
    /// 最低价
    pub lowests: Vec<f64>,
    /// 卖一价
    pub ask_price1s: Vec<f64>,
    /// 卖一量
    pub ask_volume1s: Vec<i64>,
    /// 买一价
    pub bid_price1s: Vec<f64>,
    /// 买一量
    pub bid_volume1s: Vec<i64>,
    /// 成交量
    pub volumes: Vec<i64>,
    /// 成交额
    pub amounts: Vec<f64>,
    /// 持仓量
    pub open_interests: Vec<i64>,
}

#[cfg(feature = "polars")]
impl TickBuffer {
    /// 创建空缓冲区
    pub fn new() -> Self {
        Self {
            ids: Vec::new(),
            datetimes: Vec::new(),
            last_prices: Vec::new(),
            averages: Vec::new(),
            highests: Vec::new(),
            lowests: Vec::new(),
            ask_price1s: Vec::new(),
            ask_volume1s: Vec::new(),
            bid_price1s: Vec::new(),
            bid_volume1s: Vec::new(),
            volumes: Vec::new(),
            amounts: Vec::new(),
            open_interests: Vec::new(),
        }
    }

    /// 从 Tick 数组创建缓冲区
    pub fn from_ticks(ticks: &[Tick]) -> Self {
        let mut buffer = Self::new();
        for tick in ticks {
            buffer.push(tick);
        }
        buffer
    }

    /// 添加新 Tick
    pub fn push(&mut self, tick: &Tick) {
        self.ids.push(tick.id);
        self.datetimes.push(tick.datetime);
        self.last_prices.push(tick.last_price);
        self.averages.push(tick.average);
        self.highests.push(tick.highest);
        self.lowests.push(tick.lowest);
        self.ask_price1s.push(tick.ask_price1);
        self.ask_volume1s.push(tick.ask_volume1);
        self.bid_price1s.push(tick.bid_price1);
        self.bid_volume1s.push(tick.bid_volume1);
        self.volumes.push(tick.volume);
        self.amounts.push(tick.amount);
        self.open_interests.push(tick.open_interest);
    }

    /// 更新最后一个 Tick
    pub fn update_last(&mut self, tick: &Tick) {
        if self.ids.is_empty() {
            self.push(tick);
            return;
        }

        let last_idx = self.ids.len() - 1;
        
        self.last_prices[last_idx] = tick.last_price;
        self.averages[last_idx] = tick.average;
        self.highests[last_idx] = tick.highest;
        self.lowests[last_idx] = tick.lowest;
        self.ask_price1s[last_idx] = tick.ask_price1;
        self.ask_volume1s[last_idx] = tick.ask_volume1;
        self.bid_price1s[last_idx] = tick.bid_price1;
        self.bid_volume1s[last_idx] = tick.bid_volume1;
        self.volumes[last_idx] = tick.volume;
        self.amounts[last_idx] = tick.amount;
        self.open_interests[last_idx] = tick.open_interest;
    }

    /// 获取缓冲区长度
    pub fn len(&self) -> usize {
        self.ids.len()
    }

    /// 检查缓冲区是否为空
    pub fn is_empty(&self) -> bool {
        self.ids.is_empty()
    }

    /// 清空缓冲区
    pub fn clear(&mut self) {
        self.ids.clear();
        self.datetimes.clear();
        self.last_prices.clear();
        self.averages.clear();
        self.highests.clear();
        self.lowests.clear();
        self.ask_price1s.clear();
        self.ask_volume1s.clear();
        self.bid_price1s.clear();
        self.bid_volume1s.clear();
        self.volumes.clear();
        self.amounts.clear();
        self.open_interests.clear();
    }

    /// 转换为 Polars DataFrame
    pub fn to_dataframe(&self) -> Result<DataFrame> {
        if self.is_empty() {
            return Err(TqError::Other("Tick缓冲区为空".to_string()));
        }

        let df = DataFrame::new(vec![
            Column::new("id".into(), &self.ids),
            Column::new("datetime".into(), &self.datetimes),
            Column::new("last_price".into(), &self.last_prices),
            Column::new("average".into(), &self.averages),
            Column::new("highest".into(), &self.highests),
            Column::new("lowest".into(), &self.lowests),
            Column::new("ask_price1".into(), &self.ask_price1s),
            Column::new("ask_volume1".into(), &self.ask_volume1s),
            Column::new("bid_price1".into(), &self.bid_price1s),
            Column::new("bid_volume1".into(), &self.bid_volume1s),
            Column::new("volume".into(), &self.volumes),
            Column::new("amount".into(), &self.amounts),
            Column::new("open_interest".into(), &self.open_interests),
        ])
        .map_err(|e| TqError::Other(format!("创建 DataFrame 失败: {}", e)))?;

        Ok(df)
    }

    /// 获取最后 n 行的 DataFrame
    pub fn tail(&self, n: usize) -> Result<DataFrame> {
        if self.is_empty() {
            return Err(TqError::Other("Tick缓冲区为空".to_string()));
        }

        let len = self.ids.len();
        let start = if n >= len { 0 } else { len - n };

        let df = DataFrame::new(vec![
            Column::new("id".into(), &self.ids[start..]),
            Column::new("datetime".into(), &self.datetimes[start..]),
            Column::new("last_price".into(), &self.last_prices[start..]),
            Column::new("average".into(), &self.averages[start..]),
            Column::new("highest".into(), &self.highests[start..]),
            Column::new("lowest".into(), &self.lowests[start..]),
            Column::new("ask_price1".into(), &self.ask_price1s[start..]),
            Column::new("ask_volume1".into(), &self.ask_volume1s[start..]),
            Column::new("bid_price1".into(), &self.bid_price1s[start..]),
            Column::new("bid_volume1".into(), &self.bid_volume1s[start..]),
            Column::new("volume".into(), &self.volumes[start..]),
            Column::new("amount".into(), &self.amounts[start..]),
            Column::new("open_interest".into(), &self.open_interests[start..]),
        ])
        .map_err(|e| TqError::Other(format!("创建 DataFrame 失败: {}", e)))?;

        Ok(df)
    }
}

#[cfg(feature = "polars")]
impl Default for TickBuffer {
    fn default() -> Self {
        Self::new()
    }
}

// ==================== SeriesData 扩展 ====================

#[cfg(feature = "polars")]
impl SeriesData {
    /// 转换为 Polars DataFrame
    pub fn to_dataframe(&self) -> Result<DataFrame> {
        if self.is_tick {
            self.tick_to_dataframe()
        } else if self.is_multi {
            self.multi_kline_to_dataframe()
        } else {
            self.single_kline_to_dataframe()
        }
    }

    /// 单合约 K线转 DataFrame
    fn single_kline_to_dataframe(&self) -> Result<DataFrame> {
        let kline_data = self.single.as_ref()
            .ok_or_else(|| TqError::Other("单合约数据不存在".to_string()))?;

        kline_data.to_dataframe()
    }

    /// Tick 数据转 DataFrame
    fn tick_to_dataframe(&self) -> Result<DataFrame> {
        let tick_data = self.tick_data.as_ref()
            .ok_or_else(|| TqError::Other("Tick数据不存在".to_string()))?;

        tick_data.to_dataframe()
    }

    /// 多合约 K线转 DataFrame（长表格式）
    fn multi_kline_to_dataframe(&self) -> Result<DataFrame> {
        let multi_data = self.multi.as_ref()
            .ok_or_else(|| TqError::Other("多合约数据不存在".to_string()))?;

        multi_data.to_dataframe()
    }

    /// 多合约 K线转 DataFrame（宽表格式）
    /// 每个合约的 OHLCV 数据作为独立的列
    pub fn to_wide_dataframe(&self) -> Result<DataFrame> {
        if !self.is_multi {
            return Err(TqError::Other("只有多合约数据支持宽表格式".to_string()));
        }

        let multi_data = self.multi.as_ref()
            .ok_or_else(|| TqError::Other("多合约数据不存在".to_string()))?;

        multi_data.to_wide_dataframe()
    }
}

// ==================== KlineSeriesData 扩展 ====================

#[cfg(feature = "polars")]
impl KlineSeriesData {
    /// 转换为 Polars DataFrame
    pub fn to_dataframe(&self) -> Result<DataFrame> {
        let buffer = KlineBuffer::from_klines(&self.data);
        buffer.to_dataframe()
    }

    /// 转换为 KlineBuffer
    pub fn to_buffer(&self) -> KlineBuffer {
        KlineBuffer::from_klines(&self.data)
    }
}

// ==================== TickSeriesData 扩展 ====================

#[cfg(feature = "polars")]
impl TickSeriesData {
    /// 转换为 Polars DataFrame
    pub fn to_dataframe(&self) -> Result<DataFrame> {
        let buffer = TickBuffer::from_ticks(&self.data);
        buffer.to_dataframe()
    }

    /// 转换为 TickBuffer
    pub fn to_buffer(&self) -> TickBuffer {
        TickBuffer::from_ticks(&self.data)
    }
}

// ==================== MultiKlineSeriesData 扩展 ====================

#[cfg(feature = "polars")]
impl MultiKlineSeriesData {
    /// 转换为 DataFrame（长表格式）
    pub fn to_dataframe(&self) -> Result<DataFrame> {
        if self.data.is_empty() {
            return Err(TqError::Other("多合约K线数据为空".to_string()));
        }

        // 使用长表格式：每行是一个 (main_id, timestamp, symbol, kline_data)
        let mut main_ids = Vec::new();
        let mut timestamps = Vec::new();
        let mut symbols = Vec::new();
        let mut opens = Vec::new();
        let mut highs = Vec::new();
        let mut lows = Vec::new();
        let mut closes = Vec::new();
        let mut volumes = Vec::new();
        let mut open_ois = Vec::new();
        let mut close_ois = Vec::new();

        for aligned_set in &self.data {
            for (symbol, kline) in &aligned_set.klines {
                main_ids.push(aligned_set.main_id);
                timestamps.push(aligned_set.timestamp.timestamp_nanos_opt().unwrap_or(0));
                symbols.push(symbol.clone());
                opens.push(kline.open);
                highs.push(kline.high);
                lows.push(kline.low);
                closes.push(kline.close);
                volumes.push(kline.volume);
                open_ois.push(kline.open_oi);
                close_ois.push(kline.close_oi);
            }
        }

        let df = DataFrame::new(vec![
            Column::new("main_id".into(), main_ids),
            Column::new("timestamp".into(), timestamps),
            Column::new("symbol".into(), symbols),
            Column::new("open".into(), opens),
            Column::new("high".into(), highs),
            Column::new("low".into(), lows),
            Column::new("close".into(), closes),
            Column::new("volume".into(), volumes),
            Column::new("open_oi".into(), open_ois),
            Column::new("close_oi".into(), close_ois),
        ])
        .map_err(|e| TqError::Other(format!("创建 DataFrame 失败: {}", e)))?;

        Ok(df)
    }

    /// 转换为 DataFrame（宽表格式）
    pub fn to_wide_dataframe(&self) -> Result<DataFrame> {
        if self.data.is_empty() {
            return Err(TqError::Other("多合约K线数据为空".to_string()));
        }

        // 提取主时间轴
        let main_ids: Vec<i64> = self.data.iter().map(|s| s.main_id).collect();
        let timestamps: Vec<i64> = self.data.iter()
            .map(|s| s.timestamp.timestamp_nanos_opt().unwrap_or(0))
            .collect();

        let mut columns = vec![
            Column::new("main_id".into(), main_ids),
            Column::new("timestamp".into(), timestamps),
        ];

        // 为每个合约创建列
        for symbol in &self.symbols {
            let mut opens = Vec::new();
            let mut highs = Vec::new();
            let mut lows = Vec::new();
            let mut closes = Vec::new();
            let mut volumes = Vec::new();

            for aligned_set in &self.data {
                if let Some(kline) = aligned_set.klines.get(symbol) {
                    opens.push(kline.open);
                    highs.push(kline.high);
                    lows.push(kline.low);
                    closes.push(kline.close);
                    volumes.push(kline.volume);
                } else {
                    // 如果某个时间点没有该合约的数据，填充 NaN/0
                    opens.push(f64::NAN);
                    highs.push(f64::NAN);
                    lows.push(f64::NAN);
                    closes.push(f64::NAN);
                    volumes.push(0);
                }
            }

            columns.push(Column::new(format!("{}_open", symbol).into(), opens));
            columns.push(Column::new(format!("{}_high", symbol).into(), highs));
            columns.push(Column::new(format!("{}_low", symbol).into(), lows));
            columns.push(Column::new(format!("{}_close", symbol).into(), closes));
            columns.push(Column::new(format!("{}_volume", symbol).into(), volumes));
        }

        let df = DataFrame::new(columns)
            .map_err(|e| TqError::Other(format!("创建宽表 DataFrame 失败: {}", e)))?;

        Ok(df)
    }
}

#[cfg(test)]
#[cfg(feature = "polars")]
mod tests {
    use super::*;

    #[test]
    fn test_kline_buffer() {
        let mut buffer = KlineBuffer::new();
        
        // 添加 K线
        let kline1 = Kline {
            id: 1,
            datetime: 1000000000,
            open: 100.0,
            high: 105.0,
            low: 99.0,
            close: 103.0,
            volume: 1000,
            open_oi: 500,
            close_oi: 520,
            epoch: None,
        };
        
        buffer.push(&kline1);
        assert_eq!(buffer.len(), 1);
        
        // 更新最后一根
        let kline2 = Kline {
            id: 1,
            datetime: 1000000000,
            open: 100.0,
            high: 106.0,  // 更新最高价
            low: 98.0,    // 更新最低价
            close: 104.0, // 更新收盘价
            volume: 1200, // 更新成交量
            open_oi: 500,
            close_oi: 530,
            epoch: None,
        };
        
        buffer.update_last(&kline2);
        assert_eq!(buffer.len(), 1);
        assert_eq!(buffer.highs[0], 106.0);
        assert_eq!(buffer.closes[0], 104.0);
        
        // 转换为 DataFrame
        let df = buffer.to_dataframe().unwrap();
        assert_eq!(df.height(), 1);
        assert_eq!(df.width(), 9);
    }

    #[test]
    fn test_tick_buffer() {
        let mut buffer = TickBuffer::new();
        
        let tick = Tick {
            id: 1,
            datetime: 1000000000,
            last_price: 100.0,
            average: 99.5,
            highest: 101.0,
            lowest: 98.0,
            ask_price1: 100.5,
            ask_volume1: 10,
            ask_price2: f64::NAN,
            ask_volume2: 0,
            ask_price3: f64::NAN,
            ask_volume3: 0,
            ask_price4: f64::NAN,
            ask_volume4: 0,
            ask_price5: f64::NAN,
            ask_volume5: 0,
            bid_price1: 99.5,
            bid_volume1: 20,
            bid_price2: f64::NAN,
            bid_volume2: 0,
            bid_price3: f64::NAN,
            bid_volume3: 0,
            bid_price4: f64::NAN,
            bid_volume4: 0,
            bid_price5: f64::NAN,
            bid_volume5: 0,
            volume: 1000,
            amount: 100000.0,
            open_interest: 5000,
            epoch: None,
        };
        
        buffer.push(&tick);
        assert_eq!(buffer.len(), 1);
        
        let df = buffer.to_dataframe().unwrap();
        assert_eq!(df.height(), 1);
    }
}
