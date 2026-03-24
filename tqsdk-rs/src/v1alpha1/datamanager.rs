//! 数据管理器
//!
//! 实现 DIFF 协议的数据合并和管理，包括：
//! - DIFF 数据递归合并
//! - 路径访问和版本追踪
//! - Watch/UnWatch 路径监听
//! - 数据类型转换

use super::errors::{Result, TqError};
use super::types::*;
use super::utils::{nanos_to_datetime, value_to_i64};
use async_channel::{unbounded, Receiver, Sender};
use chrono::Utc;
use serde_json::Value;
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, RwLock};
use tracing::{debug, error};

/// 数据管理器配置
#[derive(Debug, Clone)]
pub struct DataManagerConfig {
    /// 默认视图宽度
    pub default_view_width: usize,
    /// 启用自动清理
    pub enable_auto_cleanup: bool,
}

impl Default for DataManagerConfig {
    fn default() -> Self {
        DataManagerConfig {
            default_view_width: 10000,
            enable_auto_cleanup: true,
        }
    }
}

/// 路径监听器
struct PathWatcher {
    path: Vec<String>,
    tx: Sender<Value>,
}

/// 数据管理器
///
/// 管理所有 DIFF 协议数据，支持：
/// - 递归合并
/// - 版本追踪
/// - 路径监听
pub struct DataManager {
    /// 数据存储
    data: Arc<RwLock<HashMap<String, Value>>>,
    /// 版本号（使用原子变量以提高性能）
    epoch: AtomicI64,
    /// 配置
    config: DataManagerConfig,
    /// 路径监听器
    watchers: Arc<RwLock<HashMap<String, PathWatcher>>>,
    /// 数据更新回调（使用 Arc 以支持异步触发）
    on_data_callbacks: Arc<RwLock<Vec<Arc<dyn Fn() + Send + Sync>>>>,
}

impl DataManager {
    /// 创建新的数据管理器
    pub fn new(initial_data: HashMap<String, Value>, config: DataManagerConfig) -> Self {
        DataManager {
            data: Arc::new(RwLock::new(initial_data)),
            epoch: AtomicI64::new(0),
            config,
            watchers: Arc::new(RwLock::new(HashMap::new())),
            on_data_callbacks: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// 注册数据更新回调
    pub fn on_data<F>(&self, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        let mut callbacks = self.on_data_callbacks.write().unwrap();
        callbacks.push(Arc::new(callback));
    }

    /// 合并数据（DIFF 协议核心）
    ///
    /// # 参数
    ///
    /// * `source` - 源数据（可以是单个对象或数组）
    /// * `epoch_increase` - 是否增加版本号
    /// * `delete_null` - 是否删除 null 对象
    pub fn merge_data(&self, source: Value, epoch_increase: bool, delete_null: bool) {
        let should_notify_watchers = if epoch_increase {
            // 增加版本号（使用原子操作）
            let current_epoch = self.epoch.fetch_add(1, Ordering::SeqCst) + 1;

            // 转换为数组
            let source_arr = match source {
                Value::Array(arr) => arr,
                Value::Object(_) => vec![source],
                _ => {
                    debug!("merge_data: 无效的源数据类型");
                    return;
                }
            };

            // 合并数据
            let mut data = self.data.write().unwrap();
            for item in source_arr.iter() {
                if let Value::Object(obj) = item {
                    if !obj.is_empty() {
                        self.merge_object(&mut data, obj, current_epoch, delete_null);
                    }
                }
            }
            drop(data);

            // 异步触发回调（不阻塞数据合并）
            let callbacks = self.on_data_callbacks.read().unwrap();
            for callback in callbacks.iter() {
                let cb = Arc::clone(callback);
                tokio::spawn(async move {
                    cb();
                });
            }
            drop(callbacks);

            true
        } else {
            // 不增加版本号，只合并（使用原子操作读取）
            let current_epoch = self.epoch.load(Ordering::SeqCst);

            let source_arr = match source {
                Value::Array(arr) => arr,
                Value::Object(_) => vec![source],
                _ => return,
            };

            let mut data = self.data.write().unwrap();
            for item in source_arr.iter() {
                if let Value::Object(obj) = item {
                    if !obj.is_empty() {
                        self.merge_object(&mut data, obj, current_epoch, delete_null);
                    }
                }
            }

            false
        };

        // 通知 watchers
        if should_notify_watchers {
            self.notify_watchers();
        }
    }

    /// 递归合并对象
    fn merge_object(
        &self,
        target: &mut HashMap<String, Value>,
        source: &serde_json::Map<String, Value>,
        epoch: i64,
        delete_null: bool,
    ) {
        for (property, value) in source.iter() {
            if value.is_null() {
                if delete_null {
                    target.remove(property);
                }
                continue;
            }

            match value {
                Value::String(s) if s == "NaN" || s == "-" => {
                    // NaN 字符串处理
                    target.insert(property.clone(), Value::Null);
                }
                Value::Object(obj) => {
                    // 递归合并对象
                    if property == "quotes" {
                        // quotes 特殊处理
                        self.merge_quotes(target, obj, epoch, delete_null);
                    } else {
                        // 确保目标存在
                        let target_obj = target
                            .entry(property.clone())
                            .or_insert_with(|| Value::Object(serde_json::Map::new()));

                        if let Value::Object(target_map) = target_obj {
                            let mut target_hashmap: HashMap<String, Value> = target_map
                                .iter()
                                .map(|(k, v)| (k.clone(), v.clone()))
                                .collect();

                            self.merge_object(&mut target_hashmap, obj, epoch, delete_null);

                            *target_map = target_hashmap
                                .into_iter()
                                .collect::<serde_json::Map<String, Value>>();
                        }
                    }
                }
                _ => {
                    // 基本类型和数组直接赋值
                    target.insert(property.clone(), value.clone());
                }
            }
        }

        // 设置 epoch
        target.insert("_epoch".to_string(), Value::Number(epoch.into()));
    }

    /// 特殊处理 quotes 对象
    fn merge_quotes(
        &self,
        target: &mut HashMap<String, Value>,
        quotes: &serde_json::Map<String, Value>,
        epoch: i64,
        delete_null: bool,
    ) {
        let quotes_obj = target
            .entry("quotes".to_string())
            .or_insert_with(|| Value::Object(serde_json::Map::new()));

        if let Value::Object(quotes_map) = quotes_obj {
            let mut quotes_hashmap: HashMap<String, Value> = quotes_map
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();

            for (symbol, quote_data) in quotes.iter() {
                if quote_data.is_null() {
                    if delete_null {
                        quotes_hashmap.remove(symbol);
                    }
                    continue;
                }

                if let Value::Object(quote_obj) = quote_data {
                    // 确保目标存在
                    let target_quote = quotes_hashmap
                        .entry(symbol.clone())
                        .or_insert_with(|| Value::Object(serde_json::Map::new()));

                    if let Value::Object(target_quote_map) = target_quote {
                        let mut target_quote_hashmap: HashMap<String, Value> = target_quote_map
                            .iter()
                            .map(|(k, v)| (k.clone(), v.clone()))
                            .collect();

                        self.merge_object(&mut target_quote_hashmap, quote_obj, epoch, delete_null);

                        *target_quote_map = target_quote_hashmap
                            .into_iter()
                            .collect::<serde_json::Map<String, Value>>();
                    }
                }
            }

            *quotes_map = quotes_hashmap
                .into_iter()
                .collect::<serde_json::Map<String, Value>>();
        }
    }

    /// 根据路径获取数据
    pub fn get_by_path(&self, path: &[&str]) -> Option<Value> {
        let data = self.data.read().unwrap();
        let mut current: &Value = &Value::Object(
            data.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<serde_json::Map<String, Value>>(),
        );

        for &key in path.iter() {
            match current {
                Value::Object(map) => {
                    if let Some(val) = map.get(key) {
                        current = val;
                    } else {
                        return None;
                    }
                }
                _ => return None,
            }
        }
        Some(current.clone())
    }

    /// 判断指定路径的数据是否在最近一次更新中发生了变化
    pub fn is_changing(&self, path: &[&str]) -> bool {
        let current_epoch = self.epoch.load(Ordering::SeqCst);
        let data = self.data.read().unwrap();

        let mut current_value = Some(&Value::Object(
            data.iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect::<serde_json::Map<String, Value>>(),
        ));

        for (i, &key) in path.iter().enumerate() {
            if let Some(Value::Object(map)) = current_value {
                if let Some(val) = map.get(key) {
                    // 只在最后一层检查 epoch（避免父节点更新导致误判）
                    if i == path.len() - 1 {
                        if let Some(Value::Object(obj)) = Some(val) {
                            if let Some(Value::Number(e)) = obj.get("_epoch") {
                                if let Some(e_val) = e.as_i64() {
                                    if e_val == current_epoch {
                                        return true;
                                    }
                                }
                            }
                        }
                    }

                    // 继续往下查找
                    if i < path.len() - 1 {
                        current_value = Some(val);
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        false
    }

    /// 获取当前版本号
    pub fn get_epoch(&self) -> i64 {
        self.epoch.load(Ordering::SeqCst)
    }

    /// 设置默认值
    pub fn set_default(&self, path: &[&str], default_value: Value) -> Option<Value> {
        let mut data = self.data.write().unwrap();
        let current = &mut *data;

        for (i, &key) in path.iter().enumerate() {
            if i == path.len() - 1 {
                // 最后一个 key
                if !current.contains_key(key) {
                    current.insert(key.to_string(), default_value.clone());
                }
                return current.get(key).cloned();
            }

            // 中间节点
            let entry = current
                .entry(key.to_string())
                .or_insert_with(|| Value::Object(serde_json::Map::new()));

            // 无法继续（类型不匹配）
            if !entry.is_object() {
                return None;
            }
        }

        None
    }

    /// 监听指定路径的数据变化
    ///
    /// 返回一个 receiver，数据变化时会推送到这个 channel
    pub fn watch(&self, path: Vec<String>) -> Receiver<Value> {
        let path_key = path.join(".");
        let (tx, rx) = unbounded();

        let watcher = PathWatcher {
            path: path.clone(),
            tx,
        };

        let mut watchers = self.watchers.write().unwrap();
        watchers.insert(path_key, watcher);

        rx
    }

    /// 取消路径监听
    pub fn unwatch(&self, path: &[String]) -> Result<()> {
        let path_key = path.join(".");
        let mut watchers = self.watchers.write().unwrap();

        if watchers.remove(&path_key).is_none() {
            return Err(TqError::DataNotFound(format!("路径未监听: {}", path_key)));
        }

        Ok(())
    }

    /// 通知所有 watchers
    fn notify_watchers(&self) {
        let watchers = self.watchers.read().unwrap();
        for (_, watcher) in watchers.iter() {
            let path_refs: Vec<&str> = watcher.path.iter().map(|s| s.as_str()).collect();
            if self.is_changing(&path_refs) {
                if let Some(data) = self.get_by_path(&path_refs) {
                    let tx = watcher.tx.clone();
                    tokio::spawn(async move {
                        let _ = tx.send(data).await;
                    });
                }
            }
        }
    }

    /// 转换为结构体
    pub fn convert_to_struct<T: serde::de::DeserializeOwned>(&self, data: &Value) -> Result<T> {
        serde_json::from_value(data.clone())
            .map_err(|e| TqError::ParseError(format!("转换失败: {}", e)))
    }

    /// 获取 Quote 数据
    pub fn get_quote_data(&self, symbol: &str) -> Result<Quote> {
        let data = self
            .get_by_path(&["quotes", symbol])
            .ok_or_else(|| TqError::DataNotFound(format!("Quote 未找到: {}", symbol)))?;

        self.convert_to_struct(&data)
    }

    /// 获取 K线数据
    pub fn get_klines_data(
        &self,
        symbol: &str,
        duration: i64,
        view_width: usize,
        right_id: i64,
    ) -> Result<KlineSeriesData> {
        let duration_str = duration.to_string();
        let data = self
            .get_by_path(&["klines", symbol, &duration_str])
            .ok_or_else(|| TqError::DataNotFound(format!("K线未找到: {}/{}", symbol, duration)))?;

        if let Value::Object(data_map) = data {
            let mut kline_series = KlineSeriesData {
                symbol: symbol.to_string(),
                duration,
                chart_id: String::new(),
                chart: None,
                last_id: value_to_i64(data_map.get("last_id").unwrap_or(&Value::Null)),
                trading_day_start_id: value_to_i64(
                    data_map.get("trading_day_start_id").unwrap_or(&Value::Null),
                ),
                trading_day_end_id: value_to_i64(
                    data_map.get("trading_day_end_id").unwrap_or(&Value::Null),
                ),
                data: Vec::new(),
                has_new_bar: false,
            };

            // 转换 data map 为数组
            if let Some(Value::Object(kline_map)) = data_map.get("data") {
                let mut all_klines: Vec<(i64, Kline)> = Vec::new();

                for (id_str, kline_data) in kline_map.iter() {
                    if let Ok(id) = id_str.parse::<i64>() {
                        match self.convert_to_struct::<Kline>(kline_data) {
                            Ok(mut kline) => {
                                kline.id = id;
                                all_klines.push((id, kline));
                            }
                            Err(e) => {
                                error!(
                                    "{}",
                                    TqError::ParseError(format!("K线数据格式错误: {}", e))
                                );
                            }
                        }
                    }
                }
                // 按 ID 排序
                all_klines.sort_by_key(|(id, _)| *id);

                // 过滤超出 right_id 的数据
                if right_id > 0 {
                    all_klines.retain(|(id, _)| *id <= right_id);
                }

                // 应用 ViewWidth 限制
                let vw = if view_width > 0 {
                    view_width
                } else {
                    self.config.default_view_width
                };

                let klines: Vec<Kline> = all_klines
                    .into_iter()
                    .map(|(_, k)| k)
                    .rev()
                    .take(vw)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect();
                kline_series.data = klines;
            }

            Ok(kline_series)
        } else {
            Err(TqError::ParseError("K线数据格式错误".to_string()))
        }
    }

    /// 获取多合约对齐的 K线数据
    pub fn get_multi_klines_data(
        &self,
        symbols: &[String],
        duration: i64,
        chart_id: &str,
        view_width: usize,
    ) -> Result<MultiKlineSeriesData> {
        if symbols.is_empty() {
            return Err(TqError::InvalidParameter("symbols 为空".to_string()));
        }

        let main_symbol = &symbols[0];
        let duration_str = duration.to_string();

        // 获取 Chart 信息
        let (left_id, right_id) = if let Some(chart_data) = self.get_by_path(&["charts", chart_id])
        {
            if let Value::Object(chart_map) = chart_data {
                let left = value_to_i64(chart_map.get("left_id").unwrap_or(&Value::Null));
                let right = value_to_i64(chart_map.get("right_id").unwrap_or(&Value::Null));
                (left, right)
            } else {
                (-1, -1)
            }
        } else {
            (-1, -1)
        };

        let mut result = MultiKlineSeriesData {
            chart_id: chart_id.to_string(),
            duration,
            main_symbol: main_symbol.clone(),
            symbols: symbols.to_vec(),
            left_id,
            right_id,
            view_width,
            data: Vec::new(),
            has_new_bar: false,
            metadata: HashMap::new(),
        };

        // 获取每个合约的元数据
        for symbol in symbols.iter() {
            if let Some(kline_data) = self.get_by_path(&["klines", symbol, &duration_str]) {
                if let Value::Object(kline_map) = kline_data {
                    let metadata = KlineMetadata {
                        symbol: symbol.clone(),
                        last_id: value_to_i64(kline_map.get("last_id").unwrap_or(&Value::Null)),
                        trading_day_start_id: value_to_i64(
                            kline_map
                                .get("trading_day_start_id")
                                .unwrap_or(&Value::Null),
                        ),
                        trading_day_end_id: value_to_i64(
                            kline_map.get("trading_day_end_id").unwrap_or(&Value::Null),
                        ),
                    };
                    result.metadata.insert(symbol.clone(), metadata);
                }
            }
        }

        // 获取主合约的 K线数据
        if let Some(main_kline_data) = self.get_by_path(&["klines", main_symbol, &duration_str]) {
            if let Value::Object(main_kline_map) = main_kline_data {
                // 获取 binding 信息
                let mut bindings: HashMap<String, HashMap<i64, i64>> = HashMap::new();
                if let Some(Value::Object(binding_map)) = main_kline_map.get("binding") {
                    for (symbol, binding_info) in binding_map.iter() {
                        if let Value::Object(binding_id_map) = binding_info {
                            let mut id_map: HashMap<i64, i64> = HashMap::new();
                            for (main_id_str, other_id) in binding_id_map.iter() {
                                if let Ok(main_id) = main_id_str.parse::<i64>() {
                                    id_map.insert(main_id, value_to_i64(other_id));
                                }
                            }
                            bindings.insert(symbol.clone(), id_map);
                        }
                    }
                }

                // 获取主合约的 K线 map
                if let Some(Value::Object(main_data_map)) = main_kline_map.get("data") {
                    let mut main_ids: Vec<i64> = main_data_map
                        .keys()
                        .filter_map(|k| k.parse::<i64>().ok())
                        .collect();
                    main_ids.sort();

                    // 过滤超出 right_id 的数据
                    if right_id > 0 {
                        main_ids.retain(|&id| id <= right_id);
                    }

                    // 应用 ViewWidth 限制
                    if view_width > 0 && main_ids.len() > view_width {
                        main_ids = main_ids
                            .into_iter()
                            .rev()
                            .take(view_width)
                            .collect::<Vec<_>>()
                            .into_iter()
                            .rev()
                            .collect();
                        result.left_id = main_ids[0];
                        result.right_id = main_ids[main_ids.len() - 1];
                    }

                    // 对齐所有合约的 K线
                    for main_id in main_ids {
                        let main_id_str = main_id.to_string();
                        let mut set = AlignedKlineSet {
                            main_id,
                            timestamp: Utc::now(),
                            klines: HashMap::new(),
                        };

                        // 添加主合约 K线
                        if let Some(kline_data) = main_data_map.get(&main_id_str) {
                            if let Ok(mut kline) = self.convert_to_struct::<Kline>(kline_data) {
                                kline.id = main_id;
                                set.timestamp = nanos_to_datetime(kline.datetime);
                                set.klines.insert(main_symbol.clone(), kline);
                            }
                        }

                        // 添加其他合约的对齐 K线
                        for symbol in symbols.iter().skip(1) {
                            if let Some(binding) = bindings.get(symbol) {
                                if let Some(&mapped_id) = binding.get(&main_id) {
                                    if let Some(other_kline_data) = self.get_by_path(&[
                                        "klines",
                                        symbol,
                                        &duration_str,
                                        "data",
                                        &mapped_id.to_string(),
                                    ]) {
                                        if let Ok(mut kline) =
                                            self.convert_to_struct::<Kline>(&other_kline_data)
                                        {
                                            kline.id = mapped_id;
                                            set.klines.insert(symbol.clone(), kline);
                                        }
                                    }
                                }
                            }
                        }

                        result.data.push(set);
                    }
                }
            }
        }

        Ok(result)
    }

    /// 获取 Tick 数据
    pub fn get_ticks_data(
        &self,
        symbol: &str,
        view_width: usize,
        right_id: i64,
    ) -> Result<TickSeriesData> {
        let data = self
            .get_by_path(&["ticks", symbol])
            .ok_or_else(|| TqError::DataNotFound(format!("Tick 未找到: {}", symbol)))?;

        if let Value::Object(data_map) = data {
            let mut tick_series = TickSeriesData {
                symbol: symbol.to_string(),
                chart_id: String::new(),
                chart: None,
                last_id: value_to_i64(data_map.get("last_id").unwrap_or(&Value::Null)),
                data: Vec::new(),
                has_new_bar: false,
            };

            // 转换 data map 为数组
            if let Some(Value::Object(tick_map)) = data_map.get("data") {
                let mut all_ticks: Vec<(i64, Tick)> = Vec::new();

                for (id_str, tick_data) in tick_map.iter() {
                    if let Ok(id) = id_str.parse::<i64>() {
                        match self.convert_to_struct::<Tick>(tick_data) {
                            Ok(mut tick) => {
                                tick.id = id;
                                all_ticks.push((id, tick));
                            }
                            Err(e) => {
                                error!(
                                    "{}",
                                    TqError::ParseError(format!(
                                        "Tick数据格式错误: id={}, {}",
                                        id, e
                                    ))
                                );
                            }
                        }
                    }
                }

                // 按 ID 排序
                all_ticks.sort_by_key(|(id, _)| *id);

                // 过滤超出 right_id 的数据
                if right_id > 0 {
                    all_ticks.retain(|(id, _)| *id <= right_id);
                }

                // 应用 ViewWidth 限制
                let vw = if view_width > 0 {
                    view_width
                } else {
                    self.config.default_view_width
                };

                let ticks: Vec<Tick> = all_ticks
                    .into_iter()
                    .map(|(_, t)| t)
                    .rev()
                    .take(vw)
                    .collect::<Vec<_>>()
                    .into_iter()
                    .rev()
                    .collect();

                tick_series.data = ticks;
            }

            Ok(tick_series)
        } else {
            Err(TqError::ParseError("Tick数据格式错误".to_string()))
        }
    }

    /// 获取账户数据
    pub fn get_account_data(&self, user_id: &str, currency: &str) -> Result<Account> {
        let data = self
            .get_by_path(&["trade", user_id, "accounts", currency])
            .ok_or_else(|| {
                TqError::DataNotFound(format!("账户未找到: {}/{}", user_id, currency))
            })?;

        self.convert_to_struct(&data)
    }

    /// 获取持仓数据
    pub fn get_position_data(&self, user_id: &str, symbol: &str) -> Result<Position> {
        let data = self
            .get_by_path(&["trade", user_id, "positions", symbol])
            .ok_or_else(|| TqError::DataNotFound(format!("持仓未找到: {}/{}", user_id, symbol)))?;

        self.convert_to_struct(&data)
    }

    /// 获取委托单数据
    pub fn get_order_data(&self, user_id: &str, order_id: &str) -> Result<Order> {
        let data = self
            .get_by_path(&["trade", user_id, "orders", order_id])
            .ok_or_else(|| {
                TqError::DataNotFound(format!("委托单未找到: {}/{}", user_id, order_id))
            })?;

        self.convert_to_struct(&data)
    }

    /// 获取成交数据
    pub fn get_trade_data(&self, user_id: &str, trade_id: &str) -> Result<Trade> {
        let data = self
            .get_by_path(&["trade", user_id, "trades", trade_id])
            .ok_or_else(|| {
                TqError::DataNotFound(format!("成交未找到: {}/{}", user_id, trade_id))
            })?;

        self.convert_to_struct(&data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_merge_data() {
        let initial_data = HashMap::new();
        let config = DataManagerConfig::default();
        let dm = DataManager::new(initial_data, config);

        let source = json!({
            "quotes": {
                "SHFE.au2602": {
                    "last_price": 500.0,
                    "volume": 1000
                }
            }
        });

        dm.merge_data(source, true, false);

        let quote = dm.get_by_path(&["quotes", "SHFE.au2602"]);
        assert!(quote.is_some());

        if let Some(Value::Object(quote_map)) = quote {
            assert_eq!(quote_map.get("last_price"), Some(&json!(500.0)));
            assert_eq!(quote_map.get("volume"), Some(&json!(1000)));
        }
    }

    #[test]
    fn test_is_changing() {
        let initial_data = HashMap::new();
        let config = DataManagerConfig::default();
        let dm = DataManager::new(initial_data, config);

        let source = json!({
            "quotes": {
                "SHFE.au2602": {
                    "last_price": 500.0
                }
            }
        });

        dm.merge_data(source, true, false);

        assert!(dm.is_changing(&["quotes", "SHFE.au2602"]));
        assert!(!dm.is_changing(&["quotes", "SHFE.ag2512"]));
    }
}
