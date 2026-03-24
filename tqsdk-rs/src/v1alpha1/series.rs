//! Series API 模块
//!
//! 实现 K线和 Tick 订阅功能

use super::datamanager::DataManager;
use super::errors::{Result, TqError};
use super::types::{ChartInfo, SeriesData, UpdateInfo};
use super::websocket::TqQuoteWebsocket;
use async_stream::stream;
use chrono::{DateTime, Utc};
use futures::Stream;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration as StdDuration;
use tokio::sync::RwLock;
use tracing::{debug, info, trace, warn};
use uuid::Uuid;

use super::auth::Authenticator;

/// Series API
pub struct SeriesAPI {
    dm: Arc<DataManager>,
    ws: Arc<TqQuoteWebsocket>,
    auth: Arc<RwLock<dyn Authenticator>>,
    subscriptions: Arc<RwLock<HashMap<String, Arc<SeriesSubscription>>>>,
}

impl SeriesAPI {
    /// 创建 Series API
    pub fn new(
        dm: Arc<DataManager>,
        ws: Arc<TqQuoteWebsocket>,
        auth: Arc<RwLock<dyn Authenticator>>,
    ) -> Self {
        SeriesAPI {
            dm,
            ws,
            auth,
            subscriptions: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// 订阅单合约 K线
    pub async fn kline(
        &self,
        symbol: &str,
        duration: StdDuration,
        view_width: usize,
    ) -> Result<Arc<SeriesSubscription>> {
        self.subscribe(SeriesOptions {
            symbols: vec![symbol.to_string()],
            duration: duration.as_nanos() as i64,
            view_width,
            chart_id: String::new(),
            left_kline_id: None,
            focus_datetime: None,
            focus_position: None,
        })
        .await
    }

    /// 订阅多合约 K线（对齐）
    pub async fn kline_multi(
        &self,
        symbols: &[String],
        duration: StdDuration,
        view_width: usize,
    ) -> Result<Arc<SeriesSubscription>> {
        if symbols.is_empty() {
            return Err(TqError::InvalidParameter("symbols 为空".to_string()));
        }

        self.subscribe(SeriesOptions {
            symbols: symbols.to_vec(),
            duration: duration.as_nanos() as i64,
            view_width,
            chart_id: String::new(),
            left_kline_id: None,
            focus_datetime: None,
            focus_position: None,
        })
        .await
    }

    /// 订阅 Tick 数据
    pub async fn tick(&self, symbol: &str, view_width: usize) -> Result<Arc<SeriesSubscription>> {
        self.subscribe(SeriesOptions {
            symbols: vec![symbol.to_string()],
            duration: 0, // 0 表示 Tick
            view_width,
            chart_id: String::new(),
            left_kline_id: None,
            focus_datetime: None,
            focus_position: None,
        })
        .await
    }

    /// 订阅历史 K线（使用 left_kline_id）
    pub async fn kline_history(
        &self,
        symbol: &str,
        duration: StdDuration,
        view_width: usize,
        left_kline_id: i64,
    ) -> Result<Arc<SeriesSubscription>> {
        self.subscribe(SeriesOptions {
            symbols: vec![symbol.to_string()],
            duration: duration.as_nanos() as i64,
            view_width,
            chart_id: String::new(),
            left_kline_id: Some(left_kline_id),
            focus_datetime: None,
            focus_position: None,
        })
        .await
    }

    /// 订阅历史 K线（使用 focus_datetime）
    pub async fn kline_history_with_focus(
        &self,
        symbol: &str,
        duration: StdDuration,
        view_width: usize,
        focus_time: DateTime<Utc>,
        focus_position: i32,
    ) -> Result<Arc<SeriesSubscription>> {
        self.subscribe(SeriesOptions {
            symbols: vec![symbol.to_string()],
            duration: duration.as_nanos() as i64,
            view_width,
            chart_id: String::new(),
            left_kline_id: None,
            focus_datetime: Some(focus_time),
            focus_position: Some(focus_position),
        })
        .await
    }

    /// 通用订阅方法
    async fn subscribe(&self, mut options: SeriesOptions) -> Result<Arc<SeriesSubscription>> {
        if options.symbols.is_empty() {
            return Err(TqError::InvalidParameter("symbols 为空".to_string()));
        }
        {
            // 权限校验
            let auth = self.auth.read().await;
            let symbol_refs: Vec<&str> = options.symbols.iter().map(|s| s.as_str()).collect();
            auth.has_md_grants(&symbol_refs)?;
        }

        // 生成 chart_id
        if options.chart_id.is_empty() {
            options.chart_id = generate_chart_id(&options);
        }

        // 检查是否已存在
        {
            let subs = self.subscriptions.read().await;
            if let Some(sub) = subs.get(&options.chart_id) {
                return Ok(Arc::clone(sub));
            }
        }

        // 创建新订阅
        let sub = Arc::new(SeriesSubscription::new(
            Arc::clone(&self.dm),
            Arc::clone(&self.ws),
            options,
        )?);

        // // 先启动监听（注册数据更新回调）
        // sub.start().await?;

        // // 再发送 set_chart 请求（避免错过初始数据）
        // sub.send_set_chart().await?;

        // 保存订阅
        let mut subs = self.subscriptions.write().await;
        subs.insert(sub.options.chart_id.clone(), Arc::clone(&sub));

        Ok(sub)
    }
}

/// Series 订阅选项
#[derive(Debug, Clone)]
pub struct SeriesOptions {
    pub symbols: Vec<String>,
    pub duration: i64,
    pub view_width: usize,
    pub chart_id: String,
    pub left_kline_id: Option<i64>,
    pub focus_datetime: Option<DateTime<Utc>>,
    pub focus_position: Option<i32>,
}

/// 生成 chart_id
fn generate_chart_id(options: &SeriesOptions) -> String {
    let uid = Uuid::new_v4();
    if options.duration == 0 {
        format!("TQRS_tick_{}", uid)
    } else {
        format!("TQRS_kline_{}", uid)
    }
}

/// Series 订阅
pub struct SeriesSubscription {
    dm: Arc<DataManager>,
    ws: Arc<TqQuoteWebsocket>,
    options: SeriesOptions,

    // 状态跟踪
    last_ids: Arc<RwLock<HashMap<String, i64>>>,
    last_left_id: Arc<RwLock<i64>>,
    last_right_id: Arc<RwLock<i64>>,
    chart_ready: Arc<RwLock<bool>>,
    has_chart_sync: Arc<RwLock<bool>>,

    // 回调（使用 Arc 避免数据克隆）
    on_update: Arc<RwLock<Option<Arc<dyn Fn(Arc<SeriesData>, Arc<UpdateInfo>) + Send + Sync>>>>,
    on_new_bar: Arc<RwLock<Option<Arc<dyn Fn(Arc<SeriesData>) + Send + Sync>>>>,
    on_bar_update: Arc<RwLock<Option<Arc<dyn Fn(Arc<SeriesData>) + Send + Sync>>>>,
    on_error: Arc<RwLock<Option<Arc<dyn Fn(Arc<String>) + Send + Sync>>>>,

    running: Arc<RwLock<bool>>,
}

impl SeriesSubscription {
    /// 创建订阅
    fn new(
        dm: Arc<DataManager>,
        ws: Arc<TqQuoteWebsocket>,
        options: SeriesOptions,
    ) -> Result<Self> {
        let mut last_ids = HashMap::new();
        for symbol in &options.symbols {
            last_ids.insert(symbol.clone(), -1);
        }

        Ok(SeriesSubscription {
            dm,
            ws,
            options,
            last_ids: Arc::new(RwLock::new(last_ids)),
            last_left_id: Arc::new(RwLock::new(-1)),
            last_right_id: Arc::new(RwLock::new(-1)),
            chart_ready: Arc::new(RwLock::new(false)),
            has_chart_sync: Arc::new(RwLock::new(false)),
            on_update: Arc::new(RwLock::new(None)),
            on_new_bar: Arc::new(RwLock::new(None)),
            on_bar_update: Arc::new(RwLock::new(None)),
            on_error: Arc::new(RwLock::new(None)),
            running: Arc::new(RwLock::new(false)),
        })
    }

    /// 发送 set_chart 请求
    async fn send_set_chart(&self) -> Result<()> {
        let view_width = if self.options.view_width > 10000 {
            warn!("ViewWidth 超过最大限制，调整为 10000");
            10000
        } else {
            self.options.view_width
        };

        let mut chart_req = serde_json::json!({
            "aid": "set_chart",
            "chart_id": self.options.chart_id,
            "ins_list": self.options.symbols.join(","),
            "duration": self.options.duration,
            "view_width": view_width
        });

        // 添加历史数据参数
        if let Some(left_kline_id) = self.options.left_kline_id {
            chart_req["left_kline_id"] = serde_json::json!(left_kline_id);
        } else if let (Some(focus_datetime), Some(focus_position)) =
            (self.options.focus_datetime, self.options.focus_position)
        {
            chart_req["focus_datetime"] = serde_json::json!(focus_datetime.timestamp_nanos_opt());
            chart_req["focus_position"] = serde_json::json!(focus_position);
        }

        debug!(
            "发送 set_chart 请求: chart_id={}, symbols={:?}, view_width={}",
            self.options.chart_id, self.options.symbols, view_width
        );

        self.ws.send(&chart_req).await?;
        Ok(())
    }

    /// 启动监听
    pub async fn start(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if *running {
            return Ok(());
        }
        *running = true;
        drop(running);

        info!("启动 Series 订阅: {}", self.options.chart_id);

        self.start_watching().await;
        self.send_set_chart().await?;
        trace!("send_set_chart done");
        Ok(())
    }

    /// 启动监听数据更新
    async fn start_watching(&self) {
        let dm_clone = Arc::clone(&self.dm);
        let options = self.options.clone();
        let last_ids = Arc::clone(&self.last_ids);
        let last_left_id = Arc::clone(&self.last_left_id);
        let last_right_id = Arc::clone(&self.last_right_id);
        let chart_ready = Arc::clone(&self.chart_ready);
        let has_chart_sync = Arc::clone(&self.has_chart_sync);

        let on_update = Arc::clone(&self.on_update);
        let on_new_bar = Arc::clone(&self.on_new_bar);
        let on_bar_update = Arc::clone(&self.on_bar_update);
        let on_error = Arc::clone(&self.on_error);
        let running = Arc::clone(&self.running);

        // 注册数据更新回调
        let dm_for_callback = Arc::clone(&dm_clone);
        dm_clone.on_data(move || {
            let dm = Arc::clone(&dm_for_callback);
            let options = options.clone();
            let last_ids = Arc::clone(&last_ids);
            let last_left_id = Arc::clone(&last_left_id);
            let last_right_id = Arc::clone(&last_right_id);
            let chart_ready = Arc::clone(&chart_ready);
            let has_chart_sync = Arc::clone(&has_chart_sync);
            let on_update = Arc::clone(&on_update);
            let on_new_bar = Arc::clone(&on_new_bar);
            let on_bar_update = Arc::clone(&on_bar_update);
            let on_error = Arc::clone(&on_error);
            let running = Arc::clone(&running);

            tokio::spawn(async move {
                let is_running = *running.read().await;
                if !is_running {
                    return;
                }

                let chart_id = &options.chart_id;
                if dm.get_by_path(&["charts", chart_id]).is_none() {
                    return;
                }

                // 处理更新
                match process_series_update(
                    &dm,
                    &options,
                    &last_ids,
                    &last_left_id,
                    &last_right_id,
                    &chart_ready,
                    &has_chart_sync,
                )
                .await
                {
                    Ok((series_data, update_info)) => {
                        // 包装为 Arc（零拷贝共享）
                        let series_data = Arc::new(series_data);
                        let update_info = Arc::new(update_info);

                        // 调用回调
                        if update_info.has_chart_sync {
                            if update_info.chart_ready {
                                if let Some(callback) = on_update.read().await.as_ref() {
                                    let cb = Arc::clone(callback);
                                    let sd = Arc::clone(&series_data);
                                    let ui = Arc::clone(&update_info);
                                    tokio::spawn(async move {
                                        cb(sd, ui);
                                    });
                                }
                            }

                            if update_info.has_new_bar && update_info.chart_ready {
                                if let Some(callback) = on_new_bar.read().await.as_ref() {
                                    let cb = Arc::clone(callback);
                                    let sd = Arc::clone(&series_data);
                                    tokio::spawn(async move {
                                        cb(sd);
                                    });
                                }
                            }

                            if update_info.has_bar_update && update_info.chart_ready {
                                if let Some(callback) = on_bar_update.read().await.as_ref() {
                                    let cb = Arc::clone(callback);
                                    let sd = Arc::clone(&series_data);
                                    tokio::spawn(async move {
                                        cb(sd);
                                    });
                                }
                            }
                        }
                    }
                    Err(e) => {
                        warn!("处理 Series 更新失败: {}", e);
                        if let Some(callback) = on_error.read().await.as_ref() {
                            let cb = Arc::clone(callback);
                            let err_msg = Arc::new(e.to_string());
                            tokio::spawn(async move {
                                cb(err_msg);
                            });
                        }
                    }
                }
            });
        });
    }

    /// 注册更新回调
    pub async fn on_update<F>(&self, handler: F)
    where
        F: Fn(Arc<SeriesData>, Arc<UpdateInfo>) + Send + Sync + 'static,
    {
        let mut guard = self.on_update.write().await;
        *guard = Some(Arc::new(handler));
    }

    /// 注册新 K线回调
    pub async fn on_new_bar<F>(&self, handler: F)
    where
        F: Fn(Arc<SeriesData>) + Send + Sync + 'static,
    {
        let mut guard = self.on_new_bar.write().await;
        *guard = Some(Arc::new(handler));
    }

    /// 注册 K线更新回调
    pub async fn on_bar_update<F>(&self, handler: F)
    where
        F: Fn(Arc<SeriesData>) + Send + Sync + 'static,
    {
        let mut guard = self.on_bar_update.write().await;
        *guard = Some(Arc::new(handler));
    }

    /// 注册错误回调
    pub async fn on_error<F>(&self, handler: F)
    where
        F: Fn(Arc<String>) + Send + Sync + 'static,
    {
        let mut guard = self.on_error.write().await;
        *guard = Some(Arc::new(handler));
    }

    /// 获取数据流
    pub async fn data_stream(&self) -> impl Stream<Item = Arc<SeriesData>> {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();

        // 注册回调
        self.on_update(move |data, _info| {
            let _ = tx.send(data);
        })
        .await;

        stream! {
            while let Some(data) = rx.recv().await {
                yield data;
            }
        }
    }

    /// 关闭订阅
    pub async fn close(&self) -> Result<()> {
        let mut running = self.running.write().await;
        if !*running {
            return Ok(());
        }
        *running = false;

        info!("关闭 Series 订阅: {}", self.options.chart_id);
        Ok(())
    }
}

/// 处理 Series 更新
async fn process_series_update(
    dm: &DataManager,
    options: &SeriesOptions,
    last_ids: &Arc<RwLock<HashMap<String, i64>>>,
    last_left_id: &Arc<RwLock<i64>>,
    last_right_id: &Arc<RwLock<i64>>,
    chart_ready: &Arc<RwLock<bool>>,
    has_chart_sync: &Arc<RwLock<bool>>,
) -> Result<(SeriesData, UpdateInfo)> {
    let is_multi = options.symbols.len() > 1;
    let is_tick = options.duration == 0;

    // let has_chart_changed = dm.is_changing(&["charts", &options.chart_id]);
    // let has_data_changed = e
    let duration_str = options.duration.to_string();
    // 构建数据路径和图表路径
    let (data_path, chart_path): (Vec<&str>, Vec<&str>) = if is_tick {
        let data_path = vec!["ticks", &options.symbols[0]];
        let chart_path = vec!["charts", &options.chart_id];
        (data_path, chart_path)
    } else {
        let data_path = vec!["klines", &options.symbols[0], &duration_str];
        let chart_path = vec!["charts", &options.chart_id];
        (data_path, chart_path)
    };
    let has_chart_changed = dm.is_changing(&chart_path);
    let has_data_changed = dm.is_changing(&data_path);

    if !has_chart_changed && !has_data_changed {
        return Err(TqError::Other("数据未更新".to_string()));
    }

    // 获取数据
    let series_data: SeriesData = if is_tick {
        get_tick_data(dm, options).await?
    } else if is_multi {
        get_multi_kline_data(dm, options).await?
    } else {
        get_single_kline_data(dm, options).await?
    };

    // 检测更新信息
    let mut update_info = UpdateInfo {
        has_new_bar: false,
        has_bar_update: false,
        chart_range_changed: false,
        has_chart_sync: false,
        chart_ready: false,
        new_bar_ids: HashMap::new(),
        old_left_id: 0,
        old_right_id: 0,
        new_left_id: 0,
        new_right_id: 0,
    };
    // 检测新 K线
    detect_new_bars(dm, &series_data, last_ids, &mut update_info).await;

    // 检测 Chart 范围变化
    detect_chart_range_change(
        dm,
        &series_data,
        last_left_id,
        last_right_id,
        chart_ready,
        has_chart_sync,
        &mut update_info,
    )
    .await;

    Ok((series_data, update_info))
}

/// 获取单合约 K线数据
async fn get_single_kline_data(dm: &DataManager, options: &SeriesOptions) -> Result<SeriesData> {
    let symbol = &options.symbols[0];

    // 获取 Chart 信息 - 直接从 JSON 转换为 ChartInfo
    let mut right_id = -1i64;
    let chart_info = dm
        .get_by_path(&["charts", &options.chart_id])
        .and_then(|chart_data| dm.convert_to_struct::<ChartInfo>(&chart_data).ok())
        .map(|mut chart| {
            right_id = chart.right_id;
            chart.chart_id = options.chart_id.clone();
            chart.view_width = options.view_width;
            chart
        });
    let mut kline_data =
        dm.get_klines_data(symbol, options.duration, options.view_width, right_id)?;

    // 设置 Chart 信息
    kline_data.chart_id = options.chart_id.clone();
    kline_data.chart = chart_info;

    Ok(SeriesData {
        is_multi: false,
        is_tick: false,
        symbols: vec![symbol.clone()],
        single: Some(kline_data),
        multi: None,
        tick_data: None,
    })
}

/// 获取多合约 K线数据
async fn get_multi_kline_data(dm: &DataManager, options: &SeriesOptions) -> Result<SeriesData> {
    let multi_data = dm.get_multi_klines_data(
        &options.symbols,
        options.duration,
        &options.chart_id,
        options.view_width,
    )?;

    Ok(SeriesData {
        is_multi: true,
        is_tick: false,
        symbols: options.symbols.clone(),
        single: None,
        multi: Some(multi_data),
        tick_data: None,
    })
}

/// 获取 Tick 数据
async fn get_tick_data(dm: &DataManager, options: &SeriesOptions) -> Result<SeriesData> {
    let symbol = &options.symbols[0];

    // 获取 Chart 信息 - 直接从 JSON 转换为 ChartInfo
    let mut right_id = -1i64;
    let chart_info = dm
        .get_by_path(&["charts", &options.chart_id])
        .and_then(|chart_data| dm.convert_to_struct::<ChartInfo>(&chart_data).ok())
        .map(|mut chart| {
            right_id = chart.right_id;
            chart.chart_id = options.chart_id.clone();
            chart.view_width = options.view_width;
            chart
        });

    let mut tick_data = dm.get_ticks_data(symbol, options.view_width, right_id)?;

    // 设置 Chart 信息
    tick_data.chart_id = options.chart_id.clone();
    tick_data.chart = chart_info;

    Ok(SeriesData {
        is_multi: false,
        is_tick: true,
        symbols: vec![symbol.clone()],
        single: None,
        multi: None,
        tick_data: Some(tick_data),
    })
}

/// 检测新 K线
async fn detect_new_bars(
    dm: &DataManager,
    data: &SeriesData,
    last_ids: &Arc<RwLock<HashMap<String, i64>>>,
    info: &mut UpdateInfo,
) {
    let mut ids = last_ids.write().await;

    // 获取 duration 字符串（用于后续检查 K线更新）
    let duration_str = if data.is_tick {
        String::new()
    } else if data.is_multi {
        data.multi
            .as_ref()
            .map(|m| m.duration.to_string())
            .unwrap_or_default()
    } else {
        data.single
            .as_ref()
            .map(|s| s.duration.to_string())
            .unwrap_or_default()
    };

    for symbol in &data.symbols {
        let current_id = if data.is_tick {
            data.tick_data.as_ref().map(|t| t.last_id).unwrap_or(-1)
        } else if data.is_multi {
            data.multi
                .as_ref()
                .and_then(|m| m.metadata.get(symbol))
                .map(|meta| meta.last_id)
                .unwrap_or(-1)
        } else {
            data.single.as_ref().map(|s| s.last_id).unwrap_or(-1)
        };
        let last_id = ids.get(symbol).copied().unwrap_or(-1);
        trace!("current_id = {}, last_id = {}", current_id, last_id);
        if current_id > last_id {
            info.has_new_bar = true;
            info.has_bar_update = true;
            info.new_bar_ids.insert(symbol.clone(), current_id);
        }
        ids.insert(symbol.clone(), current_id);
    }
    if !info.has_new_bar && !data.is_tick {
        for symbol in &data.symbols {
            if dm.is_changing(&["klines", symbol, &duration_str]) {
                info.has_bar_update = true;
                break;
            }
        }
    }
}

/// 检测 Chart 范围变化
async fn detect_chart_range_change(
    dm: &DataManager,
    data: &SeriesData,
    last_left_id: &Arc<RwLock<i64>>,
    last_right_id: &Arc<RwLock<i64>>,
    chart_ready: &Arc<RwLock<bool>>,
    has_chart_sync: &Arc<RwLock<bool>>,
    info: &mut UpdateInfo,
) {
    let chart: Option<&ChartInfo> = if let Some(single) = &data.single {
        single.chart.as_ref()
    } else if let Some(tick) = &data.tick_data {
        tick.chart.as_ref()
    } else {
        None
    };

    let multi_chart = if let Some(multi) = &data.multi {
        if let Some(chart_data) = dm.get_by_path(&["charts", &multi.chart_id]) {
            dm.convert_to_struct::<ChartInfo>(&chart_data)
                .ok()
                .map(|mut c| {
                    c.chart_id = multi.chart_id.clone();
                    c.view_width = multi.view_width;
                    c
                })
        } else {
            None
        }
    } else {
        None
    };

    if let Some(chart) = chart.or(multi_chart.as_ref()) {
        let mut last_left = last_left_id.write().await;
        let mut last_right = last_right_id.write().await;
        let mut ready = chart_ready.write().await;
        let mut has_sync = has_chart_sync.write().await;

        trace!("before compare -> last_left: {}, last_right: {}, chart.left_id: {}, chart.right_id: {}, ready: {}, chart.ready: {}, has_sync: {}",
            *last_left, *last_right, chart.left_id, chart.right_id, *ready, chart.ready, *has_sync,
        );
        if chart.left_id != *last_left || chart.right_id != *last_right {
            info.chart_range_changed = true;
            info.old_left_id = *last_left;
            info.old_right_id = *last_right;
            info.new_left_id = chart.left_id;
            info.new_right_id = chart.right_id;

            *last_left = chart.left_id;
            *last_right = chart.right_id;
        }

        if chart.ready && !*ready {
            // 首次完成同步
            *ready = true;
            *has_sync = true;

            info.has_chart_sync = true;
            info.has_bar_update = true;
            info.has_new_bar = true;
        }

        if chart.ready && !chart.more_data {
            info.chart_ready = true;
        }
        trace!("after compare -> last_left: {}, last_right: {}, chart.left_id: {}, chart.right_id: {}, ready: {}, chart.ready: {}, has_sync: {}",
            *last_left, *last_right, chart.left_id, chart.right_id, *ready, chart.ready,  *has_sync,
        );
        info.has_chart_sync = *has_sync
    }
}
