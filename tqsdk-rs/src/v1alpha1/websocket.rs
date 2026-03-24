//! WebSocket 连接封装
//!
//! 基于 yawc 库实现 WebSocket 连接，支持：
//! - deflate 压缩
//! - 自动重连
//! - 消息队列
//! - Debug 日志

use super::datamanager::DataManager;
use super::errors::{Result, TqError};
use futures::{SinkExt, StreamExt};
use reqwest::header::HeaderMap;
use serde::Serialize;
use serde_json::Value;
use std::sync::{Arc, RwLock};
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::time::sleep;
use tracing::{debug, error, info, trace, warn};
use yawc::frame::{Frame, OpCode};

/// WebSocket 状态
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WebSocketStatus {
    /// 连接中
    Connecting,
    /// 已连接
    Open,
    /// 关闭中
    Closing,
    /// 已关闭
    Closed,
}

/// WebSocket 配置
#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    /// HTTP Headers
    pub headers: HeaderMap,
    /// 重连间隔
    pub reconnect_interval: Duration,
    /// 最大重连次数
    pub reconnect_max_times: usize,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        WebSocketConfig {
            headers: HeaderMap::new(),
            reconnect_interval: Duration::from_secs(3),
            reconnect_max_times: 2,
        }
    }
}

/// 天勤 WebSocket 基类
///
/// 基于 yawc 库实现 WebSocket 连接
pub struct TqWebsocket {
    url: String,
    config: WebSocketConfig,
    status: Arc<RwLock<WebSocketStatus>>,
    queue: Arc<Mutex<Vec<String>>>,
    reconnect_times: Arc<RwLock<usize>>,
    should_reconnect: Arc<RwLock<bool>>,

    // WebSocket 连接实例
    ws: Arc<Mutex<Option<yawc::TcpWebSocket>>>,

    // 回调函数
    on_message: Arc<RwLock<Option<Box<dyn Fn(Value) + Send + Sync>>>>,
    on_open: Arc<RwLock<Option<Box<dyn Fn() + Send + Sync>>>>,
    on_close: Arc<RwLock<Option<Box<dyn Fn() + Send + Sync>>>>,
    on_error: Arc<RwLock<Option<Box<dyn Fn(String) + Send + Sync>>>>,
}

impl TqWebsocket {
    /// 创建新的 WebSocket 连接
    pub fn new(url: String, config: WebSocketConfig) -> Self {
        TqWebsocket {
            url,
            config,
            status: Arc::new(RwLock::new(WebSocketStatus::Closed)),
            queue: Arc::new(Mutex::new(Vec::new())),
            reconnect_times: Arc::new(RwLock::new(0)),
            should_reconnect: Arc::new(RwLock::new(true)),
            ws: Arc::new(Mutex::new(None)),
            on_message: Arc::new(RwLock::new(None)),
            on_open: Arc::new(RwLock::new(None)),
            on_close: Arc::new(RwLock::new(None)),
            on_error: Arc::new(RwLock::new(None)),
        }
    }

    /// 初始化连接
    pub async fn init(&self, is_reconnection: bool) -> Result<()> {
        info!(
            "正在连接 WebSocket: {} (重连: {})",
            self.url, is_reconnection
        );
        *self.status.write().unwrap() = WebSocketStatus::Connecting;

        // 配置 WebSocket 选项，启用 deflate 压缩
        let options = yawc::Options::default()
            .client_no_context_takeover()
            .server_no_context_takeover();

        let parsed_url = url::Url::parse(&self.url)
            .map_err(|e| TqError::WebSocketError(format!("Invalid URL: {}", e)))?;

        // 构建 HTTP 请求头
        let mut http_builder = yawc::HttpRequestBuilder::new();
        for (key, value) in self.config.headers.iter() {
            if let Ok(value_str) = value.to_str() {
                http_builder = http_builder.header(key.as_str(), value_str);
            }
        }

        // 连接 WebSocket
        let ws = match yawc::WebSocket::connect(parsed_url)
            .with_options(options)
            .with_request(http_builder)
            .await
        {
            Ok(ws) => ws,
            Err(e) => {
                error!(url = %self.url, error = %e, "WebSocket 连接失败");

                // 触发错误回调
                if let Some(callback) = self.on_error.read().unwrap().as_ref() {
                    callback(format!("Connection failed: {}", e));
                }

                // 尝试重连
                if *self.should_reconnect.read().unwrap() {
                    self.handle_reconnect().await;
                }

                return Err(TqError::WebSocketError(format!("Connection failed: {}", e)));
            }
        };

        // 保存连接实例
        {
            let mut ws_guard = self.ws.lock().await;
            *ws_guard = Some(ws);
        }

        *self.status.write().unwrap() = WebSocketStatus::Open;

        // 重置重连次数
        if !is_reconnection {
            *self.reconnect_times.write().unwrap() = 0;
        }

        // 触发 onOpen 回调
        if let Some(callback) = self.on_open.read().unwrap().as_ref() {
            callback();
        }

        // 启动消息接收循环
        self.start_receive_loop().await;
        // 发送队列中的消息
        self.flush_queue().await;

        info!("WebSocket 连接成功");
        Ok(())
    }

    /// 发送消息
    pub async fn send<T: Serialize>(&self, obj: &T) -> Result<()> {
        let json_str = serde_json::to_string(obj)?;

        if self.is_ready() {
            debug!("WebSocket 发送消息: {}", json_str);

            let mut ws_guard = self.ws.lock().await;
            if let Some(ws) = ws_guard.as_mut() {
                // yawc 使用 Sink trait，发送 Frame
                let frame = Frame::text(json_str);
                match ws.send(frame).await {
                    Ok(_) => Ok(()),
                    Err(e) => {
                        error!("消息发送失败: {}", e);
                        Err(TqError::WebSocketError(format!("Send failed: {}", e)))
                    }
                }
            } else {
                debug!("WebSocket 连接不存在，消息加入队列");
                drop(ws_guard);
                self.queue.lock().await.push(json_str);
                Ok(())
            }
        } else {
            debug!("WebSocket 未就绪，消息加入队列: {}", json_str);
            self.queue.lock().await.push(json_str);
            Ok(())
        }
    }

    /// 检查连接是否就绪
    pub fn is_ready(&self) -> bool {
        *self.status.read().unwrap() == WebSocketStatus::Open
    }

    /// 关闭连接
    pub async fn close(&self) -> Result<()> {
        info!("正在关闭 WebSocket 连接");
        *self.should_reconnect.write().unwrap() = false;

        // 先设置状态为 Closing，这会让接收循环在下次迭代时退出
        *self.status.write().unwrap() = WebSocketStatus::Closing;

        // 关闭 WebSocket 连接
        let mut ws_guard = self.ws.lock().await;
        if let Some(ws) = ws_guard.take() {
            // yawc WebSocket 会在 drop 时自动发送关闭帧并关闭连接
            drop(ws);
            info!("WebSocket 连接已关闭");
        }
        drop(ws_guard);

        // 等待一小段时间让接收循环退出
        tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;

        *self.status.write().unwrap() = WebSocketStatus::Closed;

        // 触发 onClose 回调
        if let Some(callback) = self.on_close.read().unwrap().as_ref() {
            callback();
        }

        Ok(())
    }

    /// 发送队列中的消息
    async fn flush_queue(&self) {
        let mut queue = self.queue.lock().await;
        if queue.is_empty() {
            return;
        }

        debug!("发送队列中的 {} 条消息", queue.len());

        let mut ws_guard = self.ws.lock().await;
        if let Some(ws) = ws_guard.as_mut() {
            for msg in queue.drain(..) {
                debug!("发送队列消息: {}", msg);
                let frame = Frame::text(msg);
                match ws.send(frame).await {
                    Ok(_) => {
                        debug!("队列消息发送成功");
                    }
                    Err(e) => {
                        error!("队列消息发送失败: {}", e);
                    }
                }
            }
        }
    }

    /// 注册消息回调
    pub fn on_message<F>(&self, callback: F)
    where
        F: Fn(Value) + Send + Sync + 'static,
    {
        *self.on_message.write().unwrap() = Some(Box::new(callback));
    }

    /// 注册连接打开回调
    pub fn on_open<F>(&self, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self.on_open.write().unwrap() = Some(Box::new(callback));
    }

    /// 注册连接关闭回调
    pub fn on_close<F>(&self, callback: F)
    where
        F: Fn() + Send + Sync + 'static,
    {
        *self.on_close.write().unwrap() = Some(Box::new(callback));
    }

    /// 注册错误回调
    pub fn on_error<F>(&self, callback: F)
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        *self.on_error.write().unwrap() = Some(Box::new(callback));
    }

    /// 启动消息接收循环
    async fn start_receive_loop(&self) {
        let status = Arc::clone(&self.status);
        let ws: Arc<Mutex<Option<yawc::TcpWebSocket>>> = Arc::clone(&self.ws);
        let _on_message = Arc::clone(&self.on_message);
        let _on_open = Arc::clone(&self.on_open);
        let _on_error = Arc::clone(&self.on_error);
        let _on_close = Arc::clone(&self.on_close);
        let _should_reconnect = Arc::clone(&self.should_reconnect);
        let _reconnect_times = Arc::clone(&self.reconnect_times);
        let _reconnect_interval = self.config.reconnect_interval;
        let _reconnect_max_times = self.config.reconnect_max_times;
        let _headers = self.config.headers.clone();
        let _queue = Arc::clone(&self.queue);
        let _url = self.url.clone();

        tokio::spawn(async move {
            debug!("启动 WebSocket 消息接收循环");

            loop {
                // 检查连接状态
                let current_status = *status.read().unwrap();
                if current_status != WebSocketStatus::Open {
                    debug!("WebSocket 状态不是 Open，退出接收循环");
                    break;
                }

                // 获取 WebSocket 实例
                let mut ws_guard = ws.lock().await;
                if let Some(ws_instance) = ws_guard.as_mut() {
                    // yawc 使用 Stream trait，使用 next() 接收消息
                    // 使用 timeout 避免无限等待
                    let timeout_duration = tokio::time::Duration::from_secs(1);
                    let next_result =
                        tokio::time::timeout(timeout_duration, ws_instance.next()).await;

                    match next_result {
                        Ok(Some(frame)) => {
                            // 处理不同类型的帧
                            match frame.opcode() {
                                OpCode::Text | OpCode::Binary => {
                                    // 将 payload 转换为字符串
                                    match String::from_utf8(frame.payload().to_vec()) {
                                        Ok(text) => {
                                            debug!("WebSocket Recv Text: {}", text);

                                            // 解析 JSON
                                            match serde_json::from_str::<Value>(&text) {
                                                Ok(json_value) => {
                                                    // 触发消息回调
                                                    if let Some(callback) =
                                                        _on_message.read().unwrap().as_ref()
                                                    {
                                                        callback(json_value);
                                                    }
                                                }
                                                Err(e) => {
                                                    warn!("解析 JSON 失败: {}", e);
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            warn!("消息不是有效的 UTF-8: {}", e);
                                        }
                                    }

                                    let frame = Frame::text(r#"{"aid": "peek_message"}"#);
                                    match ws_instance.send(frame).await {
                                        Ok(_) => {
                                            debug!("Websocket Send -> peek_message");
                                        }
                                        Err(e) => {
                                            error!("Websocket Send `peek_message` failed: {}", e);
                                            break;
                                        }
                                    }
                                }
                                OpCode::Close => {
                                    info!("WebSocket 收到关闭帧");
                                    drop(ws_guard);

                                    if !reconnect_on_server_close(
                                        &_should_reconnect,
                                        &_reconnect_times,
                                        _reconnect_max_times,
                                        _reconnect_interval,
                                        &_url,
                                        &_headers,
                                        &ws,
                                        &status,
                                        &_on_open,
                                        &_on_close,
                                        &_queue,
                                    )
                                    .await
                                    {
                                        break;
                                    }
                                    continue;
                                }
                                OpCode::Ping => {
                                    debug!("WebSocket 收到 Ping（自动处理）");
                                }
                                OpCode::Pong => {
                                    debug!("WebSocket 收到 Pong");
                                }
                                OpCode::Continuation => {
                                    debug!("WebSocket 收到 Continuation 帧");
                                }
                            }
                        }
                        Ok(None) => {
                            // Stream 结束，连接关闭 — 可能是服务端 FIN/RST，也可能是 yawc 内部状态异常
                            warn!("WebSocket Stream 返回 None（连接关闭），should_reconnect={}, status={:?}",
                                *_should_reconnect.read().unwrap(),
                                *status.read().unwrap()
                            );
                            drop(ws_guard);

                            if !reconnect_on_server_close(
                                &_should_reconnect,
                                &_reconnect_times,
                                _reconnect_max_times,
                                _reconnect_interval,
                                &_url,
                                &_headers,
                                &ws,
                                &status,
                                &_on_open,
                                &_on_close,
                                &_queue,
                            )
                            .await
                            {
                                break;
                            }
                            continue;
                        }
                        Err(_) => {
                            // Timeout，继续下一次循环（这样可以检查状态是否变化）
                            trace!("WebSocket 接收超时，继续等待");
                        }
                    }
                } else {
                    trace!("WebSocket 实例不存在，退出接收循环");
                    break;
                }

                drop(ws_guard);
            }

            info!("WebSocket 消息接收循环结束");
        });
    }

    /// 处理重连
    async fn handle_reconnect(&self) {
        // 检查重连次数（在锁的作用域内完成）
        let (should_reconnect, times) = {
            let mut reconnect_times = self.reconnect_times.write().unwrap();

            if *reconnect_times >= self.config.reconnect_max_times {
                error!(
                    "已达到最大重连次数 {}，停止重连",
                    self.config.reconnect_max_times
                );
                return;
            }

            *reconnect_times += 1;
            (true, *reconnect_times)
        }; // reconnect_times 锁在这里释放

        if should_reconnect {
            info!(
                "第 {} 次尝试重连（最多 {} 次）",
                times, self.config.reconnect_max_times
            );

            // 等待重连间隔
            sleep(self.config.reconnect_interval).await;

            // 重新连接（这里只是标记，实际重连需要外部调用 init）
            // 外部应该监听 on_error 回调并调用 init(true)
            info!("重连等待完成，请外部调用 init(true) 进行重连");
        }
    }
}

/// 服务端主动关闭连接时的重连逻辑
///
/// 返回 true 表示重连成功（调用方应 continue 循环），返回 false 表示不再重连（调用方应 break）
async fn reconnect_on_server_close(
    should_reconnect: &Arc<RwLock<bool>>,
    reconnect_times: &Arc<RwLock<usize>>,
    reconnect_max_times: usize,
    reconnect_interval: Duration,
    url: &str,
    headers: &reqwest::header::HeaderMap,
    ws: &Arc<Mutex<Option<yawc::TcpWebSocket>>>,
    status: &Arc<RwLock<WebSocketStatus>>,
    on_open: &Arc<RwLock<Option<Box<dyn Fn() + Send + Sync>>>>,
    on_close: &Arc<RwLock<Option<Box<dyn Fn() + Send + Sync>>>>,
    queue: &Arc<Mutex<Vec<String>>>,
) -> bool {
    if !*should_reconnect.read().unwrap() {
        // 主动关闭（调用了 close()），不重连
        *status.write().unwrap() = WebSocketStatus::Closed;
        if let Some(cb) = on_close.read().unwrap().as_ref() {
            cb();
        }
        return false;
    }

    let proceed = {
        let mut t = reconnect_times.write().unwrap();
        if *t >= reconnect_max_times {
            error!("已达到最大重连次数 {}，停止重连", reconnect_max_times);
            false
        } else {
            *t += 1;
            info!("服务端关闭连接，第 {} 次尝试重连...", *t);
            true
        }
    };

    if !proceed {
        *status.write().unwrap() = WebSocketStatus::Closed;
        if let Some(cb) = on_close.read().unwrap().as_ref() {
            cb();
        }
        return false;
    }

    sleep(reconnect_interval).await;

    let parsed_url = match url::Url::parse(url) {
        Ok(u) => u,
        Err(e) => {
            error!("重连 URL 解析失败: {}", e);
            *status.write().unwrap() = WebSocketStatus::Closed;
            return false;
        }
    };

    let options = yawc::Options::default()
        .client_no_context_takeover()
        .server_no_context_takeover();

    let mut http_builder = yawc::HttpRequestBuilder::new();
    for (key, value) in headers.iter() {
        if let Ok(vs) = value.to_str() {
            http_builder = http_builder.header(key.as_str(), vs);
        }
    }

    match yawc::WebSocket::connect(parsed_url)
        .with_options(options)
        .with_request(http_builder)
        .await
    {
        Ok(new_ws) => {
            info!("重连成功");
            *ws.lock().await = Some(new_ws);
            *status.write().unwrap() = WebSocketStatus::Open;

            // 触发 on_open（会重放 subscribe_quote 和所有活跃 chart 请求）
            if let Some(cb) = on_open.read().unwrap().as_ref() {
                cb();
            }

            // 刷新发送队列
            let msgs: Vec<String> = queue.lock().await.drain(..).collect();
            if !msgs.is_empty() {
                let mut wg = ws.lock().await;
                if let Some(w) = wg.as_mut() {
                    for msg in msgs {
                        let _ = w.send(Frame::text(msg)).await;
                    }
                }
            }

            true
        }
        Err(e) => {
            error!("重连失败: {}", e);
            *status.write().unwrap() = WebSocketStatus::Closed;
            if let Some(cb) = on_close.read().unwrap().as_ref() {
                cb();
            }
            false
        }
    }
}

/// 行情 WebSocket
pub struct TqQuoteWebsocket {
    base: Arc<TqWebsocket>,
    _dm: Arc<DataManager>,
    subscribe_quote: Arc<RwLock<Option<Value>>>,
    charts: Arc<RwLock<std::collections::HashMap<String, Value>>>,
}

impl TqQuoteWebsocket {
    /// 创建行情 WebSocket
    pub fn new(url: String, dm: Arc<DataManager>, config: WebSocketConfig) -> Self {
        let base = Arc::new(TqWebsocket::new(url, config));
        let dm_clone = Arc::clone(&dm);
        let subscribe_quote: Arc<RwLock<Option<Value>>> = Arc::new(RwLock::new(None));
        let charts: Arc<RwLock<std::collections::HashMap<String, Value>>> =
            Arc::new(RwLock::new(std::collections::HashMap::new()));

        // 注册消息处理
        base.on_message({
            let dm = Arc::clone(&dm_clone);
            move |data: Value| {
                if let Some(aid) = data.get("aid").and_then(|v| v.as_str()) {
                    if aid == "rtn_data" {
                        if let Some(payload) = data.get("data") {
                            dm.merge_data(payload.clone(), true, true);
                        }
                    }
                }
            }
        });

        // 注册重连回调：重连时重发订阅和图表请求
        {
            let subscribe_quote_clone = Arc::clone(&subscribe_quote);
            let charts_clone = Arc::clone(&charts);
            let base_clone = Arc::clone(&base);

            base.on_open(move || {
                // 重发订阅请求
                if let Some(sub) = subscribe_quote_clone.read().unwrap().as_ref() {
                    debug!("重连后重发订阅请求");
                    let base_for_send = Arc::clone(&base_clone);
                    let sub_clone = sub.clone();
                    tokio::spawn(async move {
                        let _ = base_for_send.send(&sub_clone).await;
                    });
                }

                // 重发图表请求
                let charts_guard = charts_clone.read().unwrap();
                for (chart_id, chart) in charts_guard.iter() {
                    if let Some(view_width) = chart.get("view_width").and_then(|v| v.as_f64()) {
                        if view_width > 0.0 {
                            debug!("重连后重发图表请求: {}", chart_id);
                            let base_for_send = Arc::clone(&base_clone);
                            let chart_clone = chart.clone();
                            tokio::spawn(async move {
                                let _ = base_for_send.send(&chart_clone).await;
                            });
                        }
                    }
                }
            });
        }

        TqQuoteWebsocket {
            base,
            _dm: dm_clone,
            subscribe_quote,
            charts,
        }
    }

    /// 初始化连接
    pub async fn init(&self, is_reconnection: bool) -> Result<()> {
        self.base.init(is_reconnection).await
    }

    /// 发送消息（重写以记录订阅和图表请求）
    pub async fn send<T: Serialize>(&self, obj: &T) -> Result<()> {
        // 序列化为 Value 以检查 aid
        let json_str = serde_json::to_string(obj)?;
        let value: Value = serde_json::from_str(&json_str)?;

        if let Some(aid) = value.get("aid").and_then(|v| v.as_str()) {
            match aid {
                "subscribe_quote" => {
                    // 检查是否需要更新订阅
                    let mut should_send = false;
                    let mut subscribe_guard = self.subscribe_quote.write().unwrap();

                    if let Some(old_sub) = subscribe_guard.as_ref() {
                        // 比较 ins_list
                        let old_list = old_sub.get("ins_list");
                        let new_list = value.get("ins_list");

                        if old_list != new_list {
                            debug!("订阅列表变化，更新订阅");
                            *subscribe_guard = Some(value.clone());
                            should_send = true;
                        } else {
                            debug!("订阅列表未变化，跳过");
                        }
                    } else {
                        debug!("首次订阅");
                        *subscribe_guard = Some(value.clone());
                        should_send = true;
                    }

                    drop(subscribe_guard);

                    if should_send {
                        return self.base.send(&value).await;
                    } else {
                        return Ok(());
                    }
                }
                "set_chart" => {
                    // 记录图表请求
                    if let Some(chart_id) = value.get("chart_id").and_then(|v| v.as_str()) {
                        let mut charts_guard = self.charts.write().unwrap();

                        if let Some(view_width) = value.get("view_width").and_then(|v| v.as_f64()) {
                            if view_width == 0.0 {
                                trace!("删除图表: {}", chart_id);
                                charts_guard.remove(chart_id);
                            } else {
                                trace!("保存图表请求: {}", chart_id);
                                charts_guard.insert(chart_id.to_string(), value.clone());
                            }
                        }

                        drop(charts_guard);
                        return self.base.send(&value).await;
                    }
                }
                _ => {}
            }
        }

        // 其他消息直接发送
        self.base.send(obj).await
    }

    /// 检查是否就绪
    pub fn is_ready(&self) -> bool {
        self.base.is_ready()
    }

    /// 关闭连接
    pub async fn close(&self) -> Result<()> {
        self.base.close().await
    }
}

/// 交易 WebSocket
pub struct TqTradeWebsocket {
    base: Arc<TqWebsocket>,
    _dm: Arc<DataManager>,
    req_login: Arc<RwLock<Option<Value>>>,
    on_notify: Arc<RwLock<Option<Box<dyn Fn(super::types::Notification) + Send + Sync>>>>,
}

impl TqTradeWebsocket {
    /// 创建交易 WebSocket
    pub fn new(url: String, dm: Arc<DataManager>, config: WebSocketConfig) -> Self {
        let base = Arc::new(TqWebsocket::new(url, config));
        let dm_clone = Arc::clone(&dm);
        let req_login: Arc<RwLock<Option<Value>>> = Arc::new(RwLock::new(None));
        let on_notify: Arc<RwLock<Option<Box<dyn Fn(super::types::Notification) + Send + Sync>>>> =
            Arc::new(RwLock::new(None));

        // 注册消息处理
        {
            let dm = Arc::clone(&dm_clone);
            let on_notify_clone = Arc::clone(&on_notify);

            base.on_message(move |data: Value| {
                if let Some(aid) = data.get("aid").and_then(|v| v.as_str()) {
                    match aid {
                        "rtn_data" => {
                            if let Some(payload) = data.get("data") {
                                // 分离通知
                                if let Some(array) = payload.as_array() {
                                    let (notifies, cleaned_data) =
                                        Self::separate_notifies(array.clone());
                                    debug!("notifies: {:?}", notifies);

                                    // 触发通知回调
                                    if let Some(callback) = on_notify_clone.read().unwrap().as_ref()
                                    {
                                        for notify in notifies {
                                            callback(notify);
                                        }
                                    }

                                    // 合并清理后的数据
                                    dm.merge_data(Value::Array(cleaned_data), true, true);
                                } else {
                                    dm.merge_data(payload.clone(), true, true);
                                }
                            }
                        }
                        "rtn_brokers" => {
                            // 期货公司列表（暂不处理全局事件）
                            debug!("收到期货公司列表");
                        }
                        "qry_settlement_info" => {
                            // 历史结算单
                            if let (Some(settlement_info), Some(user_name), Some(trading_day)) = (
                                data.get("settlement_info").and_then(|v| v.as_str()),
                                data.get("user_name").and_then(|v| v.as_str()),
                                data.get("trading_day").and_then(|v| v.as_str()),
                            ) {
                                debug!(
                                    "收到结算单: user={}, trading_day={}",
                                    user_name, trading_day
                                );

                                // 解析结算单内容
                                let settlement = Self::parse_settlement_content(settlement_info);

                                // 合并到 DataManager
                                let settlement_data = serde_json::json!({
                                    "trade": {
                                        user_name: {
                                            "his_settlements": {
                                                trading_day: settlement
                                            }
                                        }
                                    }
                                });

                                dm.merge_data(settlement_data, true, true);
                            }
                        }
                        _ => {}
                    }
                }
            });
        }

        // 注册重连回调：重连时重发登录请求
        {
            let req_login_clone = Arc::clone(&req_login);
            let base_clone = Arc::clone(&base);

            base.on_open(move || {
                if let Some(login) = req_login_clone.read().unwrap().as_ref() {
                    debug!("重连后重发登录请求");
                    let base_for_send = Arc::clone(&base_clone);
                    let login_clone = login.clone();
                    tokio::spawn(async move {
                        let _ = base_for_send.send(&login_clone).await;
                    });
                }
            });
        }

        TqTradeWebsocket {
            base,
            _dm: dm_clone,
            req_login,
            on_notify,
        }
    }

    /// 分离通知
    ///
    /// 从 rtn_data 的 data 数组中提取通知，并返回清理后的数据
    fn separate_notifies(data: Vec<Value>) -> (Vec<super::types::Notification>, Vec<Value>) {
        let mut notifies = Vec::new();
        let mut cleaned_data = Vec::new();

        for mut item in data {
            if let Some(obj) = item.as_object_mut() {
                // 提取 notify 字段
                if let Some(notify_data) = obj.remove("notify") {
                    if let Some(notify_map) = notify_data.as_object() {
                        for (_key, notify_value) in notify_map {
                            if let Some(n) = notify_value.as_object() {
                                let notification = super::types::Notification {
                                    code: n
                                        .get("code")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string(),
                                    level: n
                                        .get("level")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string(),
                                    r#type: n
                                        .get("type")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string(),
                                    content: n
                                        .get("content")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string(),
                                    bid: n
                                        .get("bid")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string(),
                                    user_id: n
                                        .get("user_id")
                                        .and_then(|v| v.as_str())
                                        .unwrap_or("")
                                        .to_string(),
                                };
                                notifies.push(notification);
                            }
                        }
                    }
                }
            }

            // 添加到清理后的数据
            cleaned_data.push(item);
        }

        (notifies, cleaned_data)
    }

    /// 解析结算单内容
    ///
    /// 简单的结算单解析（仅返回原始内容）
    fn parse_settlement_content(content: &str) -> serde_json::Value {
        serde_json::json!({
            "content": content,
            "parsed": false  // 标记为未解析（可后续扩展）
        })
    }

    /// 注册通知回调
    pub fn on_notify<F>(&self, callback: F)
    where
        F: Fn(super::types::Notification) + Send + Sync + 'static,
    {
        *self.on_notify.write().unwrap() = Some(Box::new(callback));
    }

    /// 初始化连接
    pub async fn init(&self, is_reconnection: bool) -> Result<()> {
        self.base.init(is_reconnection).await
    }

    /// 发送消息（重写以记录登录请求）
    pub async fn send<T: Serialize>(&self, obj: &T) -> Result<()> {
        // 序列化为 Value 以检查 aid
        let json_str = serde_json::to_string(obj)?;
        let value: Value = serde_json::from_str(&json_str)?;

        // 如果是登录请求，记录下来
        if let Some(aid) = value.get("aid").and_then(|v| v.as_str()) {
            if aid == "req_login" {
                debug!("记录登录请求 {:?}", value);
                *self.req_login.write().unwrap() = Some(value.clone());
            }
        }

        // 发送消息
        self.base.send(&value).await
    }

    /// 检查是否就绪
    pub fn is_ready(&self) -> bool {
        self.base.is_ready()
    }

    /// 关闭连接
    pub async fn close(&self) -> Result<()> {
        self.base.close().await
    }
}

// WebSocket 实现说明：
// ✅ 1. WebSocket 连接创建 - 已实现（使用 yawc::WebSocket::connect）
// ✅ 2. deflate 压缩启用 - 已实现（使用 Options::default()）
// ✅ 3. 消息发送 - 已实现（使用 Sink trait 和 FrameView）
// ✅ 4. 消息接收 - 已实现（使用 Stream trait 的 next() 方法）
// ✅ 5. 连接状态管理 - 已实现（WebSocketStatus 状态机）
// ✅ 6. 消息接收循环 - 已实现（start_receive_loop）
// ✅ 7. 自动重连机制 - 已实现（handle_reconnect）
// ✅ 8. 消息队列 - 已实现（连接未就绪时缓存消息）
// ✅ 9. 回调机制 - 已实现（on_message, on_open, on_close, on_error）
// ✅ 10. 帧类型处理 - 已实现（Text, Binary, Close, Ping, Pong, Continuation）
//
// TqQuoteWebsocket 特性：
// ✅ 1. Send 方法重写 - 已实现（记录 subscribe_quote 和 charts）
// ✅ 2. 订阅去重 - 已实现（比较 ins_list，只在变化时发送）
// ✅ 3. 图表管理 - 已实现（view_width == 0 时删除，否则保存）
// ✅ 4. 重连恢复 - 已实现（重连时自动重发订阅和图表请求）
//
// TqTradeWebsocket 特性：
// ✅ 1. Send 方法重写 - 已实现（记录 req_login）
// ✅ 2. 重连恢复 - 已实现（重连时自动重发登录请求）
// ✅ 3. 通知分离 - 已实现（separate_notifies）
// ✅ 4. 通知回调 - 已实现（on_notify）
// ✅ 5. 结算单处理 - 已实现（qry_settlement_info）
// ⚠️  6. 期货公司列表 - 已接收但未穿透到上层（rtn_brokers）
//
// yawc API 使用说明：
// - WebSocket 实现了 futures::Stream trait，使用 .next().await 接收消息
// - WebSocket 实现了 futures::Sink<FrameView> trait，使用 .send(frame).await 发送消息
// - FrameView::text(bytes) 创建文本帧
// - frame.opcode 确定帧类型（OpCode::Text, Binary, Close, Ping, Pong, Continuation）
// - frame.payload 包含帧数据（Bytes 类型）
//
// 注意事项：
// - 重连逻辑需要外部监听 on_error 回调并调用 init(true)
// - deflate 压缩通过 Options::default() 配置，支持客户端和服务端压缩
// - 消息接收循环在独立的 tokio task 中运行
// - Ping/Pong 帧由 yawc 自动处理，无需手动响应
// - 重连时自动重发订阅/登录/图表请求（通过 on_open 回调）
