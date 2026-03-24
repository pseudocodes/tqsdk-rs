//! 交易会话实现
//!
//! 实现实盘交易功能

use super::datamanager::DataManager;
use super::errors::{Result, TqError};
use super::types::{
    Account, InsertOrderRequest, Notification, Order, Position, PositionUpdate, Trade,
};
use super::websocket::{TqTradeWebsocket, WebSocketConfig};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use async_channel::{Sender, Receiver, unbounded};
use tokio::sync::RwLock;
use tracing::{debug, error, info};

/// 交易会话
#[allow(unused)]
pub struct TradeSession {
    broker: String,
    user_id: String,
    password: String,
    dm: Arc<DataManager>,
    ws: Arc<TqTradeWebsocket>,

    // Channels（使用 async-channel）
    account_tx: Sender<Account>,
    account_rx: Receiver<Account>,
    position_tx: Sender<PositionUpdate>,
    position_rx: Receiver<PositionUpdate>,
    order_tx: Sender<Order>,
    order_rx: Receiver<Order>,
    trade_tx: Sender<Trade>,
    trade_rx: Receiver<Trade>,
    notification_tx: Sender<Notification>,
    notification_rx: Receiver<Notification>,

    // 回调
    on_account: Arc<RwLock<Option<Arc<dyn Fn(Account) + Send + Sync>>>>,
    on_position: Arc<RwLock<Option<Arc<dyn Fn(String, Position) + Send + Sync>>>>,
    on_order: Arc<RwLock<Option<Arc<dyn Fn(Order) + Send + Sync>>>>,
    on_trade: Arc<RwLock<Option<Arc<dyn Fn(Trade) + Send + Sync>>>>,
    on_notification: Arc<RwLock<Option<Arc<dyn Fn(Notification) + Send + Sync>>>>,
    on_error: Arc<RwLock<Option<Arc<dyn Fn(String) + Send + Sync>>>>,

    // 状态（使用 Arc<AtomicBool> 避免锁开销，支持跨线程共享）
    logged_in: Arc<AtomicBool>,
    running: Arc<AtomicBool>,
}

impl TradeSession {
    /// 创建交易会话
    pub fn new(
        broker: String,
        user_id: String,
        password: String,
        dm: Arc<DataManager>,
        ws_url: String,
        ws_config: WebSocketConfig,
    ) -> Self {
        // 创建 async-channel channels（使用 unbounded）
        let (account_tx, account_rx) = unbounded();
        let (position_tx, position_rx) = unbounded();
        let (order_tx, order_rx) = unbounded();
        let (trade_tx, trade_rx) = unbounded();
        let (notification_tx, notification_rx) = unbounded();

        let ws = Arc::new(TqTradeWebsocket::new(ws_url, Arc::clone(&dm), ws_config));

        let noti_tx = notification_tx.clone();
        ws.on_notify(move |noti| {
            let noti2 = noti.clone();
            let noti_tx2 = noti_tx.clone();
            tokio::spawn(async move {
                match noti_tx2.send(noti2).await {
                    Ok(_) => {
                        debug!("通知发送成功: {:?}", noti);
                    }
                    Err(e) => {
                        error!("通知发送失败: {:?}, error={}", noti, e);
                    }
                }
            });
        });

        TradeSession {
            broker,
            user_id,
            password,
            dm,
            ws,
            account_tx,
            account_rx,
            position_tx,
            position_rx,
            order_tx,
            order_rx,
            trade_tx,
            trade_rx,
            notification_tx,
            notification_rx,
            on_account: Arc::new(RwLock::new(None)),
            on_position: Arc::new(RwLock::new(None)),
            on_order: Arc::new(RwLock::new(None)),
            on_trade: Arc::new(RwLock::new(None)),
            on_notification: Arc::new(RwLock::new(None)),
            on_error: Arc::new(RwLock::new(None)),
            logged_in: Arc::new(AtomicBool::new(false)),
            running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// 发送登录请求
    async fn send_login(&self) -> Result<()> {
        let login_req = serde_json::json!({
            "aid": "req_login",
            "bid": self.broker,
            "user_name": self.user_id,
            "password": self.password
        });

        info!(
            "发送交易登录请求: broker={}, user_id={}",
            self.broker, self.user_id
        );
        self.ws.send(&login_req).await?;
        Ok(())
    }

    /// 启动数据监听
    async fn start_watching(&self) {
        let dm_clone = Arc::clone(&self.dm);
        let user_id = self.user_id.clone();
        let logged_in = Arc::clone(&self.logged_in);
        let running = Arc::clone(&self.running);
        let account_tx = self.account_tx.clone();
        let position_tx = self.position_tx.clone();
        let order_tx = self.order_tx.clone();
        let trade_tx = self.trade_tx.clone();
        let on_account = Arc::clone(&self.on_account);
        let on_position = Arc::clone(&self.on_position);
        let on_order = Arc::clone(&self.on_order);
        let on_trade = Arc::clone(&self.on_trade);

        info!("TradeSession 开始监听数据更新");

        // 注册数据更新回调
        let dm_for_callback = Arc::clone(&dm_clone);

        // 使用 Mutex 防止多个任务并发访问数据，避免竞态条件
        // 这确保了 is_changing() 和数据处理是串行的
        let processing_lock = Arc::new(tokio::sync::Mutex::new(()));

        dm_clone.on_data(move || {
            let dm = Arc::clone(&dm_for_callback);
            let user_id = user_id.clone();
            let logged_in = Arc::clone(&logged_in);
            let running = Arc::clone(&running);
            let account_tx = account_tx.clone();
            let position_tx = position_tx.clone();
            let order_tx = order_tx.clone();
            let trade_tx = trade_tx.clone();
            let on_account = Arc::clone(&on_account);
            let on_position = Arc::clone(&on_position);
            let on_order = Arc::clone(&on_order);
            let on_trade = Arc::clone(&on_trade);
            let lock = Arc::clone(&processing_lock);

            tokio::spawn(async move {
                // 获取锁，确保同一时刻只有一个任务在处理数据
                let _guard = lock.lock().await;

                if !running.load(Ordering::SeqCst) {
                    return;
                }

                // 检查登录状态
                if let Some(session_data) = dm.get_by_path(&["trade", &user_id, "session"]) {
                    if let serde_json::Value::Object(session_map) = session_data {
                        if let Some(trading_day) = session_map.get("trading_day") {
                            if !trading_day.is_null() {
                                // 使用 compare_exchange 实现原子的 check-and-set
                                // 只有当 logged_in 从 false 变为 true 时才打印日志
                                if logged_in
                                    .compare_exchange(
                                        false,
                                        true,
                                        Ordering::SeqCst,
                                        Ordering::SeqCst,
                                    )
                                    .is_ok()
                                {
                                    info!("交易会话已登录: user_id={}", user_id);
                                }
                            }
                        }
                    }
                }

                // 检查账户更新
                if dm.is_changing(&["trade", &user_id, "accounts", "CNY"]) {
                    match dm.get_account_data(&user_id, "CNY") {
                        Ok(account) => {
                            debug!("账户更新: balance={}", account.balance);
                            let _ = account_tx.send(account.clone()).await;

                            if let Some(callback) = on_account.read().await.as_ref() {
                                let cb = Arc::clone(callback);
                                tokio::spawn(async move {
                                    cb(account);
                                });
                            }
                        }
                        Err(e) => {
                            // 理论上不应该到这里，因为 is_changing 已经验证了数据存在
                            error!("获取账户数据失败（这不应该发生）: {}", e);
                        }
                    }
                }

                // 检查持仓更新
                if dm.is_changing(&["trade", &user_id, "positions"]) {
                    Self::process_position_update(&dm, &user_id, &position_tx, &on_position).await;
                }

                // 检查委托单更新
                if dm.is_changing(&["trade", &user_id, "orders"]) {
                    Self::process_order_update(&dm, &user_id, &order_tx, &on_order).await;
                }

                // 检查成交更新
                if dm.is_changing(&["trade", &user_id, "trades"]) {
                    Self::process_trade_update(&dm, &user_id, &trade_tx, &on_trade).await;
                }
            });
        });
    }

    /// 处理持仓更新
    async fn process_position_update(
        dm: &Arc<DataManager>,
        user_id: &str,
        position_tx: &Sender<PositionUpdate>,
        on_position: &Arc<RwLock<Option<Arc<dyn Fn(String, Position) + Send + Sync>>>>,
    ) {
        if let Some(positions_data) = dm.get_by_path(&["trade", user_id, "positions"]) {
            if let serde_json::Value::Object(positions_map) = positions_data {
                for (symbol, _) in positions_map.iter() {
                    // 跳过内部元数据字段（以 _ 开头）
                    if symbol.starts_with('_') {
                        continue;
                    }

                    // 检查单个持仓是否有更新
                    if dm.is_changing(&["trade", user_id, "positions", symbol]) {
                        match dm.get_position_data(user_id, symbol) {
                            Ok(position) => {
                                debug!("持仓更新: symbol={}", symbol);

                                let update = PositionUpdate {
                                    symbol: symbol.clone(),
                                    position: position.clone(),
                                };

                                // 发送到 async-channel
                                let _ = position_tx.send(update).await;

                                // 调用回调
                                if let Some(callback) = on_position.read().await.as_ref() {
                                    let cb = Arc::clone(callback);
                                    let symbol = symbol.clone();
                                    tokio::spawn(async move {
                                        cb(symbol, position);
                                    });
                                }
                            }
                            Err(e) => {
                                error!("获取持仓数据失败: symbol={}, error={}", symbol, e);
                            }
                        }
                    }
                }
            }
        }
    }

    /// 处理委托单更新
    async fn process_order_update(
        dm: &Arc<DataManager>,
        user_id: &str,
        order_tx: &Sender<Order>,
        on_order: &Arc<RwLock<Option<Arc<dyn Fn(Order) + Send + Sync>>>>,
    ) {
        if let Some(orders_data) = dm.get_by_path(&["trade", user_id, "orders"]) {
            if let serde_json::Value::Object(orders_map) = orders_data {
                for (order_id, order_data) in orders_map.iter() {
                    // 跳过内部元数据字段（以 _ 开头）
                    if order_id.starts_with('_') {
                        continue;
                    }

                    // 检查单个订单是否有更新
                    if dm.is_changing(&["trade", user_id, "orders", order_id]) {
                        if let Ok(order) = serde_json::from_value::<Order>(order_data.clone()) {
                            debug!("订单更新: order_id={}", order_id);

                            // 发送到 async-channel
                            let _ = order_tx.send(order.clone()).await;

                            // 调用回调
                            if let Some(callback) = on_order.read().await.as_ref() {
                                let cb = Arc::clone(callback);
                                tokio::spawn(async move {
                                    cb(order);
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    /// 处理成交更新
    async fn process_trade_update(
        dm: &Arc<DataManager>,
        user_id: &str,
        trade_tx: &Sender<Trade>,
        on_trade: &Arc<RwLock<Option<Arc<dyn Fn(Trade) + Send + Sync>>>>,
    ) {
        if let Some(trades_data) = dm.get_by_path(&["trade", user_id, "trades"]) {
            if let serde_json::Value::Object(trades_map) = trades_data {
                for (trade_id, trade_data) in trades_map.iter() {
                    // 跳过内部元数据字段（以 _ 开头）
                    if trade_id.starts_with('_') {
                        continue;
                    }

                    // 检查单个成交是否有更新
                    if dm.is_changing(&["trade", user_id, "trades", trade_id]) {
                        if let Ok(trade) = serde_json::from_value::<Trade>(trade_data.clone()) {
                            debug!("成交更新: trade_id={}", trade_id);

                            // 发送到 async-channel
                            let _ = trade_tx.send(trade.clone()).await;

                            // 调用回调
                            if let Some(callback) = on_trade.read().await.as_ref() {
                                let cb = Arc::clone(callback);
                                tokio::spawn(async move {
                                    cb(trade);
                                });
                            }
                        }
                    }
                }
            }
        }
    }

    /// 注册账户更新回调
    pub async fn on_account<F>(&self, handler: F)
    where
        F: Fn(Account) + Send + Sync + 'static,
    {
        let mut guard = self.on_account.write().await;
        *guard = Some(Arc::new(handler));
    }

    /// 注册持仓更新回调
    pub async fn on_position<F>(&self, handler: F)
    where
        F: Fn(String, Position) + Send + Sync + 'static,
    {
        let mut guard = self.on_position.write().await;
        *guard = Some(Arc::new(handler));
    }

    /// 注册委托单更新回调
    pub async fn on_order<F>(&self, handler: F)
    where
        F: Fn(Order) + Send + Sync + 'static,
    {
        let mut guard = self.on_order.write().await;
        *guard = Some(Arc::new(handler));
    }

    /// 注册成交记录回调
    pub async fn on_trade<F>(&self, handler: F)
    where
        F: Fn(Trade) + Send + Sync + 'static,
    {
        let mut guard = self.on_trade.write().await;
        *guard = Some(Arc::new(handler));
    }

    /// 注册通知回调
    pub async fn on_notification<F>(&self, handler: F)
    where
        F: Fn(Notification) + Send + Sync + 'static,
    {
        let mut guard = self.on_notification.write().await;
        *guard = Some(Arc::new(handler));
    }

    /// 注册错误回调
    pub async fn on_error<F>(&self, handler: F)
    where
        F: Fn(String) + Send + Sync + 'static,
    {
        let mut guard = self.on_error.write().await;
        *guard = Some(Arc::new(handler));
    }

    /// 获取账户更新 Channel（克隆接收端）
    pub fn account_channel(&self) -> Receiver<Account> {
        self.account_rx.clone()
    }

    /// 获取持仓更新 Channel（克隆接收端）
    pub fn position_channel(&self) -> Receiver<PositionUpdate> {
        self.position_rx.clone()
    }

    /// 获取委托单更新 Channel（克隆接收端）
    pub fn order_channel(&self) -> Receiver<Order> {
        self.order_rx.clone()
    }

    /// 获取成交记录 Channel（克隆接收端）
    pub fn trade_channel(&self) -> Receiver<Trade> {
        self.trade_rx.clone()
    }

    /// 获取通知 Channel（克隆接收端）
    pub fn notification_channel(&self) -> Receiver<Notification> {
        self.notification_rx.clone()
    }
}

impl TradeSession {
    /// 下单
    pub async fn insert_order(&self, req: &InsertOrderRequest) -> Result<Order> {
        if !self.is_ready() {
            return Err(TqError::InternalError("交易会话未就绪".to_string()));
        }

        let exchange_id = req.get_exchange_id();
        let instrument_id = req.get_instrument_id();

        // 生成订单 ID
        use uuid::Uuid;
        let order_id = format!(
            "TQRS_{}",
            Uuid::new_v4().simple().to_string()[..8].to_uppercase()
        );

        // 确定时间条件
        let time_condition = if req.price_type == "ANY" {
            "IOC"
        } else {
            "GFD"
        };

        // 发送下单请求
        let order_req = serde_json::json!({
            "aid": "insert_order",
            "user_id": self.user_id,
            "order_id": order_id,
            "exchange_id": exchange_id,
            "instrument_id": instrument_id,
            "direction": req.direction,
            "offset": req.offset,
            "volume": req.volume,
            "price_type": req.price_type,
            "limit_price": req.limit_price,
            "volume_condition": "ANY",
            "time_condition": time_condition,
        });

        info!("发送下单请求: order_id={}, symbol={}", order_id, req.symbol);
        self.ws.send(&order_req).await?;

        // 初始化订单状态到 DataManager
        let order_init = serde_json::json!({
            "user_id": self.user_id,
            "order_id": order_id,
            "exchange_id": exchange_id,
            "instrument_id": instrument_id,
            "direction": req.direction,
            "offset": req.offset,
            "volume_orign": req.volume,
            "volume_left": req.volume,
            "price_type": req.price_type,
            "limit_price": req.limit_price,
            "status": "ALIVE",
        });

        self.dm.merge_data(
            serde_json::json!({
                "trade": {
                    self.user_id.clone(): {
                        "orders": {
                            order_id.clone(): order_init
                        }
                    }
                }
            }),
            false,
            false,
        );

        // 返回初始订单对象
        Ok(Order {
            order_id: order_id.clone(),
            exchange_id: exchange_id.to_string(),
            instrument_id: instrument_id.to_string(),
            direction: req.direction.clone(),
            offset: req.offset.clone(),
            volume_orign: req.volume,
            volume_left: req.volume,
            limit_price: req.limit_price,
            price_type: req.price_type.clone(),
            volume_condition: "ANY".to_string(),
            time_condition: time_condition.to_string(),
            insert_date_time: 0,
            status: "ALIVE".to_string(),
            frozen_margin: 0f64,
            epoch: None,
            seqno: 0,
            user_id: self.user_id.clone(),
            exchange_order_id: String::default(),
            last_msg: String::default(),
        })
    }

    /// 撤单
    pub async fn cancel_order(&self, order_id: &str) -> Result<()> {
        if !self.is_ready() {
            return Err(TqError::InternalError("交易会话未就绪".to_string()));
        }

        // 发送撤单请求
        let cancel_req = serde_json::json!({
            "aid": "cancel_order",
            "user_id": self.user_id,
            "order_id": order_id
        });

        info!("发送撤单请求: order_id={}", order_id);
        self.ws.send(&cancel_req).await?;
        Ok(())
    }

    /// 获取账户信息
    pub async fn get_account(&self) -> Result<Account> {
        self.dm.get_account_data(&self.user_id, "CNY")
    }

    /// 获取指定合约的持仓
    pub async fn get_position(&self, symbol: &str) -> Result<Position> {
        self.dm.get_position_data(&self.user_id, symbol)
    }

    /// 获取所有持仓
    pub async fn get_positions(&self) -> Result<HashMap<String, Position>> {
        let data = self.dm.get_by_path(&["trade", &self.user_id, "positions"]);
        if data.is_none() {
            return Ok(HashMap::new());
        }

        let positions_map = match data.unwrap() {
            serde_json::Value::Object(map) => map,
            _ => return Ok(HashMap::new()),
        };

        let mut positions = HashMap::new();
        for (symbol, pos_data) in positions_map.iter() {
            // 跳过内部元数据字段（以 _ 开头）
            if symbol.starts_with('_') {
                continue;
            }

            if let Ok(position) = serde_json::from_value::<Position>(pos_data.clone()) {
                positions.insert(symbol.clone(), position);
            }
        }

        Ok(positions)
    }

    /// 获取所有委托单
    pub async fn get_orders(&self) -> Result<HashMap<String, Order>> {
        let data = self.dm.get_by_path(&["trade", &self.user_id, "orders"]);
        if data.is_none() {
            return Ok(HashMap::new());
        }

        let orders_map = match data.unwrap() {
            serde_json::Value::Object(map) => map,
            _ => return Ok(HashMap::new()),
        };

        let mut orders = HashMap::new();
        for (order_id, order_data) in orders_map.iter() {
            // 跳过内部元数据字段（以 _ 开头）
            if order_id.starts_with('_') {
                continue;
            }

            if let Ok(order) = serde_json::from_value::<Order>(order_data.clone()) {
                orders.insert(order_id.clone(), order);
            }
        }

        Ok(orders)
    }

    /// 获取所有成交记录
    pub async fn get_trades(&self) -> Result<HashMap<String, Trade>> {
        let data = self.dm.get_by_path(&["trade", &self.user_id, "trades"]);
        if data.is_none() {
            return Ok(HashMap::new());
        }

        let trades_map = match data.unwrap() {
            serde_json::Value::Object(map) => map,
            _ => return Ok(HashMap::new()),
        };

        let mut trades = HashMap::new();
        for (trade_id, trade_data) in trades_map.iter() {
            // 跳过内部元数据字段（以 _ 开头）
            if trade_id.starts_with('_') {
                continue;
            }

            if let Ok(trade) = serde_json::from_value::<Trade>(trade_data.clone()) {
                trades.insert(trade_id.clone(), trade);
            }
        }

        Ok(trades)
    }

    /// 连接交易服务器
    pub async fn connect(&self) -> Result<()> {
        // 使用 compare_exchange 确保只连接一次
        if self
            .running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(()); // 已经在运行
        }

        info!(
            "连接交易服务器: broker={}, user_id={}",
            self.broker, self.user_id
        );

        // 初始化 WebSocket
        self.ws.init(false).await?;

        // 先启动数据监听（注册数据更新回调）
        self.start_watching().await;

        // 再发送登录请求（避免错过初始数据）
        self.send_login().await?;

        Ok(())
    }

    /// 检查交易会话是否已就绪
    pub fn is_ready(&self) -> bool {
        // 简化实现：检查是否已登录（原子操作，无需异步）
        // TODO: 添加更详细的就绪检查
        self.logged_in.load(Ordering::SeqCst) && self.ws.is_ready()
    }

    /// 关闭交易会话
    pub async fn close(&self) -> Result<()> {
        // 使用 compare_exchange 确保只关闭一次
        if self
            .running
            .compare_exchange(true, false, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
        {
            return Ok(()); // 已经关闭
        }

        info!("关闭交易会话");
        self.ws.close().await?;
        Ok(())
    }
}
