//! 客户端模块
//!
//! 统一的客户端入口

use super::auth::{Authenticator, TqAuth};
use super::datamanager::{DataManager, DataManagerConfig};
use super::errors::{Result, TqError};
use super::quote::QuoteSubscription;
use super::series::SeriesAPI;
use super::trade_session::TradeSession;
use super::websocket::{TqQuoteWebsocket, WebSocketConfig};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// 客户端配置
#[derive(Debug, Clone)]
pub struct ClientConfig {
    /// 日志级别
    pub log_level: String,
    /// 默认视图宽度
    pub view_width: usize,
    /// 开发模式
    pub development: bool,
}

impl Default for ClientConfig {
    fn default() -> Self {
        ClientConfig {
            log_level: "info".to_string(),
            view_width: 10000,
            development: false,
        }
    }
}

/// 客户端选项
pub type ClientOption = Box<dyn Fn(&mut ClientConfig)>;

/// 客户端构建器
pub struct ClientBuilder {
    username: String,
    password: String,
    config: ClientConfig,
    auth: Option<Arc<RwLock<dyn Authenticator>>>,
}

impl ClientBuilder {
    /// 创建新的客户端构建器
    ///
    /// # 参数
    ///
    /// * `username` - 用户名
    /// * `password` - 密码
    ///
    /// # 示例
    ///
    /// ```no_run
    /// # use tqsdk_rs::*;
    /// # async fn example() -> Result<()> {
    /// let client = ClientBuilder::new("username", "password")
    ///     .log_level("debug")
    ///     .view_width(5000)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(username: impl Into<String>, password: impl Into<String>) -> Self {
        ClientBuilder {
            username: username.into(),
            password: password.into(),
            config: ClientConfig::default(),
            auth: None,
        }
    }

    /// 设置日志级别
    pub fn log_level(mut self, level: impl Into<String>) -> Self {
        self.config.log_level = level.into();
        self
    }

    /// 设置默认视图宽度
    pub fn view_width(mut self, width: usize) -> Self {
        self.config.view_width = width;
        self
    }

    /// 设置开发模式
    pub fn development(mut self, dev: bool) -> Self {
        self.config.development = dev;
        self
    }

    /// 设置完整配置
    pub fn config(mut self, config: ClientConfig) -> Self {
        self.config = config;
        self
    }

    /// 使用自定义认证器（高级用法）
    ///
    /// 如果不设置，将使用默认的 TqAuth
    ///
    /// # 示例
    ///
    /// ```no_run
    /// # use tqsdk_rs::*;
    /// # use tqsdk_rs::auth::TqAuth;
    /// # async fn example() -> Result<()> {
    /// let mut auth = TqAuth::new("username".to_string(), "password".to_string());
    /// auth.login().await?;
    ///
    /// let client = Client::builder("username", "password")
    ///     .auth(auth)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn auth<A: Authenticator + 'static>(mut self, auth: A) -> Self {
        self.auth = Some(Arc::new(RwLock::new(auth)));
        self
    }

    /// 构建客户端
    ///
    /// # 错误
    ///
    /// 如果认证失败，返回错误
    pub async fn build(self) -> Result<Client> {
        // 初始化日志
        super::logger::init_logger(&self.config.log_level, true);

        // 创建或使用提供的认证器
        let auth: Arc<RwLock<dyn Authenticator>> = if let Some(custom_auth) = self.auth {
            custom_auth
        } else {
            let mut auth = TqAuth::new(self.username.clone(), self.password.clone());
            auth.login().await?;
            Arc::new(RwLock::new(auth))
        };

        // 创建数据管理器
        let dm_config = DataManagerConfig {
            default_view_width: self.config.view_width,
            enable_auto_cleanup: true,
        };
        let initial_data = HashMap::new();
        let dm = Arc::new(DataManager::new(initial_data, dm_config));

        Ok(Client {
            _username: self.username,
            _config: self.config,
            auth,
            dm,
            quotes_ws: None,
            series_api: None,
            trade_sessions: Arc::new(RwLock::new(HashMap::new())),
        })
    }
}

/// 客户端
pub struct Client {
    _username: String,
    _config: ClientConfig,
    auth: Arc<RwLock<dyn Authenticator>>,
    dm: Arc<DataManager>,
    quotes_ws: Option<Arc<TqQuoteWebsocket>>,
    series_api: Option<Arc<SeriesAPI>>,
    trade_sessions: Arc<RwLock<HashMap<String, Arc<TradeSession>>>>,
}

impl Client {
    /// 创建新的客户端（使用默认配置）
    ///
    /// 这是一个便捷方法，等同于：
    /// ```no_run
    /// # use tqsdk_rs::*;
    /// # async fn example() -> Result<()> {
    /// let client = ClientBuilder::new("username", "password")
    ///     .config(ClientConfig::default())
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// 如需更多配置选项，请使用 `ClientBuilder`
    pub async fn new(username: &str, password: &str, config: ClientConfig) -> Result<Self> {
        ClientBuilder::new(username, password)
            .config(config)
            .build()
            .await
    }

    /// 创建客户端构建器
    ///
    /// # 示例
    ///
    /// ```no_run
    /// # use tqsdk_rs::*;
    /// # async fn example() -> Result<()> {
    /// let client = Client::builder("username", "password")
    ///     .log_level("debug")
    ///     .view_width(5000)
    ///     .development(true)
    ///     .build()
    ///     .await?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn builder(username: impl Into<String>, password: impl Into<String>) -> ClientBuilder {
        ClientBuilder::new(username, password)
    }

    /// 初始化行情功能
    pub async fn init_market(&mut self) -> Result<()> {
        let auth = self.auth.read().await;
        let md_url = auth.get_md_url(false, false).await?;

        let mut ws_config = WebSocketConfig::default();
        ws_config.headers = auth.base_header();

        let quotes_ws = Arc::new(TqQuoteWebsocket::new(
            md_url,
            Arc::clone(&self.dm),
            ws_config,
        ));

        quotes_ws.init(false).await?;

        self.quotes_ws = Some(Arc::clone(&quotes_ws));

        // 创建 SeriesAPI（传入 auth）
        let series_api = Arc::new(SeriesAPI::new(
            Arc::clone(&self.dm),
            quotes_ws,
            Arc::clone(&self.auth),
        ));
        self.series_api = Some(series_api);

        Ok(())
    }

    /// 设置认证器
    ///
    /// 允许在运行时更换认证器，例如切换账号或更新 token
    ///
    /// # 注意
    ///
    /// - 更换认证器后，需要重新调用 `init_market()` 来使用新的认证信息
    /// - 已创建的 `SeriesAPI` 和 `TradeSession` 仍使用旧的认证器
    ///
    /// # 示例
    ///
    /// ```no_run
    /// # use tqsdk_rs::*;
    /// # use tqsdk_rs::auth::TqAuth;
    /// # async fn example() -> Result<()> {
    /// let mut client = Client::new("user1", "pass1", ClientConfig::default()).await?;
    ///
    /// // 切换到另一个账号
    /// let mut new_auth = TqAuth::new("user2".to_string(), "pass2".to_string());
    /// new_auth.login().await?;
    /// client.set_auth(new_auth).await;
    ///
    /// // 重新初始化行情功能
    /// client.init_market().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn set_auth<A: Authenticator + 'static>(&mut self, auth: A) {
        self.auth = Arc::new(RwLock::new(auth));
    }

    /// 获取认证器的只读引用
    ///
    /// 用于检查当前的认证状态或权限
    ///
    /// # 示例
    ///
    /// ```no_run
    /// # use tqsdk_rs::*;
    /// # async fn example(client: &Client) -> Result<()> {
    /// let auth = client.get_auth().await;
    /// if auth.has_feature("futr") {
    ///     println!("有期货权限");
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn get_auth(&self) -> tokio::sync::RwLockReadGuard<'_, dyn Authenticator> {
        self.auth.read().await
    }

    /// 获取 Series API
    pub fn series(&self) -> Result<Arc<SeriesAPI>> {
        self.series_api
            .clone()
            .ok_or_else(|| TqError::InternalError("Series API 未初始化".to_string()))
    }

    /// 订阅 Quote
    pub async fn subscribe_quote(&self, symbols: &[&str]) -> Result<Arc<QuoteSubscription>> {
        if self.quotes_ws.is_none() {
            return Err(TqError::InternalError(
                "行情 WebSocket 未初始化".to_string(),
            ));
        }
        {
            let auth = self.auth.read().await;
            auth.has_md_grants(symbols)? 
        }
        let symbol_list: Vec<String> = symbols.iter().map(|s| s.to_string()).collect();
        let qs = Arc::new(QuoteSubscription::new(
            Arc::clone(&self.dm),
            self.quotes_ws.as_ref().unwrap().clone(),
            symbol_list,
        ));

        // 启动订阅
        qs.start().await?;

        Ok(qs)
    }

    /// 创建交易会话（不自动连接）
    ///
    /// # 重要提示
    ///
    /// 由于 TradeSession 使用 broadcast 队列，建议按以下顺序使用：
    ///
    /// ```no_run
    /// # use tqsdk_rs::*;
    /// # async fn example() -> Result<()> {
    /// # let client = Client::new("user", "pass", ClientConfig::default()).await?;
    /// // 1. 创建会话（不连接）
    /// let session = client.create_trade_session("simnow", "user_id", "password").await?;
    ///
    /// // 2. 先注册回调或订阅 channel（避免丢失消息）
    /// session.on_account(|account| {
    ///     println!("账户: {}", account.balance);
    /// }).await;
    ///
    /// // 3. 最后连接
    /// session.connect().await?;
    /// # Ok(())
    /// # }
    /// ```
    ///
    /// # 参数
    ///
    /// * `broker` - 期货公司代码（如 "simnow"）
    /// * `user_id` - 用户账号
    /// * `password` - 密码
    ///
    /// # 返回
    ///
    /// 返回 `TradeSession` 实例，需要手动调用 `connect()` 连接
    pub async fn create_trade_session(
        &self,
        broker: &str,
        user_id: &str,
        password: &str,
    ) -> Result<Arc<TradeSession>> {
        // 获取交易服务器地址
        let auth = self.auth.read().await;
        let broker_info = auth.get_td_url(broker, user_id).await?;

        let mut ws_config = WebSocketConfig::default();
        ws_config.headers = auth.base_header();
        drop(auth);

        // 创建交易会话（不自动连接）
        let session = Arc::new(TradeSession::new(
            broker.to_string(),
            user_id.to_string(),
            password.to_string(),
            Arc::clone(&self.dm),
            broker_info.url,
            ws_config,
        ));

        // 保存会话
        let key = format!("{}:{}", broker, user_id);
        let mut sessions = self.trade_sessions.write().await;
        sessions.insert(key, Arc::clone(&session));

        Ok(session)
    }

    /// 注册交易会话
    pub async fn register_trade_session(&self, key: &str, session: Arc<TradeSession>) {
        let mut sessions = self.trade_sessions.write().await;
        sessions.insert(key.to_string(), session);
    }

    /// 获取交易会话
    pub async fn get_trade_session(&self, key: &str) -> Option<Arc<TradeSession>> {
        let sessions = self.trade_sessions.read().await;
        sessions.get(key).cloned()
    }

    /// 关闭客户端
    pub async fn close(&self) -> Result<()> {
        if let Some(ws) = &self.quotes_ws {
            ws.close().await?;
        }

        let sessions = self.trade_sessions.read().await;
        for (_key, trader) in sessions.iter() {
            trader.close().await?;
        }

        Ok(())
    }
}
