//! 认证模块
//!
//! 实现天勤账号认证和权限检查

use super::errors::{Result, TqError};
use async_trait::async_trait;
// JWT 相关导入（暂时不用）
// use jsonwebtoken::{decode, Algorithm, DecodingKey, Validation};
use reqwest::header::{HeaderMap, HeaderValue, ACCEPT, AUTHORIZATION, USER_AGENT};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::time::Duration;
use tracing::{debug, info};

/// 版本号
pub const VERSION: &str = "3.8.1";
/// 认证服务器地址
pub const TQ_AUTH_URL: &str = "https://auth.shinnytech.com";
/// 客户端 ID
pub const CLIENT_ID: &str = "shinny_tq";
/// 客户端密钥
pub const CLIENT_SECRET: &str = "be30b9f4-6862-488a-99ad-21bde0400081";

/// 认证器接口
#[async_trait]
pub trait Authenticator: Send + Sync {
    /// 获取包含认证信息的 HTTP Header
    fn base_header(&self) -> HeaderMap;

    /// 执行登录操作
    async fn login(&mut self) -> Result<()>;

    /// 获取指定期货公司的交易服务器地址
    async fn get_td_url(&self, broker_id: &str, account_id: &str) -> Result<BrokerInfo>;

    /// 获取行情服务器地址
    async fn get_md_url(&self, stock: bool, backtest: bool) -> Result<String>;

    /// 检查是否具有指定的功能权限
    fn has_feature(&self, feature: &str) -> bool;

    /// 检查是否有查看指定合约行情数据的权限
    fn has_md_grants(&self, symbols: &[&str]) -> Result<()>;

    /// 检查是否有交易指定合约的权限
    fn has_td_grants(&self, symbol: &str) -> Result<()>;

    /// 获取认证 ID
    fn get_auth_id(&self) -> &str;

    /// 获取访问令牌
    fn get_access_token(&self) -> &str;
}

/// 权限信息
#[derive(Debug, Clone, Default)]
pub struct Grants {
    /// 功能权限
    pub features: HashSet<String>,
    /// 账户权限
    pub accounts: HashSet<String>,
}

/// 认证响应
#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct AuthResponse {
    access_token: String,
    expires_in: i64,
    refresh_expires_in: i64,
    refresh_token: String,
    token_type: String,
    #[serde(rename = "not-before-policy")]
    not_before_policy: i32,
    session_state: String,
    scope: String,
}

/// Access Token Claims
#[derive(Debug, Serialize, Deserialize)]
struct AccessTokenClaims {
    jti: String,
    exp: i64,
    nbf: i64,
    iat: i64,
    iss: String,
    sub: String,
    typ: String,
    azp: String,
    auth_time: i64,
    session_state: String,
    acr: String,
    scope: String,
    grants: GrantsClaims,
    creation_time: i64,
    setname: bool,
    mobile: String,
    #[serde(rename = "mobileVerified")]
    mobile_verified: String,
    preferred_username: String,
    id: String,
    username: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct GrantsClaims {
    features: Vec<String>,
    otg_ids: String,
    expiry_date: String,
    accounts: Vec<String>,
}

/// 期货公司信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerInfo {
    pub category: Vec<String>,
    pub url: String,
    pub broker_type: Option<String>,
    pub smtype: Option<String>,
    pub smconfig: Option<String>,
    pub condition_type: Option<String>,
    pub condition_config: Option<String>,
}

/// 行情服务器 URL 响应
#[derive(Debug, Deserialize)]
struct MdUrlResponse {
    mdurl: String,
}

/// 天勤认证实现
pub struct TqAuth {
    username: String,
    password: String,
    auth_url: String,
    access_token: String,
    refresh_token: String,
    auth_id: String,
    grants: Grants,
}

impl TqAuth {
    /// 创建新的认证器
    pub fn new(username: String, password: String) -> Self {
        let auth_url = std::env::var("TQ_AUTH_URL").unwrap_or_else(|_| TQ_AUTH_URL.to_string());

        TqAuth {
            username,
            password,
            auth_url,
            access_token: String::new(),
            refresh_token: String::new(),
            auth_id: String::new(),
            grants: Grants::default(),
        }
    }

    /// 请求 Token
    async fn request_token(&mut self) -> Result<()> {
        let url = format!(
            "{}/auth/realms/shinnytech/protocol/openid-connect/token",
            self.auth_url
        );

        let params = [
            ("client_id", CLIENT_ID),
            ("client_secret", CLIENT_SECRET),
            ("username", &self.username),
            ("password", &self.password),
            ("grant_type", "password"),
        ];

        info!("正在请求认证 token...");

        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(30))
            .build()?;

        let response = client
            .post(&url)
            .form(&params)
            .header(USER_AGENT, format!("tqsdk-python {}", VERSION))
            .header(ACCEPT, "application/json")
            .send()
            .await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await?;
            return Err(TqError::AuthenticationError(format!(
                "认证失败 ({}): {}",
                status, body
            )));
        }

        let auth_resp: AuthResponse = response.json().await?;
        self.access_token = auth_resp.access_token;
        self.refresh_token = auth_resp.refresh_token;

        debug!("Token 获取成功");
        Ok(())
    }

    /// 解析 JWT Token
    fn parse_token(&mut self) -> Result<()> {
        // 解析 JWT（不验证签名，因为我们信任天勤服务器）
        use jsonwebtoken::dangerous::insecure_decode;
        // 这里我们使用不验证签名的方式解析
        let token_data = insecure_decode::<AccessTokenClaims>(&self.access_token)?;

        let claims = token_data.claims;
        self.auth_id = claims.sub;

        // 提取权限
        for feature in claims.grants.features {
            self.grants.features.insert(feature);
        }

        for account in claims.grants.accounts {
            self.grants.accounts.insert(account);
        }

        debug!(
            "权限解析完成: {} 个功能, {} 个账户",
            self.grants.features.len(),
            self.grants.accounts.len()
        );

        Ok(())
    }
}

#[async_trait]
impl Authenticator for TqAuth {
    fn base_header(&self) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(USER_AGENT, HeaderValue::from_static("tqsdk-python 3.8.1"));
        headers.insert(ACCEPT, HeaderValue::from_static("application/json"));
        if !self.access_token.is_empty() {
            if let Ok(value) = HeaderValue::from_str(&format!("Bearer {}", self.access_token)) {
                headers.insert(AUTHORIZATION, value);
            }
        }
        headers
    }

    async fn login(&mut self) -> Result<()> {
        self.request_token().await?;
        self.parse_token()?;
        info!("TqAuth 登录成功, User: {},  AuthId: {}", self.username, self.auth_id);
        Ok(())
    }

    async fn get_td_url(&self, broker_id: &str, account_id: &str) -> Result<BrokerInfo> {
        let url = format!("https://files.shinnytech.com/{}.json", broker_id);

        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(30))
            .build()?;

        let response = client
            .get(&url)
            .query(&[("account_id", account_id), ("auth", &self.username)])
            .headers(self.base_header())
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(TqError::ConfigError(format!(
                "不支持该期货公司: {}",
                broker_id
            )));
        }

        let broker_infos: std::collections::HashMap<String, BrokerInfo> = response.json().await?;

        broker_infos.get(broker_id).cloned().ok_or_else(|| {
            TqError::ConfigError(format!("该期货公司 {} 暂不支持 TqSdk 登录", broker_id))
        })
    }

    async fn get_md_url(&self, stock: bool, backtest: bool) -> Result<String> {
        let url = format!(
            "https://api.shinnytech.com/ns?stock={}&backtest={}",
            stock, backtest
        );

        let client = reqwest::Client::builder()
            .no_proxy()
            .timeout(Duration::from_secs(30))
            .build()?;

        let response = client.get(&url).headers(self.base_header()).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await?;
            return Err(TqError::NetworkError(format!(
                "获取行情服务器地址失败 ({}): {}",
                status, body
            )));
        }

        let md_url_resp: MdUrlResponse = response.json().await?;
        Ok(md_url_resp.mdurl)
    }

    fn has_feature(&self, feature: &str) -> bool {
        self.grants.features.contains(feature)
    }

    fn has_md_grants(&self, symbols: &[&str]) -> Result<()> {
        for symbol in symbols {
            let prefix = symbol.split('.').next().unwrap_or("");

            // 检查期货、现货、KQ、KQD 交易所
            if matches!(
                prefix,
                "CFFEX" | "SHFE" | "DCE" | "CZCE" | "INE" | "GFEX" | "SSWE" | "KQ" | "KQD"
            ) {
                if !self.has_feature("futr") {
                    return Err(TqError::permission_denied_futures());
                }
                continue;
            }

            // 检查股票交易所
            if prefix == "CSI" || matches!(prefix, "SSE" | "SZSE") {
                if !self.has_feature("sec") {
                    return Err(TqError::permission_denied_stocks());
                }
                continue;
            }

            // 检查限制指数
            if matches!(
                *symbol,
                "SSE.000016" | "SSE.000300" | "SSE.000905" | "SSE.000852"
            ) {
                if !self.has_feature("lmt_idx") {
                    return Err(TqError::PermissionDenied(format!(
                        "您的账户不支持查看 {} 的行情数据",
                        symbol
                    )));
                }
                continue;
            }

            // 未知交易所
            return Err(TqError::PermissionDenied(format!(
                "不支持的合约: {}",
                symbol
            )));
        }

        Ok(())
    }

    fn has_td_grants(&self, symbol: &str) -> Result<()> {
        let prefix = symbol.split('.').next().unwrap_or("");

        // 检查期货、现货、KQ、KQD 交易所
        if matches!(
            prefix,
            "CFFEX" | "SHFE" | "DCE" | "CZCE" | "INE" | "GFEX" | "SSWE" | "KQ" | "KQD"
        ) {
            if self.has_feature("futr") {
                return Ok(());
            }
            return Err(TqError::PermissionDenied(format!(
                "您的账户不支持交易 {}",
                symbol
            )));
        }

        // 检查股票交易所
        if prefix == "CSI" || matches!(prefix, "SSE" | "SZSE") {
            if self.has_feature("sec") {
                return Ok(());
            }
            return Err(TqError::PermissionDenied(format!(
                "您的账户不支持交易 {}",
                symbol
            )));
        }

        Err(TqError::PermissionDenied(format!(
            "不支持的合约: {}",
            symbol
        )))
    }

    fn get_auth_id(&self) -> &str {
        &self.auth_id
    }

    fn get_access_token(&self) -> &str {
        &self.access_token
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tq_auth_creation() {
        let auth = TqAuth::new("test_user".to_string(), "test_pass".to_string());
        assert_eq!(auth.username, "test_user");
        assert_eq!(auth.password, "test_pass");
    }
}
