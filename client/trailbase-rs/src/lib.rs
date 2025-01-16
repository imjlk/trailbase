#![allow(clippy::needless_return)]

use parking_lot::RwLock;
use reqwest::header::{HeaderMap, HeaderValue};
use reqwest::Method;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct User {
  pub sub: String,
  pub email: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct Credentials {
  email: String,
  password: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RefreshTokenRequest {
  refresh_token: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct RefreshTokenResponse {
  auth_token: String,
  csrf_token: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct Tokens {
  pub auth_token: String,
  pub refresh_token: Option<String>,
  pub csrf_token: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RecordIdResponse {
  pub id: String,
}

#[derive(Clone, Debug)]
struct TokenState {
  state: Option<(Tokens, JwtTokenClaims)>,
  headers: HeaderMap,
}

#[derive(Clone, Debug)]
pub struct Pagination {
  pub cursor: Option<String>,
  pub limit: Option<usize>,
}

impl TokenState {
  fn build(tokens: Option<&Tokens>) -> TokenState {
    let headers = build_headers(tokens);
    return TokenState {
      state: tokens.and_then(|tokens| {
        Some((
          tokens.clone(),
          decode_auth_token::<JwtTokenClaims>(&tokens.auth_token).unwrap(),
        ))
      }),
      headers,
    };
  }
}

struct ThinClient {
  client: reqwest::Client,
  site: String,
}

impl ThinClient {
  async fn fetch(
    &self,
    path: String,
    state: &TokenState,
    method: Method,
    body: Option<serde_json::Value>,
    query_params: Option<Vec<(String, String)>>,
  ) -> reqwest::Result<reqwest::Response> {
    // TODO: return error
    assert!(!path.starts_with("/"));

    let mut url = url::Url::parse(&format!("{}/{path}", self.site)).unwrap();

    if let Some(query_params) = query_params {
      for (key, value) in query_params {
        url.query_pairs_mut().append_pair(&key, &value);
      }
    }

    let mut builder = self
      .client
      .request(method, url)
      .headers(state.headers.clone());

    if let Some(body) = body {
      builder = builder.body(serde_json::to_string(&body).unwrap());
    }

    return self.client.execute(builder.build()?).await;
  }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
struct JwtTokenClaims {
  sub: String,
  iat: i64,
  exp: i64,
  email: String,
  csrf_token: String,
}

pub fn decode_auth_token<T: DeserializeOwned>(
  token: &str,
) -> Result<T, jsonwebtoken::errors::Error> {
  let decoding_key = jsonwebtoken::DecodingKey::from_secret(&[]);

  // Don't validate the token, we don't have the secret key. Just deserialize the claims/contents.
  let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::EdDSA);
  validation.insecure_disable_signature_validation();

  return jsonwebtoken::decode::<T>(token, &decoding_key, &validation).map(|data| data.claims);
}

pub struct RecordApi {
  client: Arc<ClientState>,
  name: String,
}

impl RecordApi {
  // TODO: missing
  pub async fn list<T: DeserializeOwned>(
    &self,
    pagination: Option<Pagination>,
    order: Option<&[&str]>,
    filters: Option<&[&str]>,
  ) -> Result<Vec<T>, reqwest::Error> {
    let mut params: Vec<(String, String)> = vec![];
    if let Some(pagination) = pagination {
      if let Some(cursor) = pagination.cursor {
        params.push(("cursor".to_string(), cursor));
      }

      if let Some(limit) = pagination.limit {
        params.push(("limit".to_string(), limit.to_string()));
      }
    }

    if let Some(order) = order {
      params.push(("order".to_string(), order.join(",")));
    }

    if let Some(filters) = filters {
      for filter in filters {
        let Some((name_op, value)) = filter.split_once("=") else {
          panic!("Filter '{filter}' does not match: 'name[op]=value'");
        };

        params.push((name_op.to_string(), value.to_string()));
      }
    }

    let response = self
      .client
      .fetch(
        format!("{RECORD_API}/{}", self.name),
        Method::GET,
        None,
        Some(params),
      )
      .await?;

    return response.json().await;
  }

  pub async fn read<T: DeserializeOwned>(&self, id: &str) -> Result<T, reqwest::Error> {
    let response = self
      .client
      .fetch(
        format!("{RECORD_API}/{}/{id}", self.name),
        Method::GET,
        None,
        None,
      )
      .await?;

    return Ok(response.json().await.unwrap());
  }

  pub async fn create<T: Serialize>(&self, record: T) -> Result<String, reqwest::Error> {
    let response = self
      .client
      .fetch(
        format!("{RECORD_API}/{}", self.name),
        Method::POST,
        Some(serde_json::to_value(record).unwrap()),
        None,
      )
      .await?;

    return Ok(response.json::<RecordIdResponse>().await.unwrap().id);
  }

  pub async fn delete(&self, id: &str) -> Result<(), reqwest::Error> {
    self
      .client
      .fetch(
        format!("{RECORD_API}/{}/{id}", self.name),
        Method::DELETE,
        None,
        None,
      )
      .await?;

    return Ok(());
  }
}

struct ClientState {
  client: ThinClient,
  site: String,
  token_state: RwLock<TokenState>,
}

impl ClientState {
  #[inline]
  async fn fetch(
    &self,
    url: String,
    method: Method,
    body: Option<serde_json::Value>,
    query_params: Option<Vec<(String, String)>>,
  ) -> reqwest::Result<reqwest::Response> {
    let token_state = &self.token_state;
    // TODO: Add auto token refresh
    // let refreshToken = _shouldRefresh(tokenState);
    // if (refreshToken != null) {
    //   tokenState = _tokenState = await _refreshTokensImpl(refreshToken);
    // }

    return self
      .client
      .fetch(url, &token_state.read(), method, body, query_params)
      .await;
  }
}

#[derive(Clone)]
pub struct Client {
  state: Arc<ClientState>,
}

impl Client {
  pub fn new(site: &str, tokens: Option<Tokens>) -> Client {
    return Client {
      state: Arc::new(ClientState {
        client: ThinClient {
          client: reqwest::Client::new(),
          site: site.to_string(),
        },
        site: site.to_string(),
        token_state: RwLock::new(TokenState::build(tokens.as_ref())),
      }),
    };
  }

  pub fn site(&self) -> String {
    return self.state.site.clone();
  }

  pub fn tokens(&self) -> Option<Tokens> {
    return self
      .state
      .token_state
      .read()
      .state
      .as_ref()
      .map(|x| x.0.clone());
  }

  pub fn user(&self) -> Option<User> {
    if let Some(state) = &self.state.token_state.read().state {
      return Some(User {
        sub: state.1.sub.clone(),
        email: state.1.email.clone(),
      });
    }
    return None;
  }

  pub fn records(&self, api_name: &str) -> RecordApi {
    return RecordApi {
      client: self.state.clone(),
      name: api_name.to_string(),
    };
  }

  pub async fn login(&self, email: &str, password: &str) -> reqwest::Result<Tokens> {
    let response = self
      .state
      .fetch(
        format!("{AUTH_API}/login"),
        Method::POST,
        Some(serde_json::json!({
          "email": email,
          "password": password,
        })),
        None,
      )
      .await;

    let tokens: Tokens = response.unwrap().json().await.unwrap();
    self.update_tokens(Some(&tokens));
    return Ok(tokens);
  }

  pub async fn logout(&self) {
    let refresh_token: Option<String> = self
      .state
      .token_state
      .read()
      .state
      .as_ref()
      .and_then(|s| s.0.refresh_token.clone());
    if let Some(refresh_token) = refresh_token {
      let _ = self
        .state
        .fetch(
          format!("{AUTH_API}/logout"),
          Method::POST,
          Some(serde_json::json!({"refresh_token": refresh_token})),
          None,
        )
        .await;
    } else {
      let _ = self
        .state
        .fetch(format!("{AUTH_API}/logout"), Method::GET, None, None)
        .await;
    }

    self.update_tokens(None);
  }

  fn update_tokens(&self, tokens: Option<&Tokens>) -> TokenState {
    let state = TokenState::build(tokens);

    *self.state.token_state.write() = state.clone();
    // _authChange?.call(this, state.state?.$1);

    if let Some(ref s) = state.state {
      let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
      if s.1.exp < now as i64 {
        log::warn!("Token expired");
      }
    }

    return state;
  }
}

fn build_headers(tokens: Option<&Tokens>) -> HeaderMap {
  let mut base = HeaderMap::new();
  base.insert("Content-Type", HeaderValue::from_static("application/json"));

  if let Some(tokens) = tokens {
    base.insert(
      "Authorization",
      HeaderValue::from_str(&format!("Bearer {}", tokens.auth_token)).unwrap(),
    );

    if let Some(ref refresh) = tokens.refresh_token {
      base.insert("Refresh-Token", HeaderValue::from_str(refresh).unwrap());
    }

    if let Some(ref csrf) = tokens.csrf_token {
      base.insert("CSRF-Token", HeaderValue::from_str(csrf).unwrap());
    }
  }

  return base;
}

const AUTH_API: &str = "api/auth/v1";
const RECORD_API: &str = "api/records/v1";

#[cfg(test)]
mod tests {
  use crate::Client;
  use serde_json::json;

  async fn connect() -> Client {
    let client = Client::new("http://127.0.0.1:4000", None);
    let _ = client.login("admin@localhost", "secret").await.unwrap();
    return client;
  }

  #[tokio::test]
  async fn login_test() {
    let client = connect().await;

    let tokens = client.tokens().unwrap();

    assert_ne!(tokens.auth_token, "");
    assert!(tokens.refresh_token.is_some());

    let user = client.user().unwrap();
    assert_eq!(user.email, "admin@localhost");

    client.logout().await;

    assert!(client.tokens().is_none());
  }

  #[tokio::test]
  async fn records_test() {
    let client = connect().await;
    let api = client.records("simple_strict_table");

    let now = std::time::SystemTime::now()
      .duration_since(std::time::UNIX_EPOCH)
      .unwrap()
      .as_secs();

    let messages = vec![
      format!("rust client test 0: =?&{now}"),
      format!("rust client test 1: =?&{now}"),
    ];

    let mut ids = vec![];
    for msg in messages.iter() {
      ids.push(api.create(json!({"text_not_null": msg})).await.unwrap());
    }

    {
      let filter = format!("text_not_null={}", messages[0]);
      let filters = vec![filter.as_str()];
      let records = api
        .list::<serde_json::Value>(None, None, Some(filters.as_slice()))
        .await
        .unwrap();

      assert_eq!(records.len(), 1);
    }
  }
}
