//! Debug middleware for logging HTTP requests and responses

use std::sync::Arc;
use std::time::{Duration, Instant};

use http::Extensions;
use reqwest::{Request, Response};
use reqwest_middleware::{Middleware, Next, Result};

use super::auth::AuthManager;

/// The backend's last `429 rate_limit_exceeded` and when it lets us back in
/// (its `Retry-After`, delay-seconds). Noted by whoever sees the response
/// headers — the auth middleware for REST, the WS connect for the handshake
/// (the generated REST client drops headers, so this is the only route) —
/// and read by the reconnect loop, which waits out the deadline instead of
/// its own backoff.
#[derive(Default)]
pub struct RateLimit {
    until: std::sync::Mutex<Option<Instant>>,
}

impl RateLimit {
    /// A 429 came back; `retry_after` is its header when it parsed.
    pub fn note(&self, retry_after: Option<Duration>) {
        let Some(delay) = retry_after else {
            return;
        };
        let until = Instant::now() + delay;
        let mut u = self.until.lock().unwrap_or_else(|e| e.into_inner());
        if u.is_none_or(|prev| prev < until) {
            *u = Some(until);
        }
    }

    /// Time left on the last deadline, `None` when there is none or it passed.
    pub fn remaining(&self) -> Option<Duration> {
        let u = self.until.lock().unwrap_or_else(|e| e.into_inner());
        u.and_then(|until| until.checked_duration_since(Instant::now()))
            .filter(|d| !d.is_zero())
    }
}

/// `Retry-After` as delay-seconds; an HTTP-date form counts as absent.
pub fn parse_retry_after(value: Option<&http::HeaderValue>) -> Option<Duration> {
    value?.to_str().ok()?.trim().parse::<u64>().ok().map(Duration::from_secs)
}

/// Middleware that refreshes the access token if needed and attaches it as a
/// `Bearer` Authorization header — the faithful port of the TS client's
/// `onRequest` hook (`refreshAccessTokenIfNeeded` + bearer injection). Because
/// it drives [`AuthManager`], every caller through the main client (transfer
/// engine included) gets refresh-before-request, not just the proxy.
pub struct AuthMiddleware {
    pub auth: Arc<AuthManager>,
    pub rate_limit: Arc<RateLimit>,
}

#[async_trait::async_trait]
impl Middleware for AuthMiddleware {
    async fn handle(&self, mut req: Request, extensions: &mut Extensions, next: Next<'_>) -> Result<Response> {
        // Refresh proactively; on failure fall through with whatever token we
        // have — the 401 handling below is the real safety net.
        if let Err(e) = self.auth.ensure_fresh().await {
            tracing::debug!(error = %e, "auth middleware: token refresh failed");
        }
        let sent_token = self.auth.access_token();
        if let Some(token) = &sent_token {
            req.headers_mut().insert(
                reqwest::header::AUTHORIZATION,
                format!("Bearer {}", token).parse().unwrap(),
            );
        }

        // Clone before the send for the one 401 retry below. API bodies are
        // buffered JSON, so this normally succeeds; a streaming body
        // (`try_clone` → None) just skips the retry.
        let retry_req = req.try_clone();
        let resp = next.clone().run(req, extensions).await?;
        if resp.status() == reqwest::StatusCode::TOO_MANY_REQUESTS {
            self.rate_limit
                .note(parse_retry_after(resp.headers().get(reqwest::header::RETRY_AFTER)));
            return Ok(resp);
        }
        if resp.status() != reqwest::StatusCode::UNAUTHORIZED {
            return Ok(resp);
        }

        // Only an expired access token is fixable by a refresh; the envelope
        // `status` tells the 401s apart (openapi error catalog). Anything else
        // (invalid_token, session_revoked, user_session_required, …) surfaces
        // to the caller untouched. Reading the status consumes the body, so
        // the response is rebuilt for every non-retry exit below.
        let status_code = resp.status();
        let headers = resp.headers().clone();
        let body = resp.bytes().await.map_err(reqwest_middleware::Error::Reqwest)?;
        let env_status = serde_json::from_slice::<serde_json::Value>(&body)
            .ok()
            .and_then(|v| v.get("status").and_then(|s| s.as_str().map(str::to_owned)))
            .unwrap_or_default();
        let token_expired = env_status == "token_expired";
        let session_dead = matches!(env_status.as_str(), "session_revoked" | "refresh_token_reused");
        let resp = {
            let mut r = Response::from(http::Response::builder().status(status_code).body(body).unwrap());
            *r.headers_mut() = headers;
            r
        };
        // 401 with no refresh token: nothing to retry with, and a healthy
        // session always has one — the session is unrecoverable in-band.
        // Surface the 401, but drop the state to NoConnection so the reconnect
        // loop reruns the full register (Connecting → fresh session) instead of
        // leaving the app to fire dead-token 401s until the WS happens to drop.
        // Resource-level 401s (e.g. a foreign link) don't land here: with a
        // refresh token in hand they take the refresh-and-retry path below.
        if self.auth.refresh_token().is_none() {
            tracing::warn!("auth middleware: 401 with no refresh token — forcing re-register");
            self.auth.fail_offline();
            return Ok(resp);
        }

        // The server declared the session dead — a refresh with this session's
        // refresh token can only be refused. Tear the session down right away
        // (the same teardown a rejected refresh grant performs) instead of
        // bouncing through the doomed round trip.
        if session_dead {
            tracing::warn!("auth middleware: 401 {env_status} — dropping revoked session");
            self.auth.drop_revoked_session();
            return Ok(resp);
        }

        if !token_expired {
            return Ok(resp);
        }

        // 401 with a refresh token in hand: the access token is dead however
        // fresh its expiry timestamp claims to be — force-refresh and replay
        // the request ONCE. A refresh the server rejects clears the session
        // and drops the state to NoConnection inside `refresh_after_401`; a
        // transient refresh error surfaces the original 401 to the caller.
        // (The token grant itself runs on AuthManager's own client, which has
        // no AuthMiddleware — no recursion.)
        let Some(mut retry_req) = retry_req else {
            return Ok(resp);
        };
        tracing::debug!("auth middleware: 401 — refreshing token and retrying once");
        if self.auth.refresh_after_401(sent_token.as_deref()).await.is_err() {
            return Ok(resp);
        }
        let Some(token) = self.auth.access_token() else {
            return Ok(resp);
        };
        retry_req.headers_mut().insert(
            reqwest::header::AUTHORIZATION,
            format!("Bearer {}", token).parse().unwrap(),
        );
        next.run(retry_req, extensions).await
    }
}

/// How long an API call may take as a whole, connect to last body byte. A
/// backend answer is a small JSON: 30 s is a dead wire, not a slow one
/// (docs/app/reconnect.md).
pub const API_CALL_DEADLINE: Duration = Duration::from_secs(30);

/// Middleware that bounds every API call as a whole. The pool itself has no
/// read timeout — S3 bodies take as long as the wire gives and watch their
/// own progress — so the API side's bound lives here, as reqwest's
/// per-request timeout. A request that set its own keeps it.
pub struct RequestDeadline;

#[async_trait::async_trait]
impl Middleware for RequestDeadline {
    async fn handle(&self, mut req: Request, extensions: &mut Extensions, next: Next<'_>) -> Result<Response> {
        if req.timeout().is_none() {
            *req.timeout_mut() = Some(API_CALL_DEADLINE);
        }
        next.run(req, extensions).await
    }
}

/// The last middleware in a stack: runs the request on the pool as it is
/// *now*, never calling `next`. The stack is built once, over a client that
/// never serves a request; `Back::renew_http` swaps the pool under `&self`
/// on a network path change and every call from then on goes over the new
/// one — no rebuilt stacks, no `Configuration` to replace, no write lock on
/// `Back`.
pub struct PoolRouter {
    pub pool: Arc<std::sync::RwLock<reqwest::Client>>,
}

#[async_trait::async_trait]
impl Middleware for PoolRouter {
    async fn handle(&self, req: Request, _extensions: &mut Extensions, _next: Next<'_>) -> Result<Response> {
        let client = self.pool.read().unwrap_or_else(|e| e.into_inner()).clone();
        client.execute(req).await.map_err(reqwest_middleware::Error::Reqwest)
    }
}

/// Middleware that adds x-request-id header to every request
pub struct RequestIdMiddleware;

#[async_trait::async_trait]
impl Middleware for RequestIdMiddleware {
    async fn handle(&self, mut req: Request, extensions: &mut Extensions, next: Next<'_>) -> Result<Response> {
        let request_id = uuid::Uuid::new_v4().to_string();
        req.headers_mut().insert("x-request-id", request_id.parse().unwrap());
        next.run(req, extensions).await
    }
}

/// Body fields whose values are credentials in the API contract: tokens,
/// passwords and one-time codes, the TOTP secret and its provisioning URI,
/// recovery codes, the PKCE verifier, the SSO client secret, invite tokens,
/// and the share key a report attaches. `code` stays readable: it is also the
/// error category, and the OAuth code is useless without `code_verifier`.
const REDACTED_FIELDS: &[&str] = &[
    "access_token",
    "refresh_token",
    "id_token",
    "token",
    "ticket",
    "password",
    "totp",
    "email_code",
    "authorization_code",
    "recovery_code",
    "codes",
    "shared_secret",
    "provisioning_uri",
    "code_verifier",
    "client_secret",
    "invite_token",
    "link_key",
];

/// Blank out the value of every [`REDACTED_FIELDS`] key, at any depth. A
/// null stays null, so the log still tells "absent" from "present".
fn redact(value: &mut serde_json::Value) {
    match value {
        serde_json::Value::Object(map) => {
            for (key, v) in map.iter_mut() {
                if !REDACTED_FIELDS.contains(&key.as_str()) {
                    redact(v);
                } else if !v.is_null() {
                    *v = serde_json::Value::String("<redacted>".to_string());
                }
            }
        }
        serde_json::Value::Array(items) => items.iter_mut().for_each(redact),
        _ => {}
    }
}

/// Middleware that logs HTTP requests and responses for debugging. JSON
/// bodies go through [`redact`], so the log never carries a credential.
pub struct DebugLoggingMiddleware;

#[async_trait::async_trait]
impl Middleware for DebugLoggingMiddleware {
    async fn handle(&self, req: Request, extensions: &mut Extensions, next: Next<'_>) -> Result<Response> {
        // Log request
        let method = req.method().to_string();
        let url = req.url().to_string();
        tracing::debug!(">>> {} {}", method, url);

        if let Some(body) = req.body() {
            if let Some(bytes) = body.as_bytes() {
                if let Ok(text) = std::str::from_utf8(bytes)
                    && !text.is_empty()
                {
                    if let Ok(mut json) = serde_json::from_str::<serde_json::Value>(text) {
                        redact(&mut json);
                        tracing::debug!("  Body: {}", json);
                    } else {
                        tracing::debug!("  Body: {}", text);
                    }
                }
            } else {
                tracing::debug!("  (streaming body)");
            }
        }

        // Execute request
        let response = next.run(req, extensions).await?;

        // Log response
        let status = response.status();
        let headers = response.headers().clone();
        let body_bytes = response.bytes().await.map_err(reqwest_middleware::Error::Reqwest)?;

        tracing::debug!("<<< {} {} - {}", method, url, status);

        if let Ok(text) = std::str::from_utf8(&body_bytes)
            && !text.is_empty()
        {
            if let Ok(mut json) = serde_json::from_str::<serde_json::Value>(text) {
                redact(&mut json);
                tracing::debug!("  Body: {}", json);
            } else {
                tracing::debug!("  Body: {}", text);
            }
        }

        // Reconstruct the response
        let response = http::Response::builder().status(status).body(body_bytes).unwrap();

        let mut response = Response::from(response);
        *response.headers_mut() = headers;

        Ok(response)
    }
}

#[cfg(test)]
mod tests {
    use super::redact;

    #[test]
    fn redact_blanks_credentials_at_any_depth() {
        let mut body = serde_json::json!({
            "access_token": "at",
            "refresh_token": null,
            "code": "not_found",
            "user": { "email": "a@b.c", "password": "pw" },
            "items": [{ "token": "t", "url_token": "u" }],
            "codes": ["1111", "2222"]
        });
        redact(&mut body);
        assert_eq!(
            body,
            serde_json::json!({
                "access_token": "<redacted>",
                "refresh_token": null,
                "code": "not_found",
                "user": { "email": "a@b.c", "password": "<redacted>" },
                "items": [{ "token": "<redacted>", "url_token": "u" }],
                "codes": "<redacted>"
            })
        );
    }
}
