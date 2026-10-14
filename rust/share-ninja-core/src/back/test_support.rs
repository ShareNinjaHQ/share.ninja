//! `MockBackend`: an in-process backend for the transfer-session tests
//! (docs/app/reconnect.md, Harness A). Serves the session-bound endpoints
//! under `/links/{token}/…` from a per-endpoint script of answers and records
//! every call, so a test asserts exactly what went over the wire — how many
//! `update`s, which `transfer_id`, one `download/start` or two. No WS: the
//! `Back` state machine is driven directly with `Back::set_state`.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use tokio::sync::RwLock;

use super::{Back, BackParams, BackState};
use crate::secrets::{MemorySecretStore, SecureStore};

/// What the next call to an endpoint gets. An endpoint with no script left
/// answers `Ok`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Answer {
    Ok,
    /// 404 with the `transfer_not_found` status — the reaped session.
    NotFound,
    /// 500 — a backend that answered, but not usefully.
    ServerError,
    /// Never answers.
    Hang,
    /// A transfer that keeps trickling, longer in all than a whole-request
    /// bound but with every gap under the stall timeout. GET (`s3/get`):
    /// five 1-byte chunks, 20 virtual seconds apart. PUT (`s3/put`): the
    /// body is drained 4 MiB at a time, 20 virtual seconds between reads.
    Trickle,
}

/// One recorded call: the endpoint below `/links/{token}/` and its JSON body.
#[derive(Clone, Debug)]
pub(crate) struct Call {
    pub op: String,
    pub body: serde_json::Value,
}

struct Inner {
    scripts: Mutex<HashMap<String, VecDeque<Answer>>>,
    calls: Mutex<Vec<Call>>,
    starts: AtomicU32,
    file_starts: AtomicU32,
}

pub(crate) struct MockBackend {
    url: String,
    inner: Arc<Inner>,
}

impl MockBackend {
    pub(crate) async fn start() -> Self {
        let inner = Arc::new(Inner {
            scripts: Mutex::new(HashMap::new()),
            calls: Mutex::new(Vec::new()),
            starts: AtomicU32::new(0),
            file_starts: AtomicU32::new(0),
        });
        let app = Router::new()
            .route("/links/{token}/{*op}", post(handle))
            .route("/agents/call", post(handle_rtc))
            .route("/s3/{*key}", get(handle_s3_get).put(handle_s3_put))
            .with_state(inner.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });
        Self { url, inner }
    }

    /// Queue `answers` for `op` (e.g. `"download/update"`), consumed in order.
    pub(crate) fn script(&self, op: &str, answers: &[Answer]) {
        self.inner
            .scripts
            .lock()
            .unwrap()
            .entry(op.to_string())
            .or_default()
            .extend(answers.iter().copied());
    }

    pub(crate) fn calls(&self, op: &str) -> Vec<Call> {
        self.inner
            .calls
            .lock()
            .unwrap()
            .iter()
            .filter(|c| c.op == op)
            .cloned()
            .collect()
    }

    pub(crate) fn count(&self, op: &str) -> usize {
        self.calls(op).len()
    }

    /// Run `f` with tokio's paused-time auto-advance inhibited: a blocking
    /// task runs alongside for as long as `f` does, so a loopback request
    /// awaited inside `f` completes on real IO instead of being jumped past
    /// by its own timeout.
    pub(crate) async fn with_io<T>(f: impl std::future::Future<Output = T>) -> T {
        let done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let flag = done.clone();
        let guard = tokio::task::spawn_blocking(move || {
            while !flag.load(Ordering::Relaxed) {
                std::thread::sleep(std::time::Duration::from_millis(1));
            }
        });
        let out = f.await;
        done.store(true, Ordering::Relaxed);
        guard.await.unwrap();
        out
    }

    /// Wait (real time, up to 2 s) until `op` has been called `n` times.
    /// Usable under paused tokio time: the wait is a blocking task, which
    /// inhibits the auto-advance, so the virtual clock stands still while
    /// the loopback request completes.
    pub(crate) async fn wait_count(&self, op: &str, n: usize) {
        for _ in 0..400 {
            if self.count(op) >= n {
                return;
            }
            tokio::task::spawn_blocking(|| std::thread::sleep(std::time::Duration::from_millis(5)))
                .await
                .unwrap();
        }
        panic!("{op}: expected {n} calls, got {}", self.count(op));
    }

    /// A presigned-style GET URL under this mock's `/s3/` route, scripted by
    /// the `"s3/get"` op.
    pub(crate) fn s3_url(&self, key: &str) -> String {
        format!("{}/s3/{}", self.url, key)
    }

    /// A `Back` pointed at this mock, already `Online` (no WS involved).
    pub(crate) fn back(&self) -> Arc<RwLock<Back>> {
        self.back_in(BackState::Online)
    }

    pub(crate) fn back_in(&self, state: BackState) -> Arc<RwLock<Back>> {
        // `Back::new` builds a reqwest client eagerly; with `rustls-no-provider`
        // that needs the process-default provider the binaries install.
        rustls::crypto::ring::default_provider().install_default().ok();
        let params = BackParams {
            base_url: self.url.clone(),
            ws_url: self.url.replace("http", "ws"),
            oauth_url: None,
            user_agent: "test".to_string(),
            device_name: None,
            product_id: "test".to_string(),
            product_version: "0".to_string(),
            os: "test".to_string(),
            os_version: "0".to_string(),
            lang: "en".to_string(),
            response_mode: "token".to_string(),
            device_public_key: Vec::new(),
            device_serial: None,
            device_model: None,
            device_form_factor: None,
            log_api: false,
            nearby_visibility: None,
            instance_id: None,
            connect_override: None,
        };
        let secrets = Arc::new(SecureStore::new(Arc::new(MemorySecretStore::default())));
        let back = Back::new(params, secrets);
        back.set_state(state);
        Arc::new(RwLock::new(back))
    }
}

async fn handle(State(inner): State<Arc<Inner>>, Path((_token, op)): Path<(String, String)>, body: Bytes) -> Response {
    // `download/files/<id>/end` → `download/files/end`: the id is the
    // backend's, a script names the endpoint.
    let op = match op.split('/').collect::<Vec<_>>().as_slice() {
        [dir, "files", _, "end"] => format!("{dir}/files/end"),
        _ => op.clone(),
    };
    let body = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    inner.calls.lock().unwrap().push(Call { op: op.clone(), body });
    let answer = inner
        .scripts
        .lock()
        .unwrap()
        .get_mut(&op)
        .and_then(|q| q.pop_front())
        .unwrap_or(Answer::Ok);
    match answer {
        Answer::NotFound => (
            StatusCode::NOT_FOUND,
            r#"{"code":"not_found","status":"transfer_not_found","error":null}"#,
        )
            .into_response(),
        Answer::ServerError => (
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"code":"internal","status":"internal_error","error":"boom"}"#,
        )
            .into_response(),
        Answer::Hang => std::future::pending().await,
        Answer::Trickle => StatusCode::NOT_IMPLEMENTED.into_response(),
        Answer::Ok => match op.as_str() {
            "download/start" | "upload/start" => {
                let n = inner.starts.fetch_add(1, Ordering::Relaxed) + 1;
                let json = format!(
                    r#"{{"transfer_id":"tid-{n}","datasources":[{{"type":"s3","datasource_id":"ds-s3","endpoint":"http://mock","region":"r","bucket":"b","prefix":"p","presigned_files":[{{"path":"f","url":"http://127.0.0.1:9/f"}}]}}]}}"#
                );
                ([("content-type", "application/json")], json).into_response()
            }
            "download/presign" | "upload/presign" => {
                ([("content-type", "application/json")], r#"{"files":[]}"#).into_response()
            }
            "download/files" | "upload/files" => {
                let n = inner.file_starts.fetch_add(1, Ordering::Relaxed) + 1;
                let json = format!(r#"{{"file_transfer_id":"ft-{n}"}}"#);
                ([("content-type", "application/json")], json).into_response()
            }
            _ => StatusCode::NO_CONTENT.into_response(),
        },
    }
}

/// `/agents/call` — the P2P leg's `Back::call`. Scripted by the `"agents/call"`
/// op. `Ok` deliberately answers 204: the mock never completes a WebRTC call,
/// and `Back::call` reports an unparseable answer as a plain refusal
/// (`SharonResult::Fail`) — a live refusal for the fail-cap tests.
async fn handle_rtc(State(inner): State<Arc<Inner>>, body: Bytes) -> Response {
    let body = serde_json::from_slice(&body).unwrap_or(serde_json::Value::Null);
    inner.calls.lock().unwrap().push(Call {
        op: "agents/call".to_string(),
        body,
    });
    let answer = inner
        .scripts
        .lock()
        .unwrap()
        .get_mut("agents/call")
        .and_then(|q| q.pop_front())
        .unwrap_or(Answer::Ok);
    match answer {
        Answer::NotFound => (
            StatusCode::NOT_FOUND,
            r#"{"code":"not_found","status":"transfer_not_found","error":null}"#,
        )
            .into_response(),
        Answer::Hang => std::future::pending().await,
        _ => (
            StatusCode::INTERNAL_SERVER_ERROR,
            r#"{"code":"internal","status":"internal_error","error":"boom"}"#,
        )
            .into_response(),
    }
}

/// `/s3/{key}` — a presigned-GET stand-in for the storage-timeout tests.
/// Scripted by the `"s3/get"` op: `Ok` answers `b"DATA"`, `Hang` never
/// answers, `Trickle` streams five 1-byte chunks 20 s apart.
async fn handle_s3_get(State(inner): State<Arc<Inner>>, Path(key): Path<String>) -> Response {
    inner.calls.lock().unwrap().push(Call {
        op: "s3/get".to_string(),
        body: serde_json::Value::String(key),
    });
    let answer = inner
        .scripts
        .lock()
        .unwrap()
        .get_mut("s3/get")
        .and_then(|q| q.pop_front())
        .unwrap_or(Answer::Ok);
    match answer {
        Answer::Hang => std::future::pending().await,
        Answer::Trickle => {
            let stream = futures::stream::unfold(0u8, |i| async move {
                if i >= 5 {
                    return None;
                }
                if i > 0 {
                    tokio::time::sleep(std::time::Duration::from_secs(20)).await;
                }
                Some((Ok::<_, std::convert::Infallible>(Bytes::from(vec![b'a' + i])), i + 1))
            });
            Body::from_stream(stream).into_response()
        }
        _ => Bytes::from_static(b"DATA").into_response(),
    }
}

/// `/s3/{key}` PUT — a presigned-PUT stand-in for the storage-timeout tests.
/// Scripted by the `"s3/put"` op: `Ok` drains the body and answers 200 with
/// an ETag; `Hang` neither reads the body nor answers (the socket fills up
/// and the send stalls); `Trickle` drains the body 4 MiB at a time with 20 s
/// between reads, then answers 200.
async fn handle_s3_put(State(inner): State<Arc<Inner>>, Path(key): Path<String>, body: Body) -> Response {
    use futures::StreamExt;
    inner.calls.lock().unwrap().push(Call {
        op: "s3/put".to_string(),
        body: serde_json::Value::String(key),
    });
    let answer = inner
        .scripts
        .lock()
        .unwrap()
        .get_mut("s3/put")
        .and_then(|q| q.pop_front())
        .unwrap_or(Answer::Ok);
    match answer {
        Answer::Hang => {
            let _unread = body;
            std::future::pending().await
        }
        Answer::Trickle => {
            let mut stream = body.into_data_stream();
            let mut since_pause = 0usize;
            while let Some(Ok(chunk)) = stream.next().await {
                since_pause += chunk.len();
                if since_pause >= 4 * 1024 * 1024 {
                    since_pause = 0;
                    tokio::time::sleep(std::time::Duration::from_secs(20)).await;
                }
            }
            ([("etag", "\"trickled\"")], StatusCode::OK).into_response()
        }
        _ => {
            let mut stream = body.into_data_stream();
            while let Some(Ok(_)) = stream.next().await {}
            ([("etag", "\"landed\"")], StatusCode::OK).into_response()
        }
    }
}

/// Let an in-flight loopback request complete under paused tokio time: an
/// idle runtime jumps to the next timer instead of blocking on IO — except
/// while a blocking task runs, which inhibits the auto-advance, so the
/// runtime waits for IO in real time and the virtual clock stands still.
pub(crate) async fn settle() {
    tokio::task::spawn_blocking(|| std::thread::sleep(std::time::Duration::from_millis(20)))
        .await
        .unwrap();
}

/// Advance paused tokio time by `secs`, in ≤5 s steps (one transfer-loop
/// tick), settling the loopback IO after each step. Yields first so a task
/// the test just spawned or woke registers its timer at the current instant.
pub(crate) async fn advance(secs: u64) {
    tokio::task::yield_now().await;
    let mut left = std::time::Duration::from_secs(secs);
    while !left.is_zero() {
        let step = left.min(std::time::Duration::from_secs(5));
        tokio::time::advance(step).await;
        left -= step;
        settle().await;
    }
}
