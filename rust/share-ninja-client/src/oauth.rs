//! OAuth2 authorization_code (PKCE) login over a local loopback callback: the
//! consent page opens in the browser, redirects to `http://host:port/path?code=…`
//! which we serve once, then the code is exchanged for tokens (the register
//! handshake persists the refresh token through the session's keychain store).

use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::RwLock;
use url::Url;

use share_ninja_core::Back;

/// A login in progress: the listener is bound and the consent URL minted;
/// `finish` waits for the browser redirect and exchanges the code.
pub struct LoginFlow {
    pub consent_url: String,
    callback_url: Url,
    callback_path: String,
    verifier: String,
    listener: TcpListener,
}

impl LoginFlow {
    /// Bind the loopback callback and mint the consent URL. A `callback_port`
    /// of 0 lets the OS pick a free port.
    pub async fn begin(
        back: &Arc<RwLock<Back>>,
        callback_host: &str,
        callback_port: u16,
        callback_path: &str,
    ) -> Result<Self> {
        let verifier = generate_code_verifier();
        let challenge = code_challenge_s256(&verifier);

        let host: IpAddr = callback_host
            .parse()
            .with_context(|| format!("Invalid callback host: {}", callback_host))?;
        let addr = SocketAddr::new(host, callback_port);
        let listener = TcpListener::bind(addr)
            .await
            .with_context(|| format!("Failed to bind callback listener on {}", addr))?;
        let bound_port = listener
            .local_addr()
            .context("Failed to read the callback listener's bound address")?
            .port();

        let callback_url = Url::parse(&format!("http://{}:{}{}", callback_host, bound_port, callback_path))
            .context("Failed to build callback URL")?;
        let consent_url = back.read().await.gen_request_oauth(callback_url.as_str(), &challenge);

        Ok(Self {
            consent_url,
            callback_url,
            callback_path: callback_path.to_string(),
            verifier,
            listener,
        })
    }

    /// Wait for the browser redirect, then exchange the code. Runs the full
    /// register handshake (WS + `GET /me`), so `back.user` is set after.
    pub async fn finish(self, back: Arc<RwLock<Back>>) -> Result<()> {
        let code = wait_for_callback(self.listener, &self.callback_path).await?;
        Back::exchange_oauth_code(back, &code, self.callback_url.as_str(), &self.verifier)
            .await
            .context("Failed to exchange authorization code for tokens")
    }
}

fn generate_code_verifier() -> String {
    use rand::Rng;
    let mut bytes = [0u8; 32];
    rand::rng().fill_bytes(&mut bytes);
    share_ninja_core::utils::base64::to_base64_special(&bytes)
}

fn code_challenge_s256(verifier: &str) -> String {
    share_ninja_core::utils::base64::to_base64_special(&Sha256::digest(verifier.as_bytes()))
}

/// Open `url` in the user's default browser. The opener's stdio is
/// detached: the caller may own the terminal (the commander's TUI), and the
/// helper must not write into it.
pub fn open_in_browser(url: &str) -> Result<()> {
    use std::process::{Command, Stdio};
    #[cfg(target_os = "macos")]
    let mut cmd = {
        let mut c = Command::new("open");
        c.arg(url);
        c
    };
    #[cfg(target_os = "linux")]
    let mut cmd = {
        let mut c = Command::new("xdg-open");
        c.arg(url);
        c
    };
    #[cfg(target_os = "windows")]
    let mut cmd = {
        use std::os::windows::process::CommandExt;
        // `start "" "<url>"`: the first quoted argument is the window title
        // (without it the URL is taken as the title), and the quoted URL keeps
        // cmd from splitting the command at the query's `&`.
        let quoted = url.replace('"', "");
        let mut c = Command::new("cmd");
        c.raw_arg(format!("/C start \"\" \"{quoted}\""));
        c
    };
    #[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
    {
        anyhow::bail!("no browser opener on this platform — open the URL yourself: {url}");
    }
    #[allow(unreachable_code)]
    {
        cmd.stdin(Stdio::null()).stdout(Stdio::null()).stderr(Stdio::null());
        cmd.status().context("Failed to run the browser opener")?;
        Ok(())
    }
}

async fn wait_for_callback(listener: TcpListener, expected_path: &str) -> Result<String> {
    let (mut stream, _) = listener
        .accept()
        .await
        .context("Failed to accept callback connection")?;
    let mut buf = vec![0u8; 8192];
    let n = stream.read(&mut buf).await.context("Failed to read callback request")?;
    let req = String::from_utf8_lossy(&buf[..n]);
    let first_line = req.lines().next().unwrap_or_default();

    // Expect: GET /path?query HTTP/1.1
    let parts: Vec<&str> = first_line.split_whitespace().collect();
    if parts.len() < 2 {
        anyhow::bail!("Malformed HTTP request line from callback: {}", first_line);
    }
    let target = parts[1];
    let full_url = Url::parse(&format!("http://localhost{}", target)).context("Failed to parse callback URL")?;

    if full_url.path() != expected_path {
        anyhow::bail!(
            "Unexpected callback path: {} (expected {})",
            full_url.path(),
            expected_path
        );
    }

    let code = full_url
        .query_pairs()
        .find(|(k, _)| k == "code")
        .map(|(_, v)| v.into_owned());
    match code {
        Some(code) => {
            let body = "<html><body><h2>Authentication complete</h2><p>You can close this window.</p></body></html>";
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(resp.as_bytes()).await.ok();
            stream.shutdown().await.ok();
            Ok(code)
        }
        None => {
            let body = "<html><body><h2>OAuth callback error</h2><p>Missing required query parameter: <code>code</code></p></body></html>";
            let resp = format!(
                "HTTP/1.1 400 Bad Request\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\n\r\n{}",
                body.len(),
                body
            );
            stream.write_all(resp.as_bytes()).await.ok();
            stream.shutdown().await.ok();
            anyhow::bail!("OAuth callback is missing required query parameter: code");
        }
    }
}
