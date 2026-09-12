use crate::chain::{
    DEFAULT_FEED, DEFAULT_HTTP_OFFICIAL, DEFAULT_HTTP_PUBLICNODE, DEFAULT_SEQUENCER, DEFAULT_WS_PUBLICNODE,
};
use alloy::primitives::{Address, U256};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::str::FromStr;

/// Process env wins. `#` comments, optional quotes, no interpolation.
pub fn load_env_file(path: impl AsRef<Path>) {
    let Ok(text) = std::fs::read_to_string(path) else { return };
    for raw in text.lines() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some(eq) = line.find('=') else { continue };
        let key = line[..eq].trim();
        let mut val = line[eq + 1..].trim().to_string();
        if (val.starts_with('"') && val.ends_with('"')) || (val.starts_with('\'') && val.ends_with('\'')) {
            val = val[1..val.len() - 1].to_string();
        }
        if std::env::var_os(key).is_none() {
            // Safety: single-threaded at process start, before tokio work is spawned.
            unsafe { std::env::set_var(key, val) };
        }
    }
}

pub fn load_dotenv() {
    load_env_file(PathBuf::from(".env"));
}

pub fn env_str(key: &str) -> Option<String> {
    std::env::var(key).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

pub fn env_num(key: &str, fallback: f64) -> f64 {
    match env_str(key) {
        Some(v) => v.parse().unwrap_or(fallback),
        None => fallback,
    }
}

pub fn env_u64(key: &str, fallback: u64) -> u64 {
    match env_str(key) {
        Some(v) => v.parse().unwrap_or(fallback),
        None => fallback,
    }
}

pub fn env_i64(key: &str, fallback: i64) -> i64 {
    match env_str(key) {
        Some(v) => v.parse().unwrap_or(fallback),
        None => fallback,
    }
}

pub fn env_bool(key: &str, fallback: bool) -> bool {
    match env_str(key) {
        Some(v) => matches!(v.to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"),
        None => fallback,
    }
}

pub fn parse_ether(s: &str) -> anyhow::Result<U256> {
    let s = s.trim();
    if s.is_empty() {
        anyhow::bail!("empty ether amount");
    }
    let neg = s.starts_with('-');
    if neg {
        anyhow::bail!("negative ether amount");
    }
    let (whole, frac) = match s.split_once('.') {
        Some((w, f)) => (w, f),
        None => (s, ""),
    };
    if frac.len() > 18 {
        anyhow::bail!("more than 18 decimal places");
    }
    let mut digits = format!("{whole}{frac:0<18}", frac = frac);
    if digits.is_empty() {
        digits = "0".into();
    }
    Ok(U256::from_str(&digits)?)
}

#[derive(Debug, Clone)]
pub struct EndpointCfg {
    pub url: String,
    pub logs: bool,
    pub label: String,
}

#[derive(Debug, Clone)]
pub struct Config {
    pub rpc_http: Vec<EndpointCfg>,
    pub rpc_ws: Vec<String>,
    pub sequencer_url: String,
    pub feed_url: Option<String>,
    pub helper: Option<Address>,
    pub private_key: Option<String>,
    pub poll_ms: u64,
    pub rpc_in_flight: usize,
    pub rpc_spacing_ms: u64,
    pub rpc_logs_spacing_ms: u64,
    pub board_port: u16,
    pub telegram_bot: Option<String>,
    pub telegram_chat: Option<String>,
    pub ref_axiom: String,
    pub ref_fomo: String,
}

impl Config {
    pub fn from_env() -> Self {
        load_dotenv();
        let rpc_http = parse_rpc_urls();
        let rpc_ws = parse_ws_urls();
        Self {
            rpc_http,
            rpc_ws,
            sequencer_url: env_str("SEQUENCER_URL").unwrap_or_else(|| DEFAULT_SEQUENCER.into()),
            feed_url: match env_str("FEED_URL") {
                Some(v) if v.eq_ignore_ascii_case("off") => None,
                Some(v) => Some(v),
                None => Some(DEFAULT_FEED.into()),
            },
            helper: env_str("HELPER_ADDRESS").and_then(|s| s.parse().ok()),
            private_key: env_str("PRIVATE_KEY"),
            poll_ms: env_u64("POLL_MS", 300),
            rpc_in_flight: env_u64("RPC_IN_FLIGHT", 3) as usize,
            rpc_spacing_ms: env_u64("RPC_SPACING_MS", 50),
            rpc_logs_spacing_ms: env_u64("RPC_LOGS_SPACING_MS", 400),
            board_port: env_u64("BOARD_PORT", 4663) as u16,
            telegram_bot: env_str("TELEGRAM_BOT_TOKEN"),
            telegram_chat: env_str("TELEGRAM_CHAT_ID"),
            ref_axiom: env_str("REF_AXIOM").unwrap_or_else(|| "phosphen".into()),
            ref_fomo: env_str("REF_FOMO").unwrap_or_else(|| "phosphenq".into()),
        }
    }

    pub fn http_labels(&self) -> String {
        self.rpc_http.iter().map(|e| e.label.as_str()).collect::<Vec<_>>().join(" → ")
    }

    pub fn ws_labels(&self) -> String {
        if self.rpc_ws.is_empty() {
            format!("off, poll {} ms", self.poll_ms)
        } else {
            self.rpc_ws.join(", ")
        }
    }
}

fn parse_rpc_urls() -> Vec<EndpointCfg> {
    let defaults = vec![
        EndpointCfg {
            url: DEFAULT_HTTP_PUBLICNODE.into(),
            logs: false,
            label: "publicnode".into(),
        },
        EndpointCfg {
            url: DEFAULT_HTTP_OFFICIAL.into(),
            logs: true,
            label: "robinhood".into(),
        },
    ];
    let Some(raw) = env_str("RPC_URL") else { return defaults };
    raw.split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|u| {
            let no_logs = u.ends_with("#nologs");
            let url = if no_logs { u.trim_end_matches("#nologs").to_string() } else { u.to_string() };
            let known = defaults.iter().find(|d| d.url == url);
            EndpointCfg {
                url: url.clone(),
                logs: known.map(|k| k.logs).unwrap_or(!no_logs),
                label: known
                    .map(|k| k.label.clone())
                    .unwrap_or_else(|| url::Url::parse(&url).ok().and_then(|p| p.host_str().map(|h| h.to_string())).unwrap_or(url)),
            }
        })
        .collect()
}

fn parse_ws_urls() -> Vec<String> {
    match env_str("RPC_WS_URL") {
        None => vec![DEFAULT_WS_PUBLICNODE.into()],
        Some(v) if v.eq_ignore_ascii_case("off") => vec![],
        Some(v) => v.split(',').map(|s| s.trim().to_string()).filter(|s| !s.is_empty()).collect(),
    }
}

/// `34@100,33@300` → [(34, 100), (33, 300)]. Fraction of the remaining bag at that gain %.
pub fn parse_exit_ladder(raw: &str) -> Vec<(u32, i32)> {
    raw.split(',')
        .filter_map(|part| {
            let (frac, at) = part.trim().split_once('@')?;
            Some((frac.parse().ok()?, at.parse().ok()?))
        })
        .collect()
}

pub fn env_map() -> HashMap<String, String> {
    std::env::vars().collect()
}
