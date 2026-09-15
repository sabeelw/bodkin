use crate::Config;
use crate::engine::SnipeRules;
use crate::fmt::wei_to_f64;
use crate::links::Links;
use crate::pons::clock::now_ms;
use crate::rpc::Rpc;
use crate::run::{EngineHandle, start_engine};
use crate::style::{info, muted, neon, on_neon};
use alloy::primitives::U256;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::http::header;
use axum::middleware::{self, Next};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use futures_util::{Stream, StreamExt};
use serde_json::{Value, json};
use std::collections::{HashSet, VecDeque};
use std::convert::Infallible;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;
use tokio_stream::wrappers::BroadcastStream;

const HTML: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../web/board.html"));

struct BoardState {
    engine: EngineHandle,
    live: bool,
    started_at: u64,
    seen: Arc<AtomicU64>,
    fired: Arc<AtomicU64>,
    recent: Arc<parking_lot::Mutex<VecDeque<Value>>>,
    close_requests: Arc<parking_lot::Mutex<HashSet<String>>>,
    bus: broadcast::Sender<String>,
    rpc: Arc<Rpc>,
}

pub async fn start_board(
    rpc: Arc<Rpc>,
    cfg: Config,
    port: u16,
    live: bool,
    rules: SnipeRules,
) -> anyhow::Result<()> {
    anyhow::ensure!(port > 0, "board port must be greater than zero");
    let addr = format!("127.0.0.1:{port}");
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    let (bus, _) = broadcast::channel::<String>(256);
    let seen = Arc::new(AtomicU64::new(0));
    let fired = Arc::new(AtomicU64::new(0));
    let recent = Arc::new(parking_lot::Mutex::new(VecDeque::new()));
    let close_requests = Arc::new(parking_lot::Mutex::new(HashSet::new()));
    let emit = {
        let bus = bus.clone();
        let seen = seen.clone();
        let fired = fired.clone();
        let recent = recent.clone();
        let close_requests = close_requests.clone();
        Arc::new(move |event: Value| {
            match event.get("kind").and_then(Value::as_str) {
                Some("launch") => {
                    seen.fetch_add(1, Ordering::Relaxed);
                }
                Some("fire") => {
                    fired.fetch_add(1, Ordering::Relaxed);
                }
                Some("exit") => {
                    if let Some(id) = event.get("positionId").and_then(Value::as_str) {
                        close_requests.lock().remove(id);
                    }
                }
                Some("close_error" | "exit_error")
                    if event.get("pending").and_then(Value::as_bool) == Some(false) =>
                {
                    if let Some(id) = event.get("positionId").and_then(Value::as_str) {
                        close_requests.lock().remove(id);
                    }
                }
                _ => {}
            }
            if event.get("kind").and_then(Value::as_str) != Some("tick") {
                let mut queue = recent.lock();
                queue.push_back(event.clone());
                if queue.len() > 400 {
                    queue.pop_front();
                }
            }
            let _ = bus.send(event.to_string());
        }) as crate::run::Emit
    };
    let engine = start_engine(rpc.clone(), cfg.clone(), rules, live, true, emit).await?;
    let st = Arc::new(BoardState {
        engine,
        live,
        started_at: now_ms(),
        seen,
        fired,
        recent,
        close_requests,
        bus,
        rpc,
    });
    {
        let st = st.clone();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                iv.tick().await;
                if st.engine.stopped() {
                    break;
                }
                let h = health(&st);
                let _ = st.bus.send(
                    json!({"kind":"tick","t": now_ms(), "paused": st.engine.paused(), "health": h})
                        .to_string(),
                );
            }
        });
    }

    let app = board_router(st.clone(), port);

    info(format!(
        "{} {}  http://{addr}  {}  {}",
        neon("bodkin"),
        muted("board"),
        if live {
            on_neon(" LIVE ")
        } else {
            muted("dry run")
        },
        muted("feed only until you press start")
    ));
    let served = axum::serve(listener, app)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await;
    st.engine.shutdown().await;
    served?;
    Ok(())
}

fn board_router(state: Arc<BoardState>, port: u16) -> Router {
    Router::new()
        .route("/", get(page))
        .route("/events", get(events))
        .route("/api/state", get(self::state))
        .route("/api/start", post(start))
        .route("/api/resume", post(start))
        .route("/api/stop", post(stop))
        .route("/api/pause", post(stop))
        .route("/api/close/{id}", post(close))
        .route("/api/rules", post(rules_edit))
        .layer(middleware::from_fn_with_state(port, security))
        .with_state(state)
}

fn local_host(value: &str, port: u16) -> Option<String> {
    let authority: axum::http::uri::Authority = value.parse().ok()?;
    let host = authority.host().to_ascii_lowercase();
    if !matches!(host.as_str(), "localhost" | "127.0.0.1" | "[::1]")
        || authority.port_u16().unwrap_or(80) != port
    {
        return None;
    }
    Some(host)
}
fn request_allowed(
    headers: &axum::http::HeaderMap,
    method: &axum::http::Method,
    port: u16,
) -> bool {
    if headers.get_all(header::HOST).iter().count() != 1
        || headers.get_all(header::ORIGIN).iter().count() > 1
    {
        return false;
    }
    let Some(host) = headers
        .get(header::HOST)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| local_host(h, port))
    else {
        return false;
    };
    // Browsers always send Origin on fetch POSTs; curl doesn't — the Host
    // check still pins those requests to loopback.
    if let Some(origin) = headers.get(header::ORIGIN) {
        let Some(url) = origin.to_str().ok().and_then(|s| url::Url::parse(s).ok()) else {
            return false;
        };
        let origin_host = url.host_str().unwrap_or("").to_ascii_lowercase();
        if url.scheme() != "http"
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port_or_known_default() != Some(port)
            || origin_host != host
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return false;
        }
    }
    if method == axum::http::Method::POST {
        if headers
            .get("sec-fetch-site")
            .is_some_and(|v| v != "same-origin" && v != "none")
        {
            return false;
        }
        if !headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .is_some_and(|v| {
                v.split(';')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .eq_ignore_ascii_case("application/json")
            })
        {
            return false;
        }
    }
    true
}
/// Local-only hardening: every response gets security headers, and every POST
/// must come from a page we served (Origin/Host pinned to loopback) — a foreign
/// origin's fetch is rejected before it can pause the engine or sell a bag.
async fn security(State(port): State<u16>, req: axum::extract::Request, next: Next) -> Response {
    let mut res = if request_allowed(req.headers(), req.method(), port) {
        next.run(req).await
    } else {
        StatusCode::FORBIDDEN.into_response()
    };
    let h = res.headers_mut();
    h.insert(
        header::X_CONTENT_TYPE_OPTIONS,
        header::HeaderValue::from_static("nosniff"),
    );
    h.insert(
        header::X_FRAME_OPTIONS,
        header::HeaderValue::from_static("DENY"),
    );
    h.insert(
        header::REFERRER_POLICY,
        header::HeaderValue::from_static("no-referrer"),
    );
    h.insert(
        header::CACHE_CONTROL,
        header::HeaderValue::from_static("no-store"),
    );
    h.insert(header::CONTENT_SECURITY_POLICY, header::HeaderValue::from_static("default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; font-src https://fonts.gstatic.com; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'; form-action 'self'"));
    res
}

async fn page() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], HTML)
}

async fn events(
    State(st): State<Arc<BoardState>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let rx = st.bus.subscribe();
    let initial = hello(&st).await;
    let mut recent = {
        let q = st.recent.lock();
        q.iter()
            .filter(|e| e.get("kind").and_then(|k| k.as_str()) == Some("launch"))
            .rev()
            .take(60)
            .cloned()
            .map(|mut v| {
                // Replayed events are marked so the client can dedupe them
                // against anything it already rendered.
                v["replay"] = json!(true);
                v
            })
            .collect::<Vec<_>>()
    };
    recent.reverse();
    let prelude: Vec<Result<Event, Infallible>> = std::iter::once(initial)
        .chain(recent)
        .map(|v| Ok(Event::default().data(v.to_string())))
        .collect();
    let live_state = st.clone();
    let live = BroadcastStream::new(rx).scan(false, move |done, message| {
        let st = live_state.clone();
        let already_done = *done;
        if message.is_err() {
            *done = true;
        }
        async move {
            if already_done {
                return None;
            }
            let line = match message {
                Ok(line) => line,
                Err(_) => {
                    let mut snapshot = hello(&st).await;
                    snapshot["kind"] = json!("resync");
                    snapshot.to_string()
                }
            };
            Some(Ok(Event::default().data(line)))
        }
    });
    Sse::new(futures_util::stream::iter(prelude).chain(live)).keep_alive(KeepAlive::default())
}

async fn state(State(st): State<Arc<BoardState>>) -> impl IntoResponse {
    let mut h = hello(&st).await;
    h["ethUsd"] = json!(crate::alerts::eth_usd().await);
    h["recent"] = json!(
        st.recent
            .lock()
            .iter()
            .rev()
            .take(100)
            .cloned()
            .collect::<Vec<_>>()
    );
    Json(h)
}

async fn start(State(st): State<Arc<BoardState>>) -> impl IntoResponse {
    if let Err(error) = st.engine.clock().require_quality(2_000, 1_500) {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":format!("not ready: {error}")})),
        );
    }
    match st.engine.resume() {
        Ok(()) => {
            let _ = st
                .bus
                .send(json!({"kind":"paused","paused":false}).to_string());
            (StatusCode::OK, Json(json!({"paused": false})))
        }
        Err(e) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error": e.to_string()})),
        ),
    }
}

async fn stop(State(st): State<Arc<BoardState>>) -> impl IntoResponse {
    st.engine.pause();
    let _ = st
        .bus
        .send(json!({"kind":"paused","paused":true}).to_string());
    Json(json!({"paused": true}))
}

/// The close is queued on the engine, not executed inline — 202 makes the
/// asynchronous semantics explicit.
async fn close(State(st): State<Arc<BoardState>>, Path(id): Path<String>) -> impl IntoResponse {
    if id.len() > 160 || id.chars().any(char::is_control) {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"invalid position id"})),
        );
    }
    if !st
        .engine
        .positions()
        .iter()
        .any(|position| position.id == id && position.status == "open")
    {
        return (
            StatusCode::NOT_FOUND,
            Json(json!({"error":"open position not found"})),
        );
    }
    if !st.close_requests.lock().insert(id.clone()) {
        return (
            StatusCode::ACCEPTED,
            Json(json!({"ok":true,"queued":true,"alreadyPending":true})),
        );
    }
    match st.engine.request_close(id.clone()) {
        Ok(()) => (
            StatusCode::ACCEPTED,
            Json(json!({"ok": true, "queued": true})),
        ),
        Err(error) => {
            st.close_requests.lock().remove(&id);
            (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({"error": error.to_string()})),
            )
        }
    }
}

async fn rules_edit(
    State(st): State<Arc<BoardState>>,
    Json(body): Json<Value>,
) -> impl IntoResponse {
    let mut changed = serde_json::Map::new();
    let bounds = [
        ("minScore", 0.0, 100.0),
        ("maxOpenPositions", 1.0, 20.0),
        ("maxOpeningTaxBps", 0.0, 2000.0),
        ("maxDevSharePct", 0.0, 100.0),
        ("maxExemptWallets", 0.0, 50.0),
    ];
    {
        let mut r = st.engine.rules.lock();
        for (k, min, max) in bounds {
            if let Some(v) = body.get(k).and_then(|x| x.as_f64()) {
                let n = v.clamp(min, max).round();
                match k {
                    "minScore" => r.min_score = n as i32,
                    "maxOpenPositions" => r.max_open_positions = n as usize,
                    "maxOpeningTaxBps" => r.max_opening_tax_bps = n as u64,
                    "maxDevSharePct" => r.max_dev_share_pct = n,
                    "maxExemptWallets" => r.max_exempt_wallets = n as usize,
                    _ => {}
                }
                changed.insert(k.to_string(), json!(n));
            }
        }
    }
    let view = rules_view(&st.engine.rules.lock());
    let _ = st
        .bus
        .send(json!({"kind":"rules","rules": view, "changed": changed}).to_string());
    Json(json!({"changed": changed, "rules": view}))
}

async fn hello(st: &BoardState) -> Value {
    let links = Links::from_env();
    json!({
        "kind": "hello",
        "live": st.live,
        "paused": st.engine.paused(),
        "rules": rules_view(&st.engine.rules.lock()),
        "startedAt": st.started_at,
        "seen": st.seen.load(Ordering::Relaxed),
        "fired": st.fired.load(Ordering::Relaxed),
        "positions": positions_view(st),
        "refs": {"axiom": links.axiom_ref(), "fomo": links.fomo_ref()},
        "health": health(st),
    })
}

fn rules_view(r: &SnipeRules) -> Value {
    json!({
        "ethPerBuy": r.eth_per_buy.to_string(),
        "minScore": r.min_score,
        "maxOpeningTaxBps": r.max_opening_tax_bps,
        "maxDevSharePct": r.max_dev_share_pct,
        "maxCreatorTaxBps": r.max_creator_tax_bps,
        "requireSocials": r.require_socials,
        "maxExemptWallets": r.max_exempt_wallets,
        "ethPairsOnly": r.eth_pairs_only,
        "keyword": r.keyword.as_ref().map(|k| k.as_str()),
        "maxOpenPositions": r.max_open_positions,
        "sessionBudgetEth": r.session_budget_wei.to_string(),
        "exits": {
            "takeProfitPct": r.exits.take_profit_pct,
            "stopLossPct": r.exits.stop_loss_pct,
            "trailingPct": r.exits.trailing_pct,
            "maxHoldMin": r.exits.max_hold_min
        },
        "editable": ["minScore","maxOpenPositions","maxOpeningTaxBps","maxDevSharePct","maxExemptWallets"]
    })
}

fn positions_view(st: &BoardState) -> Value {
    let closing = st.close_requests.lock();
    json!(
        st.engine
            .positions()
            .into_iter()
            // A board shows only its own mode's records — a dry board never lists
            // live positions it could (incorrectly) offer to close.
            .filter(|p| p.dry_run != st.live)
            .map(|p| {
                let is_closing = closing.contains(&p.id);
                let basis = p.basis();
                let last: U256 = if p.status == "open" {
                    p.last_eth.parse().unwrap_or(basis)
                } else {
                    p.realized_out()
                };
                let entry_gas = p.entry_gas();
                let exit_gas = p.exit_gas();
                let pnl = p.net_pnl_pct(last);
                json!({
                    "id": p.id, "token": format!("{:#x}", p.token), "symbol": p.symbol,
                    "ethIn": p.entry_eth, "held": p.tokens, "status": p.status,
                    "pnl": pnl, "dryRun": p.dry_run, "openedAt": p.opened_at * 1000,
                    "closing": is_closing,
                    "reason": p.exits.last().map(|e| e.reason.clone()),
                    "closedAt": p.exits.last().map(|e| e.at * 1000),
                    "realizedWei": p.net_realized_pnl_wei(),
                    "realizedBeforeGasWei": p.realized_pnl_wei(),
                    "entryGasWei": entry_gas.to_string(), "exitGasWei": exit_gas.to_string(),
                    "valueEth": if p.status == "open" { json!(p.last_eth) } else { json!("0") },
                    "basisWei": basis.to_string(),
                    "valueEthF": wei_to_f64(last) / 1e18,
                })
            })
            .collect::<Vec<_>>()
    )
}

fn health(st: &BoardState) -> Value {
    let g = st.rpc.stats();
    let f = st.engine.feed();
    let c = st.engine.clock();
    json!({
        "feed": {
            "mode": f.mode,
            "lastLaunchAt": f.last_launch_at,
            "lastWsAt": f.last_ws_at,
            "recoveries": f.recoveries,
            "note": f.note,
        },
        "lastLaunchAgeSec": if f.last_launch_at > 0 { json!((now_ms().saturating_sub(f.last_launch_at)) / 1000) } else { Value::Null },
        "clock": {"seeded":c.seeded(),"ageMs":c.age_ms(),"jitterMs":c.jitter_ms(),"lastBlock":c.last_block()},
        "gate": {
            "active": g.active, "queued": g.queued, "inFlight": g.in_flight, "spacingMs": g.spacing_ms,
            "logsSpacingMs": g.logs_spacing_ms, "throttled": g.throttled, "coolingDown": g.cooling_down,
            "latency": {"count":g.latency.count,"p50Us":g.latency.p50_us,"p95Us":g.latency.p95_us,"p99Us":g.latency.p99_us,"maxUs":g.latency.max_us},
            "endpoints": g.endpoints.iter().map(|e| json!({"label": e.label, "logs": e.logs, "benched": e.benched})).collect::<Vec<_>>()
        },
        "spentWei": st.engine.spent().to_string(),
        "stopped": st.engine.stopped()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::EndpointCfg;
    use crate::trade::state::StateDb;
    use alloy::primitives::{Address, U256};
    use axum::body::Body;
    use axum::http::header::{HeaderName, HeaderValue};
    use axum::http::{HeaderMap, Method, Request};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    const PORT: u16 = 4663;
    type Case = (&'static [(&'static str, &'static str)], Method, bool);

    fn hdrs(pairs: &[(&'static str, &'static str)]) -> HeaderMap {
        let mut m = HeaderMap::new();
        for (k, v) in pairs {
            m.append(HeaderName::from_static(k), HeaderValue::from_static(v));
        }
        m
    }

    #[test]
    fn request_allowed_pins_host_and_origin_to_the_loopback_port() {
        let cases: &[Case] = &[
            (
                &[
                    ("host", "localhost:4663"),
                    ("origin", "http://localhost:4663"),
                    ("content-type", "application/json"),
                ],
                Method::POST,
                true,
            ),
            (
                &[
                    ("host", "127.0.0.1:4663"),
                    ("origin", "http://127.0.0.1:4663"),
                    ("content-type", "application/json"),
                ],
                Method::POST,
                true,
            ),
            (
                &[
                    ("host", "[::1]:4663"),
                    ("origin", "http://[::1]:4663"),
                    ("content-type", "application/json"),
                ],
                Method::POST,
                true,
            ),
            (
                &[
                    ("host", "localhost:4663"),
                    ("content-type", "application/json"),
                ],
                Method::POST,
                true,
            ),
            (
                &[("host", "localhost:4663"), ("host", "localhost:4663")],
                Method::GET,
                false,
            ),
            (&[("host", "localhost.evil:4663")], Method::GET, false),
            (&[("host", "127.0.0.1.evil:4663")], Method::GET, false),
            (
                &[
                    ("host", "localhost:4663"),
                    ("origin", "http://localhost.evil:4663"),
                ],
                Method::GET,
                false,
            ),
            (
                &[
                    ("host", "127.0.0.1:4663"),
                    ("origin", "http://127.0.0.1.evil:4663"),
                ],
                Method::GET,
                false,
            ),
            (
                &[("host", "localhost:4663"), ("origin", "http://localhost:1")],
                Method::GET,
                false,
            ),
            (
                &[("host", "localhost:4663"), ("origin", "http://localhost")],
                Method::GET,
                false,
            ),
            (
                &[
                    ("host", "localhost:4663"),
                    ("origin", "http://127.0.0.1:4663"),
                ],
                Method::GET,
                false,
            ),
            (
                &[("host", "localhost:4663"), ("origin", "null")],
                Method::GET,
                false,
            ),
            (
                &[
                    ("host", "localhost:4663"),
                    ("origin", "http://localhost:4663@evil.example"),
                ],
                Method::GET,
                false,
            ),
            (&[], Method::GET, false),
            (&[("host", "evil.example")], Method::GET, false),
            (
                &[
                    ("host", "localhost:4663"),
                    ("sec-fetch-site", "cross-site"),
                    ("content-type", "application/json"),
                ],
                Method::POST,
                false,
            ),
            (
                &[
                    ("host", "localhost:4663"),
                    ("content-type", "application/x-www-form-urlencoded"),
                ],
                Method::POST,
                false,
            ),
        ];
        for (pairs, method, want) in cases {
            assert_eq!(
                request_allowed(&hdrs(pairs), method, PORT),
                *want,
                "headers {pairs:?} {method}"
            );
        }
    }

    fn config() -> Config {
        Config {
            rpc_http: vec![EndpointCfg {
                url: "http://127.0.0.1:1".into(),
                logs: true,
                label: "test".into(),
            }],
            rpc_ws: vec![],
            sequencer_url: "http://127.0.0.1:1".into(),
            helper: None,
            poll_ms: 300,
            rpc_in_flight: 1,
            rpc_spacing_ms: 0,
            rpc_logs_spacing_ms: 0,
            board_port: PORT,
        }
    }

    fn request(method: Method, path: &str) -> Request<Body> {
        Request::builder()
            .method(method)
            .uri(path)
            .header(header::HOST, "localhost:4663")
            .header(header::CONTENT_TYPE, "application/json")
            .body(Body::empty())
            .unwrap()
    }

    async fn json_body(response: Response) -> Value {
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap()
    }

    #[tokio::test]
    async fn board_state_reconnects_and_close_retries_are_idempotent() {
        let dir = tempfile::tempdir().unwrap();
        let state_db = StateDb::open(dir.path()).unwrap();
        let positions =
            Arc::new(crate::trade::positions::PositionStore::from_state(state_db).unwrap());
        let position = positions.open_position(
            Address::from([1u8; 20]),
            Address::from([2u8; 20]),
            "X".into(),
            "x".into(),
            1,
            None,
            true,
            U256::from(10),
            U256::ZERO,
            U256::from(10),
            0,
            None,
        );
        let engine = EngineHandle::test_handle(positions);
        let now = now_ms();
        engine.clock().note_header(now / 1_000, now, 1, 1);
        let (bus, _) = broadcast::channel(8);
        let state = Arc::new(BoardState {
            engine,
            live: false,
            started_at: 1,
            seen: Arc::new(AtomicU64::new(3)),
            fired: Arc::new(AtomicU64::new(1)),
            recent: Arc::new(parking_lot::Mutex::new(VecDeque::new())),
            close_requests: Arc::new(parking_lot::Mutex::new(HashSet::new())),
            bus,
            rpc: Arc::new(Rpc::new(&config()).unwrap()),
        });
        let app = board_router(state.clone(), PORT);

        let first_state = json_body(
            app.clone()
                .oneshot(request(Method::GET, "/api/state"))
                .await
                .unwrap(),
        )
        .await;
        let second_state = json_body(
            app.clone()
                .oneshot(request(Method::GET, "/api/state"))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(first_state["positions"], second_state["positions"]);
        assert_eq!(first_state["seen"], second_state["seen"]);
        assert_eq!(first_state["positions"][0]["closing"], false);

        let path = format!("/api/close/{}", position.id);
        let first = json_body(
            app.clone()
                .oneshot(request(Method::POST, &path))
                .await
                .unwrap(),
        )
        .await;
        let second = json_body(
            app.clone()
                .oneshot(request(Method::POST, &path))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(first["queued"], true);
        assert_eq!(second["alreadyPending"], true);

        let after_close = json_body(
            app.clone()
                .oneshot(request(Method::GET, "/api/state"))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(after_close["positions"][0]["closing"], true);

        let response = app
            .clone()
            .oneshot(request(Method::GET, "/events"))
            .await
            .unwrap();
        for sequence in 0..32 {
            let _ = state
                .bus
                .send(json!({"kind":"tick","sequence":sequence}).to_string());
        }
        let mut body = response.into_body().into_data_stream();
        let mut saw_resync = false;
        for _ in 0..4 {
            let chunk = tokio::time::timeout(
                std::time::Duration::from_secs(1),
                futures_util::StreamExt::next(&mut body),
            )
            .await
            .unwrap()
            .unwrap()
            .unwrap();
            saw_resync |= String::from_utf8_lossy(&chunk).contains("\"kind\":\"resync\"");
            if saw_resync {
                break;
            }
        }
        assert!(saw_resync);

        let now = now_ms();
        state.engine.clock().note_header(now / 1_000, now, 1, 2);
        for _ in 0..2 {
            let response = app
                .clone()
                .oneshot(request(Method::POST, "/api/start"))
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }
        assert!(!state.engine.paused());
        state.engine.shutdown().await;
    }
}
