use crate::engine::SnipeRules;
use crate::links::Links;
use crate::pons::launches::FeedHealth;
use crate::rpc::Rpc;
use crate::run::{start_engine, EngineHandle};
use crate::style::{info, muted, neon, on_neon};
use crate::trade::positions::PositionStore;
use crate::Config;
use axum::extract::{Path, State};
use axum::http::header;
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use futures::{Stream, StreamExt};
use std::convert::Infallible;
use serde_json::{json, Value};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::{broadcast, RwLock};

const HTML: &str = include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../web/board.html"));

struct BoardState {
    engine: EngineHandle,
    live: bool,
    started_at: u64,
    seen: AtomicU64,
    fired: AtomicU64,
    recent: RwLock<VecDeque<Value>>,
    bus: broadcast::Sender<String>,
    rpc: Arc<Rpc>,
}

pub async fn start_board(rpc: Arc<Rpc>, cfg: Config, port: u16, live: bool, rules: SnipeRules) -> anyhow::Result<()> {
    let (bus, _) = broadcast::channel::<String>(256);
    let bus2 = bus.clone();
    let emit = {
        let bus = bus.clone();
        Arc::new(move |e: Value| {
            let _ = bus.send(e.to_string());
        }) as crate::run::Emit
    };
    let engine = start_engine(rpc.clone(), cfg.clone(), rules, live, true, emit);
    let st = Arc::new(BoardState {
        engine,
        live,
        started_at: now_ms(),
        seen: AtomicU64::new(0),
        fired: AtomicU64::new(0),
        recent: RwLock::new(VecDeque::new()),
        bus: bus2,
        rpc,
    });
    {
        let st = st.clone();
        let mut rx = bus.subscribe();
        tokio::spawn(async move {
            while let Ok(line) = rx.recv().await {
                if let Ok(v) = serde_json::from_str::<Value>(&line) {
                    match v.get("kind").and_then(|k| k.as_str()) {
                        Some("launch") => {
                            st.seen.fetch_add(1, Ordering::Relaxed);
                        }
                        Some("fire") => {
                            st.fired.fetch_add(1, Ordering::Relaxed);
                        }
                        Some("tick") => continue,
                        _ => {}
                    }
                    let mut q = st.recent.write().await;
                    q.push_back(v);
                    if q.len() > 400 {
                        q.pop_front();
                    }
                }
            }
        });
    }
    {
        let st = st.clone();
        tokio::spawn(async move {
            let mut iv = tokio::time::interval(std::time::Duration::from_secs(10));
            loop {
                iv.tick().await;
                let h = health(&st);
                let _ = st.bus.send(json!({"kind":"tick","t": now_ms(), "paused": st.engine.paused(), "health": h}).to_string());
            }
        });
    }

    let app = Router::new()
        .route("/", get(page))
        .route("/events", get(events))
        .route("/api/state", get(state))
        .route("/api/start", post(start))
        .route("/api/resume", post(start))
        .route("/api/stop", post(stop))
        .route("/api/pause", post(stop))
        .route("/api/close/{id}", post(close))
        .route("/api/rules", post(rules_edit))
        .with_state(st);

    let addr = format!("127.0.0.1:{port}");
    info(format!(
        "{} {}  http://{addr}  {}  {}",
        neon("bodkin"),
        muted("board"),
        if live { on_neon(" LIVE ") } else { muted("dry run") },
        muted("feed only until you press start")
    ));
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn page() -> impl IntoResponse {
    ([(header::CONTENT_TYPE, "text/html; charset=utf-8")], HTML)
}

async fn events(State(st): State<Arc<BoardState>>) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let hello = hello(&st).await;
    let rx = st.bus.subscribe();
    let recent = {
        let q = st.recent.read().await;
        q.iter().filter(|e| e.get("kind").and_then(|k| k.as_str()) == Some("launch")).rev().take(60).cloned().collect::<Vec<_>>()
    };
    let prelude: Vec<Result<Event, Infallible>> = std::iter::once(hello)
        .chain(recent)
        .map(|v| Ok(Event::default().data(v.to_string())))
        .collect();
    let live = futures::stream::unfold(rx, |mut rx| async move {
        match rx.recv().await {
            Ok(line) => Some((Ok(Event::default().data(line)), rx)),
            Err(_) => None,
        }
    });
    Sse::new(futures::stream::iter(prelude).chain(live)).keep_alive(KeepAlive::default())
}

async fn state(State(st): State<Arc<BoardState>>) -> impl IntoResponse {
    let mut h = hello(&st).await;
    h["ethUsd"] = json!(crate::alerts::eth_usd().await);
    h["recent"] = json!(st.recent.read().await.iter().rev().take(100).cloned().collect::<Vec<_>>());
    Json(h)
}

async fn start(State(st): State<Arc<BoardState>>) -> impl IntoResponse {
    st.engine.resume();
    let _ = st.bus.send(json!({"kind":"paused","paused":false}).to_string());
    Json(json!({"paused": false}))
}

async fn stop(State(st): State<Arc<BoardState>>) -> impl IntoResponse {
    st.engine.pause();
    let _ = st.bus.send(json!({"kind":"paused","paused":true}).to_string());
    Json(json!({"paused": true}))
}

async fn close(State(st): State<Arc<BoardState>>, Path(id): Path<String>) -> impl IntoResponse {
    st.engine.request_close(id);
    Json(json!({"ok": true}))
}

async fn rules_edit(State(st): State<Arc<BoardState>>, Json(body): Json<Value>) -> impl IntoResponse {
    let mut changed = serde_json::Map::new();
    let bounds = [
        ("minScore", 0.0, 100.0),
        ("maxOpenPositions", 0.0, 20.0),
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
    let _ = st.bus.send(json!({"kind":"rules","rules": view, "changed": changed}).to_string());
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
        "positions": positions_view(),
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

fn positions_view() -> Value {
    let store = PositionStore::open("data");
    let now = now_ms() / 1000;
    json!(store
        .load()
        .into_iter()
        .filter(|p| p.status == "open" || p.exits.last().map(|e| e.at + 900 > now).unwrap_or(false))
        .map(|p| {
            let entry: u128 = p.entry_eth.parse().unwrap_or(0);
            let last: u128 = if p.status == "open" { p.last_eth.parse().unwrap_or(entry) } else { p.exits.last().map(|e| e.eth_out.parse().unwrap_or(0)).unwrap_or(0) };
            let pnl = if entry > 0 { (last as i128 - entry as i128) as f64 / entry as f64 * 100.0 } else { 0.0 };
            json!({"id": p.id, "token": format!("{:#x}", p.token), "symbol": p.symbol, "ethIn": p.entry_eth, "status": p.status, "pnl": pnl, "dryRun": p.dry_run, "openedAt": p.opened_at * 1000, "reason": p.exits.last().map(|e| e.reason.clone()), "closedAt": p.exits.last().map(|e| e.at * 1000)})
        })
        .collect::<Vec<_>>())
}

fn health(st: &BoardState) -> Value {
    let g = st.rpc.stats();
    json!({
        "feed": {"mode": "websocket", "lastLaunchAt": 0, "lastWsAt": now_ms(), "recoveries": 0, "note": ""},
        "lastLaunchAgeSec": null,
        "gate": {
            "active": g.active, "queued": g.queued, "inFlight": g.in_flight, "spacingMs": g.spacing_ms,
            "logsSpacingMs": g.logs_spacing_ms, "throttled": g.throttled, "coolingDown": g.cooling_down,
            "endpoints": g.endpoints.iter().map(|e| json!({"label": e.label, "logs": e.logs, "benched": e.benched})).collect::<Vec<_>>()
        },
        "spentWei": st.engine.spent().to_string()
    })
}

fn now_ms() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

#[allow(dead_code)]
fn _feed(_: FeedHealth) {}
