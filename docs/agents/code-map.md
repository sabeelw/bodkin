# Code map

One workspace member: `crates/bodkin`. Binary `src/main.rs`, library `src/lib.rs`. Tests in `crates/bodkin/tests/`.

```
Cargo.toml                 workspace, rust-version 1.91, edition 2024
rust-toolchain.toml        channel = stable
crates/bodkin/src/
  main.rs                  clap: doctor hunt board snipe watch scan fees
                           dev buy sell positions wallet claim helper
                           outcomes replay. Imports bodkin:: not crate::
  lib.rs                   pub mods + re-exports
  chain.rs                 CHAIN_ID, ADDR, sequencer/feed defaults, grad constants
  config.rs                dotenvy iterator + validated env overlay; no process-global mutation
  rpc.rs                   lanes hot > enrich > background; BackON retry; endpoint bench
  abi.rs                   alloy sol! ABIs
  score.rs                 score_launch, Verdict
  engine.rs                SnipeRules, decide, live_gate, pick_exit
  run.rs                   engine loop: burst fire, 1 s marks, SSE emit
  board.rs                 axum 127.0.0.1 + SSE + /api/*
  pons/                    curve math, tax, clock, launches, enrich, stream,
                           fingerprint, deployer, fees
  trade/                   wallet, exec, journal, state, positions, submitter,
                           burst, curve, pool, v4
  outcomes.rs / replay.rs       typed provenance, causal reserve replay, manifests
  view.rs / links.rs / fmt.rs / style.rs; alerts.rs is ETH/USD only
crates/bodkin/bytecode/    committed BodkinBuyOnce.json
crates/bodkin/tests/       curve.rs + oracle.rs (no network)
contracts/                 Foundry BodkinBuyOnce.sol + tests
web/board.html             board UI (vanilla). Positions read p.pnl / e.pnlPct
docs/                      human product docs
start-*.cmd                Windows: cargo build --release then bodkin.exe
```

## Who owns what

| Job | File |
|---|---|
| Chain id, contract addresses, explorer, feed signer | `chain.rs` |
| Tax staircase | `pons/tax.rs` (`snipe_tax_bps`) |
| Integer curve quote / minOut-as-last | `pons/curve.rs` |
| TokenLaunched watch, watchdog | `pons/launches.rs`, `pons/stream.rs` |
| Multicall3 enrich, exemptions, socials | `pons/enrich.rs` |
| Farm fingerprint | `pons/fingerprint.rs` |
| Deployer index (~2 days) | `pons/deployer.rs` |
| Fee escrow forensics | `pons/fees.rs` |
| newHeads clock | `pons/clock.rs` |
| Score | `score.rs` |
| Rules, decide, live gate, pick_exit | `engine.rs` |
| Fire + manage loop, outcomes emit | `run.rs` |
| Sequencer HTTP send, IP pin, spray | `trade/submitter.rs` |
| Pre-sign burst | `trade/burst.rs` |
| Curve buy / helper call | `trade/curve.rs` |
| sell_anywhere, value_now | `trade/pool.rs` |
| v4 key / quoter / router layout | `trade/v4.rs` |
| Sign type-2 locally (no fillers) | `trade/wallet.rs` |
| Receipt-backed execution | `trade/exec.rs` |
| Durable operation recovery | `trade/journal.rs` |
| Authoritative redb state/process lock | `trade/state.rs` |
| Position accounting + JSON export | `trade/positions.rs` |
| Board HTTP/SSE | `board.rs` + `web/board.html` |
| CLI surface | `main.rs` |

## Data files (gitignored `data/`)

| Path | Writer |
|---|---|
| `data/bodkin.redb` | engine/live commands: authoritative positions + operation journal |
| `data/positions.json` | atomic export from redb; legacy import only when redb has no snapshot |
| `data/transactions.json` | best-effort operation export; legacy import only when redb has no operations |
| `data/launches.jsonl` | `hunt` |
| `data/outcomes.jsonl` | engine observations for outcomes/replay |

redb's exclusive database lock enforces one engine or live command against a given `data/` at a time.

## Helper bytecode lookup (in order)

1. `BODKIN_HELPER_BYTECODE` / `BODKIN_HELPER_RUNTIME_BYTECODE` env (advanced custom artifact)
2. `contracts/out/BodkinBuyOnce.sol/BodkinBuyOnce.json` after `forge build`
3. embedded `crates/bodkin/bytecode/BodkinBuyOnce.json` (committed)

Deployment succeeds only after the receipt contract address and exact runtime bytecode match.

`contracts/lib/` and `contracts/out/` are gitignored. `forge install foundry-rs/forge-std --no-git`.

## Board API (loopback only)

| Method | Path |
|---|---|
| GET | `/`, `/events`, `/api/state` |
| POST | `/api/start`, `/api/stop` (also `/api/resume`, `/api/pause`) |
| POST | `/api/close/<positionId>` |
| POST | `/api/rules` — only minScore, maxOpen, max tax, dev share, exempt wallets |

No buy-on-demand route. `--live` is not a button.

## Tests you must not break

`cargo test --workspace` (104 tests in the current offline suite):

- Curve quote matches the 3.00% dev buy that 0.0535 ETH gives on a fresh curve
- Live tax staircase 9900 / 618 / 19 / 0
- Score on a builder-shaped launch and a serial deployer
- Sniper refusals, farm fingerprint, session budget, unreadable launch
- Limiter, deployer index, burst pause/nonce reconciliation
- Ladder / stale / insider
- v4 ETH-is-currency0
- Links

`~/.foundry/bin/forge test` in `contracts/`: too-early, already-bought, happy path + refund, reserve cap.

## Docs vs code

The current loop is documented in [docs/COMMANDS.md](../COMMANDS.md): detect → strict enrichment → atomic admission → flow gate at +2 − lead → sequencer burst → reconcile every attempted receipt → 1 s marks for the first minute, then every MANAGE_SLOW_SEC (default 5) → ladder / stale / insider / graduation-boundary exits, with TP/SL/trail/max-hold only after graduation. Do not restore the deleted Node poll-and-assume loop.
