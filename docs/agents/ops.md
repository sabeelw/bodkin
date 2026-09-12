# Ops for agents

## Build and test

```sh
# from the checkout
cargo test --locked --offline --workspace # no network, 104 tests
cargo fmt --all -- --check
cargo clippy --locked --offline --workspace --all-targets --all-features -- -D warnings
cargo bench --locked --offline --workspace --bench hot_paths
cargo audit                                 # no known vulnerabilities; reports upstream maintenance warnings
cargo run --release -- doctor
cargo run --release -- doctor --probe   # hits chain + sequencer IPs + dummy Conditional
```

Foundry (real binary, not `/usr/bin/forge` if that is ZOE):

```sh
cd contracts
~/.foundry/bin/forge test
~/.foundry/bin/forge build
```

Windows launchers `start-hunt.cmd` / `start-snipe.cmd` / `start-board.cmd` run `cargo build --release` then `bodkin.exe`.

`rust-toolchain.toml` pins Rust 1.91.0 with rustfmt and clippy. CI also tests current stable.

Binary path is `$CARGO_TARGET_DIR/release/bodkin` when that env is set, else `./target/release/bodkin`. Prefer `cargo run --release -- …` in this environment.

## `.env`

Copy `.env.example` → `.env`. Empty `RPC_URL` / `RPC_WS_URL` uses the public pair. No key needed for doctor, hunt, watch, scan, fees, dev, positions, outcomes, replay, or any dry run.

| Needed for `--live` / money movement | |
|---|---|
| `PRIVATE_KEY` | signer |
| `HELPER_ADDRESS` | after `bodkin helper deploy --live` |

`bodkin helper check` verifies bytecode at `HELPER_ADDRESS`.

## Useful commands

```sh
bodkin hunt --for 60
bodkin board                    # http://127.0.0.1:4663 , engine starts paused
bodkin snipe --for 3600         # hour dry run; read data/outcomes.jsonl after
bodkin snipe --live             # prints address/balance/limits; type arm
bodkin helper deploy --live
bodkin helper check
bodkin outcomes
bodkin replay --hours 6         # needs captured launch snapshots and RPC logs; never invents missing data
```

`--yes` skips `arm` (systemd only, after you have armed by hand once).

`data/bodkin.redb` is authoritative and exclusively locked by an engine or live command. `positions.json` and `transactions.json` are exports and one-time legacy import sources; editing them after redb has state has no effect.

## Board check

Engine starts **stopped**. Launches still score. Firing begins on **start demo** (dry) or **arm live sniping** (`--live`).

```sh
curl -s http://127.0.0.1:4663/api/state | python3 -m json.tool
# POST /api/start  /api/stop  /api/rules  /api/close/<id>
```

SSE kinds include `hello`, lag-triggered `resync`, `tick` (10 s), `launch`, `hold`, `entry`, `fire`, `mark`, `exit`, `exit_error`, `close_error`, `engine_error`, `paused`, `rules`, and `index`.

If you use a browser tool: lock the existing tab, click through start/stop and a launch drawer, do not trust a single screenshot (the webfont tofu in Cursor’s browser). Unlock when done.

A 0.05 ETH dry budget admits five 0.01 simulations. Live admission reserves worst-case burst gas, then commits actual mined gas after reconciliation; unresolved attempts retain the full reservation. Later launches must report a budget refusal and stop firing.

## Doctor --probe expectations

- Chain 4663
- Live tax 9900 / 618 / 19 / 0
- Some launches in the last ~3000 blocks (tempo is tens to low hundreds per 5 min)
- Three sequencer IPs in us-east-2 with an RTT
- Dummy Conditional: immediate reject. `-32003` is the “reject-not-wait” you want. `-32000 typed transaction too short` means the dummy payload was too short, not that Conditional queued
- v4 quoter answers on a recent graduation
- `HELPER_ADDRESS` unset is normal before deploy

## Live path (user does this, you do not surprise-arm)

1. Hour dry `snipe` / board. Tune `BURST_LEAD_MS` from outcomes.
2. `helper deploy --live`, set `HELPER_ADDRESS`.
3. Tiny `--live` session on a fresh wallet that holds only the budget.
4. us-east-2 later. Chrony → `169.254.169.123`. Board via SSH tunnel, not a public port.

## Verification after a code change

| Change | Prove it |
|---|---|
| tax / curve / score / decide / exits | `cargo test --workspace` |
| helper | `forge test` in `contracts/` |
| send / burst | canonical receipt, spray-priority and recovery tests; do not add a live Conditional send |
| performance | `cargo bench --bench hot_paths`; compare the same toolchain/machine/dataset hash |
| board HTML / SSE | in-process snapshot/idempotency tests plus keyboard/mobile browser checks; `/api/state` counters match the page |
| docs | invariants here still match `engine.rs` / `score.rs` / `chain.rs` |

Do not declare a UI change done from a screenshot alone.
