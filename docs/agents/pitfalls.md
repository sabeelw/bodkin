# Pitfalls

## This is not a Node repo

The TypeScript/viem app was deleted after `cargo test` matched its fixtures. There is no `legacy/`, no root `package.json`. A “port this helper to viem” or “add npm scripts” change is a regression.

Do **not** port these Node behaviors. They were fail-opens or races:

| Old Node behavior | What Rust must do |
|---|---|
| `catch` on tax read → treat tax as 0 | Fail the read. Never fire as if tax is gone |
| `decide()` with a null launch tx | Refuse: `unreadable: launch tx missing` |
| Overlapping `manage()` | `managing` swap; skip the tick if already in |
| Watchdog that only reads `head` after 45 s silence | Walk **last-seen → head** |
| 11 separate `eth_call`s per launch | One Multicall3 |
| Per-send `walletClient()` / viem nonce-gas fillers | Local type-2: nonce, gas, max fee = 3× cached base, priority 0, chain 4663. `PrivateKeySigner::sign_transaction_sync` |
| 4 s receipt poll on the fire path | `sendRaw` already waits for the built block. Sync only after fill |
| Telegram +3 | Telegram +8, same as website |
| Short deployer window | ~2 days / ~1.73M blocks |
| On-curve stop-loss | Stale + insider. SL only post-grad |
| “Pool gets 28.57%” | 20.41% + 4.2 ETH; 8.16% locked |
| Dropping `address(0)` from exemption arrays | Keep declared length |
| Conditional `timestampMin` on send | Probe only |
| ponsfamily HTTP for deployer stats | In-memory index |

## rustc / Alloy

Alloy 1.8 on the 1.0 umbrella needs **rustc ≥ 1.91**. Workspace `rust-version = "1.91"`. `rustup update stable` if the box is on 1.86.

```toml
# correct for Bodkin: ABI/signing/RPC types plus websocket provider, no contract bindings
alloy = { version = "1.0", default-features = false, features = ["std", "consensus", "eips", "network", "rpc-types", "signer-local", "sol-types"] }
# wrong — `full` restores unused contract/IPC/trace/txpool/debug/Anvil/KZG branches
alloy = { version = "1.0", features = ["full"] }
```

Alloy 1.8 API notes that already bit this repo:

- `ProviderBuilder::connect_ws`, not `on_ws`
- `log.address()` is a **method**, not a field
- Pool keys use `U24` / `I24`
- No `Address::into_word()`. Use `B256::left_padding_from(addr.as_slice())`
- `std::env::set_var` is `unsafe` on 1.87+. Only used at process start in `config.rs`. Do not scatter more `set_var`
- Edition 2024 / pinned rustc 1.91: `Arc` is **not** `Copy`. Clone the `Arc`s *before* moving them into spawn closures
- Open `trade/state.rs::StateDb` once and pass it to `PositionStore::from_state` and `TxJournal::from_state`. redb intentionally refuses a second process/open writer.
- `positions.json` and `transactions.json` are exports after migration. Editing them does not modify authoritative redb state.

## Foundry on PATH

`/usr/bin/forge` on some boxes is **ZOE 2013**, not Foundry. Real forge: `~/.foundry/bin/forge` (v1.8.x). `contracts/README.md` assumes the Foundry one.

```sh
~/.foundry/bin/forge install foundry-rs/forge-std --no-git
~/.foundry/bin/forge test
~/.foundry/bin/forge build
```

## Where the binary is

Do not assume `./target/release/bodkin` exists. Cursor/sandbox often sets `CARGO_TARGET_DIR` to a cache dir. Use:

```sh
cargo run --release -- <command>
# or
"$CARGO_TARGET_DIR/release/bodkin" <command>
```

## Resolved and remaining runtime observations (2026-09-12)

A prior live dry board session saw 119 launches and 5 simulated fires; the budget cap worked.

1. **Resolved: SSE PnL was stuck at +0.0%.** Marks and exits now emit basis-aware PnL, realized leg/total wei, remaining inventory, and closure state. The board rebuilds cumulative realized PnL from authoritative position snapshots and applies absolute, idempotent exit totals.

2. **Still requires an authorized dry-chain investigation: HASHDOGE / AMBR marked ≈ −99%** then stale-exited. CASTLEDAO was +1.5%, DUCH −8.5%, $RPGE −14.3% on the same rules. Treat −99% as either a dead curve or a bad historical quote until measured; do not infer a live-ready strategy from the offline suite.

3. **Resolved: feed health could remain zero while launches flowed.** The engine and board now share the actual `FeedHealth` cell and expose stopped/feed/RPC state.

4. **Environment-only: embedded-browser webfont tofu.** The accessible tree and normal Chrome rendering were previously fine. Do not replace the typeface to work around one embedded browser.

## Secrets

An older `.env.example` once contained a live Alchemy WSS URL. That line is gone. If that key was ever used, rotate it. Never put provider URLs with keys in example files.

## Hosting

Develop and dry-run on the laptop first. us-east-2 later ([docs/DEPLOY.md](../DEPLOY.md)). Distance to the sequencer *is* queue position. Chrony against `169.254.169.123` on the AWS box.
