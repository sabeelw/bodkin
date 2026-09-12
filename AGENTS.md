# Agent context

Bodkin is a **local, non-custodial pons v2 sniper** for **Robinhood Chain (id 4663)**. One Rust binary (`alloy` + `tokio`). No Node, no `package.json`, no viem. Dry run unless `--live`.

Read this file first, then the page that matches the work:

| If you are… | Read |
|---|---|
| changing tax, score, exits, graduation, or sequencer send | [docs/agents/invariants.md](docs/agents/invariants.md) |
| finding a module or adding a command | [docs/agents/code-map.md](docs/agents/code-map.md) |
| compiling, using Alloy, or “porting” old Node behavior | [docs/agents/pitfalls.md](docs/agents/pitfalls.md) |
| running, testing, or checking the board | [docs/agents/ops.md](docs/agents/ops.md) |

Human product docs (do not duplicate; do not contradict):

- [docs/STRATEGY.md](docs/STRATEGY.md) — score, rules, exits
- [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) — process shape
- [docs/COMMANDS.md](docs/COMMANDS.md) — flags and `.env` (the `snipe` *loop* paragraph is stale; trust `engine.rs` / `run.rs` / STRATEGY)
- [docs/BOARD.md](docs/BOARD.md), [docs/SAFETY.md](docs/SAFETY.md), [docs/DEPLOY.md](docs/DEPLOY.md)

Index of these files: [docs/agents/README.md](docs/agents/README.md).

## Hard rules

1. **Do not race the first block.** Default entry is the first block of Unix second `launchedAt + 2` (19 bps). A ceiling in `[19, 617]` is the same rule.
2. **Do not put Conditional txs on the fire path.** `eth_sendRawTransactionConditional` is `doctor --probe` only.
3. **Do not claim the graduated pool holds 28.57%.** Reserved is 28.57%. The v4 pool gets **20.41% + 4.2 ETH**. 8.16% is locked.
4. **Do not drop `address(0)` from declared exemption count.** Length is the declared bundle, zeros included. Node fixtures used `[A, B, ZERO, ZERO]` → count 4.
5. **Do not port Node fail-opens.** Missing launch tx is a refuse. Tax math must not `catch → 0`. Do not fire `decide` on a null tx.
6. **Do not call the official pons HTTP API.** It is v1-only. Deployer history is an in-memory index (~2 days / ~1.73M blocks).
7. **Do not send through Alloy.** Sequencer submitter is reqwest HTTP/1.1 (`TCP_NODELAY`, 15 s timeout), IP pin + same-nonce spray. Priority fee is 0.
8. **Do not add a mempool, Timeboost, or gas auction.** There is none. FCFS. `eth_sendRawTransaction` blocks until the block is built (~100 ms).
9. **One engine per `data/` directory.** `data/bodkin.redb` is authoritative and its exclusive redb lock makes concurrent `snipe`, `board`, or live commands refuse; JSON files are exports.
10. **Never commit `.env` or provider keys.** `.env.example` must stay key-free. `PRIVATE_KEY` is live-only.

## Code conventions

- Workspace rustc **≥ 1.91**, edition **2024**. `main.rs` imports `bodkin::…`. Library modules import `crate::…`.
- Addresses live in `crates/bodkin/src/chain.rs`. `bodkin doctor` re-checks them against the live factory.
- Helper: `contracts/src/BodkinBuyOnce.sol`. Committed bytecode: `crates/bodkin/bytecode/BodkinBuyOnce.json`.
- Python, if any: `uv`, not pip.

## Do not

- Recreate completed rewrite todos or edit any leftover plan file.
- Reintroduce Node / npm / viem / `legacy/`.
- Switch a dry board to live from the page. `--live` is a process flag.
- Open inbound ports for the board. It binds `127.0.0.1` only.
