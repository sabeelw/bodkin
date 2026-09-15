# Safety

## Custody

- The only secret is `PRIVATE_KEY` in your own `.env`. It is read by `trade/wallet.rs` and used to sign transactions sent to the sequencer
  (or the fallback RPC). It is never printed, logged, written to `data/`, or sent anywhere else.
- Use a fresh wallet with only what you are willing to lose in a session. Bodkin never asks for more than one buy at a time.
- `.env` is git-ignored. `data/bodkin.redb` contains positions and transaction metadata, never keys. `positions.json` and `transactions.json` are human-readable exports with addresses and amounts.

## Dry run is the default

`snipe`, `buy`, `sell`, `claim` and `board` quote and log without sending unless you pass `--live`. Dry `snipe` accepts a paper entry only when it captured canonical curve state during second +1 or +2, then applies the configured +2 tax. Marks and exits use the live chain without injecting the hypothetical buy's reserve impact, so dry PnL is useful for rejecting a strategy but cannot prove exact live profitability. The board goes one step
further: even in dry run it opens as a feed and fires nothing until you press start.

## Four walls around a live session

1. **The confirmation.** `--live` prints the signer, balance, buy size, worst-case burst-gas reservation, position cap, and session budget. It refuses unless balance and budget cover one buy plus that gas reserve, then waits for `arm`; the live board also needs its button.
2. **The buy size** (`--eth`, `SNIPE_ETH`): what one entry costs. Default 0.01 ETH.
3. **The position cap** (`--max-open`): how many entries can be open at once. Default 3.
4. **The session budget** (`--budget`, `SNIPE_BUDGET_ETH`): entry value plus conservative worst-case gas reserved for every live burst. Resolved receipts replace the reservation with actual entry value and mined gas; unresolved attempts retain the full reservation. Default 0.05 ETH. Once exhausted, later launches refuse regardless of score.

Start with a fresh wallet holding the budget and nothing else. Raise the numbers after you have watched the exits work for a session.

## What can still go wrong with `--live`

| Risk | What bodkin does | What it cannot do |
|---|---|---|
| the curve graduates between quote and send | `minTokensOut` bounds the rate; a clamped fill at that rate settles, a worse one reverts | recover gas spent on a revert |
| a launch is a honeypot on the pool side | the curve itself is protocol code; the pons v4 hook is the same singleton for every launch | guarantee a token's *pool* behaves if the protocol changes |
| a public RPC rate-limits or challenges the client | one lane-aware gate for every request, reserved hot capacity, endpoint penalties, and bounded BackON retries with jitter and `Retry-After`; websocket detection is reconciled by polling | make a public endpoint faster; set `RPC_URL` / `RPC_WS_URL` to a provider |
| a submitted transaction has no definitive receipt | records hash/nonce before sending, retains risk reservations, and blocks execution until recovery | prove failure merely from a timeout |
| a receipt or launch is reorged | verifies receipt block hashes, rechecks applied operations for 64 blocks, reverses removed flow logs, and rebuilds changed cursors | prevent the chain itself from reorganizing |
| the sequencer's compliance filter voids a transaction | none; it is protocol-level | anything |
| stop-loss fires into a thin curve | marks are real quotes for the full position size, so the exit price is what the mark showed | avoid slippage on an illiquid curve |
| a launch farm passes every per-launch rule | the fingerprint rule refuses the third identical launch inside half an hour | catch a farm that varies its numbers |
| you relax `maxExemptWallets` | prints the exact number of exempt wallets and their addresses in `scan` and the drawer | tell you who they are |

## What the board can and cannot do

It listens on 127.0.0.1 only. It can pause and resume firing, close an open position at the current quote, and change five numeric rules
inside fixed bounds. It cannot buy on demand and cannot switch a dry run to live: `--live` is decided when you start it. Anyone on your
machine can open it; nobody outside can. Start/resume refuses while the chain clock is stale or unstable, close retries are idempotent, and an SSE lag forces a fresh authoritative snapshot. If several people share the machine, start it with a different `--port` and assume they can click.

## Fees and taxes you pay on every trade

- Curve: 1 % base fee plus the creator tax (0–10 %, shown per launch) on the input of a buy and the output of a sell.
- Pool: the pons hook takes 1 % plus the creator tax from the unspecified currency of each swap; the pool's own LP fee is 0.
- Opening tax: 99 % decaying to 0 over 3 s on buys only. Bodkin waits it out; if you call `buy` by hand in the first second, you pay it.

## Not investment advice

Bodkin reads state and executes rules you configured. It has no opinion about any token, and neither does this repository.
