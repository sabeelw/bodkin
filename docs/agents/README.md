# Agent docs

These pages are for other coding agents. They hold rewrite decisions, do-not-regress facts, and toolchain traps that the human docs assume you already know.

Start at the repo-root [AGENTS.md](../../AGENTS.md). Then open one of:

| File | Contents |
|---|---|
| [invariants.md](invariants.md) | Chain, tax staircase, score, exits, sequencer, exemptions. Changing any of these without reading this is how the bot regresses. |
| [code-map.md](code-map.md) | Which file owns which job. |
| [pitfalls.md](pitfalls.md) | Alloy / rustc / redb / Foundry landmines, Node behavior you must not port, resolved and remaining runtime observations. |
| [ops.md](ops.md) | Build, test, doctor, board, data files, where the binary actually lands. |

Human docs stay the source for *why a default is 60 not 70*. These files stay the source for *what you must not undo*.
