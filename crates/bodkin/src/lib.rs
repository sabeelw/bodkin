//! Bodkin: a local, non-custodial pons v2 sniper for Robinhood Chain.
//!
//! Library surface exists so the CLI and the oracle tests share one implementation.

pub mod abi;
pub mod alerts;
pub mod board;
pub mod chain;
pub mod config;
pub mod engine;
pub mod fmt;
pub mod links;
pub mod outcomes;
pub mod pons;
pub mod replay;
pub mod rpc;
pub mod run;
pub mod score;
pub mod style;
pub mod trade;
pub mod view;

pub use chain::{ADDR, CHAIN_ID, ZERO};
pub use config::Config;
pub use engine::{decide, live_gate, rules_from_env, Decision, SnipeRules};
pub use score::{score_launch, Score, ScoreContext, Verdict};
