//! Optional sequencer feed. Send `Arbitrum-Requested-Sequence-Number` or you eat a ~2 minute backlog.
//! Verify `signatureV2` against [`crate::chain::FEED_SIGNER`]. Decode launchAndBuy / CREATE2 when present.

use crate::abi::{deployer, router};
use crate::chain::{ADDR, FEED_SIGNER};
use crate::rpc::{Lane, Rpc};
use alloy::primitives::{Address, B256, Bytes, U256};
use alloy::sol_types::SolCall;
use futures::StreamExt;
use serde_json::Value;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::Message;

#[derive(Debug, Clone)]
pub struct FeedLaunch {
    pub salt: B256,
    pub quote_in: U256,
    pub exemptions: Vec<Address>,
    pub recipient: Address,
    pub name: String,
    pub symbol: String,
    pub token: Option<Address>,
    pub curve: Option<Address>,
}

pub async fn connect_feed(url: &str, last_seq: Option<u64>) -> anyhow::Result<impl StreamExt<Item = Value>> {
    let mut req = url.into_client_request()?;
    let seq = last_seq.map(|n| n.to_string()).unwrap_or_default();
    req.headers_mut().insert("Arbitrum-Requested-Sequence-Number", seq.parse()?);
    let (ws, _) = tokio_tungstenite::connect_async(req).await?;
    Ok(ws.filter_map(|m| async move {
        let Ok(Message::Text(t)) = m else { return None };
        serde_json::from_str(&t).ok()
    }))
}

pub fn verify_signer(msg: &Value) -> bool {
    msg.get("signatureV2")
        .and_then(|s| s.get("signer"))
        .and_then(|s| s.as_str())
        .and_then(|s| s.parse::<Address>().ok())
        .is_some_and(|a| a == FEED_SIGNER)
}

pub fn decode_launch_and_buy(input: &[u8]) -> Option<FeedLaunch> {
    if input.len() < 4 || input[..4] != crate::abi::launch_and_buy_selector() {
        return None;
    }
    let d = router::launchAndBuyCall::abi_decode(input).ok()?;
    Some(FeedLaunch {
        salt: d.params.salt,
        quote_in: d.quoteIn,
        exemptions: d.snipeTaxExemptions,
        recipient: d.recipient,
        name: d.params.name,
        symbol: d.params.symbol,
        token: None,
        curve: None,
    })
}

pub async fn predict(rpc: &Rpc, salt: B256, pair: Address, config_id: U256) -> anyhow::Result<(Address, Address)> {
    let r = rpc
        .eth_call(Lane::Hot, ADDR.pons_deployer, deployer::predictLaunchAddressesCall { salt, pairToken: pair, launchConfigId: config_id }, None)
        .await?;
    Ok((r.token, r.curve))
}

#[allow(dead_code)]
fn _bytes(_: Bytes) {}
