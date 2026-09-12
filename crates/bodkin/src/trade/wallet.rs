use crate::chain::CHAIN_ID;
use crate::pons::clock::ChainClock;
use alloy::consensus::{SignableTransaction, TxEip1559, TxEnvelope};
use alloy::eips::eip2718::Encodable2718;
use alloy::network::TxSignerSync;
use alloy::primitives::{Address, Bytes, TxKind, B256, U256};
use alloy::signers::local::PrivateKeySigner;
use std::str::FromStr;
use std::sync::atomic::{AtomicU64, Ordering};

pub struct Wallet {
    signer: PrivateKeySigner,
    nonce: AtomicU64,
    gas_limit: AtomicU64,
}

impl Wallet {
    pub fn from_env() -> anyhow::Result<Option<Self>> {
        let Some(raw) = crate::config::env_str("PRIVATE_KEY") else { return Ok(None) };
        let hex = raw.strip_prefix("0x").unwrap_or(&raw);
        let signer = PrivateKeySigner::from_str(&format!("0x{hex}"))?;
        Ok(Some(Self { signer, nonce: AtomicU64::new(0), gas_limit: AtomicU64::new(350_000) }))
    }

    pub fn require() -> anyhow::Result<Self> {
        Self::from_env()?.ok_or_else(|| anyhow::anyhow!("PRIVATE_KEY is not set in .env; live trades need a signer (dry-run does not)"))
    }

    pub fn address(&self) -> Address {
        self.signer.address()
    }

    pub fn set_nonce(&self, n: u64) {
        self.nonce.store(n, Ordering::SeqCst);
    }

    pub fn peek_nonce(&self) -> u64 {
        self.nonce.load(Ordering::SeqCst)
    }

    pub fn take_nonces(&self, n: u32) -> Vec<u64> {
        let start = self.nonce.fetch_add(n as u64, Ordering::SeqCst);
        (0..n).map(|i| start + i as u64).collect()
    }

    pub fn set_gas_limit(&self, g: u64) {
        self.gas_limit.store(g, Ordering::SeqCst);
    }

    pub fn gas_limit(&self) -> u64 {
        self.gas_limit.load(Ordering::SeqCst)
    }

    /// Type-2, priority 0, max_fee = 3 × cached base. No fillers.
    pub fn sign_eip1559(&self, to: Address, value: U256, input: Bytes, nonce: u64, clock: &ChainClock) -> anyhow::Result<(B256, Bytes)> {
        let base = clock.base_fee().max(1);
        let max_fee = (base as u128).saturating_mul(3);
        let mut tx = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce,
            gas_limit: self.gas_limit(),
            max_fee_per_gas: max_fee,
            max_priority_fee_per_gas: 0,
            to: to.into(),
            value,
            access_list: Default::default(),
            input,
        };
        let sig = self.signer.sign_transaction_sync(&mut tx)?;
        let signed = tx.into_signed(sig);
        let hash = *signed.hash();
        let envelope = TxEnvelope::Eip1559(signed);
        Ok((hash, Bytes::from(envelope.encoded_2718())))
    }

    pub fn sign_create(&self, bytecode: Bytes, nonce: u64, clock: &ChainClock) -> anyhow::Result<(B256, Bytes)> {
        let base = clock.base_fee().max(1);
        let max_fee = (base as u128).saturating_mul(3);
        let mut tx = TxEip1559 {
            chain_id: CHAIN_ID,
            nonce,
            gas_limit: 800_000,
            max_fee_per_gas: max_fee,
            max_priority_fee_per_gas: 0,
            to: TxKind::Create,
            value: U256::ZERO,
            access_list: Default::default(),
            input: bytecode,
        };
        let sig = self.signer.sign_transaction_sync(&mut tx)?;
        let signed = tx.into_signed(sig);
        let hash = *signed.hash();
        let envelope = TxEnvelope::Eip1559(signed);
        Ok((hash, Bytes::from(envelope.encoded_2718())))
    }
}
