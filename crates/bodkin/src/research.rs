use alloy::primitives::{Address, B256, U256};
use atomic_write_file::AtomicWriteFile;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

pub const INITIAL_CAPTURE_MAX_SECONDS: u64 = 86_400;
pub const INITIAL_CAPTURE_MAX_BYTES: u64 = 1_073_741_824;
pub const DEFAULT_STARTING_EQUITY_WEI: u64 = 50_000_000_000_000_000;
pub const DEFAULT_COMMITMENT_BPS: u64 = 200;
pub const DEFAULT_DRAWDOWN_BPS: u64 = 1_000;
pub const DEFAULT_MAX_OPEN_POSITIONS: usize = 3;
const CAPTURE_MANIFEST_RESERVE_BYTES: u64 = 4_096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GraduationPolicy {
    ExitBeforeGraduation,
    HoldThroughGraduation,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchProfile {
    pub name: String,
    pub starting_equity_wei: U256,
    pub max_commitment_bps: u64,
    pub max_drawdown_bps: u64,
    pub max_open_positions: usize,
    pub entry_attempts: u8,
    pub entry_gas_limit: u64,
    pub exit_gas_limit: u64,
    pub max_fee_multiplier_bps: u64,
    pub min_taxed_buyers_s1: u32,
    pub inactivity_seconds: Option<u64>,
    pub max_curve_hold_seconds: Option<u64>,
    pub graduation_policy: GraduationPolicy,
}

impl Default for ResearchProfile {
    fn default() -> Self {
        Self {
            name: "risk-normalized-v1".into(),
            starting_equity_wei: U256::from(DEFAULT_STARTING_EQUITY_WEI),
            max_commitment_bps: DEFAULT_COMMITMENT_BPS,
            max_drawdown_bps: DEFAULT_DRAWDOWN_BPS,
            max_open_positions: DEFAULT_MAX_OPEN_POSITIONS,
            entry_attempts: 1,
            entry_gas_limit: 350_000,
            exit_gas_limit: 500_000,
            max_fee_multiplier_bps: 30_000,
            min_taxed_buyers_s1: 0,
            inactivity_seconds: None,
            max_curve_hold_seconds: None,
            graduation_policy: GraduationPolicy::ExitBeforeGraduation,
        }
    }
}

impl ResearchProfile {
    pub fn validate(&self) -> Result<(), ResearchError> {
        if self.name.trim().is_empty() {
            return Err(ResearchError::InvalidProfile(
                "profile name is empty".into(),
            ));
        }
        if self.starting_equity_wei.is_zero() {
            return Err(ResearchError::InvalidProfile(
                "starting equity must be positive".into(),
            ));
        }
        if !(1..=10_000).contains(&self.max_commitment_bps) {
            return Err(ResearchError::InvalidProfile(
                "commitment bps must be 1..=10000".into(),
            ));
        }
        if !(1..=10_000).contains(&self.max_drawdown_bps) {
            return Err(ResearchError::InvalidProfile(
                "drawdown bps must be 1..=10000".into(),
            ));
        }
        if self.max_open_positions == 0 {
            return Err(ResearchError::InvalidProfile(
                "max open positions must be positive".into(),
            ));
        }
        if !matches!(self.entry_attempts, 1 | 2 | 4 | 8) {
            return Err(ResearchError::InvalidProfile(
                "entry attempts must be one of 1, 2, 4, or 8".into(),
            ));
        }
        if self.entry_gas_limit == 0 || self.exit_gas_limit == 0 || self.max_fee_multiplier_bps == 0
        {
            return Err(ResearchError::InvalidProfile(
                "gas limits and fee multiplier must be positive".into(),
            ));
        }
        Ok(())
    }

    pub fn modeled_entry_gas(&self, base_fee_wei: u64) -> Result<U256, ResearchError> {
        modeled_gas(
            self.entry_gas_limit,
            u64::from(self.entry_attempts),
            base_fee_wei,
            self.max_fee_multiplier_bps,
        )
    }

    pub fn modeled_exit_gas(&self, base_fee_wei: u64) -> Result<U256, ResearchError> {
        modeled_gas(
            self.exit_gas_limit,
            1,
            base_fee_wei,
            self.max_fee_multiplier_bps,
        )
    }

    pub fn entry_value_for_equity(
        &self,
        equity_wei: U256,
        base_fee_wei: u64,
    ) -> Result<Option<U256>, ResearchError> {
        let ceiling = bps_of(equity_wei, self.max_commitment_bps)?;
        Ok(ceiling.checked_sub(self.modeled_entry_gas(base_fee_wei)?))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaptureLimits {
    pub duration_seconds: u64,
    pub max_bytes: u64,
}

impl Default for CaptureLimits {
    fn default() -> Self {
        Self {
            duration_seconds: INITIAL_CAPTURE_MAX_SECONDS,
            max_bytes: INITIAL_CAPTURE_MAX_BYTES,
        }
    }
}

impl CaptureLimits {
    pub fn validate(self) -> Result<Self, ResearchError> {
        if self.duration_seconds == 0 || self.duration_seconds > INITIAL_CAPTURE_MAX_SECONDS {
            return Err(ResearchError::InvalidCaptureLimits(
                "capture duration must be 1..=86400 seconds".into(),
            ));
        }
        if self.max_bytes <= CAPTURE_MANIFEST_RESERVE_BYTES
            || self.max_bytes > INITIAL_CAPTURE_MAX_BYTES
        {
            return Err(ResearchError::InvalidCaptureLimits(
                "capture size must be 4097..=1073741824 bytes".into(),
            ));
        }
        Ok(self)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchEventKind {
    Launch,
    Refusal,
    Flow,
    Outcome,
    Coverage,
    Missing,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CanonicalObservation {
    pub block_number: u64,
    pub block_hash: B256,
    pub block_timestamp: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchEnvelope {
    pub schema: u32,
    pub run_id: String,
    pub sequence: u64,
    pub chain_id: u64,
    pub version: String,
    pub observed_at_ms: u64,
    pub kind: ResearchEventKind,
    pub canonical: Option<CanonicalObservation>,
    pub data: Value,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CaptureStopReason {
    Requested,
    DurationLimit,
    SizeLimit,
    SourceClosed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CaptureManifest {
    pub schema: u32,
    pub run_id: String,
    pub chain_id: u64,
    pub version: String,
    pub started_at_ms: u64,
    pub ended_at_ms: u64,
    pub event_count: u64,
    pub event_bytes: u64,
    pub total_bytes: u64,
    pub rolling_hash: B256,
    pub limits: CaptureLimits,
    pub stop_reason: CaptureStopReason,
    pub events_file: String,
}

#[derive(Debug, Clone)]
pub enum CaptureRecordStatus {
    Written(u64),
    Stopped(CaptureManifest),
}

pub struct ResearchRecorder {
    output: PathBuf,
    writer: Option<BufWriter<std::fs::File>>,
    limits: CaptureLimits,
    run_id: String,
    started_at_ms: u64,
    started: Instant,
    sequence: u64,
    event_bytes: u64,
    rolling_hash: B256,
    manifest: Option<CaptureManifest>,
}

impl ResearchRecorder {
    pub fn create(output: impl AsRef<Path>, limits: CaptureLimits) -> Result<Self, ResearchError> {
        let limits = limits.validate()?;
        let output = output.as_ref();
        if output.as_os_str().is_empty() {
            return Err(ResearchError::InvalidCapturePath(
                "capture output directory is empty".into(),
            ));
        }
        if output.exists() {
            return Err(ResearchError::CaptureExists(output.to_path_buf()));
        }
        let parent = output.parent().unwrap_or_else(|| Path::new("."));
        if !parent.exists() || !parent.is_dir() {
            return Err(ResearchError::InvalidCapturePath(format!(
                "capture parent does not exist: {}",
                parent.display()
            )));
        }
        std::fs::create_dir(output).map_err(|error| ResearchError::CaptureIo(error.to_string()))?;
        let events_path = output.join("events.jsonl");
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&events_path)
            .map_err(|error| ResearchError::CaptureIo(error.to_string()))?;
        let started_at_ms = crate::pons::clock::now_ms();
        Ok(Self {
            output: output.to_path_buf(),
            writer: Some(BufWriter::new(file)),
            limits,
            run_id: format!("{started_at_ms}-{}", std::process::id()),
            started_at_ms,
            started: Instant::now(),
            sequence: 0,
            event_bytes: 0,
            rolling_hash: B256::ZERO,
            manifest: None,
        })
    }

    pub fn run_id(&self) -> &str {
        &self.run_id
    }

    pub fn record(
        &mut self,
        kind: ResearchEventKind,
        canonical: Option<CanonicalObservation>,
        data: Value,
    ) -> Result<CaptureRecordStatus, ResearchError> {
        if let Some(manifest) = &self.manifest {
            return Ok(CaptureRecordStatus::Stopped(manifest.clone()));
        }
        if self.started.elapsed().as_secs() >= self.limits.duration_seconds {
            return self
                .finish(CaptureStopReason::DurationLimit)
                .map(CaptureRecordStatus::Stopped);
        }
        let envelope = ResearchEnvelope {
            schema: 1,
            run_id: self.run_id.clone(),
            sequence: self.sequence,
            chain_id: crate::chain::CHAIN_ID,
            version: env!("CARGO_PKG_VERSION").into(),
            observed_at_ms: crate::pons::clock::now_ms(),
            kind,
            canonical,
            data,
        };
        let mut encoded = serde_json::to_vec(&envelope)
            .map_err(|error| ResearchError::CaptureEncoding(error.to_string()))?;
        encoded.push(b'\n');
        let encoded_len =
            u64::try_from(encoded.len()).map_err(|_| ResearchError::ArithmeticOverflow)?;
        let next_bytes = self
            .event_bytes
            .checked_add(encoded_len)
            .and_then(|bytes| bytes.checked_add(CAPTURE_MANIFEST_RESERVE_BYTES))
            .ok_or(ResearchError::ArithmeticOverflow)?;
        if next_bytes > self.limits.max_bytes {
            return self
                .finish(CaptureStopReason::SizeLimit)
                .map(CaptureRecordStatus::Stopped);
        }
        let writer = self.writer.as_mut().ok_or(ResearchError::CaptureFinished)?;
        writer
            .write_all(&encoded)
            .map_err(|error| ResearchError::CaptureIo(error.to_string()))?;
        let mut digest_input = Vec::with_capacity(B256::len_bytes() + encoded.len());
        digest_input.extend_from_slice(self.rolling_hash.as_slice());
        digest_input.extend_from_slice(&encoded);
        self.rolling_hash = alloy::primitives::keccak256(digest_input);
        let sequence = self.sequence;
        self.sequence = self.sequence.saturating_add(1);
        self.event_bytes = self
            .event_bytes
            .checked_add(encoded_len)
            .ok_or(ResearchError::ArithmeticOverflow)?;
        Ok(CaptureRecordStatus::Written(sequence))
    }

    pub fn finish(
        &mut self,
        stop_reason: CaptureStopReason,
    ) -> Result<CaptureManifest, ResearchError> {
        if let Some(manifest) = &self.manifest {
            return Ok(manifest.clone());
        }
        let mut writer = self.writer.take().ok_or(ResearchError::CaptureFinished)?;
        writer
            .flush()
            .map_err(|error| ResearchError::CaptureIo(error.to_string()))?;
        writer
            .get_ref()
            .sync_all()
            .map_err(|error| ResearchError::CaptureIo(error.to_string()))?;
        let mut manifest = CaptureManifest {
            schema: 1,
            run_id: self.run_id.clone(),
            chain_id: crate::chain::CHAIN_ID,
            version: env!("CARGO_PKG_VERSION").into(),
            started_at_ms: self.started_at_ms,
            ended_at_ms: crate::pons::clock::now_ms(),
            event_count: self.sequence,
            event_bytes: self.event_bytes,
            total_bytes: 0,
            rolling_hash: self.rolling_hash,
            limits: self.limits,
            stop_reason,
            events_file: "events.jsonl".into(),
        };
        let encoded = loop {
            let encoded = serde_json::to_vec_pretty(&manifest)
                .map_err(|error| ResearchError::CaptureEncoding(error.to_string()))?;
            let total = self
                .event_bytes
                .checked_add(
                    u64::try_from(encoded.len()).map_err(|_| ResearchError::ArithmeticOverflow)?,
                )
                .ok_or(ResearchError::ArithmeticOverflow)?;
            if manifest.total_bytes == total {
                break encoded;
            }
            manifest.total_bytes = total;
        };
        if manifest.total_bytes > self.limits.max_bytes {
            return Err(ResearchError::CaptureManifestTooLarge);
        }
        let manifest_path = self.output.join("manifest.json");
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(manifest_path)
            .map_err(|error| ResearchError::CaptureIo(error.to_string()))?;
        file.write_all(&encoded)
            .map_err(|error| ResearchError::CaptureIo(error.to_string()))?;
        file.flush()
            .map_err(|error| ResearchError::CaptureIo(error.to_string()))?;
        file.sync_all()
            .map_err(|error| ResearchError::CaptureIo(error.to_string()))?;
        self.manifest = Some(manifest.clone());
        Ok(manifest)
    }
}

impl Drop for ResearchRecorder {
    fn drop(&mut self) {
        if self.manifest.is_none() {
            let _ = self.finish(CaptureStopReason::Requested);
        }
    }
}

pub fn verify_capture(directory: impl AsRef<Path>) -> Result<CaptureManifest, ResearchError> {
    let directory = directory.as_ref();
    let manifest_path = directory.join("manifest.json");
    let manifest_bytes = std::fs::read(&manifest_path)
        .map_err(|error| ResearchError::CaptureIo(error.to_string()))?;
    let manifest: CaptureManifest = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| ResearchError::CaptureEncoding(error.to_string()))?;
    if manifest.schema != 1
        || manifest.chain_id != crate::chain::CHAIN_ID
        || manifest.events_file != "events.jsonl"
    {
        return Err(ResearchError::InvalidCaptureManifest);
    }
    let scanned = scan_capture(directory, &manifest, |_| Ok(()))?;
    let total_bytes = scanned
        .event_bytes
        .checked_add(
            u64::try_from(manifest_bytes.len()).map_err(|_| ResearchError::ArithmeticOverflow)?,
        )
        .ok_or(ResearchError::ArithmeticOverflow)?;
    if scanned.event_count != manifest.event_count
        || scanned.event_bytes != manifest.event_bytes
        || scanned.rolling_hash != manifest.rolling_hash
        || total_bytes != manifest.total_bytes
        || total_bytes > manifest.limits.max_bytes
    {
        return Err(ResearchError::CaptureIntegrityMismatch);
    }
    Ok(manifest)
}

pub fn visit_capture(
    directory: impl AsRef<Path>,
    visitor: impl FnMut(ResearchEnvelope) -> Result<(), ResearchError>,
) -> Result<CaptureManifest, ResearchError> {
    let manifest = verify_capture(&directory)?;
    let scanned = scan_capture(directory.as_ref(), &manifest, visitor)?;
    if scanned.event_count != manifest.event_count
        || scanned.event_bytes != manifest.event_bytes
        || scanned.rolling_hash != manifest.rolling_hash
    {
        return Err(ResearchError::CaptureIntegrityMismatch);
    }
    Ok(manifest)
}

struct CaptureScan {
    event_count: u64,
    event_bytes: u64,
    rolling_hash: B256,
}

fn scan_capture(
    directory: &Path,
    manifest: &CaptureManifest,
    mut visitor: impl FnMut(ResearchEnvelope) -> Result<(), ResearchError>,
) -> Result<CaptureScan, ResearchError> {
    let path = directory.join(&manifest.events_file);
    let file =
        std::fs::File::open(path).map_err(|error| ResearchError::CaptureIo(error.to_string()))?;
    let mut reader = BufReader::new(file);
    let mut encoded = Vec::new();
    let mut event_count = 0u64;
    let mut event_bytes = 0u64;
    let mut rolling_hash = B256::ZERO;
    loop {
        encoded.clear();
        let read = reader
            .read_until(b'\n', &mut encoded)
            .map_err(|error| ResearchError::CaptureIo(error.to_string()))?;
        if read == 0 {
            break;
        }
        if encoded.last() != Some(&b'\n') {
            return Err(ResearchError::CaptureIntegrityMismatch);
        }
        let envelope: ResearchEnvelope = serde_json::from_slice(&encoded)
            .map_err(|error| ResearchError::CaptureEncoding(error.to_string()))?;
        if envelope.schema != 1
            || envelope.run_id != manifest.run_id
            || envelope.chain_id != manifest.chain_id
            || envelope.version != manifest.version
            || envelope.sequence != event_count
        {
            return Err(ResearchError::CaptureIntegrityMismatch);
        }
        let mut digest_input = Vec::with_capacity(32 + encoded.len());
        digest_input.extend_from_slice(rolling_hash.as_slice());
        digest_input.extend_from_slice(&encoded);
        rolling_hash = alloy::primitives::keccak256(digest_input);
        event_count = event_count.saturating_add(1);
        event_bytes = event_bytes
            .checked_add(u64::try_from(read).map_err(|_| ResearchError::ArithmeticOverflow)?)
            .ok_or(ResearchError::ArithmeticOverflow)?;
        visitor(envelope)?;
    }
    Ok(CaptureScan {
        event_count,
        event_bytes,
        rolling_hash,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResearchRiskState {
    Active,
    ValuationUnknown,
    Liquidating,
    Halted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResearchPosition {
    pub id: String,
    pub token: Address,
    pub tokens: U256,
    pub entry_value_wei: U256,
    pub entry_gas_wei: U256,
    pub reserved_exit_gas_wei: U256,
    pub conservative_liquidation_wei: Option<U256>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PortfolioSnapshot {
    pub cash_wei: U256,
    pub equity_wei: Option<U256>,
    pub peak_equity_wei: U256,
    pub drawdown_bps: Option<u64>,
    pub reserved_exit_gas_wei: U256,
    pub gas_spent_wei: U256,
    pub realized_proceeds_wei: U256,
    pub risk_state: ResearchRiskState,
    pub open_positions: usize,
    pub liquidation_targets: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EntrySettlement {
    pub id: String,
    pub token: Address,
    pub tokens: U256,
    pub entry_value_wei: U256,
    pub entry_gas_wei: U256,
    pub conservative_liquidation_wei: Option<U256>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Admission {
    revision: u64,
    entry_value_wei: U256,
    entry_gas_limit_wei: U256,
    exit_gas_reserve_wei: U256,
    commitment_wei: U256,
}

impl Admission {
    pub fn commitment_wei(&self) -> U256 {
        self.commitment_wei
    }

    pub fn entry_value_wei(&self) -> U256 {
        self.entry_value_wei
    }

    pub fn entry_gas_limit_wei(&self) -> U256 {
        self.entry_gas_limit_wei
    }

    pub fn exit_gas_reserve_wei(&self) -> U256 {
        self.exit_gas_reserve_wei
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResearchPortfolio {
    profile: ResearchProfile,
    cash_wei: U256,
    peak_equity_wei: U256,
    drawdown_bps: Option<u64>,
    reserved_exit_gas_wei: U256,
    gas_spent_wei: U256,
    realized_proceeds_wei: U256,
    positions: Vec<ResearchPosition>,
    risk_state: ResearchRiskState,
    drawdown_breached: bool,
    revision: u64,
}

impl ResearchPortfolio {
    pub fn new(profile: ResearchProfile) -> Result<Self, ResearchError> {
        profile.validate()?;
        Ok(Self {
            cash_wei: profile.starting_equity_wei,
            peak_equity_wei: profile.starting_equity_wei,
            profile,
            drawdown_bps: Some(0),
            reserved_exit_gas_wei: U256::ZERO,
            gas_spent_wei: U256::ZERO,
            realized_proceeds_wei: U256::ZERO,
            positions: Vec::new(),
            risk_state: ResearchRiskState::Active,
            drawdown_breached: false,
            revision: 0,
        })
    }

    pub fn profile(&self) -> &ResearchProfile {
        &self.profile
    }

    pub fn positions(&self) -> &[ResearchPosition] {
        &self.positions
    }

    pub fn admission(
        &self,
        entry_value_wei: U256,
        entry_gas_limit_wei: U256,
        exit_gas_reserve_wei: U256,
    ) -> Result<Admission, ResearchError> {
        if self.risk_state != ResearchRiskState::Active {
            return Err(ResearchError::AdmissionsHalted(self.risk_state));
        }
        if self.positions.len() >= self.profile.max_open_positions {
            return Err(ResearchError::OpenPositionLimit);
        }
        let equity = self
            .known_equity()?
            .ok_or(ResearchError::ValuationUnknown)?;
        let commitment_wei = entry_value_wei
            .checked_add(entry_gas_limit_wei)
            .ok_or(ResearchError::ArithmeticOverflow)?;
        if commitment_wei.is_zero() {
            return Err(ResearchError::ZeroCommitment);
        }
        let ceiling = bps_of(equity, self.profile.max_commitment_bps)?;
        if commitment_wei > ceiling {
            return Err(ResearchError::CommitmentLimit {
                requested: commitment_wei,
                ceiling,
            });
        }
        let required = commitment_wei
            .checked_add(exit_gas_reserve_wei)
            .ok_or(ResearchError::ArithmeticOverflow)?;
        let available = self
            .cash_wei
            .checked_sub(self.reserved_exit_gas_wei)
            .ok_or(ResearchError::ArithmeticOverflow)?;
        if required > available {
            return Err(ResearchError::InsufficientCash {
                required,
                available,
            });
        }
        Ok(Admission {
            revision: self.revision,
            entry_value_wei,
            entry_gas_limit_wei,
            exit_gas_reserve_wei,
            commitment_wei,
        })
    }

    pub fn settle_entry(
        &mut self,
        admission: Admission,
        settlement: EntrySettlement,
    ) -> Result<(), ResearchError> {
        self.validate_admission(&admission)?;
        if settlement.id.is_empty()
            || self
                .positions
                .iter()
                .any(|position| position.id == settlement.id)
        {
            return Err(ResearchError::DuplicatePosition(settlement.id));
        }
        if settlement.tokens.is_zero() {
            return Err(ResearchError::ZeroInventory);
        }
        let actual_commitment = settlement
            .entry_value_wei
            .checked_add(settlement.entry_gas_wei)
            .ok_or(ResearchError::ArithmeticOverflow)?;
        if settlement.entry_value_wei > admission.entry_value_wei
            || settlement.entry_gas_wei > admission.entry_gas_limit_wei
            || actual_commitment > admission.commitment_wei
        {
            return Err(ResearchError::AdmissionExceeded);
        }
        self.cash_wei = self.cash_wei.checked_sub(actual_commitment).ok_or(
            ResearchError::InsufficientCash {
                required: actual_commitment,
                available: self.cash_wei,
            },
        )?;
        self.reserved_exit_gas_wei = self
            .reserved_exit_gas_wei
            .checked_add(admission.exit_gas_reserve_wei)
            .ok_or(ResearchError::ArithmeticOverflow)?;
        self.gas_spent_wei = self
            .gas_spent_wei
            .checked_add(settlement.entry_gas_wei)
            .ok_or(ResearchError::ArithmeticOverflow)?;
        self.positions.push(ResearchPosition {
            id: settlement.id,
            token: settlement.token,
            tokens: settlement.tokens,
            entry_value_wei: settlement.entry_value_wei,
            entry_gas_wei: settlement.entry_gas_wei,
            reserved_exit_gas_wei: admission.exit_gas_reserve_wei,
            conservative_liquidation_wei: settlement.conservative_liquidation_wei,
        });
        self.bump_revision();
        self.refresh_risk()
    }

    pub fn fail_entry(
        &mut self,
        admission: Admission,
        actual_gas_wei: U256,
    ) -> Result<(), ResearchError> {
        self.validate_admission(&admission)?;
        if actual_gas_wei > admission.entry_gas_limit_wei
            || actual_gas_wei > admission.commitment_wei
        {
            return Err(ResearchError::AdmissionExceeded);
        }
        self.cash_wei =
            self.cash_wei
                .checked_sub(actual_gas_wei)
                .ok_or(ResearchError::InsufficientCash {
                    required: actual_gas_wei,
                    available: self.cash_wei,
                })?;
        self.gas_spent_wei = self
            .gas_spent_wei
            .checked_add(actual_gas_wei)
            .ok_or(ResearchError::ArithmeticOverflow)?;
        self.bump_revision();
        self.refresh_risk()
    }

    pub fn mark(
        &mut self,
        id: &str,
        conservative_liquidation_wei: Option<U256>,
    ) -> Result<(), ResearchError> {
        let position = self
            .positions
            .iter_mut()
            .find(|position| position.id == id)
            .ok_or_else(|| ResearchError::PositionNotFound(id.into()))?;
        position.conservative_liquidation_wei = conservative_liquidation_wei;
        self.bump_revision();
        self.refresh_risk()
    }

    pub fn settle_exit(
        &mut self,
        id: &str,
        tokens_sold: U256,
        proceeds_wei: U256,
        gas_wei: U256,
    ) -> Result<(), ResearchError> {
        let index = self
            .positions
            .iter()
            .position(|position| position.id == id)
            .ok_or_else(|| ResearchError::PositionNotFound(id.into()))?;
        let old_tokens = self.positions[index].tokens;
        if tokens_sold.is_zero() || tokens_sold > old_tokens {
            return Err(ResearchError::InvalidExitQuantity);
        }
        let remaining = old_tokens
            .checked_sub(tokens_sold)
            .ok_or(ResearchError::ArithmeticOverflow)?;
        let cash_with_proceeds = self
            .cash_wei
            .checked_add(proceeds_wei)
            .ok_or(ResearchError::ArithmeticOverflow)?;
        self.cash_wei =
            cash_with_proceeds
                .checked_sub(gas_wei)
                .ok_or(ResearchError::InsufficientCash {
                    required: gas_wei,
                    available: cash_with_proceeds,
                })?;
        self.realized_proceeds_wei = self
            .realized_proceeds_wei
            .checked_add(proceeds_wei)
            .ok_or(ResearchError::ArithmeticOverflow)?;
        self.gas_spent_wei = self
            .gas_spent_wei
            .checked_add(gas_wei)
            .ok_or(ResearchError::ArithmeticOverflow)?;
        if remaining.is_zero() {
            self.reserved_exit_gas_wei = self
                .reserved_exit_gas_wei
                .checked_sub(self.positions[index].reserved_exit_gas_wei)
                .ok_or(ResearchError::ArithmeticOverflow)?;
            self.positions.remove(index);
        } else {
            self.positions[index].tokens = remaining;
            self.positions[index].conservative_liquidation_wei = self.positions[index]
                .conservative_liquidation_wei
                .and_then(|value| value.checked_mul(remaining))
                .and_then(|value| value.checked_div(old_tokens));
        }
        self.bump_revision();
        self.refresh_risk()
    }

    pub fn snapshot(&self) -> Result<PortfolioSnapshot, ResearchError> {
        let equity_wei = self.known_equity()?;
        Ok(PortfolioSnapshot {
            cash_wei: self.cash_wei,
            equity_wei,
            peak_equity_wei: self.peak_equity_wei,
            drawdown_bps: self.drawdown_bps,
            reserved_exit_gas_wei: self.reserved_exit_gas_wei,
            gas_spent_wei: self.gas_spent_wei,
            realized_proceeds_wei: self.realized_proceeds_wei,
            risk_state: self.risk_state,
            open_positions: self.positions.len(),
            liquidation_targets: if self.drawdown_breached {
                self.positions
                    .iter()
                    .map(|position| position.id.clone())
                    .collect()
            } else {
                Vec::new()
            },
        })
    }

    fn validate_admission(&self, admission: &Admission) -> Result<(), ResearchError> {
        if admission.revision != self.revision {
            return Err(ResearchError::StaleAdmission);
        }
        if self.risk_state != ResearchRiskState::Active {
            return Err(ResearchError::AdmissionsHalted(self.risk_state));
        }
        if self.positions.len() >= self.profile.max_open_positions {
            return Err(ResearchError::OpenPositionLimit);
        }
        Ok(())
    }

    fn known_equity(&self) -> Result<Option<U256>, ResearchError> {
        let mut equity = self.cash_wei;
        for position in &self.positions {
            let Some(value) = position.conservative_liquidation_wei else {
                return Ok(None);
            };
            equity = equity
                .checked_add(value)
                .ok_or(ResearchError::ArithmeticOverflow)?;
        }
        Ok(Some(equity))
    }

    fn refresh_risk(&mut self) -> Result<(), ResearchError> {
        let Some(equity) = self.known_equity()? else {
            if !self.drawdown_breached {
                self.risk_state = ResearchRiskState::ValuationUnknown;
                self.drawdown_bps = None;
            }
            return Ok(());
        };
        self.peak_equity_wei = self.peak_equity_wei.max(equity);
        self.drawdown_bps = Some(drawdown_bps(self.peak_equity_wei, equity)?);
        if !self.drawdown_breached
            && self
                .drawdown_bps
                .is_some_and(|drawdown| drawdown >= self.profile.max_drawdown_bps)
        {
            self.drawdown_breached = true;
        }
        self.risk_state = if self.drawdown_breached {
            if self.positions.is_empty() {
                ResearchRiskState::Halted
            } else {
                ResearchRiskState::Liquidating
            }
        } else {
            ResearchRiskState::Active
        };
        Ok(())
    }

    fn bump_revision(&mut self) {
        self.revision = self.revision.wrapping_add(1);
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PersistedResearchPortfolio {
    schema: u32,
    run_id: String,
    portfolio: ResearchPortfolio,
}

pub struct ResearchPortfolioBook {
    path: PathBuf,
    state: parking_lot::Mutex<PersistedResearchPortfolio>,
}

impl ResearchPortfolioBook {
    pub fn open(
        directory: impl AsRef<Path>,
        profile: ResearchProfile,
    ) -> Result<Self, ResearchError> {
        profile.validate()?;
        let path = directory.as_ref().join("research-portfolio.json");
        let state = if path.exists() {
            let encoded = std::fs::read(&path)
                .map_err(|error| ResearchError::PortfolioIo(error.to_string()))?;
            let state: PersistedResearchPortfolio = serde_json::from_slice(&encoded)
                .map_err(|error| ResearchError::PortfolioEncoding(error.to_string()))?;
            if state.schema != 1 || state.portfolio.profile() != &profile {
                return Err(ResearchError::IncompatiblePortfolioState);
            }
            state
        } else {
            PersistedResearchPortfolio {
                schema: 1,
                run_id: format!("{}-{}", crate::pons::clock::now_ms(), std::process::id()),
                portfolio: ResearchPortfolio::new(profile)?,
            }
        };
        let book = Self {
            path,
            state: parking_lot::Mutex::new(state),
        };
        book.persist()?;
        Ok(book)
    }

    pub fn run_id(&self) -> String {
        self.state.lock().run_id.clone()
    }

    pub fn profile(&self) -> ResearchProfile {
        self.state.lock().portfolio.profile().clone()
    }

    pub fn snapshot(&self) -> Result<PortfolioSnapshot, ResearchError> {
        self.state.lock().portfolio.snapshot()
    }

    pub fn positions(&self) -> Vec<ResearchPosition> {
        self.state.lock().portfolio.positions().to_vec()
    }

    pub fn admission(
        &self,
        entry_value_wei: U256,
        entry_gas_limit_wei: U256,
        exit_gas_reserve_wei: U256,
    ) -> Result<Admission, ResearchError> {
        self.state.lock().portfolio.admission(
            entry_value_wei,
            entry_gas_limit_wei,
            exit_gas_reserve_wei,
        )
    }

    pub fn settle_entry(
        &self,
        admission: Admission,
        settlement: EntrySettlement,
    ) -> Result<(), ResearchError> {
        let mut state = self.state.lock();
        state.portfolio.settle_entry(admission, settlement)?;
        persist_portfolio(&self.path, &state)
    }

    pub fn fail_entry(
        &self,
        admission: Admission,
        actual_gas_wei: U256,
    ) -> Result<(), ResearchError> {
        let mut state = self.state.lock();
        state.portfolio.fail_entry(admission, actual_gas_wei)?;
        persist_portfolio(&self.path, &state)
    }

    pub fn mark(
        &self,
        id: &str,
        conservative_liquidation_wei: Option<U256>,
    ) -> Result<(), ResearchError> {
        let mut state = self.state.lock();
        state.portfolio.mark(id, conservative_liquidation_wei)?;
        persist_portfolio(&self.path, &state)
    }

    pub fn settle_exit(
        &self,
        id: &str,
        tokens_sold: U256,
        proceeds_wei: U256,
        gas_wei: U256,
    ) -> Result<(), ResearchError> {
        let mut state = self.state.lock();
        state
            .portfolio
            .settle_exit(id, tokens_sold, proceeds_wei, gas_wei)?;
        persist_portfolio(&self.path, &state)
    }

    fn persist(&self) -> Result<(), ResearchError> {
        persist_portfolio(&self.path, &self.state.lock())
    }
}

fn persist_portfolio(path: &Path, state: &PersistedResearchPortfolio) -> Result<(), ResearchError> {
    let encoded = serde_json::to_vec_pretty(state)
        .map_err(|error| ResearchError::PortfolioEncoding(error.to_string()))?;
    let mut file = AtomicWriteFile::open(path)
        .map_err(|error| ResearchError::PortfolioIo(error.to_string()))?;
    file.write_all(&encoded)
        .map_err(|error| ResearchError::PortfolioIo(error.to_string()))?;
    file.commit()
        .map_err(|error| ResearchError::PortfolioIo(error.to_string()))
}

fn modeled_gas(
    gas_limit: u64,
    attempts: u64,
    base_fee_wei: u64,
    fee_multiplier_bps: u64,
) -> Result<U256, ResearchError> {
    U256::from(gas_limit)
        .checked_mul(U256::from(attempts))
        .and_then(|gas| gas.checked_mul(U256::from(base_fee_wei)))
        .and_then(|gas| gas.checked_mul(U256::from(fee_multiplier_bps)))
        .and_then(|gas| gas.checked_div(U256::from(10_000u64)))
        .ok_or(ResearchError::ArithmeticOverflow)
}

fn bps_of(value: U256, bps: u64) -> Result<U256, ResearchError> {
    value
        .checked_mul(U256::from(bps))
        .and_then(|scaled| scaled.checked_div(U256::from(10_000u64)))
        .ok_or(ResearchError::ArithmeticOverflow)
}

fn drawdown_bps(peak: U256, equity: U256) -> Result<u64, ResearchError> {
    if peak.is_zero() || equity >= peak {
        return Ok(0);
    }
    let scaled = peak
        .checked_sub(equity)
        .and_then(|loss| loss.checked_mul(U256::from(10_000u64)))
        .and_then(|loss| loss.checked_div(peak))
        .ok_or(ResearchError::ArithmeticOverflow)?;
    u64::try_from(scaled).map_err(|_| ResearchError::ArithmeticOverflow)
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum ResearchError {
    #[error("invalid research profile: {0}")]
    InvalidProfile(String),
    #[error("invalid capture limits: {0}")]
    InvalidCaptureLimits(String),
    #[error("invalid capture path: {0}")]
    InvalidCapturePath(String),
    #[error("capture output already exists: {0}")]
    CaptureExists(PathBuf),
    #[error("capture I/O: {0}")]
    CaptureIo(String),
    #[error("capture encoding: {0}")]
    CaptureEncoding(String),
    #[error("capture is already finished")]
    CaptureFinished,
    #[error("capture manifest exceeds the size limit")]
    CaptureManifestTooLarge,
    #[error("capture manifest is invalid")]
    InvalidCaptureManifest,
    #[error("capture integrity does not match its manifest")]
    CaptureIntegrityMismatch,
    #[error("research portfolio I/O: {0}")]
    PortfolioIo(String),
    #[error("research portfolio encoding: {0}")]
    PortfolioEncoding(String),
    #[error("research portfolio state is incompatible with the selected profile")]
    IncompatiblePortfolioState,
    #[error("research admissions halted: {0:?}")]
    AdmissionsHalted(ResearchRiskState),
    #[error("portfolio valuation is unknown")]
    ValuationUnknown,
    #[error("maximum open positions reached")]
    OpenPositionLimit,
    #[error("zero entry commitment")]
    ZeroCommitment,
    #[error("entry commitment {requested} exceeds ceiling {ceiling}")]
    CommitmentLimit { requested: U256, ceiling: U256 },
    #[error("insufficient simulated cash: need {required}, have {available}")]
    InsufficientCash { required: U256, available: U256 },
    #[error("research arithmetic overflow")]
    ArithmeticOverflow,
    #[error("admission was exceeded")]
    AdmissionExceeded,
    #[error("admission is stale")]
    StaleAdmission,
    #[error("duplicate or empty position id: {0}")]
    DuplicatePosition(String),
    #[error("entry produced zero inventory")]
    ZeroInventory,
    #[error("position not found: {0}")]
    PositionNotFound(String),
    #[error("invalid exit quantity")]
    InvalidExitQuantity,
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    const MILLI_ETH: u64 = 1_000_000_000_000_000;

    fn portfolio() -> ResearchPortfolio {
        ResearchPortfolio::new(ResearchProfile::default()).unwrap()
    }

    fn settlement(
        id: &str,
        entry_value_wei: U256,
        conservative_liquidation_wei: Option<U256>,
    ) -> EntrySettlement {
        EntrySettlement {
            id: id.into(),
            token: Address::from([1; 20]),
            tokens: U256::from(100u64),
            entry_value_wei,
            entry_gas_wei: U256::ZERO,
            conservative_liquidation_wei,
        }
    }

    #[test]
    fn default_profile_enforces_two_percent_commitment_including_entry_gas() {
        let portfolio = portfolio();
        let admitted = portfolio
            .admission(
                U256::from(900_000_000_000_000u64),
                U256::from(100_000_000_000_000u64),
                U256::from(20_000_000_000_000u64),
            )
            .unwrap();
        assert_eq!(admitted.commitment_wei(), U256::from(MILLI_ETH));
        assert!(matches!(
            portfolio.admission(
                U256::from(900_000_000_000_001u64),
                U256::from(100_000_000_000_000u64),
                U256::ZERO,
            ),
            Err(ResearchError::CommitmentLimit { .. })
        ));
    }

    #[test]
    fn modeled_costs_reduce_the_two_percent_entry_value() {
        let profile = ResearchProfile::default();
        assert_eq!(
            profile.modeled_entry_gas(100_000_000).unwrap(),
            U256::from(105_000_000_000_000u64)
        );
        assert_eq!(
            profile
                .entry_value_for_equity(profile.starting_equity_wei, 100_000_000)
                .unwrap(),
            Some(U256::from(895_000_000_000_000u64))
        );
    }

    #[test]
    fn unknown_liquidation_value_halts_admissions_until_priced() {
        let mut portfolio = portfolio();
        let admission = portfolio
            .admission(U256::from(MILLI_ETH), U256::ZERO, U256::from(10u64))
            .unwrap();
        portfolio
            .settle_entry(admission, settlement("one", U256::from(MILLI_ETH), None))
            .unwrap();
        assert_eq!(
            portfolio.snapshot().unwrap().risk_state,
            ResearchRiskState::ValuationUnknown
        );
        assert!(matches!(
            portfolio.admission(U256::from(1u64), U256::ZERO, U256::ZERO),
            Err(ResearchError::AdmissionsHalted(
                ResearchRiskState::ValuationUnknown
            ))
        ));
        portfolio.mark("one", Some(U256::from(MILLI_ETH))).unwrap();
        assert_eq!(
            portfolio.snapshot().unwrap().risk_state,
            ResearchRiskState::Active
        );
    }

    #[test]
    fn drawdown_breach_latches_liquidation_and_cannot_rearm() {
        let profile = ResearchProfile {
            max_commitment_bps: 10_000,
            ..ResearchProfile::default()
        };
        let mut portfolio = ResearchPortfolio::new(profile).unwrap();
        let admission = portfolio
            .admission(U256::from(5 * MILLI_ETH), U256::ZERO, U256::from(10u64))
            .unwrap();
        portfolio
            .settle_entry(
                admission,
                settlement("loss", U256::from(5 * MILLI_ETH), Some(U256::ZERO)),
            )
            .unwrap();
        let snapshot = portfolio.snapshot().unwrap();
        assert_eq!(snapshot.drawdown_bps, Some(1_000));
        assert_eq!(snapshot.risk_state, ResearchRiskState::Liquidating);
        assert_eq!(snapshot.liquidation_targets, vec!["loss"]);
        portfolio
            .mark("loss", Some(U256::from(10 * MILLI_ETH)))
            .unwrap();
        assert_eq!(
            portfolio.snapshot().unwrap().risk_state,
            ResearchRiskState::Liquidating
        );
        assert!(matches!(
            portfolio.admission(U256::from(1u64), U256::ZERO, U256::ZERO),
            Err(ResearchError::AdmissionsHalted(
                ResearchRiskState::Liquidating
            ))
        ));
    }

    #[test]
    fn realized_proceeds_are_recycled_and_exit_reserve_releases_on_close() {
        let mut portfolio = portfolio();
        let reserve = U256::from(10_000u64);
        let admission = portfolio
            .admission(U256::from(MILLI_ETH), U256::ZERO, reserve)
            .unwrap();
        portfolio
            .settle_entry(
                admission,
                settlement(
                    "winner",
                    U256::from(MILLI_ETH),
                    Some(U256::from(2 * MILLI_ETH)),
                ),
            )
            .unwrap();
        portfolio
            .settle_exit(
                "winner",
                U256::from(40u64),
                U256::from(MILLI_ETH),
                U256::from(1_000u64),
            )
            .unwrap();
        assert_eq!(portfolio.snapshot().unwrap().reserved_exit_gas_wei, reserve);
        portfolio
            .settle_exit(
                "winner",
                U256::from(60u64),
                U256::from(2 * MILLI_ETH),
                U256::from(2_000u64),
            )
            .unwrap();
        let snapshot = portfolio.snapshot().unwrap();
        assert_eq!(snapshot.open_positions, 0);
        assert_eq!(snapshot.reserved_exit_gas_wei, U256::ZERO);
        assert_eq!(snapshot.realized_proceeds_wei, U256::from(3 * MILLI_ETH));
        assert!(
            portfolio
                .admission(U256::from(MILLI_ETH), U256::ZERO, reserve)
                .is_ok()
        );
    }

    #[test]
    fn portfolio_mutation_invalidates_old_admissions() {
        let mut portfolio = portfolio();
        let first = portfolio
            .admission(U256::from(1u64), U256::ZERO, U256::ZERO)
            .unwrap();
        let stale = portfolio
            .admission(U256::from(1u64), U256::ZERO, U256::ZERO)
            .unwrap();
        portfolio.fail_entry(first, U256::ZERO).unwrap();
        assert_eq!(
            portfolio.fail_entry(stale, U256::ZERO),
            Err(ResearchError::StaleAdmission)
        );
    }

    #[test]
    fn portfolio_book_preserves_run_identity_profile_and_drawdown_latch() {
        let directory = tempfile::tempdir().unwrap();
        let profile = ResearchProfile {
            max_commitment_bps: 10_000,
            ..ResearchProfile::default()
        };
        let book = ResearchPortfolioBook::open(directory.path(), profile.clone()).unwrap();
        let run_id = book.run_id();
        let admission = book
            .admission(U256::from(5 * MILLI_ETH), U256::ZERO, U256::from(10u64))
            .unwrap();
        book.settle_entry(
            admission,
            settlement("loss", U256::from(5 * MILLI_ETH), Some(U256::ZERO)),
        )
        .unwrap();
        assert_eq!(
            book.snapshot().unwrap().risk_state,
            ResearchRiskState::Liquidating
        );
        drop(book);
        let reopened = ResearchPortfolioBook::open(directory.path(), profile).unwrap();
        assert_eq!(reopened.run_id(), run_id);
        assert_eq!(
            reopened.snapshot().unwrap().risk_state,
            ResearchRiskState::Liquidating
        );
        assert!(ResearchPortfolioBook::open(directory.path(), ResearchProfile::default()).is_err());
    }

    #[test]
    fn recorder_is_bounded_manifested_and_never_overwrites() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("capture");
        let limits = CaptureLimits {
            duration_seconds: 60,
            max_bytes: 20_000,
        };
        let mut recorder = ResearchRecorder::create(&output, limits).unwrap();
        assert!(matches!(
            recorder
                .record(
                    ResearchEventKind::Launch,
                    Some(CanonicalObservation {
                        block_number: 1,
                        block_hash: B256::from([1; 32]),
                        block_timestamp: 2,
                    }),
                    serde_json::json!({"token":format!("{:#x}", Address::from([2; 20]))}),
                )
                .unwrap(),
            CaptureRecordStatus::Written(0)
        ));
        let manifest = recorder.finish(CaptureStopReason::Requested).unwrap();
        assert_eq!(manifest.event_count, 1);
        assert!(manifest.total_bytes <= limits.max_bytes);
        assert_ne!(manifest.rolling_hash, B256::ZERO);
        assert_eq!(
            std::fs::read_to_string(output.join("events.jsonl"))
                .unwrap()
                .lines()
                .count(),
            1
        );
        let verified = verify_capture(&output).unwrap();
        assert_eq!(verified.rolling_hash, manifest.rolling_hash);
        let mut visited = 0;
        visit_capture(&output, |_| {
            visited += 1;
            Ok(())
        })
        .unwrap();
        assert_eq!(visited, 1);
        assert!(ResearchRecorder::create(&output, limits).is_err());
    }

    #[test]
    fn capture_verification_rejects_tampering() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("capture");
        let mut recorder = ResearchRecorder::create(
            &output,
            CaptureLimits {
                duration_seconds: 60,
                max_bytes: 20_000,
            },
        )
        .unwrap();
        recorder
            .record(
                ResearchEventKind::Coverage,
                None,
                serde_json::json!({"complete":true}),
            )
            .unwrap();
        recorder.finish(CaptureStopReason::Requested).unwrap();
        let mut events = OpenOptions::new()
            .append(true)
            .open(output.join("events.jsonl"))
            .unwrap();
        writeln!(events, "{{}}").unwrap();
        assert!(verify_capture(&output).is_err());
    }

    #[test]
    fn recorder_stops_before_exceeding_tiny_size_limit() {
        let directory = tempfile::tempdir().unwrap();
        let output = directory.path().join("capture");
        let limits = CaptureLimits {
            duration_seconds: 60,
            max_bytes: CAPTURE_MANIFEST_RESERVE_BYTES + 1,
        };
        let mut recorder = ResearchRecorder::create(&output, limits).unwrap();
        let status = recorder
            .record(
                ResearchEventKind::Launch,
                None,
                serde_json::json!({"large":"x".repeat(1_000)}),
            )
            .unwrap();
        let CaptureRecordStatus::Stopped(manifest) = status else {
            panic!("size-limited capture wrote an oversized event");
        };
        assert_eq!(manifest.event_count, 0);
        assert_eq!(manifest.stop_reason, CaptureStopReason::SizeLimit);
        assert!(manifest.total_bytes <= limits.max_bytes);
    }

    #[test]
    fn capture_limits_never_exceed_initial_budget() {
        assert_eq!(
            CaptureLimits::default().validate().unwrap().max_bytes,
            1 << 30
        );
        assert!(
            CaptureLimits {
                duration_seconds: INITIAL_CAPTURE_MAX_SECONDS + 1,
                max_bytes: 1,
            }
            .validate()
            .is_err()
        );
        assert!(
            CaptureLimits {
                duration_seconds: 1,
                max_bytes: INITIAL_CAPTURE_MAX_BYTES + 1,
            }
            .validate()
            .is_err()
        );
    }

    proptest! {
        #[test]
        fn admission_never_exceeds_two_percent(
            entry in 0u64..1_500_000_000_000_000,
            gas in 0u64..1_500_000_000_000_000,
        ) {
            let portfolio = portfolio();
            let requested = U256::from(entry).checked_add(U256::from(gas)).unwrap();
            let admitted = portfolio
                .admission(U256::from(entry), U256::from(gas), U256::ZERO)
                .is_ok();
            prop_assert_eq!(
                admitted,
                !requested.is_zero() && requested <= U256::from(MILLI_ETH)
            );
        }
    }
}
