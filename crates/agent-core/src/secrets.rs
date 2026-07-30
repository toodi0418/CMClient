//! Agent-owned access to the sole runtime secret backend.
//!
//! Runtime credentials live in `<runtime-root>/secrets.json`. Secret values
//! deliberately have no `Debug` or `Display` implementation.

use atomic_write_file::AtomicWriteFile;
use serde::{Deserialize, Serialize};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};
#[cfg(unix)]
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};

const PLAINTEXT_DOCUMENT_VERSION: u8 = 1;
const MAX_SECRET_BYTES: usize = 4_096;
const MAX_PLAINTEXT_FILE_BYTES: usize = 32 * 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SecretKind {
    CallMeshApiKey,
    AprsPasscode,
    /// Deprecated compatibility identifier; the runtime backend never persists it.
    ManagementAdminToken,
}

impl SecretKind {
    pub const ALL: [Self; 2] = [Self::CallMeshApiKey, Self::AprsPasscode];

    pub const fn identifier(self) -> &'static str {
        match self {
            Self::CallMeshApiKey => "callmesh-api-key",
            Self::AprsPasscode => "aprs-passcode",
            Self::ManagementAdminToken => "management-admin-token",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "callmesh-api-key" => Some(Self::CallMeshApiKey),
            "aprs-passcode" => Some(Self::AprsPasscode),
            "management-admin-token" => Some(Self::ManagementAdminToken),
            _ => None,
        }
    }
}

/// An owned secret that zeroes its storage when dropped.
pub struct SecretValue(Zeroizing<String>);

impl SecretValue {
    fn new(value: String) -> Self {
        Self(Zeroizing::new(value))
    }

    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SecretStoreError {
    InvalidValue,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallMeshSetupSecretState {
    None,
    Staged,
    Promoted,
}

impl SecretStoreError {
    pub const fn code(self) -> &'static str {
        match self {
            Self::InvalidValue => "AGENT_SECRET_VALUE_INVALID",
            Self::Unavailable => "AGENT_SECRET_STORE_UNAVAILABLE",
        }
    }
}

trait SecretBackend: Send + Sync {
    fn set(&self, kind: SecretKind, value: &str) -> Result<(), SecretStoreError>;
    fn get(&self, kind: SecretKind) -> Result<Option<SecretValue>, SecretStoreError>;
    fn delete(&self, kind: SecretKind) -> Result<bool, SecretStoreError>;
    fn clear_for_reset(&self) -> Result<(), SecretStoreError>;
    fn stage_callmesh_setup(&self, candidate: &str) -> Result<(), SecretStoreError>;
    fn promote_callmesh_setup(&self) -> Result<(), SecretStoreError>;
    fn rollback_callmesh_setup(&self) -> Result<bool, SecretStoreError>;
    fn finalize_callmesh_setup(&self) -> Result<bool, SecretStoreError>;
    fn callmesh_setup_state(&self) -> Result<CallMeshSetupSecretState, SecretStoreError>;

    #[cfg(test)]
    fn kind(&self) -> SecretBackendKind;
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecretBackendKind {
    Plaintext,
    Memory,
}

struct PlaintextSecretBackend {
    path: PathBuf,
    parent: PathBuf,
    #[cfg(unix)]
    owner: u32,
    lock: Mutex<()>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct CallMeshSetupSecretTransaction {
    candidate: String,
    previous: Option<String>,
    promoted: bool,
}

impl Drop for CallMeshSetupSecretTransaction {
    fn drop(&mut self) {
        self.candidate.zeroize();
        if let Some(previous) = self.previous.as_mut() {
            previous.zeroize();
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PlaintextSecretDocument {
    version: u8,
    #[serde(
        rename = "callmesh-api-key",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    callmesh_api_key: Option<String>,
    #[serde(
        rename = "aprs-passcode",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    aprs_passcode: Option<String>,
    #[serde(
        rename = "setup-callmesh-transaction",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    setup_callmesh_transaction: Option<CallMeshSetupSecretTransaction>,
}

impl PlaintextSecretDocument {
    fn empty() -> Self {
        Self {
            version: PLAINTEXT_DOCUMENT_VERSION,
            callmesh_api_key: None,
            aprs_passcode: None,
            setup_callmesh_transaction: None,
        }
    }

    fn value_mut(&mut self, kind: SecretKind) -> Option<&mut Option<String>> {
        match kind {
            SecretKind::CallMeshApiKey => Some(&mut self.callmesh_api_key),
            SecretKind::AprsPasscode => Some(&mut self.aprs_passcode),
            SecretKind::ManagementAdminToken => None,
        }
    }

    fn replace(&mut self, kind: SecretKind, value: String) -> bool {
        let Some(entry) = self.value_mut(kind) else {
            return false;
        };
        if let Some(mut previous) = entry.replace(value) {
            previous.zeroize();
        }
        true
    }

    fn take(&mut self, kind: SecretKind) -> Option<String> {
        self.value_mut(kind).and_then(Option::take)
    }

    fn validate(&self) -> Result<(), SecretStoreError> {
        if self.version != PLAINTEXT_DOCUMENT_VERSION {
            return Err(SecretStoreError::Unavailable);
        }
        for value in [
            self.callmesh_api_key.as_deref(),
            self.aprs_passcode.as_deref(),
            self.setup_callmesh_transaction
                .as_ref()
                .map(|transaction| transaction.candidate.as_str()),
            self.setup_callmesh_transaction
                .as_ref()
                .and_then(|transaction| transaction.previous.as_deref()),
        ]
        .into_iter()
        .flatten()
        {
            validate_secret(value).map_err(|_| SecretStoreError::Unavailable)?;
        }
        if self
            .setup_callmesh_transaction
            .as_ref()
            .is_some_and(|transaction| {
                transaction.promoted
                    && self.callmesh_api_key.as_deref() != Some(transaction.candidate.as_str())
            })
        {
            return Err(SecretStoreError::Unavailable);
        }
        Ok(())
    }
}

/// Validate a staged plaintext secret document without creating or changing its parent.
pub fn validate_migrated_plaintext_secrets(path: &Path) -> Result<(), SecretStoreError> {
    if !path.is_absolute() || path.file_name().is_none() {
        return Err(SecretStoreError::Unavailable);
    }
    let mut input = open_regular_file_no_follow(path)?.ok_or(SecretStoreError::Unavailable)?;
    let metadata = input
        .metadata()
        .map_err(|_| SecretStoreError::Unavailable)?;
    if metadata.len() > MAX_PLAINTEXT_FILE_BYTES as u64 {
        return Err(SecretStoreError::Unavailable);
    }
    let mut bytes = Zeroizing::new(Vec::with_capacity(metadata.len() as usize));
    Read::by_ref(&mut input)
        .take(MAX_PLAINTEXT_FILE_BYTES as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| SecretStoreError::Unavailable)?;
    if bytes.len() > MAX_PLAINTEXT_FILE_BYTES {
        return Err(SecretStoreError::Unavailable);
    }
    let document: PlaintextSecretDocument =
        serde_json::from_slice(bytes.as_slice()).map_err(|_| SecretStoreError::Unavailable)?;
    document.validate()
}

impl Drop for PlaintextSecretDocument {
    fn drop(&mut self) {
        for value in [&mut self.callmesh_api_key, &mut self.aprs_passcode] {
            if let Some(value) = value.as_mut() {
                value.zeroize();
            }
        }
    }
}

impl PlaintextSecretBackend {
    fn new(path: PathBuf) -> Result<Self, SecretStoreError> {
        if !path.is_absolute() || path.file_name().is_none() {
            return Err(SecretStoreError::Unavailable);
        }
        let file_name = path
            .file_name()
            .ok_or(SecretStoreError::Unavailable)?
            .to_owned();
        let requested_parent = path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
            .ok_or(SecretStoreError::Unavailable)?;
        ensure_private_directory(requested_parent)?;
        let parent =
            fs::canonicalize(requested_parent).map_err(|_| SecretStoreError::Unavailable)?;
        let path = parent.join(file_name);

        #[cfg(unix)]
        let owner = private_directory_owner(&parent)?;
        #[cfg(unix)]
        validate_optional_private_file(&path, owner)?;
        #[cfg(not(unix))]
        validate_optional_regular_file(&path)?;

        Ok(Self {
            path,
            parent,
            #[cfg(unix)]
            owner,
            lock: Mutex::new(()),
        })
    }

    fn validate_parent(&self) -> Result<(), SecretStoreError> {
        #[cfg(unix)]
        return validate_private_directory(&self.parent, self.owner);

        #[cfg(not(unix))]
        validate_directory_no_follow(&self.parent)
    }

    fn load(&self) -> Result<PlaintextSecretDocument, SecretStoreError> {
        self.validate_parent()?;
        let Some(mut input) = open_regular_file_no_follow(&self.path)? else {
            return Ok(PlaintextSecretDocument::empty());
        };
        #[cfg(unix)]
        validate_private_file(&input, self.owner)?;
        let metadata = input
            .metadata()
            .map_err(|_| SecretStoreError::Unavailable)?;
        if metadata.len() > MAX_PLAINTEXT_FILE_BYTES as u64 {
            return Err(SecretStoreError::Unavailable);
        }
        let mut bytes = Zeroizing::new(Vec::with_capacity(metadata.len() as usize));
        Read::by_ref(&mut input)
            .take(MAX_PLAINTEXT_FILE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(|_| SecretStoreError::Unavailable)?;
        if bytes.len() > MAX_PLAINTEXT_FILE_BYTES {
            return Err(SecretStoreError::Unavailable);
        }
        let document: PlaintextSecretDocument =
            serde_json::from_slice(bytes.as_slice()).map_err(|_| SecretStoreError::Unavailable)?;
        document.validate()?;
        Ok(document)
    }

    fn save(&self, document: &PlaintextSecretDocument) -> Result<(), SecretStoreError> {
        self.validate_parent()?;
        document.validate()?;
        #[cfg(unix)]
        validate_optional_private_file(&self.path, self.owner)?;
        #[cfg(not(unix))]
        validate_optional_regular_file(&self.path)?;

        let bytes = Zeroizing::new(
            serde_json::to_vec(document).map_err(|_| SecretStoreError::Unavailable)?,
        );
        if bytes.len() > MAX_PLAINTEXT_FILE_BYTES {
            return Err(SecretStoreError::Unavailable);
        }
        atomic_write_private(&self.path, bytes.as_slice())?;

        #[cfg(unix)]
        validate_optional_private_file(&self.path, self.owner)?;
        #[cfg(not(unix))]
        validate_optional_regular_file(&self.path)
    }
}

impl SecretBackend for PlaintextSecretBackend {
    fn set(&self, kind: SecretKind, value: &str) -> Result<(), SecretStoreError> {
        if kind == SecretKind::ManagementAdminToken {
            return Err(SecretStoreError::Unavailable);
        }
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let mut document = self.load()?;
        if kind == SecretKind::CallMeshApiKey && document.setup_callmesh_transaction.is_some() {
            return Err(SecretStoreError::Unavailable);
        }
        if !document.replace(kind, value.to_owned()) {
            return Err(SecretStoreError::Unavailable);
        }
        self.save(&document)
    }

    fn get(&self, kind: SecretKind) -> Result<Option<SecretValue>, SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let mut document = self.load()?;
        Ok(document.take(kind).map(SecretValue::new))
    }

    fn delete(&self, kind: SecretKind) -> Result<bool, SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let mut document = self.load()?;
        if kind == SecretKind::CallMeshApiKey && document.setup_callmesh_transaction.is_some() {
            return Err(SecretStoreError::Unavailable);
        }
        let Some(mut removed) = document.take(kind) else {
            return Ok(false);
        };
        removed.zeroize();
        self.save(&document)?;
        Ok(true)
    }

    fn clear_for_reset(&self) -> Result<(), SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let mut document = self.load()?;
        for kind in SecretKind::ALL {
            if let Some(mut value) = document.take(kind) {
                value.zeroize();
            }
        }
        if let Some(mut transaction) = document.setup_callmesh_transaction.take() {
            transaction.candidate.zeroize();
            if let Some(previous) = transaction.previous.as_mut() {
                previous.zeroize();
            }
        }
        self.save(&document)
    }

    fn stage_callmesh_setup(&self, candidate: &str) -> Result<(), SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let mut document = self.load()?;
        if document.setup_callmesh_transaction.is_some() {
            return Err(SecretStoreError::Unavailable);
        }
        document.setup_callmesh_transaction = Some(CallMeshSetupSecretTransaction {
            candidate: candidate.to_owned(),
            previous: document.callmesh_api_key.clone(),
            promoted: false,
        });
        self.save(&document)
    }

    fn promote_callmesh_setup(&self) -> Result<(), SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let mut document = self.load()?;
        let candidate = document
            .setup_callmesh_transaction
            .as_ref()
            .filter(|transaction| !transaction.promoted)
            .map(|transaction| transaction.candidate.clone())
            .ok_or(SecretStoreError::Unavailable)?;
        document.replace(SecretKind::CallMeshApiKey, candidate);
        document
            .setup_callmesh_transaction
            .as_mut()
            .ok_or(SecretStoreError::Unavailable)?
            .promoted = true;
        self.save(&document)
    }

    fn rollback_callmesh_setup(&self) -> Result<bool, SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let mut document = self.load()?;
        let Some(mut transaction) = document.setup_callmesh_transaction.take() else {
            return Ok(false);
        };
        match transaction.previous.take() {
            Some(previous) => {
                document.replace(SecretKind::CallMeshApiKey, previous);
            }
            None => {
                if let Some(mut active) = document.callmesh_api_key.take() {
                    active.zeroize();
                }
            }
        }
        self.save(&document)?;
        Ok(true)
    }

    fn finalize_callmesh_setup(&self) -> Result<bool, SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let mut document = self.load()?;
        let Some(transaction) = document.setup_callmesh_transaction.as_ref() else {
            return Ok(false);
        };
        if !transaction.promoted {
            return Err(SecretStoreError::Unavailable);
        }
        document.setup_callmesh_transaction = None;
        self.save(&document)?;
        Ok(true)
    }

    fn callmesh_setup_state(&self) -> Result<CallMeshSetupSecretState, SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let document = self.load()?;
        Ok(match document.setup_callmesh_transaction.as_ref() {
            None => CallMeshSetupSecretState::None,
            Some(transaction) if transaction.promoted => CallMeshSetupSecretState::Promoted,
            Some(_) => CallMeshSetupSecretState::Staged,
        })
    }

    #[cfg(test)]
    fn kind(&self) -> SecretBackendKind {
        SecretBackendKind::Plaintext
    }
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Default)]
struct MemorySecretState {
    values: std::collections::BTreeMap<SecretKind, String>,
    setup_callmesh_transaction: Option<CallMeshSetupSecretTransaction>,
    fail_next_setup_finalize: bool,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Default)]
struct MemorySecretBackend(Mutex<MemorySecretState>);

#[cfg(any(test, feature = "test-support"))]
impl SecretBackend for MemorySecretBackend {
    fn set(&self, kind: SecretKind, value: &str) -> Result<(), SecretStoreError> {
        let mut state = self.0.lock().map_err(|_| SecretStoreError::Unavailable)?;
        if kind == SecretKind::CallMeshApiKey && state.setup_callmesh_transaction.is_some() {
            return Err(SecretStoreError::Unavailable);
        }
        state.values.insert(kind, value.to_owned());
        Ok(())
    }

    fn get(&self, kind: SecretKind) -> Result<Option<SecretValue>, SecretStoreError> {
        Ok(self
            .0
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?
            .values
            .get(&kind)
            .cloned()
            .map(SecretValue::new))
    }

    fn delete(&self, kind: SecretKind) -> Result<bool, SecretStoreError> {
        let mut state = self.0.lock().map_err(|_| SecretStoreError::Unavailable)?;
        if kind == SecretKind::CallMeshApiKey && state.setup_callmesh_transaction.is_some() {
            return Err(SecretStoreError::Unavailable);
        }
        Ok(state.values.remove(&kind).is_some())
    }

    fn clear_for_reset(&self) -> Result<(), SecretStoreError> {
        let mut state = self.0.lock().map_err(|_| SecretStoreError::Unavailable)?;
        for value in state.values.values_mut() {
            value.zeroize();
        }
        state.values.clear();
        state.setup_callmesh_transaction = None;
        Ok(())
    }

    fn stage_callmesh_setup(&self, candidate: &str) -> Result<(), SecretStoreError> {
        let mut state = self.0.lock().map_err(|_| SecretStoreError::Unavailable)?;
        if state.setup_callmesh_transaction.is_some() {
            return Err(SecretStoreError::Unavailable);
        }
        state.setup_callmesh_transaction = Some(CallMeshSetupSecretTransaction {
            candidate: candidate.to_owned(),
            previous: state.values.get(&SecretKind::CallMeshApiKey).cloned(),
            promoted: false,
        });
        Ok(())
    }

    fn promote_callmesh_setup(&self) -> Result<(), SecretStoreError> {
        let mut state = self.0.lock().map_err(|_| SecretStoreError::Unavailable)?;
        let candidate = state
            .setup_callmesh_transaction
            .as_ref()
            .filter(|transaction| !transaction.promoted)
            .map(|transaction| transaction.candidate.clone())
            .ok_or(SecretStoreError::Unavailable)?;
        if let Some(mut previous) = state.values.insert(SecretKind::CallMeshApiKey, candidate) {
            previous.zeroize();
        }
        state
            .setup_callmesh_transaction
            .as_mut()
            .ok_or(SecretStoreError::Unavailable)?
            .promoted = true;
        Ok(())
    }

    fn rollback_callmesh_setup(&self) -> Result<bool, SecretStoreError> {
        let mut state = self.0.lock().map_err(|_| SecretStoreError::Unavailable)?;
        let Some(mut transaction) = state.setup_callmesh_transaction.take() else {
            return Ok(false);
        };
        match transaction.previous.take() {
            Some(previous) => {
                if let Some(mut current) = state.values.insert(SecretKind::CallMeshApiKey, previous)
                {
                    current.zeroize();
                }
            }
            None => {
                if let Some(mut current) = state.values.remove(&SecretKind::CallMeshApiKey) {
                    current.zeroize();
                }
            }
        }
        Ok(true)
    }

    fn finalize_callmesh_setup(&self) -> Result<bool, SecretStoreError> {
        let mut state = self.0.lock().map_err(|_| SecretStoreError::Unavailable)?;
        if state.fail_next_setup_finalize {
            state.fail_next_setup_finalize = false;
            return Err(SecretStoreError::Unavailable);
        }
        let Some(transaction) = state.setup_callmesh_transaction.as_ref() else {
            return Ok(false);
        };
        if !transaction.promoted {
            return Err(SecretStoreError::Unavailable);
        }
        state.setup_callmesh_transaction = None;
        Ok(true)
    }

    fn callmesh_setup_state(&self) -> Result<CallMeshSetupSecretState, SecretStoreError> {
        let state = self.0.lock().map_err(|_| SecretStoreError::Unavailable)?;
        Ok(match state.setup_callmesh_transaction.as_ref() {
            None => CallMeshSetupSecretState::None,
            Some(transaction) if transaction.promoted => CallMeshSetupSecretState::Promoted,
            Some(_) => CallMeshSetupSecretState::Staged,
        })
    }

    #[cfg(test)]
    fn kind(&self) -> SecretBackendKind {
        SecretBackendKind::Memory
    }
}

/// Access point for the Agent's fixed plaintext credential backend.
#[derive(Clone)]
pub struct AgentSecretStore {
    backend: Arc<dyn SecretBackend>,
}

impl AgentSecretStore {
    /// Opens `<runtime-root>/secrets.json` without consulting process environment.
    pub fn runtime(runtime_root: &Path) -> Result<Self, SecretStoreError> {
        if !runtime_root.is_absolute() {
            return Err(SecretStoreError::Unavailable);
        }
        Ok(Self {
            backend: Arc::new(PlaintextSecretBackend::new(
                runtime_root.join("secrets.json"),
            )?),
        })
    }

    pub fn store(&self, kind: SecretKind, value: &str) -> Result<(), SecretStoreError> {
        validate_secret(value)?;
        self.backend.set(kind, value)
    }

    pub fn read(&self, kind: SecretKind) -> Result<Option<SecretValue>, SecretStoreError> {
        self.backend.get(kind)
    }

    pub fn remove(&self, kind: SecretKind) -> Result<bool, SecretStoreError> {
        self.backend.delete(kind)
    }

    /// Remove every runtime secret and any interrupted setup transaction as one
    /// reset operation. This never restores a previous setup credential.
    pub fn clear_for_reset(&self) -> Result<(), SecretStoreError> {
        self.backend.clear_for_reset()
    }

    pub fn stage_callmesh_setup(&self, candidate: &str) -> Result<(), SecretStoreError> {
        validate_secret(candidate)?;
        self.backend.stage_callmesh_setup(candidate)
    }

    pub fn promote_callmesh_setup(&self) -> Result<(), SecretStoreError> {
        self.backend.promote_callmesh_setup()
    }

    pub fn rollback_callmesh_setup(&self) -> Result<bool, SecretStoreError> {
        self.backend.rollback_callmesh_setup()
    }

    pub fn finalize_callmesh_setup(&self) -> Result<bool, SecretStoreError> {
        self.backend.finalize_callmesh_setup()
    }

    pub fn callmesh_setup_state(&self) -> Result<CallMeshSetupSecretState, SecretStoreError> {
        self.backend.callmesh_setup_state()
    }

    #[cfg(test)]
    fn backend_kind(&self) -> SecretBackendKind {
        self.backend.kind()
    }

    #[cfg(any(test, feature = "test-support"))]
    fn with_backend(backend: Arc<dyn SecretBackend>) -> Self {
        Self { backend }
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn memory() -> Self {
        Self::with_backend(Arc::new(MemorySecretBackend::default()))
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn memory_with_finalize_failure_once() -> Self {
        Self::with_backend(Arc::new(MemorySecretBackend(Mutex::new(
            MemorySecretState {
                fail_next_setup_finalize: true,
                ..MemorySecretState::default()
            },
        ))))
    }

    /// Legacy test fixture alias. This is never compiled into a runtime build.
    #[cfg(any(test, feature = "test-support"))]
    pub fn platform() -> Self {
        Self::memory()
    }
}

fn ensure_private_directory(path: &Path) -> Result<(), SecretStoreError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => return Err(SecretStoreError::Unavailable),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(|_| SecretStoreError::Unavailable)?;
        }
        Err(_) => return Err(SecretStoreError::Unavailable),
    }
    validate_directory_no_follow(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|_| SecretStoreError::Unavailable)?;
    }
    Ok(())
}

fn validate_directory_no_follow(path: &Path) -> Result<(), SecretStoreError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        _ => Err(SecretStoreError::Unavailable),
    }
}

fn open_regular_file_no_follow(path: &Path) -> Result<Option<File>, SecretStoreError> {
    #[cfg(not(unix))]
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => {}
        Ok(_) => return Err(SecretStoreError::Unavailable),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(SecretStoreError::Unavailable),
    }

    let mut options = OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
    }
    let input = match options.open(path) {
        Ok(input) => input,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(_) => return Err(SecretStoreError::Unavailable),
    };
    if !input
        .metadata()
        .map_err(|_| SecretStoreError::Unavailable)?
        .file_type()
        .is_file()
    {
        return Err(SecretStoreError::Unavailable);
    }
    Ok(Some(input))
}

#[cfg(not(unix))]
fn validate_optional_regular_file(path: &Path) -> Result<(), SecretStoreError> {
    let _ = open_regular_file_no_follow(path)?;
    Ok(())
}

#[cfg(unix)]
fn open_directory_no_follow(path: &Path) -> Result<File, SecretStoreError> {
    let mut options = OpenOptions::new();
    options.read(true);
    use std::os::unix::fs::OpenOptionsExt;
    options.custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW);
    let directory = options
        .open(path)
        .map_err(|_| SecretStoreError::Unavailable)?;
    if !directory
        .metadata()
        .map_err(|_| SecretStoreError::Unavailable)?
        .file_type()
        .is_dir()
    {
        return Err(SecretStoreError::Unavailable);
    }
    Ok(directory)
}

#[cfg(unix)]
fn private_directory_owner(path: &Path) -> Result<u32, SecretStoreError> {
    use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};

    let directory = open_directory_no_follow(path)?;
    let metadata = directory
        .metadata()
        .map_err(|_| SecretStoreError::Unavailable)?;
    if metadata.mode() & 0o7777 != 0o700 {
        return Err(SecretStoreError::Unavailable);
    }

    let probe = path.join(format!(".cmclient-owner-{}.tmp", Uuid::new_v4().simple()));
    let mut options = OpenOptions::new();
    options
        .write(true)
        .create_new(true)
        .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
        .mode(0o600);
    let result = (|| {
        let probe_file = options
            .open(&probe)
            .map_err(|_| SecretStoreError::Unavailable)?;
        probe_file
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|_| SecretStoreError::Unavailable)?;
        let probe_owner = probe_file
            .metadata()
            .map_err(|_| SecretStoreError::Unavailable)?
            .uid();
        if probe_owner != metadata.uid() {
            return Err(SecretStoreError::Unavailable);
        }
        Ok(probe_owner)
    })();
    let _ = fs::remove_file(&probe);
    result
}

#[cfg(unix)]
fn validate_private_directory(path: &Path, owner: u32) -> Result<(), SecretStoreError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = open_directory_no_follow(path)?
        .metadata()
        .map_err(|_| SecretStoreError::Unavailable)?;
    if metadata.uid() != owner || metadata.mode() & 0o7777 != 0o700 {
        return Err(SecretStoreError::Unavailable);
    }
    Ok(())
}

#[cfg(unix)]
fn validate_private_file(file: &File, owner: u32) -> Result<(), SecretStoreError> {
    use std::os::unix::fs::MetadataExt;

    let metadata = file.metadata().map_err(|_| SecretStoreError::Unavailable)?;
    if !metadata.file_type().is_file()
        || metadata.uid() != owner
        || metadata.mode() & 0o7777 != 0o600
        || metadata.nlink() != 1
    {
        return Err(SecretStoreError::Unavailable);
    }
    Ok(())
}

#[cfg(unix)]
fn validate_optional_private_file(path: &Path, owner: u32) -> Result<(), SecretStoreError> {
    let Some(file) = open_regular_file_no_follow(path)? else {
        return Ok(());
    };
    validate_private_file(&file, owner)
}

fn atomic_write_private(path: &Path, bytes: &[u8]) -> Result<(), SecretStoreError> {
    let mut output = AtomicWriteFile::open(path).map_err(|_| SecretStoreError::Unavailable)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        output
            .as_file()
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|_| SecretStoreError::Unavailable)?;
    }
    output
        .write_all(bytes)
        .map_err(|_| SecretStoreError::Unavailable)?;
    output.commit().map_err(|_| SecretStoreError::Unavailable)
}

fn validate_secret(value: &str) -> Result<(), SecretStoreError> {
    if value.is_empty()
        || value.len() > MAX_SECRET_BYTES
        || value.bytes().any(|byte| byte.is_ascii_control())
    {
        return Err(SecretStoreError::InvalidValue);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        AgentSecretStore, MAX_PLAINTEXT_FILE_BYTES, SecretBackendKind, SecretKind,
        SecretStoreError, validate_migrated_plaintext_secrets,
    };
    use std::{fs, path::PathBuf};
    use uuid::Uuid;

    fn fixture(label: &str) -> PathBuf {
        std::env::temp_dir().join(format!("cmclient-secrets-{label}-{}", Uuid::new_v4()))
    }

    #[test]
    fn memory_backend_validates_stores_reads_and_removes_values() {
        let store = AgentSecretStore::memory();
        assert_eq!(store.backend_kind(), SecretBackendKind::Memory);
        store
            .store(SecretKind::CallMeshApiKey, "callmesh-value")
            .expect("secret should store");
        assert_eq!(
            store
                .read(SecretKind::CallMeshApiKey)
                .expect("secret read should succeed")
                .expect("secret should exist")
                .expose_secret(),
            "callmesh-value"
        );
        assert!(
            store
                .remove(SecretKind::CallMeshApiKey)
                .expect("secret remove should succeed")
        );
        assert!(
            store
                .read(SecretKind::CallMeshApiKey)
                .expect("secret read should succeed")
                .is_none()
        );
        for invalid in [String::new(), String::from("line\nfeed"), "a".repeat(4_097)] {
            assert_eq!(
                store.store(SecretKind::AprsPasscode, &invalid),
                Err(SecretStoreError::InvalidValue)
            );
        }
    }

    #[test]
    fn runtime_uses_only_root_secrets_json_and_persists_supported_secret_kinds() {
        let root = fixture("runtime");
        let store = AgentSecretStore::runtime(&root).expect("runtime store should initialize");
        assert_eq!(store.backend_kind(), SecretBackendKind::Plaintext);
        for (kind, value) in [
            (SecretKind::CallMeshApiKey, "fixture-api-key"),
            (SecretKind::AprsPasscode, "fixture-passcode"),
        ] {
            store.store(kind, value).expect("secret should store");
        }

        let path = root.join("secrets.json");
        assert!(path.is_file());
        assert_eq!(
            fs::read_dir(&root)
                .expect("runtime root should be readable")
                .count(),
            1
        );
        let raw = fs::read_to_string(&path).expect("plaintext file should be readable");
        assert!(raw.contains("fixture-api-key"));
        assert!(raw.contains("fixture-passcode"));
        assert_eq!(
            store.store(SecretKind::ManagementAdminToken, "fixture-control-token"),
            Err(SecretStoreError::Unavailable)
        );
        assert!(!raw.contains("management-admin-token"));

        let reopened = AgentSecretStore::runtime(&root).expect("runtime store should reopen");
        assert_eq!(
            reopened
                .read(SecretKind::AprsPasscode)
                .expect("secret should read")
                .expect("secret should exist")
                .expose_secret(),
            "fixture-passcode"
        );
        assert!(
            reopened
                .remove(SecretKind::AprsPasscode)
                .expect("secret should remove")
        );
        assert!(
            reopened
                .read(SecretKind::AprsPasscode)
                .expect("missing secret should be valid")
                .is_none()
        );
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[test]
    fn callmesh_setup_transaction_reopens_and_rolls_back_within_secrets_json() {
        let root = fixture("setup-transaction-rollback");
        let store = AgentSecretStore::runtime(&root).expect("runtime store should initialize");
        store
            .store(SecretKind::CallMeshApiKey, "fixture-previous-key")
            .expect("previous key should store");
        store
            .stage_callmesh_setup("fixture-candidate-key")
            .expect("candidate should stage");
        assert_eq!(
            store
                .read(SecretKind::CallMeshApiKey)
                .expect("active key should read")
                .expect("active key should remain present")
                .expose_secret(),
            "fixture-previous-key",
        );
        assert_eq!(
            store
                .callmesh_setup_state()
                .expect("transaction state should read"),
            super::CallMeshSetupSecretState::Staged,
        );
        assert_eq!(
            store.store(SecretKind::CallMeshApiKey, "fixture-concurrent-key"),
            Err(SecretStoreError::Unavailable),
        );
        assert_eq!(
            store.remove(SecretKind::CallMeshApiKey),
            Err(SecretStoreError::Unavailable),
        );
        assert_eq!(
            fs::read_dir(&root)
                .expect("runtime root should read")
                .count(),
            1,
            "candidate and rollback key must stay in the sole secrets.json backend",
        );

        let reopened = AgentSecretStore::runtime(&root).expect("secret store should reopen");
        reopened
            .promote_callmesh_setup()
            .expect("candidate should promote atomically");
        assert_eq!(
            reopened
                .callmesh_setup_state()
                .expect("promoted state should read"),
            super::CallMeshSetupSecretState::Promoted,
        );
        assert_eq!(
            reopened.store(SecretKind::CallMeshApiKey, "fixture-concurrent-key"),
            Err(SecretStoreError::Unavailable),
        );
        assert_eq!(
            reopened.remove(SecretKind::CallMeshApiKey),
            Err(SecretStoreError::Unavailable),
        );
        drop(reopened);

        let recovered = AgentSecretStore::runtime(&root).expect("secret store should reopen again");
        assert!(
            recovered
                .rollback_callmesh_setup()
                .expect("promoted transaction should rollback")
        );
        assert_eq!(
            recovered
                .read(SecretKind::CallMeshApiKey)
                .expect("restored key should read")
                .expect("restored key should be present")
                .expose_secret(),
            "fixture-previous-key",
        );
        assert_eq!(
            recovered
                .callmesh_setup_state()
                .expect("cleared state should read"),
            super::CallMeshSetupSecretState::None,
        );
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[test]
    fn reset_clear_removes_active_and_staged_secrets_without_rollback() {
        let root = fixture("reset-clear");
        let store = AgentSecretStore::runtime(&root).expect("runtime store should initialize");
        store
            .store(SecretKind::CallMeshApiKey, "fixture-previous-key")
            .expect("previous key should store");
        store
            .store(SecretKind::AprsPasscode, "fixture-aprs-passcode")
            .expect("APRS passcode should store");
        store
            .stage_callmesh_setup("fixture-candidate-key")
            .expect("candidate should stage");

        store
            .clear_for_reset()
            .expect("reset should clear every secret state");
        for kind in SecretKind::ALL {
            assert!(
                store
                    .read(kind)
                    .expect("cleared secret should be readable")
                    .is_none(),
                "reset must not retain {kind:?}",
            );
        }
        assert_eq!(
            store
                .callmesh_setup_state()
                .expect("transaction state should read"),
            super::CallMeshSetupSecretState::None,
        );
        let serialized =
            fs::read_to_string(root.join("secrets.json")).expect("cleared document should persist");
        assert!(!serialized.contains("fixture-previous-key"));
        assert!(!serialized.contains("fixture-aprs-passcode"));
        assert!(!serialized.contains("fixture-candidate-key"));
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[test]
    fn promoted_callmesh_setup_transaction_can_finalize_after_reopen() {
        let root = fixture("setup-transaction-finalize");
        let store = AgentSecretStore::runtime(&root).expect("runtime store should initialize");
        store
            .stage_callmesh_setup("fixture-candidate-key")
            .expect("candidate should stage");
        store
            .promote_callmesh_setup()
            .expect("candidate should promote");
        drop(store);

        let reopened = AgentSecretStore::runtime(&root).expect("secret store should reopen");
        assert!(
            reopened
                .finalize_callmesh_setup()
                .expect("promoted transaction should finalize")
        );
        assert_eq!(
            reopened
                .read(SecretKind::CallMeshApiKey)
                .expect("committed key should read")
                .expect("committed key should remain present")
                .expose_secret(),
            "fixture-candidate-key",
        );
        assert_eq!(
            reopened
                .callmesh_setup_state()
                .expect("final state should read"),
            super::CallMeshSetupSecretState::None,
        );
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[test]
    fn runtime_rejects_relative_roots_non_files_and_malformed_documents() {
        assert!(matches!(
            AgentSecretStore::runtime(PathBuf::from("relative-root").as_path()),
            Err(SecretStoreError::Unavailable)
        ));

        let root = fixture("invalid");
        fs::create_dir_all(root.join("secrets.json"))
            .expect("non-file secret entry should be created");
        assert!(matches!(
            AgentSecretStore::runtime(&root),
            Err(SecretStoreError::Unavailable)
        ));
        fs::remove_dir_all(&root).expect("fixture should reset");

        let store = AgentSecretStore::runtime(&root).expect("runtime store should initialize");
        fs::write(root.join("secrets.json"), b"{").expect("malformed file should write");
        assert!(matches!(
            store.read(SecretKind::CallMeshApiKey),
            Err(SecretStoreError::Unavailable)
        ));
        fs::write(
            root.join("secrets.json"),
            vec![b'a'; MAX_PLAINTEXT_FILE_BYTES + 1],
        )
        .expect("oversized file should write");
        assert!(matches!(
            store.read(SecretKind::CallMeshApiKey),
            Err(SecretStoreError::Unavailable)
        ));
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[test]
    fn migrated_secrets_use_the_production_document_schema() {
        let root = fixture("migrated-validator");
        fs::create_dir_all(&root).expect("fixture root should exist");
        let path = root.join("secrets.json");
        fs::write(&path, br#"{"version":1,"callmesh-api-key":"fixture-key"}"#)
            .expect("valid migrated secrets should write");
        validate_migrated_plaintext_secrets(&path)
            .expect("production-valid migrated secrets should pass");

        fs::write(&path, br#"{"version":2}"#).expect("invalid version should write");
        assert_eq!(
            validate_migrated_plaintext_secrets(&path),
            Err(SecretStoreError::Unavailable)
        );
        fs::write(&path, br#"{"version":1,"unknown":"value"}"#)
            .expect("unknown field should write");
        assert_eq!(
            validate_migrated_plaintext_secrets(&path),
            Err(SecretStoreError::Unavailable)
        );
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[cfg(unix)]
    #[test]
    fn unix_runtime_enforces_private_directory_and_file_modes() {
        use std::os::unix::fs::PermissionsExt;

        let root = fixture("modes");
        let store = AgentSecretStore::runtime(&root).expect("runtime store should initialize");
        store
            .store(SecretKind::CallMeshApiKey, "fixture-token")
            .expect("secret should store");
        assert_eq!(
            fs::metadata(&root)
                .expect("runtime root should exist")
                .permissions()
                .mode()
                & 0o7777,
            0o700
        );
        assert_eq!(
            fs::metadata(root.join("secrets.json"))
                .expect("secret file should exist")
                .permissions()
                .mode()
                & 0o7777,
            0o600
        );
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[cfg(unix)]
    #[test]
    fn unix_runtime_rejects_symlink_and_hardlink_secret_entries() {
        use std::os::unix::fs::symlink;

        let root = fixture("links");
        let store = AgentSecretStore::runtime(&root).expect("runtime store should initialize");
        store
            .store(SecretKind::CallMeshApiKey, "fixture-key")
            .expect("secret should store");
        let path = root.join("secrets.json");
        let outside = root.join("outside.json");
        fs::rename(&path, &outside).expect("secret should move");
        symlink(&outside, &path).expect("symlink should create");
        assert!(matches!(
            store.read(SecretKind::CallMeshApiKey),
            Err(SecretStoreError::Unavailable)
        ));
        fs::remove_file(&path).expect("symlink should remove");
        fs::hard_link(&outside, &path).expect("hardlink should create");
        assert!(matches!(
            store.read(SecretKind::CallMeshApiKey),
            Err(SecretStoreError::Unavailable)
        ));
        fs::remove_dir_all(root).expect("fixture should clean up");
    }
}
