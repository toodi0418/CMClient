//! Agent-owned access to the selected secret-storage backend.
//!
//! The secret values never have `Debug` or `Display` implementations. Callers
//! must deliberately expose a value only for the immediate privileged handoff.

#[cfg(any(target_os = "linux", test))]
use chacha20poly1305::{
    XChaCha20Poly1305, XNonce,
    aead::{AeadInPlace, KeyInit, OsRng, rand_core::RngCore},
};
use keyring::{Entry, Error as KeyringError};
#[cfg(unix)]
use serde::{Deserialize, Serialize};
#[cfg(not(any(unix, test)))]
use std::ffi::OsStr;
#[cfg(any(unix, test))]
use std::{
    ffi::OsStr,
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
    sync::Mutex,
};
use std::{path::Path, sync::Arc};
#[cfg(any(unix, test))]
use uuid::Uuid;
#[cfg(unix)]
use zeroize::Zeroize;
use zeroize::Zeroizing;

const SERVICE_NAME: &str = "io.cmclient.CMClient";
const MAX_SECRET_BYTES: usize = 4_096;
const PLAINTEXT_SECRET_FILE_ENVIRONMENT: &str = "CMCLIENT_PLAINTEXT_SECRET_FILE";
#[cfg(target_os = "linux")]
const SYSTEMD_SECRET_STORE_ENVIRONMENT: &str = "CMCLIENT_SYSTEMD_SECRET_STORE";
#[cfg(target_os = "linux")]
const SYSTEMD_CREDENTIALS_DIRECTORY: &str = "CREDENTIALS_DIRECTORY";
#[cfg(any(target_os = "linux", test))]
const SYSTEMD_WRAPPING_KEY_NAME: &str = "cmclient-secret-store-key";
#[cfg(any(target_os = "linux", test))]
const WRAPPING_KEY_BYTES: usize = 32;
#[cfg(any(target_os = "linux", test))]
const VAULT_MAGIC: &[u8; 8] = b"CMCSV01\0";
#[cfg(any(target_os = "linux", test))]
const VAULT_NONCE_BYTES: usize = 24;
#[cfg(any(target_os = "linux", test))]
const VAULT_TAG_BYTES: usize = 16;
#[cfg(any(target_os = "linux", test))]
const MAX_VAULT_FILE_BYTES: usize =
    VAULT_MAGIC.len() + VAULT_NONCE_BYTES + MAX_SECRET_BYTES + VAULT_TAG_BYTES;
#[cfg(unix)]
const PLAINTEXT_DOCUMENT_VERSION: u8 = 1;
#[cfg(unix)]
const MAX_PLAINTEXT_FILE_BYTES: usize = 32 * 1_024;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum SecretKind {
    CallMeshApiKey,
    AprsPasscode,
    ManagementAdminToken,
}

impl SecretKind {
    pub const ALL: [Self; 3] = [
        Self::CallMeshApiKey,
        Self::AprsPasscode,
        Self::ManagementAdminToken,
    ];

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

/// A deliberately non-printable secret value that zeroes its owned bytes on drop.
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

    #[cfg(test)]
    fn kind(&self) -> SecretBackendKind;
}

struct PlatformSecretBackend;

#[cfg(test)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SecretBackendKind {
    Platform,
    #[cfg(unix)]
    Plaintext,
    Service,
    Memory,
}

#[cfg(unix)]
struct PlaintextSecretBackend {
    path: PathBuf,
    parent: PathBuf,
    owner: u32,
    lock: Mutex<()>,
}

#[cfg(unix)]
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
        rename = "management-admin-token",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    management_admin_token: Option<String>,
}

#[cfg(any(target_os = "linux", test))]
struct ServiceSecretBackend {
    root: PathBuf,
    wrapping_key: Zeroizing<[u8; WRAPPING_KEY_BYTES]>,
    lock: Mutex<()>,
}

#[cfg(any(test, feature = "test-support"))]
#[derive(Default)]
struct MemorySecretBackend(std::sync::Mutex<std::collections::BTreeMap<SecretKind, String>>);

#[cfg(any(test, feature = "test-support"))]
impl SecretBackend for MemorySecretBackend {
    fn set(&self, kind: SecretKind, value: &str) -> Result<(), SecretStoreError> {
        self.0
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?
            .insert(kind, value.to_owned());
        Ok(())
    }

    fn get(&self, kind: SecretKind) -> Result<Option<SecretValue>, SecretStoreError> {
        Ok(self
            .0
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?
            .get(&kind)
            .cloned()
            .map(SecretValue::new))
    }

    fn delete(&self, kind: SecretKind) -> Result<bool, SecretStoreError> {
        Ok(self
            .0
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?
            .remove(&kind)
            .is_some())
    }

    #[cfg(test)]
    fn kind(&self) -> SecretBackendKind {
        SecretBackendKind::Memory
    }
}

#[cfg(unix)]
impl PlaintextSecretDocument {
    fn empty() -> Self {
        Self {
            version: PLAINTEXT_DOCUMENT_VERSION,
            callmesh_api_key: None,
            aprs_passcode: None,
            management_admin_token: None,
        }
    }

    fn value_mut(&mut self, kind: SecretKind) -> &mut Option<String> {
        match kind {
            SecretKind::CallMeshApiKey => &mut self.callmesh_api_key,
            SecretKind::AprsPasscode => &mut self.aprs_passcode,
            SecretKind::ManagementAdminToken => &mut self.management_admin_token,
        }
    }

    fn replace(&mut self, kind: SecretKind, value: String) {
        if let Some(mut previous) = self.value_mut(kind).replace(value) {
            previous.zeroize();
        }
    }

    fn take(&mut self, kind: SecretKind) -> Option<String> {
        self.value_mut(kind).take()
    }

    fn validate(&self) -> Result<(), SecretStoreError> {
        if self.version != PLAINTEXT_DOCUMENT_VERSION {
            return Err(SecretStoreError::Unavailable);
        }
        for value in [
            self.callmesh_api_key.as_deref(),
            self.aprs_passcode.as_deref(),
            self.management_admin_token.as_deref(),
        ]
        .into_iter()
        .flatten()
        {
            validate_secret(value).map_err(|_| SecretStoreError::Unavailable)?;
        }
        Ok(())
    }
}

#[cfg(unix)]
impl Drop for PlaintextSecretDocument {
    fn drop(&mut self) {
        for value in [
            &mut self.callmesh_api_key,
            &mut self.aprs_passcode,
            &mut self.management_admin_token,
        ] {
            if let Some(value) = value.as_mut() {
                value.zeroize();
            }
        }
    }
}

#[cfg(unix)]
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
        open_directory_no_follow(requested_parent)?;
        let parent =
            fs::canonicalize(requested_parent).map_err(|_| SecretStoreError::Unavailable)?;
        let path = parent.join(file_name);
        let owner = private_directory_owner(&parent)?;
        validate_optional_private_file(&path, owner)?;
        Ok(Self {
            path,
            parent,
            owner,
            lock: Mutex::new(()),
        })
    }

    fn validate_parent(&self) -> Result<(), SecretStoreError> {
        validate_private_directory(&self.parent, self.owner)
    }

    fn load(&self) -> Result<PlaintextSecretDocument, SecretStoreError> {
        self.validate_parent()?;
        let Some(mut input) = open_regular_file_no_follow(&self.path)? else {
            return Ok(PlaintextSecretDocument::empty());
        };
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
        validate_optional_private_file(&self.path, self.owner)?;
        let bytes = Zeroizing::new(
            serde_json::to_vec(document).map_err(|_| SecretStoreError::Unavailable)?,
        );
        if bytes.len() > MAX_PLAINTEXT_FILE_BYTES {
            return Err(SecretStoreError::Unavailable);
        }
        atomic_write_private(&self.parent, &self.path, bytes.as_slice())?;
        validate_optional_private_file(&self.path, self.owner)
    }
}

#[cfg(unix)]
impl SecretBackend for PlaintextSecretBackend {
    fn set(&self, kind: SecretKind, value: &str) -> Result<(), SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let mut document = self.load()?;
        document.replace(kind, value.to_owned());
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
        let Some(mut removed) = document.take(kind) else {
            return Ok(false);
        };
        removed.zeroize();
        self.save(&document)?;
        Ok(true)
    }

    #[cfg(test)]
    fn kind(&self) -> SecretBackendKind {
        SecretBackendKind::Plaintext
    }
}

impl PlatformSecretBackend {
    fn entry(kind: SecretKind) -> Result<Entry, SecretStoreError> {
        Entry::new(SERVICE_NAME, kind.identifier()).map_err(|_| SecretStoreError::Unavailable)
    }
}

impl SecretBackend for PlatformSecretBackend {
    fn set(&self, kind: SecretKind, value: &str) -> Result<(), SecretStoreError> {
        Self::entry(kind)?
            .set_password(value)
            .map_err(|_| SecretStoreError::Unavailable)
    }

    fn get(&self, kind: SecretKind) -> Result<Option<SecretValue>, SecretStoreError> {
        match Self::entry(kind)?.get_password() {
            Ok(value) => Ok(Some(SecretValue::new(value))),
            Err(KeyringError::NoEntry) => Ok(None),
            Err(_) => Err(SecretStoreError::Unavailable),
        }
    }

    fn delete(&self, kind: SecretKind) -> Result<bool, SecretStoreError> {
        match Self::entry(kind)?.delete_credential() {
            Ok(()) => Ok(true),
            Err(KeyringError::NoEntry) => Ok(false),
            Err(_) => Err(SecretStoreError::Unavailable),
        }
    }

    #[cfg(test)]
    fn kind(&self) -> SecretBackendKind {
        SecretBackendKind::Platform
    }
}

#[cfg(any(target_os = "linux", test))]
impl ServiceSecretBackend {
    fn new(
        root: PathBuf,
        wrapping_key: Zeroizing<[u8; WRAPPING_KEY_BYTES]>,
    ) -> Result<Self, SecretStoreError> {
        if !root.is_absolute() {
            return Err(SecretStoreError::Unavailable);
        }
        ensure_secure_directory(&root)?;
        Ok(Self {
            root,
            wrapping_key,
            lock: Mutex::new(()),
        })
    }

    fn from_credential_directory(
        data_dir: &Path,
        credentials_directory: &Path,
    ) -> Result<Self, SecretStoreError> {
        if !credentials_directory.is_absolute() {
            return Err(SecretStoreError::Unavailable);
        }
        validate_directory_no_follow(credentials_directory)?;
        let key_path = credentials_directory.join(SYSTEMD_WRAPPING_KEY_NAME);
        let wrapping_key = read_exact_regular_file(&key_path)?;
        Self::new(data_dir.join("secrets"), wrapping_key)
    }

    fn secret_path(&self, kind: SecretKind) -> PathBuf {
        self.root.join(format!("{}.secret", kind.identifier()))
    }

    fn cipher(&self) -> Result<XChaCha20Poly1305, SecretStoreError> {
        XChaCha20Poly1305::new_from_slice(self.wrapping_key.as_ref())
            .map_err(|_| SecretStoreError::Unavailable)
    }

    fn aad(kind: SecretKind) -> Vec<u8> {
        let mut aad = Vec::with_capacity(SERVICE_NAME.len() + kind.identifier().len() + 8);
        aad.extend_from_slice(SERVICE_NAME.as_bytes());
        aad.extend_from_slice(b"\0v1\0");
        aad.extend_from_slice(kind.identifier().as_bytes());
        aad
    }
}

#[cfg(any(target_os = "linux", test))]
impl SecretBackend for ServiceSecretBackend {
    fn set(&self, kind: SecretKind, value: &str) -> Result<(), SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        ensure_secure_directory(&self.root)?;
        let mut nonce = [0_u8; VAULT_NONCE_BYTES];
        OsRng.fill_bytes(&mut nonce);
        let mut ciphertext = Zeroizing::new(value.as_bytes().to_vec());
        self.cipher()?
            .encrypt_in_place(
                XNonce::from_slice(&nonce),
                Self::aad(kind).as_slice(),
                &mut *ciphertext,
            )
            .map_err(|_| SecretStoreError::Unavailable)?;
        let mut envelope = Vec::with_capacity(VAULT_MAGIC.len() + nonce.len() + ciphertext.len());
        envelope.extend_from_slice(VAULT_MAGIC);
        envelope.extend_from_slice(&nonce);
        envelope.extend_from_slice(ciphertext.as_slice());
        atomic_write_private(&self.root, &self.secret_path(kind), &envelope)
    }

    fn get(&self, kind: SecretKind) -> Result<Option<SecretValue>, SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let path = self.secret_path(kind);
        let envelope = match read_optional_regular_file(&path, MAX_VAULT_FILE_BYTES)? {
            Some(envelope) => envelope,
            None => return Ok(None),
        };
        let header_bytes = VAULT_MAGIC.len() + VAULT_NONCE_BYTES;
        if envelope.len() < header_bytes + VAULT_TAG_BYTES
            || &envelope[..VAULT_MAGIC.len()] != VAULT_MAGIC
        {
            return Err(SecretStoreError::Unavailable);
        }
        let nonce = XNonce::from_slice(&envelope[VAULT_MAGIC.len()..header_bytes]);
        let mut ciphertext = Zeroizing::new(envelope[header_bytes..].to_vec());
        self.cipher()?
            .decrypt_in_place(nonce, Self::aad(kind).as_slice(), &mut *ciphertext)
            .map_err(|_| SecretStoreError::Unavailable)?;
        let value = std::str::from_utf8(ciphertext.as_slice())
            .map_err(|_| SecretStoreError::Unavailable)?;
        validate_secret(value)?;
        Ok(Some(SecretValue::new(value.to_owned())))
    }

    fn delete(&self, kind: SecretKind) -> Result<bool, SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let path = self.secret_path(kind);
        if open_regular_file_no_follow(&path)?.is_none() {
            return Ok(false);
        }
        fs::remove_file(path).map_err(|_| SecretStoreError::Unavailable)?;
        sync_directory(&self.root)?;
        Ok(true)
    }

    #[cfg(test)]
    fn kind(&self) -> SecretBackendKind {
        SecretBackendKind::Service
    }
}

/// Access point for the Agent-selected credential backend.
#[derive(Clone)]
pub struct AgentSecretStore {
    backend: Arc<dyn SecretBackend>,
}

impl AgentSecretStore {
    pub fn platform() -> Self {
        Self {
            backend: Arc::new(PlatformSecretBackend),
        }
    }

    /// Selects an explicit private file, the packaged systemd vault, or the platform store.
    pub fn runtime(data_dir: &Path) -> Result<Self, SecretStoreError> {
        let plaintext_path =
            std::env::var_os(PLAINTEXT_SECRET_FILE_ENVIRONMENT).filter(|path| !path.is_empty());
        #[cfg(target_os = "linux")]
        let systemd_mode_value = std::env::var_os(SYSTEMD_SECRET_STORE_ENVIRONMENT);
        #[cfg(target_os = "linux")]
        let credentials_directory_value = std::env::var_os(SYSTEMD_CREDENTIALS_DIRECTORY);
        #[cfg(target_os = "linux")]
        let systemd_mode = systemd_mode_value.as_deref();
        #[cfg(target_os = "linux")]
        let credentials_directory = credentials_directory_value.as_deref();
        #[cfg(not(target_os = "linux"))]
        let systemd_mode: Option<&OsStr> = None;
        #[cfg(not(target_os = "linux"))]
        let credentials_directory: Option<&OsStr> = None;
        Self::from_runtime_environment(
            data_dir,
            plaintext_path.as_deref(),
            systemd_mode,
            credentials_directory,
        )
    }

    fn from_runtime_environment(
        data_dir: &Path,
        plaintext_path: Option<&OsStr>,
        mode: Option<&OsStr>,
        credentials_directory: Option<&OsStr>,
    ) -> Result<Self, SecretStoreError> {
        let plaintext_path = plaintext_path.filter(|path| !path.is_empty());
        #[cfg(any(target_os = "linux", test))]
        {
            let systemd_requested = mode.is_some() || credentials_directory.is_some();
            if plaintext_path.is_some() && systemd_requested {
                return Err(SecretStoreError::Unavailable);
            }
            if systemd_requested {
                if mode != Some(OsStr::new("1")) {
                    return Err(SecretStoreError::Unavailable);
                }
                let credentials_directory = credentials_directory
                    .map(PathBuf::from)
                    .filter(|path| path.is_absolute())
                    .ok_or(SecretStoreError::Unavailable)?;
                return Ok(Self {
                    backend: Arc::new(ServiceSecretBackend::from_credential_directory(
                        data_dir,
                        &credentials_directory,
                    )?),
                });
            }
        }
        let _ = (mode, credentials_directory);

        if let Some(path) = plaintext_path {
            #[cfg(unix)]
            {
                return Ok(Self {
                    backend: Arc::new(PlaintextSecretBackend::new(PathBuf::from(path))?),
                });
            }
            #[cfg(not(unix))]
            {
                let _ = path;
                return Err(SecretStoreError::Unavailable);
            }
        }

        let _ = data_dir;
        Ok(Self::platform())
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
}

#[cfg(any(target_os = "linux", test))]
fn ensure_secure_directory(path: &Path) -> Result<(), SecretStoreError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => return Err(SecretStoreError::Unavailable),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(|_| SecretStoreError::Unavailable)?;
        }
        Err(_) => return Err(SecretStoreError::Unavailable),
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let directory = open_directory_no_follow(path)?;
        directory
            .set_permissions(fs::Permissions::from_mode(0o700))
            .map_err(|_| SecretStoreError::Unavailable)?;
    }
    #[cfg(not(unix))]
    validate_directory_no_follow(path)?;
    Ok(())
}

#[cfg(any(target_os = "linux", test))]
fn read_exact_regular_file<const N: usize>(
    path: &Path,
) -> Result<Zeroizing<[u8; N]>, SecretStoreError> {
    let Some(mut input) = open_regular_file_no_follow(path)? else {
        return Err(SecretStoreError::Unavailable);
    };
    let metadata = input
        .metadata()
        .map_err(|_| SecretStoreError::Unavailable)?;
    if metadata.len() != N as u64 {
        return Err(SecretStoreError::Unavailable);
    }
    let mut bytes = Zeroizing::new([0_u8; N]);
    input
        .read_exact(bytes.as_mut())
        .map_err(|_| SecretStoreError::Unavailable)?;
    let mut trailing = [0_u8; 1];
    if input
        .read(&mut trailing)
        .map_err(|_| SecretStoreError::Unavailable)?
        != 0
    {
        return Err(SecretStoreError::Unavailable);
    }
    Ok(bytes)
}

#[cfg(any(target_os = "linux", test))]
fn read_optional_regular_file(
    path: &Path,
    maximum_bytes: usize,
) -> Result<Option<Vec<u8>>, SecretStoreError> {
    let Some(input) = open_regular_file_no_follow(path)? else {
        return Ok(None);
    };
    let metadata = input
        .metadata()
        .map_err(|_| SecretStoreError::Unavailable)?;
    if metadata.len() > maximum_bytes as u64 {
        return Err(SecretStoreError::Unavailable);
    }
    let mut bytes = Vec::with_capacity(metadata.len() as usize);
    input
        .take(maximum_bytes as u64 + 1)
        .read_to_end(&mut bytes)
        .map_err(|_| SecretStoreError::Unavailable)?;
    if bytes.len() > maximum_bytes {
        return Err(SecretStoreError::Unavailable);
    }
    Ok(Some(bytes))
}

#[cfg(any(unix, test))]
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
    let metadata = input
        .metadata()
        .map_err(|_| SecretStoreError::Unavailable)?;
    if !metadata.file_type().is_file() {
        return Err(SecretStoreError::Unavailable);
    }
    Ok(Some(input))
}

#[cfg(unix)]
fn open_directory_no_follow(path: &Path) -> Result<File, SecretStoreError> {
    let mut options = OpenOptions::new();
    options.read(true);
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_CLOEXEC | libc::O_DIRECTORY | libc::O_NOFOLLOW);
    }
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

#[cfg(any(target_os = "linux", test))]
fn validate_directory_no_follow(path: &Path) -> Result<(), SecretStoreError> {
    #[cfg(unix)]
    {
        open_directory_no_follow(path)?;
        Ok(())
    }

    #[cfg(not(unix))]
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(()),
        _ => Err(SecretStoreError::Unavailable),
    }
}

#[cfg(unix)]
fn private_directory_owner(path: &Path) -> Result<u32, SecretStoreError> {
    let directory = open_directory_no_follow(path)?;
    let metadata = directory
        .metadata()
        .map_err(|_| SecretStoreError::Unavailable)?;
    use std::os::unix::fs::MetadataExt;
    if metadata.mode() & 0o7777 != 0o700 {
        return Err(SecretStoreError::Unavailable);
    }

    // A probe created through the same directory is the portable std-only way
    // to identify the current effective owner without introducing unsafe FFI.
    let probe = path.join(format!(".cmclient-owner-{}.tmp", Uuid::new_v4().simple()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    use std::os::unix::fs::PermissionsExt;
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        options.mode(0o600);
    }
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
    let directory = open_directory_no_follow(path)?;
    use std::os::unix::fs::MetadataExt;
    let metadata = directory
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

#[cfg(any(unix, test))]
fn atomic_write_private(root: &Path, path: &Path, bytes: &[u8]) -> Result<(), SecretStoreError> {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    let temporary = root.join(format!(".secret-{}.tmp", Uuid::new_v4().simple()));
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW);
        options.mode(0o600);
    }
    let result = (|| {
        let mut output = options
            .open(&temporary)
            .map_err(|_| SecretStoreError::Unavailable)?;
        #[cfg(unix)]
        output
            .set_permissions(fs::Permissions::from_mode(0o600))
            .map_err(|_| SecretStoreError::Unavailable)?;
        output
            .write_all(bytes)
            .map_err(|_| SecretStoreError::Unavailable)?;
        output
            .sync_all()
            .map_err(|_| SecretStoreError::Unavailable)?;
        fs::rename(&temporary, path).map_err(|_| SecretStoreError::Unavailable)?;
        sync_directory(root)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

#[cfg(unix)]
fn sync_directory(path: &Path) -> Result<(), SecretStoreError> {
    open_directory_no_follow(path)?
        .sync_all()
        .map_err(|_| SecretStoreError::Unavailable)
}

#[cfg(all(test, not(unix)))]
fn sync_directory(_path: &Path) -> Result<(), SecretStoreError> {
    Ok(())
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
    #[cfg(unix)]
    use super::MAX_PLAINTEXT_FILE_BYTES;
    use super::{
        AgentSecretStore, SecretBackend, SecretBackendKind, SecretKind, SecretStoreError,
        ServiceSecretBackend, VAULT_MAGIC,
    };
    #[cfg(unix)]
    use std::path::PathBuf;
    use std::{ffi::OsStr, fs, path::Path, sync::Arc};
    use uuid::Uuid;
    use zeroize::Zeroizing;

    #[test]
    fn stores_reads_and_removes_values_without_printable_secret_types() {
        let store = AgentSecretStore::memory();
        store
            .store(SecretKind::CallMeshApiKey, "callmesh-value")
            .expect("secret should store");
        let value = store
            .read(SecretKind::CallMeshApiKey)
            .expect("secret read should succeed")
            .expect("secret should exist");
        assert_eq!(value.expose_secret(), "callmesh-value");
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
    }

    #[test]
    fn rejects_empty_control_and_oversized_values() {
        let store = AgentSecretStore::memory();
        for invalid in [String::new(), String::from("line\nfeed"), "a".repeat(4_097)] {
            assert_eq!(
                store.store(SecretKind::AprsPasscode, &invalid),
                Err(SecretStoreError::InvalidValue)
            );
        }
    }

    #[test]
    fn service_vault_persists_authenticated_ciphertext_without_plaintext() {
        let root = std::env::temp_dir().join(format!("cmclient-secret-vault-{}", Uuid::new_v4()));
        let key = [0x5a; 32];
        let store = AgentSecretStore::with_backend(Arc::new(
            ServiceSecretBackend::new(root.clone(), Zeroizing::new(key))
                .expect("vault should initialize"),
        ));
        store
            .store(SecretKind::AprsPasscode, "fixture-passcode")
            .expect("secret should store");
        let path = root.join("aprs-passcode.secret");
        let envelope = fs::read(&path).expect("ciphertext should exist");
        assert!(envelope.starts_with(VAULT_MAGIC));
        assert!(
            !envelope
                .windows(b"fixture-passcode".len())
                .any(|window| window == b"fixture-passcode")
        );

        let reopened = AgentSecretStore::with_backend(Arc::new(
            ServiceSecretBackend::new(root.clone(), Zeroizing::new(key))
                .expect("vault should reopen"),
        ));
        assert_eq!(
            reopened
                .read(SecretKind::AprsPasscode)
                .expect("secret should decrypt")
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
    fn service_vault_fails_closed_for_tamper_wrong_key_and_non_file_entry() {
        let root = std::env::temp_dir().join(format!("cmclient-secret-tamper-{}", Uuid::new_v4()));
        let key = [0x31; 32];
        let store = AgentSecretStore::with_backend(Arc::new(
            ServiceSecretBackend::new(root.clone(), Zeroizing::new(key))
                .expect("vault should initialize"),
        ));
        store
            .store(SecretKind::CallMeshApiKey, "fixture-api-key")
            .expect("secret should store");
        let path = root.join("callmesh-api-key.secret");

        let wrong_key = AgentSecretStore::with_backend(Arc::new(
            ServiceSecretBackend::new(root.clone(), Zeroizing::new([0x32; 32]))
                .expect("vault should initialize with another key"),
        ));
        assert!(matches!(
            wrong_key.read(SecretKind::CallMeshApiKey),
            Err(SecretStoreError::Unavailable)
        ));

        let mut envelope = fs::read(&path).expect("ciphertext should exist");
        let last = envelope.last_mut().expect("ciphertext should not be empty");
        *last ^= 0x80;
        fs::write(&path, envelope).expect("fixture should tamper ciphertext");
        assert!(matches!(
            store.read(SecretKind::CallMeshApiKey),
            Err(SecretStoreError::Unavailable)
        ));

        fs::remove_file(&path).expect("ciphertext should remove");
        fs::create_dir(&path).expect("fixture should create invalid entry");
        assert!(matches!(
            store.remove(SecretKind::CallMeshApiKey),
            Err(SecretStoreError::Unavailable)
        ));
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[cfg(unix)]
    #[test]
    fn service_vault_rejects_symlinked_roots_credentials_and_ciphertext() {
        use std::os::unix::fs::symlink;

        let fixture = std::env::temp_dir().join(format!(
            "cmclient-secret-symlink-hardening-{}",
            Uuid::new_v4()
        ));
        let root = fixture.join("vault");
        let store = AgentSecretStore::with_backend(Arc::new(
            ServiceSecretBackend::new(root.clone(), Zeroizing::new([0x61; 32]))
                .expect("vault should initialize"),
        ));
        store
            .store(SecretKind::CallMeshApiKey, "fixture-api-key")
            .expect("secret should store");

        let ciphertext = root.join("callmesh-api-key.secret");
        let outside_ciphertext = fixture.join("outside.secret");
        fs::rename(&ciphertext, &outside_ciphertext).expect("ciphertext should move outside");
        symlink(&outside_ciphertext, &ciphertext).expect("ciphertext symlink should create");
        assert!(matches!(
            store.read(SecretKind::CallMeshApiKey),
            Err(SecretStoreError::Unavailable)
        ));
        assert!(matches!(
            store.remove(SecretKind::CallMeshApiKey),
            Err(SecretStoreError::Unavailable)
        ));
        assert!(outside_ciphertext.is_file());

        let linked_root = fixture.join("linked-vault");
        symlink(&root, &linked_root).expect("vault symlink should create");
        assert!(matches!(
            ServiceSecretBackend::new(linked_root, Zeroizing::new([0x61; 32])),
            Err(SecretStoreError::Unavailable)
        ));

        let credentials = fixture.join("credentials");
        fs::create_dir(&credentials).expect("credentials should create");
        fs::write(credentials.join("cmclient-secret-store-key"), [0x61; 32])
            .expect("credential should write");
        let linked_credentials = fixture.join("linked-credentials");
        symlink(&credentials, &linked_credentials).expect("credential symlink should create");
        assert!(matches!(
            ServiceSecretBackend::from_credential_directory(
                &fixture.join("linked-data"),
                &linked_credentials,
            ),
            Err(SecretStoreError::Unavailable)
        ));

        fs::remove_dir_all(fixture).expect("fixture should clean up");
    }

    #[cfg(unix)]
    #[test]
    fn service_vault_uses_private_directory_and_file_modes() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!("cmclient-secret-mode-{}", Uuid::new_v4()));
        let store = AgentSecretStore::with_backend(Arc::new(
            ServiceSecretBackend::new(root.clone(), Zeroizing::new([0x44; 32]))
                .expect("vault should initialize"),
        ));
        store
            .store(SecretKind::ManagementAdminToken, "fixture-token")
            .expect("secret should store");
        assert_eq!(
            fs::metadata(&root)
                .expect("vault should exist")
                .permissions()
                .mode()
                & 0o777,
            0o700
        );
        assert_eq!(
            fs::metadata(root.join("management-admin-token.secret"))
                .expect("ciphertext should exist")
                .permissions()
                .mode()
                & 0o777,
            0o600
        );
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[test]
    fn service_vault_requires_an_exact_regular_systemd_wrapping_key() {
        let fixture =
            std::env::temp_dir().join(format!("cmclient-systemd-credential-{}", Uuid::new_v4()));
        let credentials = fixture.join("credentials");
        let data = fixture.join("data");
        fs::create_dir_all(&credentials).expect("credential directory should exist");
        let key_path = credentials.join("cmclient-secret-store-key");
        fs::write(&key_path, [0x71; 32]).expect("wrapping key should write");
        let backend = ServiceSecretBackend::from_credential_directory(&data, &credentials)
            .expect("exact wrapping key should load");
        backend
            .set(SecretKind::AprsPasscode, "fixture-passcode")
            .expect("loaded key should encrypt");
        let runtime = AgentSecretStore::from_runtime_environment(
            &data,
            None,
            Some(OsStr::new("1")),
            Some(credentials.as_os_str()),
        )
        .expect("systemd environment should select the service vault");
        assert_eq!(
            runtime
                .read(SecretKind::AprsPasscode)
                .expect("runtime vault should decrypt")
                .expect("runtime secret should exist")
                .expose_secret(),
            "fixture-passcode"
        );
        assert!(matches!(
            AgentSecretStore::from_runtime_environment(
                &data,
                None,
                Some(OsStr::new("unexpected")),
                Some(credentials.as_os_str()),
            ),
            Err(SecretStoreError::Unavailable)
        ));

        fs::write(&key_path, [0x71; 31]).expect("short key should write");
        assert!(matches!(
            ServiceSecretBackend::from_credential_directory(&data, &credentials),
            Err(SecretStoreError::Unavailable)
        ));

        #[cfg(unix)]
        {
            use std::os::unix::fs::symlink;

            let outside = fixture.join("outside-key");
            fs::write(&outside, [0x72; 32]).expect("outside key should write");
            fs::remove_file(&key_path).expect("short key should remove");
            symlink(&outside, &key_path).expect("key symlink should create");
            assert!(matches!(
                ServiceSecretBackend::from_credential_directory(&data, &credentials),
                Err(SecretStoreError::Unavailable)
            ));
        }
        fs::remove_dir_all(fixture).expect("fixture should clean up");
    }

    #[cfg(unix)]
    fn private_fixture(label: &str) -> (PathBuf, PathBuf) {
        use std::os::unix::fs::PermissionsExt;

        let root =
            std::env::temp_dir().join(format!("cmclient-plaintext-{label}-{}", Uuid::new_v4()));
        fs::create_dir(&root).expect("fixture directory should exist");
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("fixture directory should be private");
        let path = root.join("secrets.json");
        (root, path)
    }

    #[cfg(unix)]
    #[test]
    fn plaintext_runtime_selects_file_backend_and_never_platform_backend() {
        use std::os::unix::fs::PermissionsExt;

        let (root, path) = private_fixture("selector");
        let store =
            AgentSecretStore::from_runtime_environment(&root, Some(path.as_os_str()), None, None)
                .expect("explicit plaintext path should select file backend");
        assert_eq!(store.backend_kind(), SecretBackendKind::Plaintext);
        store
            .store(SecretKind::CallMeshApiKey, "fixture-\"quoted\\value")
            .expect("plaintext secret should store");
        store
            .store(SecretKind::AprsPasscode, "fixture-passcode")
            .expect("second plaintext secret should store");
        let metadata = fs::metadata(&path).expect("plaintext file should exist");
        assert_eq!(metadata.permissions().mode() & 0o7777, 0o600);
        let raw = fs::read_to_string(&path).expect("plaintext document should be readable");
        assert!(raw.contains("\"version\":1"));
        assert!(raw.contains("callmesh-api-key"));
        assert_eq!(
            store
                .read(SecretKind::CallMeshApiKey)
                .expect("plaintext read should succeed")
                .expect("stored secret should exist")
                .expose_secret(),
            "fixture-\"quoted\\value"
        );
        assert!(
            store
                .remove(SecretKind::CallMeshApiKey)
                .expect("remove should succeed")
        );
        assert!(
            store
                .remove(SecretKind::AprsPasscode)
                .expect("remove should succeed")
        );
        assert!(
            !store
                .remove(SecretKind::AprsPasscode)
                .expect("second remove should be a no-op")
        );
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[cfg(unix)]
    #[test]
    fn plaintext_backend_rejects_relative_paths_unsafe_modes_and_final_links() {
        use std::os::unix::fs::{PermissionsExt, symlink};

        let (root, path) = private_fixture("permissions");
        assert!(
            AgentSecretStore::from_runtime_environment(
                &root,
                Some(OsStr::new("relative-secrets.json")),
                None,
                None
            )
            .is_err()
        );
        fs::set_permissions(&root, fs::Permissions::from_mode(0o755))
            .expect("fixture mode should change");
        assert!(
            AgentSecretStore::from_runtime_environment(&root, Some(path.as_os_str()), None, None)
                .is_err()
        );
        fs::set_permissions(&root, fs::Permissions::from_mode(0o700))
            .expect("fixture mode should restore");

        fs::write(&path, br#"{"version":1,"callmesh-api-key":"fixture"}"#)
            .expect("fixture file should write");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o644))
            .expect("fixture file mode should change");
        assert!(
            AgentSecretStore::from_runtime_environment(&root, Some(path.as_os_str()), None, None)
                .is_err()
        );
        fs::remove_file(&path).expect("fixture file should remove");

        let outside = root.join("outside.json");
        fs::write(&outside, br#"{"version":1}"#).expect("outside file should write");
        symlink(&outside, &path).expect("secret symlink should create");
        assert!(
            AgentSecretStore::from_runtime_environment(&root, Some(path.as_os_str()), None, None)
                .is_err()
        );
        fs::remove_file(&path).expect("secret symlink should remove");

        fs::write(&path, br#"{"version":1}"#).expect("hardlink source should write");
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
            .expect("hardlink source should be private");
        let hardlink = root.join("linked-secrets.json");
        fs::hard_link(&path, &hardlink).expect("hardlink should create");
        assert!(
            AgentSecretStore::from_runtime_environment(&root, Some(path.as_os_str()), None, None)
                .is_err()
        );
        fs::remove_file(&hardlink).expect("hardlink should remove");
        fs::remove_file(&path).expect("hardlink source should remove");

        let linked_root = root.with_extension("linked");
        symlink(&root, &linked_root).expect("parent symlink should create");
        let linked_path = linked_root.join("secrets.json");
        assert!(
            AgentSecretStore::from_runtime_environment(
                &root,
                Some(linked_path.as_os_str()),
                None,
                None,
            )
            .is_err()
        );
        fs::remove_file(&linked_root).expect("parent symlink should remove");

        fs::create_dir(&path).expect("directory fixture should create");
        assert!(
            AgentSecretStore::from_runtime_environment(&root, Some(path.as_os_str()), None, None)
                .is_err()
        );
        fs::remove_dir(&path).expect("directory fixture should remove");
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[cfg(unix)]
    #[test]
    fn plaintext_backend_rejects_duplicate_unknown_corrupt_and_oversized_documents() {
        use std::os::unix::fs::PermissionsExt;

        let (root, path) = private_fixture("parser");
        let store =
            AgentSecretStore::from_runtime_environment(&root, Some(path.as_os_str()), None, None)
                .expect("plaintext backend should initialize");
        for document in [
            br#"{"version":1,"callmesh-api-key":"a","callmesh-api-key":"b"}"#.to_vec(),
            br#"{"version":1,"unexpected":"a"}"#.to_vec(),
            br#"{"version":2}"#.to_vec(),
            vec![b'{'; MAX_PLAINTEXT_FILE_BYTES + 1],
        ] {
            fs::write(&path, document).expect("invalid document should write");
            fs::set_permissions(&path, fs::Permissions::from_mode(0o600))
                .expect("invalid document should be private");
            assert!(store.read(SecretKind::CallMeshApiKey).is_err());
        }
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[test]
    fn plaintext_and_systemd_secret_modes_fail_closed_when_both_are_requested() {
        let fixture =
            std::env::temp_dir().join(format!("cmclient-secret-conflict-{}", Uuid::new_v4()));
        #[cfg(any(target_os = "linux", test))]
        assert!(
            AgentSecretStore::from_runtime_environment(
                &fixture,
                Some(OsStr::new("/absolute/plaintext-secrets.json")),
                Some(OsStr::new("1")),
                None,
            )
            .is_err()
        );
    }

    #[test]
    fn empty_plaintext_opt_in_keeps_the_platform_backend() {
        let store = AgentSecretStore::from_runtime_environment(
            Path::new("/unused"),
            Some(OsStr::new("")),
            None,
            None,
        )
        .expect("empty opt-in should be treated as unset");
        assert_eq!(store.backend_kind(), SecretBackendKind::Platform);
    }
}
