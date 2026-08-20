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
use uuid::Uuid;
use zeroize::{Zeroize, Zeroizing};

const PLAINTEXT_DOCUMENT_VERSION: u8 = 1;
const MAX_SECRET_BYTES: usize = 4_096;
const MAX_PLAINTEXT_FILE_BYTES: usize = 32 * 1_024;
const MAX_CMCLOUD_ENDPOINT_BYTES: usize = 2_048;
const MAX_CMCLOUD_CLIENT_VERSION_BYTES: usize = 64;
const MAX_CMCLOUD_DEVICE_CREDENTIAL_BYTES: usize = 512;
const MAX_SAFE_JAVASCRIPT_INTEGER: u64 = 9_007_199_254_740_991;

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

/// The non-secret fence associated with one CMCloud device credential.
///
/// The endpoint is retained with the credential so an Agent cannot accidentally
/// hand a credential issued by one CMCloud deployment to another deployment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CMCloudInstallationIdentity {
    endpoint: String,
    installation_id: String,
    installation_generation: u64,
    credential_version: u64,
}

impl CMCloudInstallationIdentity {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn installation_id(&self) -> &str {
        &self.installation_id
    }

    pub const fn installation_generation(&self) -> u64 {
        self.installation_generation
    }

    pub const fn credential_version(&self) -> u64 {
        self.credential_version
    }
}

/// The sole CMCloud credential eligible for a Gateway private bootstrap.
///
/// Pairing codes and pending enrollment credentials are deliberately not
/// represented by this type.
pub struct CMCloudActiveDeviceCredential {
    identity: CMCloudInstallationIdentity,
    device_credential: SecretValue,
}

impl CMCloudActiveDeviceCredential {
    pub fn identity(&self) -> &CMCloudInstallationIdentity {
        &self.identity
    }

    pub fn device_credential(&self) -> &SecretValue {
        &self.device_credential
    }
}

/// A durable, Agent-only CMCloud pairing attempt.
///
/// It retains the original boot and installation fences so a process restart
/// can complete the server's ten-minute pairing recovery protocol without
/// asking the Gateway to see a pairing credential.
pub struct CMCloudEnrollmentAttempt {
    endpoint: String,
    pairing_code: SecretValue,
    installation_id: String,
    requested_installation_generation: u64,
    boot_id: String,
    client_version: String,
    issued: Option<CMCloudIssuedDeviceCredential>,
}

impl CMCloudEnrollmentAttempt {
    pub fn endpoint(&self) -> &str {
        &self.endpoint
    }

    pub fn pairing_code(&self) -> &SecretValue {
        &self.pairing_code
    }

    pub fn installation_id(&self) -> &str {
        &self.installation_id
    }

    pub const fn requested_installation_generation(&self) -> u64 {
        self.requested_installation_generation
    }

    pub fn boot_id(&self) -> &str {
        &self.boot_id
    }

    pub fn client_version(&self) -> &str {
        &self.client_version
    }

    pub fn issued(&self) -> Option<&CMCloudIssuedDeviceCredential> {
        self.issued.as_ref()
    }
}

/// A pending device credential issued by CMCloud but not yet acknowledged.
///
/// This remains Agent-only until CMCloud confirms `enrollment_ack`; then the
/// store atomically promotes it to [`CMCloudActiveDeviceCredential`].
pub struct CMCloudIssuedDeviceCredential {
    identity: CMCloudInstallationIdentity,
    connection_epoch: u64,
    device_credential: SecretValue,
}

impl CMCloudIssuedDeviceCredential {
    pub fn identity(&self) -> &CMCloudInstallationIdentity {
        &self.identity
    }

    pub const fn connection_epoch(&self) -> u64 {
        self.connection_epoch
    }

    pub fn device_credential(&self) -> &SecretValue {
        &self.device_credential
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
    fn begin_cmcloud_enrollment(
        &self,
        endpoint: &str,
        pairing_code: &str,
        client_version: &str,
    ) -> Result<CMCloudEnrollmentAttempt, SecretStoreError>;
    fn cmcloud_enrollment_attempt(
        &self,
    ) -> Result<Option<CMCloudEnrollmentAttempt>, SecretStoreError>;
    fn discard_cmcloud_enrollment(&self) -> Result<bool, SecretStoreError>;
    fn record_cmcloud_issued_credential(
        &self,
        installation_generation: u64,
        credential_version: u64,
        connection_epoch: u64,
        device_credential: &str,
    ) -> Result<(), SecretStoreError>;
    fn activate_cmcloud_credential(
        &self,
        installation_generation: u64,
        credential_version: u64,
        connection_epoch: u64,
    ) -> Result<CMCloudInstallationIdentity, SecretStoreError>;
    fn cmcloud_active_device_credential(
        &self,
    ) -> Result<Option<CMCloudActiveDeviceCredential>, SecretStoreError>;

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

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CMCloudActiveDeviceSecret {
    endpoint: String,
    installation_id: String,
    installation_generation: u64,
    credential_version: u64,
    device_credential: String,
}

impl CMCloudActiveDeviceSecret {
    fn identity(&self) -> CMCloudInstallationIdentity {
        CMCloudInstallationIdentity {
            endpoint: self.endpoint.clone(),
            installation_id: self.installation_id.clone(),
            installation_generation: self.installation_generation,
            credential_version: self.credential_version,
        }
    }

    fn into_active_credential(mut self) -> CMCloudActiveDeviceCredential {
        CMCloudActiveDeviceCredential {
            identity: CMCloudInstallationIdentity {
                endpoint: std::mem::take(&mut self.endpoint),
                installation_id: std::mem::take(&mut self.installation_id),
                installation_generation: self.installation_generation,
                credential_version: self.credential_version,
            },
            device_credential: SecretValue::new(std::mem::take(&mut self.device_credential)),
        }
    }

    fn validate(&self) -> Result<(), SecretStoreError> {
        validate_cmcloud_endpoint(&self.endpoint)?;
        validate_uuid(&self.installation_id)?;
        validate_generation(self.installation_generation)?;
        validate_credential_version(self.credential_version)?;
        validate_cmcloud_bearer(&self.device_credential)
    }
}

impl Drop for CMCloudActiveDeviceSecret {
    fn drop(&mut self) {
        self.device_credential.zeroize();
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CMCloudEnrollmentTransaction {
    endpoint: String,
    pairing_code: String,
    installation_id: String,
    requested_installation_generation: u64,
    boot_id: String,
    client_version: String,
    issued_device_credential: Option<String>,
    issued_installation_generation: Option<u64>,
    issued_credential_version: Option<u64>,
    issued_connection_epoch: Option<u64>,
}

impl CMCloudEnrollmentTransaction {
    fn new(
        endpoint: &str,
        pairing_code: &str,
        client_version: &str,
        active: Option<&CMCloudActiveDeviceSecret>,
    ) -> Self {
        // Re-pairing the same CMCloud installation must preserve its lane
        // cursor. The server advances generation and credential version.
        let (installation_id, requested_installation_generation) = active
            .filter(|credential| credential.endpoint == endpoint)
            .map(|credential| {
                (
                    credential.installation_id.clone(),
                    credential.installation_generation,
                )
            })
            .unwrap_or_else(|| (Uuid::new_v4().to_string(), 0));
        Self {
            endpoint: endpoint.to_owned(),
            pairing_code: pairing_code.to_owned(),
            installation_id,
            requested_installation_generation,
            boot_id: Uuid::new_v4().to_string(),
            client_version: client_version.to_owned(),
            issued_device_credential: None,
            issued_installation_generation: None,
            issued_credential_version: None,
            issued_connection_epoch: None,
        }
    }

    fn into_attempt(mut self) -> CMCloudEnrollmentAttempt {
        let endpoint = std::mem::take(&mut self.endpoint);
        let installation_id = std::mem::take(&mut self.installation_id);
        let issued = match (
            self.issued_device_credential.take(),
            self.issued_installation_generation.take(),
            self.issued_credential_version.take(),
            self.issued_connection_epoch.take(),
        ) {
            (
                Some(device_credential),
                Some(installation_generation),
                Some(credential_version),
                Some(connection_epoch),
            ) => Some(CMCloudIssuedDeviceCredential {
                identity: CMCloudInstallationIdentity {
                    endpoint: endpoint.clone(),
                    installation_id: installation_id.clone(),
                    installation_generation,
                    credential_version,
                },
                connection_epoch,
                device_credential: SecretValue::new(device_credential),
            }),
            _ => None,
        };
        CMCloudEnrollmentAttempt {
            endpoint,
            pairing_code: SecretValue::new(std::mem::take(&mut self.pairing_code)),
            installation_id,
            requested_installation_generation: self.requested_installation_generation,
            boot_id: std::mem::take(&mut self.boot_id),
            client_version: std::mem::take(&mut self.client_version),
            issued,
        }
    }

    fn validate(&self) -> Result<(), SecretStoreError> {
        validate_cmcloud_endpoint(&self.endpoint)?;
        validate_cmcloud_bearer(&self.pairing_code)?;
        validate_uuid(&self.installation_id)?;
        validate_generation(self.requested_installation_generation)?;
        validate_uuid(&self.boot_id)?;
        validate_cmcloud_client_version(&self.client_version)?;
        match (
            self.issued_device_credential.as_deref(),
            self.issued_installation_generation,
            self.issued_credential_version,
            self.issued_connection_epoch,
        ) {
            (None, None, None, None) => Ok(()),
            (
                Some(device_credential),
                Some(installation_generation),
                Some(credential_version),
                Some(connection_epoch),
            ) => {
                validate_cmcloud_bearer(device_credential)?;
                validate_generation(installation_generation)?;
                validate_credential_version(credential_version)?;
                validate_connection_epoch(connection_epoch)
            }
            _ => Err(SecretStoreError::Unavailable),
        }
    }

    fn record_issued(
        &mut self,
        installation_generation: u64,
        credential_version: u64,
        connection_epoch: u64,
        device_credential: &str,
    ) -> Result<(), SecretStoreError> {
        validate_generation(installation_generation)?;
        validate_credential_version(credential_version)?;
        validate_connection_epoch(connection_epoch)?;
        validate_cmcloud_bearer(device_credential)?;
        if let Some(previous) = self.issued_device_credential.as_deref() {
            if previous != device_credential
                || self.issued_installation_generation != Some(installation_generation)
                || self.issued_credential_version != Some(credential_version)
            {
                return Err(SecretStoreError::Unavailable);
            }
        }
        if let Some(mut previous) = self
            .issued_device_credential
            .replace(device_credential.to_owned())
        {
            previous.zeroize();
        }
        self.issued_installation_generation = Some(installation_generation);
        self.issued_credential_version = Some(credential_version);
        self.issued_connection_epoch = Some(connection_epoch);
        Ok(())
    }

    fn activate(
        mut self,
        installation_generation: u64,
        credential_version: u64,
        connection_epoch: u64,
    ) -> Result<CMCloudActiveDeviceSecret, SecretStoreError> {
        if self.issued_installation_generation != Some(installation_generation)
            || self.issued_credential_version != Some(credential_version)
            || self.issued_connection_epoch != Some(connection_epoch)
        {
            return Err(SecretStoreError::Unavailable);
        }
        let device_credential = self
            .issued_device_credential
            .take()
            .ok_or(SecretStoreError::Unavailable)?;
        Ok(CMCloudActiveDeviceSecret {
            endpoint: std::mem::take(&mut self.endpoint),
            installation_id: std::mem::take(&mut self.installation_id),
            installation_generation,
            credential_version,
            device_credential,
        })
    }
}

impl Drop for CMCloudEnrollmentTransaction {
    fn drop(&mut self) {
        self.pairing_code.zeroize();
        if let Some(issued) = self.issued_device_credential.as_mut() {
            issued.zeroize();
        }
    }
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
    #[serde(
        rename = "cmcloud-active-device",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    cmcloud_active_device: Option<CMCloudActiveDeviceSecret>,
    #[serde(
        rename = "cmcloud-enrollment-transaction",
        default,
        skip_serializing_if = "Option::is_none"
    )]
    cmcloud_enrollment_transaction: Option<CMCloudEnrollmentTransaction>,
}

impl PlaintextSecretDocument {
    fn empty() -> Self {
        Self {
            version: PLAINTEXT_DOCUMENT_VERSION,
            callmesh_api_key: None,
            aprs_passcode: None,
            setup_callmesh_transaction: None,
            cmcloud_active_device: None,
            cmcloud_enrollment_transaction: None,
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
        if let Some(active) = self.cmcloud_active_device.as_ref() {
            active
                .validate()
                .map_err(|_| SecretStoreError::Unavailable)?;
        }
        if let Some(transaction) = self.cmcloud_enrollment_transaction.as_ref() {
            transaction
                .validate()
                .map_err(|_| SecretStoreError::Unavailable)?;
            if let Some(active) = self.cmcloud_active_device.as_ref() {
                if active.installation_id == transaction.installation_id
                    && (active.endpoint != transaction.endpoint
                        || active.installation_generation
                            != transaction.requested_installation_generation)
                {
                    return Err(SecretStoreError::Unavailable);
                }
            }
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
        self.cmcloud_active_device = None;
        self.cmcloud_enrollment_transaction = None;
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
        return validate_optional_private_file(&self.path, self.owner);
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
        document.cmcloud_active_device = None;
        document.cmcloud_enrollment_transaction = None;
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

    fn begin_cmcloud_enrollment(
        &self,
        endpoint: &str,
        pairing_code: &str,
        client_version: &str,
    ) -> Result<CMCloudEnrollmentAttempt, SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let mut document = self.load()?;
        if document.cmcloud_enrollment_transaction.is_some() {
            return Err(SecretStoreError::Unavailable);
        }
        let transaction = CMCloudEnrollmentTransaction::new(
            endpoint,
            pairing_code,
            client_version,
            document.cmcloud_active_device.as_ref(),
        );
        transaction.validate()?;
        let attempt = transaction.clone().into_attempt();
        document.cmcloud_enrollment_transaction = Some(transaction);
        self.save(&document)?;
        Ok(attempt)
    }

    fn cmcloud_enrollment_attempt(
        &self,
    ) -> Result<Option<CMCloudEnrollmentAttempt>, SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let mut document = self.load()?;
        Ok(document
            .cmcloud_enrollment_transaction
            .take()
            .map(CMCloudEnrollmentTransaction::into_attempt))
    }

    fn discard_cmcloud_enrollment(&self) -> Result<bool, SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let mut document = self.load()?;
        let discarded = document.cmcloud_enrollment_transaction.take().is_some();
        if discarded {
            self.save(&document)?;
        }
        Ok(discarded)
    }

    fn record_cmcloud_issued_credential(
        &self,
        installation_generation: u64,
        credential_version: u64,
        connection_epoch: u64,
        device_credential: &str,
    ) -> Result<(), SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let mut document = self.load()?;
        document
            .cmcloud_enrollment_transaction
            .as_mut()
            .ok_or(SecretStoreError::Unavailable)?
            .record_issued(
                installation_generation,
                credential_version,
                connection_epoch,
                device_credential,
            )?;
        self.save(&document)
    }

    fn activate_cmcloud_credential(
        &self,
        installation_generation: u64,
        credential_version: u64,
        connection_epoch: u64,
    ) -> Result<CMCloudInstallationIdentity, SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let mut document = self.load()?;
        let transaction = document
            .cmcloud_enrollment_transaction
            .as_ref()
            .cloned()
            .ok_or(SecretStoreError::Unavailable)?;
        let active = transaction.activate(
            installation_generation,
            credential_version,
            connection_epoch,
        )?;
        let identity = active.identity();
        document.cmcloud_enrollment_transaction = None;
        document.cmcloud_active_device = Some(active);
        self.save(&document)?;
        Ok(identity)
    }

    fn cmcloud_active_device_credential(
        &self,
    ) -> Result<Option<CMCloudActiveDeviceCredential>, SecretStoreError> {
        let _guard = self
            .lock
            .lock()
            .map_err(|_| SecretStoreError::Unavailable)?;
        let mut document = self.load()?;
        if document.cmcloud_enrollment_transaction.is_some() {
            return Ok(None);
        }
        Ok(document
            .cmcloud_active_device
            .take()
            .map(CMCloudActiveDeviceSecret::into_active_credential))
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
    cmcloud_active_device: Option<CMCloudActiveDeviceSecret>,
    cmcloud_enrollment_transaction: Option<CMCloudEnrollmentTransaction>,
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
        state.cmcloud_active_device = None;
        state.cmcloud_enrollment_transaction = None;
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

    fn begin_cmcloud_enrollment(
        &self,
        endpoint: &str,
        pairing_code: &str,
        client_version: &str,
    ) -> Result<CMCloudEnrollmentAttempt, SecretStoreError> {
        let mut state = self.0.lock().map_err(|_| SecretStoreError::Unavailable)?;
        if state.cmcloud_enrollment_transaction.is_some() {
            return Err(SecretStoreError::Unavailable);
        }
        let transaction = CMCloudEnrollmentTransaction::new(
            endpoint,
            pairing_code,
            client_version,
            state.cmcloud_active_device.as_ref(),
        );
        transaction.validate()?;
        let attempt = transaction.clone().into_attempt();
        state.cmcloud_enrollment_transaction = Some(transaction);
        Ok(attempt)
    }

    fn cmcloud_enrollment_attempt(
        &self,
    ) -> Result<Option<CMCloudEnrollmentAttempt>, SecretStoreError> {
        let state = self.0.lock().map_err(|_| SecretStoreError::Unavailable)?;
        Ok(state
            .cmcloud_enrollment_transaction
            .clone()
            .map(CMCloudEnrollmentTransaction::into_attempt))
    }

    fn discard_cmcloud_enrollment(&self) -> Result<bool, SecretStoreError> {
        let mut state = self.0.lock().map_err(|_| SecretStoreError::Unavailable)?;
        Ok(state.cmcloud_enrollment_transaction.take().is_some())
    }

    fn record_cmcloud_issued_credential(
        &self,
        installation_generation: u64,
        credential_version: u64,
        connection_epoch: u64,
        device_credential: &str,
    ) -> Result<(), SecretStoreError> {
        let mut state = self.0.lock().map_err(|_| SecretStoreError::Unavailable)?;
        state
            .cmcloud_enrollment_transaction
            .as_mut()
            .ok_or(SecretStoreError::Unavailable)?
            .record_issued(
                installation_generation,
                credential_version,
                connection_epoch,
                device_credential,
            )
    }

    fn activate_cmcloud_credential(
        &self,
        installation_generation: u64,
        credential_version: u64,
        connection_epoch: u64,
    ) -> Result<CMCloudInstallationIdentity, SecretStoreError> {
        let mut state = self.0.lock().map_err(|_| SecretStoreError::Unavailable)?;
        let transaction = state
            .cmcloud_enrollment_transaction
            .as_ref()
            .cloned()
            .ok_or(SecretStoreError::Unavailable)?;
        let active = transaction.activate(
            installation_generation,
            credential_version,
            connection_epoch,
        )?;
        let identity = active.identity();
        state.cmcloud_enrollment_transaction = None;
        state.cmcloud_active_device = Some(active);
        Ok(identity)
    }

    fn cmcloud_active_device_credential(
        &self,
    ) -> Result<Option<CMCloudActiveDeviceCredential>, SecretStoreError> {
        let state = self.0.lock().map_err(|_| SecretStoreError::Unavailable)?;
        if state.cmcloud_enrollment_transaction.is_some() {
            return Ok(None);
        }
        Ok(state
            .cmcloud_active_device
            .clone()
            .map(CMCloudActiveDeviceSecret::into_active_credential))
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

    /// Start a fresh CMCloud pairing transaction. The pairing code never becomes
    /// a general-purpose secret and is never available to Gateway bootstrap.
    pub fn begin_cmcloud_enrollment(
        &self,
        endpoint: &str,
        pairing_code: &str,
        client_version: &str,
    ) -> Result<CMCloudEnrollmentAttempt, SecretStoreError> {
        validate_cmcloud_endpoint(endpoint)?;
        validate_cmcloud_bearer(pairing_code)?;
        validate_cmcloud_client_version(client_version)?;
        self.backend
            .begin_cmcloud_enrollment(endpoint, pairing_code, client_version)
    }

    /// Read the durable Agent-only enrollment transaction for a restart-safe
    /// recovery attempt.
    pub fn cmcloud_enrollment_attempt(
        &self,
    ) -> Result<Option<CMCloudEnrollmentAttempt>, SecretStoreError> {
        self.backend.cmcloud_enrollment_attempt()
    }

    /// Discard an unissued enrollment transaction after a terminal rejection.
    /// Callers must retain any transaction that already has an issued credential
    /// so CMCloud's bounded enrollment recovery remains possible.
    pub fn discard_cmcloud_enrollment(&self) -> Result<bool, SecretStoreError> {
        self.backend.discard_cmcloud_enrollment()
    }

    /// Persist an issued credential before the Agent acknowledges enrollment to
    /// CMCloud. Repeated recovery responses must carry the exact same issued
    /// credential and identity fence.
    pub fn record_cmcloud_issued_credential(
        &self,
        installation_generation: u64,
        credential_version: u64,
        connection_epoch: u64,
        device_credential: &str,
    ) -> Result<(), SecretStoreError> {
        validate_generation(installation_generation)?;
        validate_credential_version(credential_version)?;
        validate_connection_epoch(connection_epoch)?;
        validate_cmcloud_bearer(device_credential)?;
        self.backend.record_cmcloud_issued_credential(
            installation_generation,
            credential_version,
            connection_epoch,
            device_credential,
        )
    }

    /// Atomically promote the pending CMCloud credential only after a matching
    /// `enrollment_acknowledged` reply was received from CMCloud.
    pub fn activate_cmcloud_credential(
        &self,
        installation_generation: u64,
        credential_version: u64,
        connection_epoch: u64,
    ) -> Result<CMCloudInstallationIdentity, SecretStoreError> {
        validate_generation(installation_generation)?;
        validate_credential_version(credential_version)?;
        validate_connection_epoch(connection_epoch)?;
        self.backend.activate_cmcloud_credential(
            installation_generation,
            credential_version,
            connection_epoch,
        )
    }

    /// Return an active CMCloud credential only when no pairing transaction is
    /// outstanding. This is the sole credential Gateway may receive.
    pub fn cmcloud_active_device_credential(
        &self,
    ) -> Result<Option<CMCloudActiveDeviceCredential>, SecretStoreError> {
        self.backend.cmcloud_active_device_credential()
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

fn validate_cmcloud_endpoint(value: &str) -> Result<(), SecretStoreError> {
    if value.is_empty()
        || value.len() > MAX_CMCLOUD_ENDPOINT_BYTES
        || value
            .bytes()
            .any(|byte| byte.is_ascii_control() || byte.is_ascii_whitespace())
        || !value.starts_with("wss://")
        || value[6..].is_empty()
    {
        return Err(SecretStoreError::InvalidValue);
    }
    Ok(())
}

fn validate_cmcloud_bearer(value: &str) -> Result<(), SecretStoreError> {
    if value.len() < 16
        || value.len() > MAX_CMCLOUD_DEVICE_CREDENTIAL_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
    {
        return Err(SecretStoreError::InvalidValue);
    }
    Ok(())
}

fn validate_cmcloud_client_version(value: &str) -> Result<(), SecretStoreError> {
    if value.is_empty()
        || value.len() > MAX_CMCLOUD_CLIENT_VERSION_BYTES
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'+' | b'v'))
    {
        return Err(SecretStoreError::InvalidValue);
    }
    Ok(())
}

fn validate_uuid(value: &str) -> Result<(), SecretStoreError> {
    Uuid::parse_str(value)
        .map(|_| ())
        .map_err(|_| SecretStoreError::Unavailable)
}

fn validate_generation(value: u64) -> Result<(), SecretStoreError> {
    if value > MAX_SAFE_JAVASCRIPT_INTEGER {
        return Err(SecretStoreError::InvalidValue);
    }
    Ok(())
}

fn validate_credential_version(value: u64) -> Result<(), SecretStoreError> {
    if value == 0 || value > MAX_SAFE_JAVASCRIPT_INTEGER {
        return Err(SecretStoreError::InvalidValue);
    }
    Ok(())
}

fn validate_connection_epoch(value: u64) -> Result<(), SecretStoreError> {
    if value == 0 || value > MAX_SAFE_JAVASCRIPT_INTEGER {
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
    fn cmcloud_pairing_is_restart_safe_and_gateway_gated_until_acknowledged() {
        let root = fixture("cmcloud-pairing");
        let store = AgentSecretStore::runtime(&root).expect("runtime store should initialize");
        let attempt = store
            .begin_cmcloud_enrollment(
                "wss://cmcloud.example.invalid/agent/v1",
                "abcdefghijklmnopqrstuvwxyz012345",
                "2.0.0-rc.1",
            )
            .expect("pairing transaction should persist before connecting");
        let installation_id = attempt.installation_id().to_owned();
        let boot_id = attempt.boot_id().to_owned();
        assert!(
            store
                .cmcloud_active_device_credential()
                .expect("active credential lookup should work")
                .is_none(),
            "pairing credentials must not reach Gateway bootstrap",
        );
        drop(store);

        let reopened = AgentSecretStore::runtime(&root).expect("runtime store should reopen");
        let recovered = reopened
            .cmcloud_enrollment_attempt()
            .expect("pending enrollment should load")
            .expect("pending enrollment should remain durable");
        assert_eq!(recovered.installation_id(), installation_id);
        assert_eq!(recovered.boot_id(), boot_id);
        assert_eq!(
            recovered.pairing_code().expose_secret(),
            "abcdefghijklmnopqrstuvwxyz012345"
        );
        reopened
            .record_cmcloud_issued_credential(7, 1, 4, "abcdefghijklmno_pqrstuvwxyz012345")
            .expect("issued credential should persist before ACK");
        assert!(
            reopened
                .cmcloud_active_device_credential()
                .expect("active credential lookup should work")
                .is_none(),
            "an issued-but-unacknowledged credential must remain Agent-only",
        );
        assert_eq!(
            reopened.activate_cmcloud_credential(7, 1, 5),
            Err(SecretStoreError::Unavailable),
            "a mismatched enrollment ACK fence must not promote a credential",
        );
        let identity = reopened
            .activate_cmcloud_credential(7, 1, 4)
            .expect("matching enrollment acknowledgement should promote atomically");
        assert_eq!(identity.installation_id(), installation_id);
        assert_eq!(identity.installation_generation(), 7);
        assert_eq!(identity.credential_version(), 1);
        let active = reopened
            .cmcloud_active_device_credential()
            .expect("active credential should load")
            .expect("active credential should exist");
        assert_eq!(active.identity(), &identity);
        assert_eq!(
            active.device_credential().expose_secret(),
            "abcdefghijklmno_pqrstuvwxyz012345"
        );
        assert!(
            reopened
                .cmcloud_enrollment_attempt()
                .expect("pending enrollment lookup should work")
                .is_none(),
            "activation must clear the pairing code and boot recovery material",
        );
        let serialized =
            fs::read_to_string(root.join("secrets.json")).expect("secrets document should read");
        assert!(serialized.contains("cmcloud-active-device"));
        assert!(!serialized.contains("abcdefghijklmnopqrstuvwxyz012345"));
        fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[test]
    fn cmcloud_recovery_rejects_a_substituted_issued_credential() {
        let store = AgentSecretStore::memory();
        store
            .begin_cmcloud_enrollment(
                "wss://cmcloud.example.invalid/agent/v1",
                "abcdefghijklmnopqrstuvwxyz012345",
                "2.0.0-rc.1",
            )
            .expect("pairing transaction should begin");
        store
            .record_cmcloud_issued_credential(0, 1, 2, "abcdefghijklmno_pqrstuvwxyz012345")
            .expect("first issued credential should persist");
        store
            .record_cmcloud_issued_credential(0, 1, 3, "abcdefghijklmno_pqrstuvwxyz012345")
            .expect("recovery may advance only the connection epoch");
        assert_eq!(
            store.record_cmcloud_issued_credential(0, 1, 4, "different-credential-value-123456"),
            Err(SecretStoreError::Unavailable),
        );
    }

    #[test]
    fn cmcloud_reenrollment_reuses_the_active_installation_and_generation_fence() {
        let store = AgentSecretStore::memory();
        let endpoint = "wss://cmcloud.example.invalid/agent/v1";
        store
            .begin_cmcloud_enrollment(endpoint, "pairing-code-fixture-0123456789", "2.0.0-rc.1")
            .expect("initial pairing should begin");
        store
            .record_cmcloud_issued_credential(0, 1, 1, "device-credential-fixture-0123456789")
            .expect("initial credential should persist");
        let initial = store
            .activate_cmcloud_credential(0, 1, 1)
            .expect("initial credential should activate");

        let retry = store
            .begin_cmcloud_enrollment(
                endpoint,
                "replacement-code-fixture-0123456789",
                "2.0.0-rc.1",
            )
            .expect("re-pairing should begin");
        assert_eq!(retry.installation_id(), initial.installation_id());
        assert_eq!(
            retry.requested_installation_generation(),
            initial.installation_generation(),
            "the server advances this existing installation rather than creating a lane with cursor zero",
        );
        store
            .record_cmcloud_issued_credential(1, 2, 2, "replacement-device-credential-0123456789")
            .expect("replacement credential should persist");
        let replacement = store
            .activate_cmcloud_credential(1, 2, 2)
            .expect("replacement credential should activate");
        assert_eq!(replacement.installation_id(), initial.installation_id());
        assert_eq!(replacement.installation_generation(), 1);
        assert_eq!(replacement.credential_version(), 2);
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
