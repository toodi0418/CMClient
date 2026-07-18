//! Agent-owned access to platform credential storage.
//!
//! The secret values never have `Debug` or `Display` implementations. Callers
//! must deliberately expose a value only for the immediate privileged handoff.

use keyring::{Entry, Error as KeyringError};
use std::sync::Arc;
use zeroize::Zeroize;

const SERVICE_NAME: &str = "io.cmclient.CMClient";
const MAX_SECRET_BYTES: usize = 4_096;

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
pub struct SecretValue(String);

impl SecretValue {
    pub fn expose_secret(&self) -> &str {
        &self.0
    }
}

impl Drop for SecretValue {
    fn drop(&mut self) {
        self.0.zeroize();
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
}

struct PlatformSecretBackend;

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
            Ok(value) => Ok(Some(SecretValue(value))),
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
}

/// Access point for the operating-system credential store.
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
    fn with_backend(backend: Arc<dyn SecretBackend>) -> Self {
        Self { backend }
    }
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
    use super::{AgentSecretStore, SecretBackend, SecretKind, SecretStoreError, SecretValue};
    use std::{
        collections::BTreeMap,
        sync::{Arc, Mutex},
    };

    #[derive(Default)]
    struct MemoryBackend(Mutex<BTreeMap<SecretKind, String>>);

    impl SecretBackend for MemoryBackend {
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
                .map(SecretValue))
        }

        fn delete(&self, kind: SecretKind) -> Result<bool, SecretStoreError> {
            Ok(self
                .0
                .lock()
                .map_err(|_| SecretStoreError::Unavailable)?
                .remove(&kind)
                .is_some())
        }
    }

    #[test]
    fn stores_reads_and_removes_values_without_printable_secret_types() {
        let store = AgentSecretStore::with_backend(Arc::new(MemoryBackend::default()));
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
        let store = AgentSecretStore::with_backend(Arc::new(MemoryBackend::default()));
        for invalid in [String::new(), String::from("line\nfeed"), "a".repeat(4_097)] {
            assert_eq!(
                store.store(SecretKind::AprsPasscode, &invalid),
                Err(SecretStoreError::InvalidValue)
            );
        }
    }
}
