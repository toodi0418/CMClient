//! Signed update and rollback support owned by the Rust Agent.
//!
//! The manifest format is deliberately small and stable. Later updater stages
//! download and install a bundle only after this boundary authenticates the
//! manifest and selects an exact component/target pair.

use std::{error::Error, fmt};

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Stable workspace identity for the updater boundary.
pub const COMPONENT: &str = "updater";

/// The only manifest schema understood by this release line.
pub const MANIFEST_SCHEMA_VERSION: u8 = 1;

/// Release channels supported by the updater.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum UpdateChannel {
    /// Production releases.
    Stable,
    /// Opt-in preview releases.
    Beta,
    /// Development releases.
    Dev,
}

/// Platform target encoded in an update bundle.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum UpdateTarget {
    /// Apple Silicon macOS.
    #[serde(rename = "darwin-aarch64")]
    DarwinAarch64,
    /// Intel macOS.
    #[serde(rename = "darwin-x86_64")]
    DarwinX86_64,
    /// ARM64 Linux.
    #[serde(rename = "linux-aarch64")]
    LinuxAarch64,
    /// x86_64 Linux.
    #[serde(rename = "linux-x86_64")]
    LinuxX86_64,
    /// x86_64 Windows.
    #[serde(rename = "windows-x86_64")]
    WindowsX86_64,
}

/// Installable product surface represented by a bundle.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum UpdateComponent {
    /// Tauri desktop supervisor distribution.
    Desktop,
    /// Agent-only headless distribution.
    Headless,
    /// Command line client distribution.
    Cli,
    /// Managed service distribution.
    Service,
}

/// Archive encoding used by a bundle.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum UpdateArchive {
    /// Tar archive compressed with Zstandard.
    #[serde(rename = "tar.zst")]
    TarZst,
    /// Zip archive.
    #[serde(rename = "zip")]
    Zip,
}

/// A component-specific, platform-specific release archive.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateBundle {
    /// Product surface this archive installs.
    pub component: UpdateComponent,
    /// Target platform this archive supports.
    pub target: UpdateTarget,
    /// Archive encoding used to stage this archive.
    pub archive: UpdateArchive,
    /// HTTPS-only immutable bundle URL.
    pub url: String,
    /// Lowercase hexadecimal SHA-256 digest of the archive bytes.
    pub sha256: String,
    /// Exact archive size in bytes.
    pub size_bytes: u64,
}

/// The signed payload of a release manifest.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct UpdateManifest {
    /// Protocol version for this document.
    pub schema_version: u8,
    /// Requested release channel.
    pub channel: UpdateChannel,
    /// Release SemVer.
    pub version: String,
    /// UTC publish timestamp using `YYYY-MM-DDTHH:MM:SS.mmmZ`.
    pub published_at: String,
    /// Minimum compatible Agent SemVer.
    pub minimum_agent_version: String,
    /// Installable archives for this release.
    pub bundles: Vec<UpdateBundle>,
}

/// The sole signature algorithm supported by the release manifest protocol.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SignatureAlgorithm {
    /// Ed25519 over [`UpdateManifest::canonical_bytes`].
    #[serde(rename = "ed25519")]
    Ed25519,
}

/// A manifest payload and the detached metadata required to authenticate it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct SignedUpdateManifest {
    /// Canonical payload covered by the signature.
    pub manifest: UpdateManifest,
    /// Identifier of the preconfigured trusted public key.
    pub signing_key_id: String,
    /// Algorithm used to create `signature`.
    pub signature_algorithm: SignatureAlgorithm,
    /// Unpadded standard Base64 Ed25519 signature of the canonical payload.
    pub signature: String,
}

/// Stable errors returned before a release can enter staging.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateManifestError {
    /// The payload is for a newer or incompatible manifest protocol.
    UnsupportedSchemaVersion,
    /// A SemVer field is malformed.
    InvalidVersion,
    /// The UTC timestamp does not follow the wire format.
    InvalidPublishedAt,
    /// The manifest does not contain an installable archive.
    MissingBundles,
    /// More than one bundle targets the same component/platform pair.
    DuplicateBundle,
    /// A bundle URL is not an allowed HTTPS URL.
    InvalidBundleUrl,
    /// A bundle checksum is not a lowercase SHA-256 hex value.
    InvalidBundleDigest,
    /// A bundle cannot be empty.
    InvalidBundleSize,
    /// Key identifiers may not contain arbitrary user-controlled text.
    InvalidSigningKeyId,
    /// The provided signer is not the trusted manifest signer.
    SigningKeyMismatch,
    /// The signature is not valid Base64.
    InvalidSignatureEncoding,
    /// The decoded signature is not exactly an Ed25519 signature.
    InvalidSignatureLength,
    /// The manifest signature cannot be authenticated.
    SignatureVerificationFailed,
    /// The requested component/platform pair is not in this release.
    BundleNotFound,
    /// Serializing a validated manifest unexpectedly failed.
    CanonicalizationFailed,
}

impl UpdateManifestError {
    /// Stable machine-readable code for API and job projections.
    pub const fn code(self) -> &'static str {
        match self {
            Self::UnsupportedSchemaVersion => "UPDATE_MANIFEST_SCHEMA_UNSUPPORTED",
            Self::InvalidVersion => "UPDATE_MANIFEST_VERSION_INVALID",
            Self::InvalidPublishedAt => "UPDATE_MANIFEST_PUBLISHED_AT_INVALID",
            Self::MissingBundles => "UPDATE_MANIFEST_BUNDLES_MISSING",
            Self::DuplicateBundle => "UPDATE_MANIFEST_BUNDLE_DUPLICATE",
            Self::InvalidBundleUrl => "UPDATE_MANIFEST_BUNDLE_URL_INVALID",
            Self::InvalidBundleDigest => "UPDATE_MANIFEST_BUNDLE_DIGEST_INVALID",
            Self::InvalidBundleSize => "UPDATE_MANIFEST_BUNDLE_SIZE_INVALID",
            Self::InvalidSigningKeyId => "UPDATE_MANIFEST_SIGNING_KEY_ID_INVALID",
            Self::SigningKeyMismatch => "UPDATE_MANIFEST_SIGNING_KEY_MISMATCH",
            Self::InvalidSignatureEncoding => "UPDATE_MANIFEST_SIGNATURE_ENCODING_INVALID",
            Self::InvalidSignatureLength => "UPDATE_MANIFEST_SIGNATURE_LENGTH_INVALID",
            Self::SignatureVerificationFailed => "UPDATE_MANIFEST_SIGNATURE_INVALID",
            Self::BundleNotFound => "UPDATE_MANIFEST_BUNDLE_NOT_FOUND",
            Self::CanonicalizationFailed => "UPDATE_MANIFEST_CANONICALIZATION_FAILED",
        }
    }
}

impl fmt::Display for UpdateManifestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl Error for UpdateManifestError {}

impl UpdateManifest {
    /// Checks invariant fields before a manifest is signed or trusted.
    pub fn validate(&self) -> Result<(), UpdateManifestError> {
        if self.schema_version != MANIFEST_SCHEMA_VERSION {
            return Err(UpdateManifestError::UnsupportedSchemaVersion);
        }
        if Version::parse(&self.version).is_err()
            || Version::parse(&self.minimum_agent_version).is_err()
        {
            return Err(UpdateManifestError::InvalidVersion);
        }
        if !is_utc_millisecond_timestamp(&self.published_at) {
            return Err(UpdateManifestError::InvalidPublishedAt);
        }
        if self.bundles.is_empty() {
            return Err(UpdateManifestError::MissingBundles);
        }

        for (index, bundle) in self.bundles.iter().enumerate() {
            if !is_https_url(&bundle.url) {
                return Err(UpdateManifestError::InvalidBundleUrl);
            }
            if !is_sha256_hex(&bundle.sha256) {
                return Err(UpdateManifestError::InvalidBundleDigest);
            }
            if bundle.size_bytes == 0 {
                return Err(UpdateManifestError::InvalidBundleSize);
            }
            if self.bundles[..index].iter().any(|previous| {
                previous.component == bundle.component && previous.target == bundle.target
            }) {
                return Err(UpdateManifestError::DuplicateBundle);
            }
        }

        Ok(())
    }

    /// Returns the exact UTF-8 JSON sequence covered by an Ed25519 signature.
    pub fn canonical_bytes(&self) -> Result<Vec<u8>, UpdateManifestError> {
        self.validate()?;
        serde_json::to_vec(self).map_err(|_| UpdateManifestError::CanonicalizationFailed)
    }

    /// Finds the single archive usable by an installed product surface.
    pub fn bundle_for(
        &self,
        component: UpdateComponent,
        target: UpdateTarget,
    ) -> Result<&UpdateBundle, UpdateManifestError> {
        self.validate()?;
        self.bundles
            .iter()
            .find(|bundle| bundle.component == component && bundle.target == target)
            .ok_or(UpdateManifestError::BundleNotFound)
    }
}

impl SignedUpdateManifest {
    /// Signs a validated manifest with the offline release signing key.
    pub fn sign(
        manifest: UpdateManifest,
        signing_key_id: String,
        signing_key: &SigningKey,
    ) -> Result<Self, UpdateManifestError> {
        validate_signing_key_id(&signing_key_id)?;
        let signature = signing_key.sign(&manifest.canonical_bytes()?);

        Ok(Self {
            manifest,
            signing_key_id,
            signature_algorithm: SignatureAlgorithm::Ed25519,
            signature: STANDARD_NO_PAD.encode(signature.to_bytes()),
        })
    }

    /// Authenticates the payload using the caller-selected trusted key.
    pub fn verify(
        &self,
        expected_signing_key_id: &str,
        verifying_key: &VerifyingKey,
    ) -> Result<&UpdateManifest, UpdateManifestError> {
        validate_signing_key_id(&self.signing_key_id)?;
        if self.signing_key_id != expected_signing_key_id {
            return Err(UpdateManifestError::SigningKeyMismatch);
        }

        let decoded_signature = STANDARD_NO_PAD
            .decode(&self.signature)
            .map_err(|_| UpdateManifestError::InvalidSignatureEncoding)?;
        let signature_bytes: [u8; 64] = decoded_signature
            .try_into()
            .map_err(|_| UpdateManifestError::InvalidSignatureLength)?;
        let signature = Signature::from_bytes(&signature_bytes);
        let payload = self.manifest.canonical_bytes()?;

        verifying_key
            .verify_strict(&payload, &signature)
            .map_err(|_| UpdateManifestError::SignatureVerificationFailed)?;
        Ok(&self.manifest)
    }
}

/// Computes the canonical lowercase SHA-256 digest used by bundle manifests.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let digest = Sha256::digest(bytes);
    let mut rendered = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use fmt::Write as _;
        let _ = write!(rendered, "{byte:02x}");
    }
    rendered
}

fn validate_signing_key_id(value: &str) -> Result<(), UpdateManifestError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
    {
        return Err(UpdateManifestError::InvalidSigningKeyId);
    }
    Ok(())
}

fn is_sha256_hex(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_https_url(value: &str) -> bool {
    let Some(authority_and_path) = value.strip_prefix("https://") else {
        return false;
    };
    let authority = authority_and_path
        .split(['/', '?'])
        .next()
        .unwrap_or_default();

    !authority.is_empty()
        && !value
            .bytes()
            .any(|byte| byte.is_ascii_whitespace() || byte.is_ascii_control())
        && !value.contains(['@', '#'])
}

fn is_utc_millisecond_timestamp(value: &str) -> bool {
    let bytes = value.as_bytes();
    bytes.len() == 24
        && bytes[4] == b'-'
        && bytes[7] == b'-'
        && bytes[10] == b'T'
        && bytes[13] == b':'
        && bytes[16] == b':'
        && bytes[19] == b'.'
        && bytes[23] == b'Z'
        && [0..4, 5..7, 8..10, 11..13, 14..16, 17..19, 20..23]
            .into_iter()
            .flatten()
            .all(|index| bytes[index].is_ascii_digit())
}

#[cfg(test)]
mod tests {
    use super::{
        MANIFEST_SCHEMA_VERSION, SignatureAlgorithm, SignedUpdateManifest, UpdateArchive,
        UpdateBundle, UpdateChannel, UpdateComponent, UpdateManifest, UpdateManifestError,
        UpdateTarget, sha256_hex,
    };
    use ed25519_dalek::{SigningKey, VerifyingKey};

    fn signing_key() -> SigningKey {
        SigningKey::from_bytes(&[7; 32])
    }

    fn manifest() -> UpdateManifest {
        UpdateManifest {
            schema_version: MANIFEST_SCHEMA_VERSION,
            channel: UpdateChannel::Dev,
            version: "2.0.0-dev.1".to_owned(),
            published_at: "2026-07-18T02:40:00.000Z".to_owned(),
            minimum_agent_version: "2.0.0-dev.0".to_owned(),
            bundles: vec![UpdateBundle {
                component: UpdateComponent::Desktop,
                target: UpdateTarget::DarwinAarch64,
                archive: UpdateArchive::TarZst,
                url: "https://releases.example.invalid/cmclient/2.0.0-dev.1/darwin-aarch64.tar.zst"
                    .to_owned(),
                sha256: "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
                    .to_owned(),
                size_bytes: 4_096,
            }],
        }
    }

    #[test]
    fn exposes_its_component_identity() {
        assert_eq!(super::COMPONENT, "updater");
    }

    #[test]
    fn signs_and_verifies_a_canonical_manifest() {
        let key = signing_key();
        let signed =
            SignedUpdateManifest::sign(manifest(), "release-2026".to_owned(), &key).unwrap();

        assert_eq!(signed.signature_algorithm, SignatureAlgorithm::Ed25519);
        assert_eq!(
            String::from_utf8(signed.manifest.canonical_bytes().unwrap()).unwrap(),
            r#"{"schemaVersion":1,"channel":"dev","version":"2.0.0-dev.1","publishedAt":"2026-07-18T02:40:00.000Z","minimumAgentVersion":"2.0.0-dev.0","bundles":[{"component":"desktop","target":"darwin-aarch64","archive":"tar.zst","url":"https://releases.example.invalid/cmclient/2.0.0-dev.1/darwin-aarch64.tar.zst","sha256":"0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef","sizeBytes":4096}]}"#
        );
        assert_eq!(
            signed
                .verify("release-2026", &VerifyingKey::from(&key))
                .unwrap(),
            &signed.manifest
        );
    }

    #[test]
    fn rejects_a_tampered_payload_even_when_manifest_fields_still_validate() {
        let key = signing_key();
        let mut signed =
            SignedUpdateManifest::sign(manifest(), "release-2026".to_owned(), &key).unwrap();
        signed.manifest.bundles[0].url =
            "https://releases.example.invalid/cmclient/2.0.0-dev.1/replaced.tar.zst".to_owned();

        assert_eq!(
            signed.verify("release-2026", &VerifyingKey::from(&key)),
            Err(UpdateManifestError::SignatureVerificationFailed)
        );
    }

    #[test]
    fn rejects_an_unexpected_signing_key_identifier() {
        let key = signing_key();
        let signed =
            SignedUpdateManifest::sign(manifest(), "release-2026".to_owned(), &key).unwrap();

        assert_eq!(
            signed.verify("release-2027", &VerifyingKey::from(&key)),
            Err(UpdateManifestError::SigningKeyMismatch)
        );
    }

    #[test]
    fn rejects_invalid_bundle_invariants_before_signing() {
        let key = signing_key();
        let mut invalid = manifest();
        invalid.bundles[0].sha256 = "A".repeat(64);
        assert_eq!(
            SignedUpdateManifest::sign(invalid, "release-2026".to_owned(), &key),
            Err(UpdateManifestError::InvalidBundleDigest)
        );

        let mut duplicate = manifest();
        duplicate.bundles.push(duplicate.bundles[0].clone());
        assert_eq!(
            duplicate.validate(),
            Err(UpdateManifestError::DuplicateBundle)
        );
    }

    #[test]
    fn rejects_unknown_wire_fields_during_deserialization() {
        let document = r#"{
            "schemaVersion": 1,
            "channel": "dev",
            "version": "2.0.0-dev.1",
            "publishedAt": "2026-07-18T02:40:00.000Z",
            "minimumAgentVersion": "2.0.0-dev.0",
            "bundles": [],
            "unexpected": true
        }"#;

        assert!(serde_json::from_str::<UpdateManifest>(document).is_err());
    }

    #[test]
    fn uses_lowercase_sha256_hex() {
        assert_eq!(
            sha256_hex(b"cmclient"),
            "187f502695c49638eca51c7ef7fcc77b99e2c27aa4edfbb3c9c3b6f3e8a0842d"
        );
    }
}
