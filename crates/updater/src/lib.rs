//! Signed update and rollback support owned by the Rust Agent.
//!
//! The manifest format is deliberately small and stable. Later updater stages
//! download and install a bundle only after this boundary authenticates the
//! manifest and selects an exact component/target pair.

use std::{
    error::Error,
    fmt,
    fs::{self, File, OpenOptions},
    io::{self, Read, Write},
    path::{Path, PathBuf},
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::STANDARD_NO_PAD};
use ed25519_dalek::{Signature, Signer, SigningKey, VerifyingKey};
use reqwest::blocking::Client;
use reqwest::redirect::Policy;
use semver::Version;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Stable workspace identity for the updater boundary.
pub const COMPONENT: &str = "updater";

/// The only manifest schema understood by this release line.
pub const MANIFEST_SCHEMA_VERSION: u8 = 1;

const DOWNLOAD_BUFFER_SIZE: usize = 64 * 1024;

/// Conservative extraction ceiling for one signed update archive.
pub const DEFAULT_MAX_UNPACKED_BYTES: u64 = 8 * 1024 * 1024 * 1024;

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
    format_sha256_digest(&Sha256::digest(bytes))
}

fn format_sha256_digest(digest: &[u8]) -> String {
    let mut rendered = String::with_capacity(digest.len() * 2);
    for &byte in digest {
        use fmt::Write as _;
        let _ = write!(rendered, "{byte:02x}");
    }
    rendered
}

/// A response stream returned by a bundle transport.
pub struct BundleResponse {
    /// Archive stream. Callers must consume it exactly once while staging.
    pub reader: Box<dyn Read>,
    /// Declared HTTP response length when the transport makes it available.
    pub content_length: Option<u64>,
}

/// Boundary used to fetch an authenticated bundle without coupling staging to HTTP.
pub trait BundleTransport {
    /// Opens one exact bundle URL and returns its unconsumed response stream.
    fn download(&self, url: &str) -> Result<BundleResponse, UpdateStageError>;
}

/// Production HTTPS bundle transport used by the Agent updater.
pub struct HttpBundleTransport {
    client: Client,
}

impl HttpBundleTransport {
    /// Creates a client that refuses redirects and uses the platform trust store.
    pub fn new() -> Result<Self, UpdateStageError> {
        let client = Client::builder()
            .connect_timeout(Duration::from_secs(15))
            .timeout(Duration::from_secs(15 * 60))
            .redirect(Policy::none())
            .build()
            .map_err(|_| UpdateStageError::TransportUnavailable)?;
        Ok(Self { client })
    }
}

impl BundleTransport for HttpBundleTransport {
    fn download(&self, url: &str) -> Result<BundleResponse, UpdateStageError> {
        let response = self
            .client
            .get(url)
            .send()
            .map_err(|_| UpdateStageError::DownloadFailed)?;
        if !response.status().is_success() {
            return Err(UpdateStageError::UnexpectedHttpStatus);
        }

        Ok(BundleResponse {
            content_length: response.content_length(),
            reader: Box::new(response),
        })
    }
}

/// Inputs required to authenticate, select, download, and stage one bundle.
pub struct UpdateStageRequest<'a> {
    /// Manifest obtained from the release service.
    pub signed_manifest: &'a SignedUpdateManifest,
    /// Locally configured key identifier, not a remote selection.
    pub expected_signing_key_id: &'a str,
    /// Locally configured trusted Ed25519 public key.
    pub verifying_key: &'a VerifyingKey,
    /// Installed product surface to update.
    pub component: UpdateComponent,
    /// Current platform target.
    pub target: UpdateTarget,
    /// Agent-owned OS cache directory.
    pub cache_dir: &'a Path,
}

/// An archive that has passed manifest and byte verification in Agent staging.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StagedBundle {
    /// Absolute path to the verified archive in the Agent cache.
    pub path: PathBuf,
    /// Product surface represented by the staged archive.
    pub component: UpdateComponent,
    /// Platform supported by the staged archive.
    pub target: UpdateTarget,
    /// Archive encoding used by the staged archive.
    pub archive: UpdateArchive,
    /// Authenticated lowercase SHA-256 digest.
    pub sha256: String,
    /// Authenticated archive size.
    pub size_bytes: u64,
    /// Whether an earlier verified staging artifact was safely reused.
    pub reused: bool,
}

/// Stable errors returned while downloading or staging an update bundle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateStageError {
    /// Manifest authentication or selection failed.
    Manifest(UpdateManifestError),
    /// The HTTPS client could not be configured.
    TransportUnavailable,
    /// The HTTPS request could not reach the authenticated endpoint.
    DownloadFailed,
    /// The endpoint did not return a success response.
    UnexpectedHttpStatus,
    /// Response metadata conflicts with the authenticated size.
    ContentLengthMismatch,
    /// The streamed response could not be read.
    DownloadReadFailed,
    /// Staging directories or files could not be prepared.
    StagingIoFailed,
    /// A previous writer is still staging the same archive.
    StagingInProgress,
    /// The response has fewer or more bytes than its authenticated size.
    DownloadSizeMismatch,
    /// The streamed archive digest does not match the authenticated digest.
    DownloadChecksumMismatch,
    /// An already staged archive is not a verified regular file.
    ExistingStageInvalid,
    /// The temporary archive could not be synced before publishing.
    StagingSyncFailed,
    /// The verified temporary archive could not be atomically published.
    StagingFinalizeFailed,
}

impl UpdateStageError {
    /// Stable machine-readable code for API and persistent job projections.
    pub const fn code(self) -> &'static str {
        match self {
            Self::Manifest(error) => error.code(),
            Self::TransportUnavailable => "UPDATE_TRANSPORT_UNAVAILABLE",
            Self::DownloadFailed => "UPDATE_DOWNLOAD_FAILED",
            Self::UnexpectedHttpStatus => "UPDATE_DOWNLOAD_HTTP_STATUS_INVALID",
            Self::ContentLengthMismatch => "UPDATE_DOWNLOAD_CONTENT_LENGTH_MISMATCH",
            Self::DownloadReadFailed => "UPDATE_DOWNLOAD_READ_FAILED",
            Self::StagingIoFailed => "UPDATE_STAGING_IO_FAILED",
            Self::StagingInProgress => "UPDATE_STAGING_IN_PROGRESS",
            Self::DownloadSizeMismatch => "UPDATE_DOWNLOAD_SIZE_MISMATCH",
            Self::DownloadChecksumMismatch => "UPDATE_DOWNLOAD_CHECKSUM_MISMATCH",
            Self::ExistingStageInvalid => "UPDATE_STAGING_EXISTING_INVALID",
            Self::StagingSyncFailed => "UPDATE_STAGING_SYNC_FAILED",
            Self::StagingFinalizeFailed => "UPDATE_STAGING_FINALIZE_FAILED",
        }
    }
}

impl fmt::Display for UpdateStageError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl Error for UpdateStageError {}

/// Verifies a signed manifest, streams one exact bundle, and atomically stages it.
pub fn stage_verified_bundle(
    request: &UpdateStageRequest<'_>,
    transport: &impl BundleTransport,
) -> Result<StagedBundle, UpdateStageError> {
    let manifest = request
        .signed_manifest
        .verify(request.expected_signing_key_id, request.verifying_key)
        .map_err(UpdateStageError::Manifest)?;
    let bundle = manifest
        .bundle_for(request.component, request.target)
        .map_err(UpdateStageError::Manifest)?
        .clone();

    let staging_dir = request.cache_dir.join("updates").join("staging");
    fs::create_dir_all(&staging_dir).map_err(|_| UpdateStageError::StagingIoFailed)?;
    if !fs::metadata(&staging_dir)
        .map_err(|_| UpdateStageError::StagingIoFailed)?
        .is_dir()
    {
        return Err(UpdateStageError::StagingIoFailed);
    }

    let staged_path = staging_dir.join(format!("{}.bundle", bundle.sha256));
    if staged_path.exists() {
        verify_staged_bundle(&staged_path, &bundle)?;
        return Ok(staged_bundle(staged_path, bundle, true));
    }

    let mut response = transport.download(&bundle.url)?;
    if response
        .content_length
        .is_some_and(|length| length != bundle.size_bytes)
    {
        return Err(UpdateStageError::ContentLengthMismatch);
    }

    let temporary_path = staging_dir.join(format!("{}.part-{}", bundle.sha256, std::process::id()));
    let mut temporary = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary_path)
        .map_err(|error| {
            if error.kind() == io::ErrorKind::AlreadyExists {
                UpdateStageError::StagingInProgress
            } else {
                UpdateStageError::StagingIoFailed
            }
        })?;

    let download_result = write_verified_stream(
        response.reader.as_mut(),
        &mut temporary,
        bundle.size_bytes,
        &bundle.sha256,
    );
    if let Err(error) = download_result {
        drop(temporary);
        let _ = fs::remove_file(&temporary_path);
        return Err(error);
    }
    if temporary.sync_all().is_err() {
        drop(temporary);
        let _ = fs::remove_file(&temporary_path);
        return Err(UpdateStageError::StagingSyncFailed);
    }
    drop(temporary);

    let reused = match fs::hard_link(&temporary_path, &staged_path) {
        Ok(()) => {
            fs::remove_file(&temporary_path)
                .map_err(|_| UpdateStageError::StagingFinalizeFailed)?;
            false
        }
        Err(_) if staged_path.exists() => {
            let verified = verify_staged_bundle(&staged_path, &bundle);
            let _ = fs::remove_file(&temporary_path);
            verified?;
            true
        }
        Err(_) => {
            let _ = fs::remove_file(&temporary_path);
            return Err(UpdateStageError::StagingFinalizeFailed);
        }
    };

    Ok(staged_bundle(staged_path, bundle, reused))
}

fn staged_bundle(path: PathBuf, bundle: UpdateBundle, reused: bool) -> StagedBundle {
    StagedBundle {
        path,
        component: bundle.component,
        target: bundle.target,
        archive: bundle.archive,
        sha256: bundle.sha256,
        size_bytes: bundle.size_bytes,
        reused,
    }
}

fn write_verified_stream(
    reader: &mut dyn Read,
    output: &mut File,
    expected_size: u64,
    expected_sha256: &str,
) -> Result<(), UpdateStageError> {
    let mut digest = Sha256::new();
    let mut total = 0_u64;
    let mut buffer = [0_u8; DOWNLOAD_BUFFER_SIZE];

    while total < expected_size {
        let remaining = (expected_size - total).min(DOWNLOAD_BUFFER_SIZE as u64) as usize;
        let read = reader
            .read(&mut buffer[..remaining])
            .map_err(|_| UpdateStageError::DownloadReadFailed)?;
        if read == 0 {
            break;
        }
        output
            .write_all(&buffer[..read])
            .map_err(|_| UpdateStageError::StagingIoFailed)?;
        digest.update(&buffer[..read]);
        total += read as u64;
    }

    if total != expected_size {
        return Err(UpdateStageError::DownloadSizeMismatch);
    }
    let mut extra = [0_u8; 1];
    if reader
        .read(&mut extra)
        .map_err(|_| UpdateStageError::DownloadReadFailed)?
        != 0
    {
        return Err(UpdateStageError::DownloadSizeMismatch);
    }
    if format_sha256_digest(&digest.finalize()) != expected_sha256 {
        return Err(UpdateStageError::DownloadChecksumMismatch);
    }
    Ok(())
}

fn verify_staged_bundle(path: &Path, bundle: &UpdateBundle) -> Result<(), UpdateStageError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|_| UpdateStageError::ExistingStageInvalid)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() != bundle.size_bytes
    {
        return Err(UpdateStageError::ExistingStageInvalid);
    }

    let mut file = File::open(path).map_err(|_| UpdateStageError::ExistingStageInvalid)?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; DOWNLOAD_BUFFER_SIZE];
    loop {
        let read = file
            .read(&mut buffer)
            .map_err(|_| UpdateStageError::ExistingStageInvalid)?;
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    if format_sha256_digest(&digest.finalize()) != bundle.sha256 {
        return Err(UpdateStageError::ExistingStageInvalid);
    }
    Ok(())
}

/// Inputs for one install transaction after a bundle has entered staging.
pub struct UpdateInstallRequest<'a> {
    /// Archive that has already passed manifest and byte verification.
    pub staged_bundle: &'a StagedBundle,
    /// Product-owned root that contains release slots and the active pointer.
    pub installation_root: &'a Path,
    /// Agent data directory to preserve before migration.
    pub data_dir: &'a Path,
    /// Agent configuration directory to preserve before migration.
    pub config_dir: &'a Path,
    /// Durable backup root outside data and configuration directories.
    pub backup_root: &'a Path,
    /// Safe, caller-assigned identity for this backup snapshot.
    pub backup_id: &'a str,
    /// Maximum extracted byte count accepted from the archive.
    pub maximum_unpacked_bytes: u64,
}

/// Active release selection persisted separately from user data.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ActiveRelease {
    /// Pointer file schema version.
    pub schema_version: u8,
    /// Digest-named release slot selected by the transaction.
    pub release_id: String,
    /// Digest of the archive that created this release slot.
    pub bundle_sha256: String,
}

/// Result of a successful update install transaction.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstalledRelease {
    /// Release slot containing the safely extracted bundle.
    pub release_path: PathBuf,
    /// Durable data/config snapshot created after the prior release stopped.
    pub backup_path: PathBuf,
    /// Active pointer that now selects `release_path`.
    pub active_release: ActiveRelease,
    /// Pointer selected before this transaction, when one existed.
    pub previous_active_release: Option<ActiveRelease>,
}

/// Agent-owned runtime boundary used by the install transaction.
pub trait UpdateLifecycle {
    /// Stops processes that may write user data before a filesystem snapshot.
    fn stop(&mut self) -> Result<(), UpdateInstallError>;
    /// Runs the new release's forward-only migration journal.
    fn migrate(&mut self, release_path: &Path) -> Result<(), UpdateInstallError>;
    /// Starts services from the newly selected release.
    fn start(&mut self, release_path: &Path) -> Result<(), UpdateInstallError>;
    /// Returns true only when the new release passes its health gate.
    fn health_check(&mut self) -> Result<bool, UpdateInstallError>;
}

/// Stable failures from backup, archive extraction, installation, or health gating.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpdateInstallError {
    /// The staged archive was changed or is not a regular file.
    StagedBundleInvalid,
    /// Caller-provided update paths or backup identity are unsafe.
    InvalidInstallLayout,
    /// The archive could not be decoded.
    ArchiveReadFailed,
    /// An archive path is absolute, empty, or escapes the release slot.
    ArchivePathInvalid,
    /// An archive contains a symlink, special file, or other unsupported entry.
    ArchiveEntryUnsupported,
    /// The archive expands beyond the configured safety limit.
    ArchiveTooLarge,
    /// The archive contains no installable files.
    ArchiveEmpty,
    /// Filesystem preparation, copying, or release extraction failed.
    InstallIoFailed,
    /// The digest-named release slot already exists.
    ReleaseSlotExists,
    /// A prepared release slot could not be published.
    ReleasePublishFailed,
    /// A backup with this identity already exists.
    BackupExists,
    /// Backup copying or publication failed.
    BackupFailed,
    /// The persisted active-release pointer is malformed.
    ActiveReleaseInvalid,
    /// The active-release pointer could not be atomically written.
    ActivationFailed,
    /// The Agent failed while stopping the previous runtime.
    LifecycleStopFailed,
    /// The new release's migration journal failed.
    MigrationFailed,
    /// The new release failed to start.
    LifecycleStartFailed,
    /// The new release did not pass its health check.
    HealthCheckFailed,
}

impl UpdateInstallError {
    /// Stable machine-readable code for update state and rollback decisions.
    pub const fn code(self) -> &'static str {
        match self {
            Self::StagedBundleInvalid => "UPDATE_INSTALL_STAGED_BUNDLE_INVALID",
            Self::InvalidInstallLayout => "UPDATE_INSTALL_LAYOUT_INVALID",
            Self::ArchiveReadFailed => "UPDATE_INSTALL_ARCHIVE_READ_FAILED",
            Self::ArchivePathInvalid => "UPDATE_INSTALL_ARCHIVE_PATH_INVALID",
            Self::ArchiveEntryUnsupported => "UPDATE_INSTALL_ARCHIVE_ENTRY_UNSUPPORTED",
            Self::ArchiveTooLarge => "UPDATE_INSTALL_ARCHIVE_TOO_LARGE",
            Self::ArchiveEmpty => "UPDATE_INSTALL_ARCHIVE_EMPTY",
            Self::InstallIoFailed => "UPDATE_INSTALL_IO_FAILED",
            Self::ReleaseSlotExists => "UPDATE_INSTALL_RELEASE_SLOT_EXISTS",
            Self::ReleasePublishFailed => "UPDATE_INSTALL_RELEASE_PUBLISH_FAILED",
            Self::BackupExists => "UPDATE_BACKUP_ALREADY_EXISTS",
            Self::BackupFailed => "UPDATE_BACKUP_FAILED",
            Self::ActiveReleaseInvalid => "UPDATE_ACTIVE_RELEASE_INVALID",
            Self::ActivationFailed => "UPDATE_ACTIVATION_FAILED",
            Self::LifecycleStopFailed => "UPDATE_LIFECYCLE_STOP_FAILED",
            Self::MigrationFailed => "UPDATE_MIGRATION_FAILED",
            Self::LifecycleStartFailed => "UPDATE_LIFECYCLE_START_FAILED",
            Self::HealthCheckFailed => "UPDATE_HEALTH_CHECK_FAILED",
        }
    }
}

impl fmt::Display for UpdateInstallError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.code())
    }
}

impl Error for UpdateInstallError {}

/// Performs an update transaction through backup, activation, migration, and health.
pub fn install_verified_release(
    request: &UpdateInstallRequest<'_>,
    lifecycle: &mut impl UpdateLifecycle,
) -> Result<InstalledRelease, UpdateInstallError> {
    validate_install_request(request)?;
    verify_staged_archive(request.staged_bundle)?;

    let releases_dir = request.installation_root.join("releases");
    fs::create_dir_all(&releases_dir).map_err(|_| UpdateInstallError::InstallIoFailed)?;
    let release_id = &request.staged_bundle.sha256;
    let release_path = releases_dir.join(release_id);
    if release_path.exists() {
        return Err(UpdateInstallError::ReleaseSlotExists);
    }
    let temporary_release = releases_dir.join(format!(".{release_id}.part-{}", std::process::id()));
    if temporary_release.exists() {
        return Err(UpdateInstallError::InstallIoFailed);
    }
    fs::create_dir(&temporary_release).map_err(|_| UpdateInstallError::InstallIoFailed)?;

    let extraction_result = extract_archive(
        request.staged_bundle,
        &temporary_release,
        request.maximum_unpacked_bytes,
    );
    if let Err(error) = extraction_result {
        let _ = fs::remove_dir_all(&temporary_release);
        return Err(error);
    }
    if fs::rename(&temporary_release, &release_path).is_err() {
        let _ = fs::remove_dir_all(&temporary_release);
        return Err(UpdateInstallError::ReleasePublishFailed);
    }

    lifecycle.stop()?;
    let backup_path = create_backup(request)?;
    let previous_active_release = read_active_release(request.installation_root)?;
    let active_release = ActiveRelease {
        schema_version: 1,
        release_id: release_id.clone(),
        bundle_sha256: release_id.clone(),
    };
    write_active_release(request.installation_root, &active_release)?;
    lifecycle.migrate(&release_path)?;
    lifecycle.start(&release_path)?;
    if !lifecycle.health_check()? {
        return Err(UpdateInstallError::HealthCheckFailed);
    }

    Ok(InstalledRelease {
        release_path,
        backup_path,
        active_release,
        previous_active_release,
    })
}

/// Reads the selected release pointer without touching user data.
pub fn read_active_release(
    installation_root: &Path,
) -> Result<Option<ActiveRelease>, UpdateInstallError> {
    let pointer_path = installation_root.join("active-release.json");
    if !pointer_path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(pointer_path).map_err(|_| UpdateInstallError::ActiveReleaseInvalid)?;
    let release = serde_json::from_slice::<ActiveRelease>(&bytes)
        .map_err(|_| UpdateInstallError::ActiveReleaseInvalid)?;
    if release.schema_version != 1
        || !is_sha256_hex(&release.release_id)
        || release.release_id != release.bundle_sha256
    {
        return Err(UpdateInstallError::ActiveReleaseInvalid);
    }
    Ok(Some(release))
}

fn validate_install_request(request: &UpdateInstallRequest<'_>) -> Result<(), UpdateInstallError> {
    if request.maximum_unpacked_bytes == 0
        || !request.installation_root.is_absolute()
        || !request.data_dir.is_absolute()
        || !request.config_dir.is_absolute()
        || !request.backup_root.is_absolute()
        || !is_safe_backup_id(request.backup_id)
        || request.backup_root.starts_with(request.data_dir)
        || request.backup_root.starts_with(request.config_dir)
    {
        return Err(UpdateInstallError::InvalidInstallLayout);
    }
    Ok(())
}

fn verify_staged_archive(staged_bundle: &StagedBundle) -> Result<(), UpdateInstallError> {
    if !is_sha256_hex(&staged_bundle.sha256) || staged_bundle.size_bytes == 0 {
        return Err(UpdateInstallError::StagedBundleInvalid);
    }
    let metadata = fs::symlink_metadata(&staged_bundle.path)
        .map_err(|_| UpdateInstallError::StagedBundleInvalid)?;
    if metadata.file_type().is_symlink()
        || !metadata.is_file()
        || metadata.len() != staged_bundle.size_bytes
    {
        return Err(UpdateInstallError::StagedBundleInvalid);
    }
    let mut file =
        File::open(&staged_bundle.path).map_err(|_| UpdateInstallError::StagedBundleInvalid)?;
    let digest = digest_reader(&mut file).map_err(|_| UpdateInstallError::StagedBundleInvalid)?;
    if digest != staged_bundle.sha256 {
        return Err(UpdateInstallError::StagedBundleInvalid);
    }
    Ok(())
}

fn extract_archive(
    staged_bundle: &StagedBundle,
    destination: &Path,
    maximum_unpacked_bytes: u64,
) -> Result<(), UpdateInstallError> {
    match staged_bundle.archive {
        UpdateArchive::TarZst => {
            extract_tar_zst(&staged_bundle.path, destination, maximum_unpacked_bytes)
        }
        UpdateArchive::Zip => extract_zip(&staged_bundle.path, destination, maximum_unpacked_bytes),
    }
}

fn extract_tar_zst(
    archive_path: &Path,
    destination: &Path,
    maximum_unpacked_bytes: u64,
) -> Result<(), UpdateInstallError> {
    let file = File::open(archive_path).map_err(|_| UpdateInstallError::ArchiveReadFailed)?;
    let decoder = zstd::stream::read::Decoder::new(file)
        .map_err(|_| UpdateInstallError::ArchiveReadFailed)?;
    let mut archive = tar::Archive::new(decoder);
    let entries = archive
        .entries()
        .map_err(|_| UpdateInstallError::ArchiveReadFailed)?;
    let mut extracted_files = 0_u64;
    let mut total_bytes = 0_u64;

    for entry in entries {
        let mut entry = entry.map_err(|_| UpdateInstallError::ArchiveReadFailed)?;
        let path = entry
            .path()
            .map_err(|_| UpdateInstallError::ArchivePathInvalid)?;
        let output_path = archive_output_path(destination, &path)?;
        let entry_type = entry.header().entry_type();
        if entry_type.is_dir() {
            fs::create_dir_all(output_path).map_err(|_| UpdateInstallError::InstallIoFailed)?;
            continue;
        }
        if !entry_type.is_file() {
            return Err(UpdateInstallError::ArchiveEntryUnsupported);
        }
        let size = entry.size();
        total_bytes = total_bytes
            .checked_add(size)
            .filter(|total| *total <= maximum_unpacked_bytes)
            .ok_or(UpdateInstallError::ArchiveTooLarge)?;
        write_archive_entry(&mut entry, &output_path, size)?;
        set_safe_archive_permissions(&output_path, entry.header().mode().unwrap_or(0o644))?;
        extracted_files += 1;
    }

    if extracted_files == 0 {
        return Err(UpdateInstallError::ArchiveEmpty);
    }
    Ok(())
}

fn extract_zip(
    archive_path: &Path,
    destination: &Path,
    maximum_unpacked_bytes: u64,
) -> Result<(), UpdateInstallError> {
    let file = File::open(archive_path).map_err(|_| UpdateInstallError::ArchiveReadFailed)?;
    let mut archive =
        zip::ZipArchive::new(file).map_err(|_| UpdateInstallError::ArchiveReadFailed)?;
    let mut extracted_files = 0_u64;
    let mut total_bytes = 0_u64;

    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .map_err(|_| UpdateInstallError::ArchiveReadFailed)?;
        let raw_name = entry.name().to_owned();
        let output_path = archive_output_path(destination, Path::new(&raw_name))?;
        if entry.enclosed_name().is_none() {
            return Err(UpdateInstallError::ArchivePathInvalid);
        }
        if entry.is_dir() {
            fs::create_dir_all(output_path).map_err(|_| UpdateInstallError::InstallIoFailed)?;
            continue;
        }
        if entry.is_symlink() {
            return Err(UpdateInstallError::ArchiveEntryUnsupported);
        }
        let size = entry.size();
        total_bytes = total_bytes
            .checked_add(size)
            .filter(|total| *total <= maximum_unpacked_bytes)
            .ok_or(UpdateInstallError::ArchiveTooLarge)?;
        write_archive_entry(&mut entry, &output_path, size)?;
        set_safe_archive_permissions(&output_path, entry.unix_mode().unwrap_or(0o644))?;
        extracted_files += 1;
    }

    if extracted_files == 0 {
        return Err(UpdateInstallError::ArchiveEmpty);
    }
    Ok(())
}

fn archive_output_path(
    destination: &Path,
    archive_path: &Path,
) -> Result<PathBuf, UpdateInstallError> {
    use std::path::Component;

    if archive_path.as_os_str().is_empty()
        || archive_path
            .components()
            .any(|component| !matches!(component, Component::Normal(_)))
    {
        return Err(UpdateInstallError::ArchivePathInvalid);
    }
    Ok(destination.join(archive_path))
}

fn write_archive_entry(
    input: &mut dyn Read,
    output_path: &Path,
    expected_size: u64,
) -> Result<(), UpdateInstallError> {
    let parent = output_path
        .parent()
        .ok_or(UpdateInstallError::ArchivePathInvalid)?;
    fs::create_dir_all(parent).map_err(|_| UpdateInstallError::InstallIoFailed)?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(output_path)
        .map_err(|_| UpdateInstallError::InstallIoFailed)?;
    let copied = io::copy(input, &mut output).map_err(|_| UpdateInstallError::ArchiveReadFailed)?;
    if copied != expected_size {
        return Err(UpdateInstallError::ArchiveReadFailed);
    }
    output
        .sync_all()
        .map_err(|_| UpdateInstallError::InstallIoFailed)
}

#[cfg(unix)]
fn set_safe_archive_permissions(path: &Path, mode: u32) -> Result<(), UpdateInstallError> {
    use std::os::unix::fs::PermissionsExt;

    fs::set_permissions(path, fs::Permissions::from_mode(mode & 0o777))
        .map_err(|_| UpdateInstallError::InstallIoFailed)
}

#[cfg(not(unix))]
fn set_safe_archive_permissions(_path: &Path, _mode: u32) -> Result<(), UpdateInstallError> {
    Ok(())
}

fn create_backup(request: &UpdateInstallRequest<'_>) -> Result<PathBuf, UpdateInstallError> {
    if request.backup_root.join(request.backup_id).exists() {
        return Err(UpdateInstallError::BackupExists);
    }
    fs::create_dir_all(request.backup_root).map_err(|_| UpdateInstallError::BackupFailed)?;
    let temporary = request.backup_root.join(format!(
        ".{}.part-{}",
        request.backup_id,
        std::process::id()
    ));
    if temporary.exists() {
        return Err(UpdateInstallError::BackupFailed);
    }
    fs::create_dir(&temporary).map_err(|_| UpdateInstallError::BackupFailed)?;
    let copy_result = (|| {
        copy_directory(request.data_dir, &temporary.join("data"))?;
        copy_directory(request.config_dir, &temporary.join("config"))?;
        Ok(())
    })();
    if let Err(error) = copy_result {
        let _ = fs::remove_dir_all(&temporary);
        return Err(error);
    }

    let backup_path = request.backup_root.join(request.backup_id);
    if fs::rename(&temporary, &backup_path).is_err() {
        let _ = fs::remove_dir_all(&temporary);
        return Err(UpdateInstallError::BackupFailed);
    }
    Ok(backup_path)
}

fn copy_directory(source: &Path, destination: &Path) -> Result<(), UpdateInstallError> {
    let metadata = fs::symlink_metadata(source).map_err(|_| UpdateInstallError::BackupFailed)?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(UpdateInstallError::BackupFailed);
    }
    fs::create_dir_all(destination).map_err(|_| UpdateInstallError::BackupFailed)?;
    for entry in fs::read_dir(source).map_err(|_| UpdateInstallError::BackupFailed)? {
        let entry = entry.map_err(|_| UpdateInstallError::BackupFailed)?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let metadata =
            fs::symlink_metadata(&source_path).map_err(|_| UpdateInstallError::BackupFailed)?;
        if metadata.file_type().is_symlink() {
            return Err(UpdateInstallError::BackupFailed);
        }
        if metadata.is_dir() {
            copy_directory(&source_path, &destination_path)?;
        } else if metadata.is_file() {
            copy_backup_file(&source_path, &destination_path)?;
        } else {
            return Err(UpdateInstallError::BackupFailed);
        }
    }
    Ok(())
}

fn copy_backup_file(source: &Path, destination: &Path) -> Result<(), UpdateInstallError> {
    let mut input = File::open(source).map_err(|_| UpdateInstallError::BackupFailed)?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(destination)
        .map_err(|_| UpdateInstallError::BackupFailed)?;
    io::copy(&mut input, &mut output).map_err(|_| UpdateInstallError::BackupFailed)?;
    output
        .sync_all()
        .map_err(|_| UpdateInstallError::BackupFailed)
}

fn write_active_release(
    installation_root: &Path,
    active_release: &ActiveRelease,
) -> Result<(), UpdateInstallError> {
    fs::create_dir_all(installation_root).map_err(|_| UpdateInstallError::ActivationFailed)?;
    let pointer_path = installation_root.join("active-release.json");
    let temporary = installation_root.join(format!(".active-release.part-{}", std::process::id()));
    let bytes =
        serde_json::to_vec(active_release).map_err(|_| UpdateInstallError::ActivationFailed)?;
    let mut output = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)
        .map_err(|_| UpdateInstallError::ActivationFailed)?;
    if output.write_all(&bytes).is_err() || output.sync_all().is_err() {
        drop(output);
        let _ = fs::remove_file(&temporary);
        return Err(UpdateInstallError::ActivationFailed);
    }
    drop(output);
    if fs::rename(&temporary, &pointer_path).is_err() {
        let _ = fs::remove_file(&temporary);
        return Err(UpdateInstallError::ActivationFailed);
    }
    Ok(())
}

fn digest_reader(reader: &mut dyn Read) -> io::Result<String> {
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; DOWNLOAD_BUFFER_SIZE];
    loop {
        let read = reader.read(&mut buffer)?;
        if read == 0 {
            return Ok(format_sha256_digest(&digest.finalize()));
        }
        digest.update(&buffer[..read]);
    }
}

fn is_safe_backup_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'))
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
        ActiveRelease, BundleResponse, BundleTransport, DEFAULT_MAX_UNPACKED_BYTES,
        MANIFEST_SCHEMA_VERSION, SignatureAlgorithm, SignedUpdateManifest, StagedBundle,
        UpdateArchive, UpdateBundle, UpdateChannel, UpdateComponent, UpdateInstallError,
        UpdateInstallRequest, UpdateLifecycle, UpdateManifest, UpdateManifestError,
        UpdateStageError, UpdateStageRequest, UpdateTarget, install_verified_release,
        read_active_release, sha256_hex, stage_verified_bundle,
    };
    use ed25519_dalek::{SigningKey, VerifyingKey};
    use std::{
        fs,
        fs::File,
        io::{Cursor, Write},
        path::{Path, PathBuf},
        sync::atomic::{AtomicUsize, Ordering},
    };

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

    fn manifest_for(bytes: &[u8]) -> UpdateManifest {
        let mut update_manifest = manifest();
        update_manifest.bundles[0].sha256 = sha256_hex(bytes);
        update_manifest.bundles[0].size_bytes = bytes.len() as u64;
        update_manifest
    }

    fn stage_directory(test_name: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "cmclient-updater-{test_name}-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&path);
        path
    }

    struct FixtureTransport {
        bytes: Vec<u8>,
        content_length: Option<u64>,
        fail: bool,
        calls: AtomicUsize,
    }

    impl FixtureTransport {
        fn bytes(bytes: &[u8]) -> Self {
            Self {
                bytes: bytes.to_vec(),
                content_length: Some(bytes.len() as u64),
                fail: false,
                calls: AtomicUsize::new(0),
            }
        }

        fn unavailable() -> Self {
            Self {
                bytes: Vec::new(),
                content_length: None,
                fail: true,
                calls: AtomicUsize::new(0),
            }
        }
    }

    impl BundleTransport for FixtureTransport {
        fn download(&self, _url: &str) -> Result<BundleResponse, UpdateStageError> {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err(UpdateStageError::DownloadFailed);
            }
            Ok(BundleResponse {
                reader: Box::new(Cursor::new(self.bytes.clone())),
                content_length: self.content_length,
            })
        }
    }

    fn stage_request<'a>(
        signed_manifest: &'a SignedUpdateManifest,
        verifying_key: &'a VerifyingKey,
        cache_dir: &'a Path,
    ) -> UpdateStageRequest<'a> {
        UpdateStageRequest {
            signed_manifest,
            expected_signing_key_id: "release-2026",
            verifying_key,
            component: UpdateComponent::Desktop,
            target: UpdateTarget::DarwinAarch64,
            cache_dir,
        }
    }

    fn staged_archive(path: &Path, archive: UpdateArchive) -> StagedBundle {
        let bytes = fs::read(path).unwrap();
        StagedBundle {
            path: path.to_path_buf(),
            component: UpdateComponent::Desktop,
            target: UpdateTarget::DarwinAarch64,
            archive,
            sha256: sha256_hex(&bytes),
            size_bytes: bytes.len() as u64,
            reused: false,
        }
    }

    fn write_tar_zst(path: &Path, entries: &[(&str, &[u8])]) {
        let file = File::create(path).unwrap();
        let encoder = zstd::stream::write::Encoder::new(file, 0).unwrap();
        let mut archive = tar::Builder::new(encoder);
        for (name, contents) in entries {
            let mut header = tar::Header::new_gnu();
            header.set_size(contents.len() as u64);
            header.set_mode(0o755);
            header.set_cksum();
            archive.append_data(&mut header, name, *contents).unwrap();
        }
        archive.finish().unwrap();
        archive.into_inner().unwrap().finish().unwrap();
    }

    fn write_zip(path: &Path, entries: &[(&str, &[u8])]) {
        let file = File::create(path).unwrap();
        let mut archive = zip::ZipWriter::new(file);
        let options = zip::write::SimpleFileOptions::default()
            .compression_method(zip::CompressionMethod::Deflated);
        for (name, contents) in entries {
            archive.start_file(*name, options).unwrap();
            archive.write_all(contents).unwrap();
        }
        archive.finish().unwrap();
    }

    struct RecordingLifecycle {
        events: Vec<&'static str>,
        healthy: bool,
    }

    impl RecordingLifecycle {
        fn healthy() -> Self {
            Self {
                events: Vec::new(),
                healthy: true,
            }
        }
    }

    impl UpdateLifecycle for RecordingLifecycle {
        fn stop(&mut self) -> Result<(), UpdateInstallError> {
            self.events.push("stop");
            Ok(())
        }

        fn migrate(&mut self, _release_path: &Path) -> Result<(), UpdateInstallError> {
            self.events.push("migrate");
            Ok(())
        }

        fn start(&mut self, _release_path: &Path) -> Result<(), UpdateInstallError> {
            self.events.push("start");
            Ok(())
        }

        fn health_check(&mut self) -> Result<bool, UpdateInstallError> {
            self.events.push("health");
            Ok(self.healthy)
        }
    }

    fn install_request<'a>(
        staged_bundle: &'a StagedBundle,
        installation_root: &'a Path,
        data_dir: &'a Path,
        config_dir: &'a Path,
        backup_root: &'a Path,
    ) -> UpdateInstallRequest<'a> {
        UpdateInstallRequest {
            staged_bundle,
            installation_root,
            data_dir,
            config_dir,
            backup_root,
            backup_id: "backup-2.0.0-dev.1",
            maximum_unpacked_bytes: DEFAULT_MAX_UNPACKED_BYTES,
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
    fn stages_only_an_authenticated_exact_bundle() {
        let bytes = b"verified release archive";
        let key = signing_key();
        let verifying_key = VerifyingKey::from(&key);
        let signed =
            SignedUpdateManifest::sign(manifest_for(bytes), "release-2026".to_owned(), &key)
                .unwrap();
        let cache_dir = stage_directory("verified");
        let transport = FixtureTransport::bytes(bytes);

        let staged = stage_verified_bundle(
            &stage_request(&signed, &verifying_key, &cache_dir),
            &transport,
        )
        .unwrap();

        assert!(!staged.reused);
        assert_eq!(
            staged.path,
            cache_dir
                .join("updates/staging")
                .join(format!("{}.bundle", sha256_hex(bytes)))
        );
        assert_eq!(fs::read(&staged.path).unwrap(), bytes);
        assert_eq!(transport.calls.load(Ordering::SeqCst), 1);
        let _ = fs::remove_dir_all(cache_dir);
    }

    #[test]
    fn verifies_the_manifest_before_opening_a_network_stream() {
        let bytes = b"verified release archive";
        let key = signing_key();
        let verifying_key = VerifyingKey::from(&key);
        let mut signed =
            SignedUpdateManifest::sign(manifest_for(bytes), "release-2026".to_owned(), &key)
                .unwrap();
        signed.manifest.version = "2.0.0-dev.2".to_owned();
        let transport = FixtureTransport::bytes(bytes);
        let cache_dir = stage_directory("tampered-manifest");

        assert_eq!(
            stage_verified_bundle(
                &stage_request(&signed, &verifying_key, &cache_dir),
                &transport,
            ),
            Err(UpdateStageError::Manifest(
                UpdateManifestError::SignatureVerificationFailed
            ))
        );
        assert_eq!(transport.calls.load(Ordering::SeqCst), 0);
        assert!(!cache_dir.exists());
    }

    #[test]
    fn rejects_checksum_or_length_mismatch_without_publishing_a_bundle() {
        let expected = b"verified release archive";
        let key = signing_key();
        let verifying_key = VerifyingKey::from(&key);
        let signed =
            SignedUpdateManifest::sign(manifest_for(expected), "release-2026".to_owned(), &key)
                .unwrap();
        let cache_dir = stage_directory("checksum-mismatch");
        let mut altered = expected.to_vec();
        altered[0] ^= 1;
        let transport = FixtureTransport::bytes(&altered);

        assert_eq!(
            stage_verified_bundle(
                &stage_request(&signed, &verifying_key, &cache_dir),
                &transport,
            ),
            Err(UpdateStageError::DownloadChecksumMismatch)
        );
        assert!(
            !cache_dir
                .join("updates/staging")
                .join(format!("{}.bundle", sha256_hex(expected)))
                .exists()
        );

        let mut wrong_length = FixtureTransport::bytes(expected);
        wrong_length.content_length = Some(expected.len() as u64 + 1);
        assert_eq!(
            stage_verified_bundle(
                &stage_request(&signed, &verifying_key, &cache_dir),
                &wrong_length,
            ),
            Err(UpdateStageError::ContentLengthMismatch)
        );

        let mut oversized_bytes = expected.to_vec();
        oversized_bytes.push(0);
        let mut oversized = FixtureTransport::bytes(&oversized_bytes);
        oversized.content_length = None;
        assert_eq!(
            stage_verified_bundle(
                &stage_request(&signed, &verifying_key, &cache_dir),
                &oversized,
            ),
            Err(UpdateStageError::DownloadSizeMismatch)
        );
        let _ = fs::remove_dir_all(cache_dir);
    }

    #[test]
    fn reuses_only_a_reverified_existing_stage() {
        let bytes = b"verified release archive";
        let key = signing_key();
        let verifying_key = VerifyingKey::from(&key);
        let signed =
            SignedUpdateManifest::sign(manifest_for(bytes), "release-2026".to_owned(), &key)
                .unwrap();
        let cache_dir = stage_directory("reused");
        let first_transport = FixtureTransport::bytes(bytes);
        stage_verified_bundle(
            &stage_request(&signed, &verifying_key, &cache_dir),
            &first_transport,
        )
        .unwrap();
        let unavailable = FixtureTransport::unavailable();

        let staged = stage_verified_bundle(
            &stage_request(&signed, &verifying_key, &cache_dir),
            &unavailable,
        )
        .unwrap();
        assert!(staged.reused);
        assert_eq!(unavailable.calls.load(Ordering::SeqCst), 0);
        let _ = fs::remove_dir_all(cache_dir);
    }

    #[test]
    fn rejects_a_poisoned_existing_stage_without_downloading() {
        let bytes = b"verified release archive";
        let key = signing_key();
        let verifying_key = VerifyingKey::from(&key);
        let signed =
            SignedUpdateManifest::sign(manifest_for(bytes), "release-2026".to_owned(), &key)
                .unwrap();
        let cache_dir = stage_directory("poisoned");
        let staging_dir = cache_dir.join("updates/staging");
        fs::create_dir_all(&staging_dir).unwrap();
        fs::write(
            staging_dir.join(format!("{}.bundle", sha256_hex(bytes))),
            b"altered release archive",
        )
        .unwrap();
        let transport = FixtureTransport::bytes(bytes);

        assert_eq!(
            stage_verified_bundle(
                &stage_request(&signed, &verifying_key, &cache_dir),
                &transport,
            ),
            Err(UpdateStageError::ExistingStageInvalid)
        );
        assert_eq!(transport.calls.load(Ordering::SeqCst), 0);
        let _ = fs::remove_dir_all(cache_dir);
    }

    #[test]
    fn installs_a_release_after_backup_and_health_gate() {
        let root = stage_directory("install-transaction");
        fs::create_dir_all(&root).unwrap();
        let archive_path = root.join("release.tar.zst");
        write_tar_zst(
            &archive_path,
            &[
                ("bin/cmclient-agent", b"new agent"),
                ("web/index.html", b"new web"),
            ],
        );
        let staged = staged_archive(&archive_path, UpdateArchive::TarZst);
        let installation_root = root.join("install");
        let data_dir = root.join("data");
        let config_dir = root.join("config");
        let backup_root = root.join("backups");
        fs::create_dir_all(data_dir.join("sqlite")).unwrap();
        fs::create_dir_all(&config_dir).unwrap();
        fs::write(data_dir.join("sqlite/gateway.sqlite"), b"database").unwrap();
        fs::write(config_dir.join("agent.toml"), b"[agent]\n").unwrap();
        let mut lifecycle = RecordingLifecycle::healthy();

        let installed = install_verified_release(
            &install_request(
                &staged,
                &installation_root,
                &data_dir,
                &config_dir,
                &backup_root,
            ),
            &mut lifecycle,
        )
        .unwrap();

        assert_eq!(lifecycle.events, vec!["stop", "migrate", "start", "health"]);
        assert_eq!(
            fs::read(installed.release_path.join("bin/cmclient-agent")).unwrap(),
            b"new agent"
        );
        assert_eq!(
            fs::read(installed.backup_path.join("data/sqlite/gateway.sqlite")).unwrap(),
            b"database"
        );
        assert_eq!(
            fs::read(installed.backup_path.join("config/agent.toml")).unwrap(),
            b"[agent]\n"
        );
        assert_eq!(
            read_active_release(&installation_root).unwrap(),
            Some(ActiveRelease {
                schema_version: 1,
                release_id: staged.sha256.clone(),
                bundle_sha256: staged.sha256.clone(),
            })
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn rejects_zip_path_traversal_before_stopping_the_runtime() {
        let root = stage_directory("zip-path-traversal");
        fs::create_dir_all(&root).unwrap();
        let archive_path = root.join("release.zip");
        write_zip(&archive_path, &[("../outside", b"escape")]);
        let staged = staged_archive(&archive_path, UpdateArchive::Zip);
        let installation_root = root.join("install");
        let data_dir = root.join("data");
        let config_dir = root.join("config");
        let backup_root = root.join("backups");
        fs::create_dir_all(&data_dir).unwrap();
        fs::create_dir_all(&config_dir).unwrap();
        let mut lifecycle = RecordingLifecycle::healthy();

        assert_eq!(
            install_verified_release(
                &install_request(
                    &staged,
                    &installation_root,
                    &data_dir,
                    &config_dir,
                    &backup_root,
                ),
                &mut lifecycle,
            ),
            Err(UpdateInstallError::ArchivePathInvalid)
        );
        assert!(lifecycle.events.is_empty());
        assert!(!root.join("outside").exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn installs_a_verified_zip_release() {
        let root = stage_directory("zip-install");
        fs::create_dir_all(&root).unwrap();
        let archive_path = root.join("release.zip");
        write_zip(&archive_path, &[("bin/cmclient.exe", b"windows agent")]);
        let staged = staged_archive(&archive_path, UpdateArchive::Zip);
        let installation_root = root.join("install");
        let data_dir = root.join("data");
        let config_dir = root.join("config");
        let backup_root = root.join("backups");
        fs::create_dir_all(&data_dir).unwrap();
        fs::create_dir_all(&config_dir).unwrap();
        let mut lifecycle = RecordingLifecycle::healthy();

        let installed = install_verified_release(
            &install_request(
                &staged,
                &installation_root,
                &data_dir,
                &config_dir,
                &backup_root,
            ),
            &mut lifecycle,
        )
        .unwrap();

        assert_eq!(
            fs::read(installed.release_path.join("bin/cmclient.exe")).unwrap(),
            b"windows agent"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn fails_the_health_gate_after_start_without_reporting_success() {
        let root = stage_directory("health-gate");
        fs::create_dir_all(&root).unwrap();
        let archive_path = root.join("release.tar.zst");
        write_tar_zst(&archive_path, &[("bin/cmclient-agent", b"new agent")]);
        let staged = staged_archive(&archive_path, UpdateArchive::TarZst);
        let installation_root = root.join("install");
        let data_dir = root.join("data");
        let config_dir = root.join("config");
        let backup_root = root.join("backups");
        fs::create_dir_all(&data_dir).unwrap();
        fs::create_dir_all(&config_dir).unwrap();
        let mut lifecycle = RecordingLifecycle {
            events: Vec::new(),
            healthy: false,
        };

        assert_eq!(
            install_verified_release(
                &install_request(
                    &staged,
                    &installation_root,
                    &data_dir,
                    &config_dir,
                    &backup_root,
                ),
                &mut lifecycle,
            ),
            Err(UpdateInstallError::HealthCheckFailed)
        );
        assert_eq!(lifecycle.events, vec!["stop", "migrate", "start", "health"]);
        assert!(read_active_release(&installation_root).unwrap().is_some());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn uses_lowercase_sha256_hex() {
        assert_eq!(
            sha256_hex(b"cmclient"),
            "187f502695c49638eca51c7ef7fcc77b99e2c27aa4edfbb3c9c3b6f3e8a0842d"
        );
    }
}
