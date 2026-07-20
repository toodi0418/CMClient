use semver::Version;
use serde::{Deserialize, Serialize};

pub const PRODUCT_NAME: &str = "CMClient";
pub const IDENTITY_SCHEMA_VERSION: u8 = 1;

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ReleaseChannel {
    Dev,
    Candidate,
    Stable,
}

impl ReleaseChannel {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Dev => "dev",
            Self::Candidate => "candidate",
            Self::Stable => "stable",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RuntimeProfile {
    Native,
    Docker,
}

impl RuntimeProfile {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::Docker => "docker",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum TargetOperatingSystem {
    Windows,
    Macos,
    Linux,
}

impl TargetOperatingSystem {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Windows => "windows",
            Self::Macos => "macos",
            Self::Linux => "linux",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum TargetArchitecture {
    X86_64,
    Aarch64,
    Universal,
}

impl TargetArchitecture {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::X86_64 => "x86_64",
            Self::Aarch64 => "aarch64",
            Self::Universal => "universal",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum PackageProfile {
    Workspace,
    Setup,
    Dmg,
    Appimage,
    Oci,
}

impl PackageProfile {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Workspace => "workspace",
            Self::Setup => "setup",
            Self::Dmg => "dmg",
            Self::Appimage => "appimage",
            Self::Oci => "oci",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum InternalComponent {
    Agent,
    Gateway,
    Web,
    GraphicalMode,
    CommandMode,
    Updater,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ReleaseIdentity {
    pub schema_version: u8,
    pub product: String,
    pub version: String,
    pub source_commit: String,
    pub source_tree: String,
    pub channel: ReleaseChannel,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProductTarget {
    pub os: TargetOperatingSystem,
    pub architecture: TargetArchitecture,
    pub profile: RuntimeProfile,
    pub package_profile: PackageProfile,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ProductIdentity {
    pub schema_version: u8,
    pub product: String,
    pub version: String,
    pub source_commit: String,
    pub source_tree: String,
    pub channel: ReleaseChannel,
    pub target: ProductTarget,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ComponentIdentityReport {
    pub schema_version: u8,
    pub component: InternalComponent,
    pub identity: ProductIdentity,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum IdentityError {
    SchemaVersion,
    Product,
    Version,
    SourceCommit,
    SourceTree,
    Target,
    BuildInput,
}

impl ReleaseIdentity {
    pub fn validate(&self) -> Result<(), IdentityError> {
        validate_common(
            self.schema_version,
            &self.product,
            &self.version,
            &self.source_commit,
            &self.source_tree,
        )
    }
}

impl ProductTarget {
    pub const fn is_supported(self) -> bool {
        matches!(
            self,
            Self {
                os: TargetOperatingSystem::Windows,
                architecture: TargetArchitecture::X86_64,
                profile: RuntimeProfile::Native,
                package_profile: PackageProfile::Workspace | PackageProfile::Setup,
            } | Self {
                os: TargetOperatingSystem::Macos,
                architecture: TargetArchitecture::X86_64 | TargetArchitecture::Aarch64,
                profile: RuntimeProfile::Native,
                package_profile: PackageProfile::Workspace,
            } | Self {
                os: TargetOperatingSystem::Macos,
                architecture: TargetArchitecture::Universal,
                profile: RuntimeProfile::Native,
                package_profile: PackageProfile::Dmg,
            } | Self {
                os: TargetOperatingSystem::Linux,
                architecture: TargetArchitecture::X86_64 | TargetArchitecture::Aarch64,
                profile: RuntimeProfile::Native,
                package_profile: PackageProfile::Workspace | PackageProfile::Appimage,
            } | Self {
                os: TargetOperatingSystem::Linux,
                architecture: TargetArchitecture::X86_64 | TargetArchitecture::Aarch64,
                profile: RuntimeProfile::Docker,
                package_profile: PackageProfile::Oci,
            }
        )
    }

    pub const fn is_native_distribution(self) -> bool {
        self.is_supported()
            && matches!(self.profile, RuntimeProfile::Native)
            && !matches!(self.package_profile, PackageProfile::Workspace)
    }
}

impl ProductIdentity {
    pub fn validate(&self) -> Result<(), IdentityError> {
        validate_common(
            self.schema_version,
            &self.product,
            &self.version,
            &self.source_commit,
            &self.source_tree,
        )?;
        if !self.target.is_supported() {
            return Err(IdentityError::Target);
        }
        Ok(())
    }

    pub fn release(&self) -> ReleaseIdentity {
        ReleaseIdentity {
            schema_version: self.schema_version,
            product: self.product.clone(),
            version: self.version.clone(),
            source_commit: self.source_commit.clone(),
            source_tree: self.source_tree.clone(),
            channel: self.channel,
        }
    }
}

impl ComponentIdentityReport {
    pub fn validate(&self) -> Result<(), IdentityError> {
        if self.schema_version != IDENTITY_SCHEMA_VERSION {
            return Err(IdentityError::SchemaVersion);
        }
        self.identity.validate()
    }
}

pub fn compiled_component_identity(
    component: InternalComponent,
) -> Result<ComponentIdentityReport, IdentityError> {
    let identity = ProductIdentity {
        schema_version: IDENTITY_SCHEMA_VERSION,
        product: String::from(PRODUCT_NAME),
        version: String::from(env!("CARGO_PKG_VERSION")),
        source_commit: String::from(env!("CMCLIENT_IDENTITY_SOURCE_COMMIT")),
        source_tree: String::from(env!("CMCLIENT_IDENTITY_SOURCE_TREE")),
        channel: parse_channel(env!("CMCLIENT_IDENTITY_CHANNEL"))?,
        target: ProductTarget {
            os: parse_os(env!("CMCLIENT_IDENTITY_TARGET_OS"))?,
            architecture: parse_architecture(env!("CMCLIENT_IDENTITY_TARGET_ARCHITECTURE"))?,
            profile: parse_profile(env!("CMCLIENT_IDENTITY_RUNTIME_PROFILE"))?,
            package_profile: parse_package_profile(env!("CMCLIENT_IDENTITY_PACKAGE_PROFILE"))?,
        },
    };
    identity.validate()?;
    Ok(ComponentIdentityReport {
        schema_version: IDENTITY_SCHEMA_VERSION,
        component,
        identity,
    })
}

fn validate_common(
    schema_version: u8,
    product: &str,
    version: &str,
    source_commit: &str,
    source_tree: &str,
) -> Result<(), IdentityError> {
    if schema_version != IDENTITY_SCHEMA_VERSION {
        return Err(IdentityError::SchemaVersion);
    }
    if product != PRODUCT_NAME {
        return Err(IdentityError::Product);
    }
    if Version::parse(version).is_err() {
        return Err(IdentityError::Version);
    }
    if !is_git_object_id(source_commit) {
        return Err(IdentityError::SourceCommit);
    }
    if !is_source_tree_id(source_tree) {
        return Err(IdentityError::SourceTree);
    }
    Ok(())
}

fn is_git_object_id(value: &str) -> bool {
    value.len() == 40
        && value
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
}

fn is_source_tree_id(value: &str) -> bool {
    is_git_object_id(value)
        || value.strip_prefix("sha256:").is_some_and(|digest| {
            digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
}

fn parse_channel(value: &str) -> Result<ReleaseChannel, IdentityError> {
    match value {
        "dev" => Ok(ReleaseChannel::Dev),
        "candidate" => Ok(ReleaseChannel::Candidate),
        "stable" => Ok(ReleaseChannel::Stable),
        _ => Err(IdentityError::BuildInput),
    }
}

fn parse_profile(value: &str) -> Result<RuntimeProfile, IdentityError> {
    match value {
        "native" => Ok(RuntimeProfile::Native),
        "docker" => Ok(RuntimeProfile::Docker),
        _ => Err(IdentityError::BuildInput),
    }
}

fn parse_os(value: &str) -> Result<TargetOperatingSystem, IdentityError> {
    match value {
        "windows" => Ok(TargetOperatingSystem::Windows),
        "macos" => Ok(TargetOperatingSystem::Macos),
        "linux" => Ok(TargetOperatingSystem::Linux),
        _ => Err(IdentityError::BuildInput),
    }
}

fn parse_architecture(value: &str) -> Result<TargetArchitecture, IdentityError> {
    match value {
        "x86_64" => Ok(TargetArchitecture::X86_64),
        "aarch64" => Ok(TargetArchitecture::Aarch64),
        "universal" => Ok(TargetArchitecture::Universal),
        _ => Err(IdentityError::BuildInput),
    }
}

fn parse_package_profile(value: &str) -> Result<PackageProfile, IdentityError> {
    match value {
        "workspace" => Ok(PackageProfile::Workspace),
        "setup" => Ok(PackageProfile::Setup),
        "dmg" => Ok(PackageProfile::Dmg),
        "appimage" => Ok(PackageProfile::Appimage),
        "oci" => Ok(PackageProfile::Oci),
        _ => Err(IdentityError::BuildInput),
    }
}

#[cfg(test)]
mod tests {
    use super::{
        InternalComponent, PackageProfile, ProductTarget, RuntimeProfile, TargetArchitecture,
        TargetOperatingSystem, compiled_component_identity,
    };

    #[test]
    fn compiled_workspace_identity_is_exact_and_valid() {
        let report = compiled_component_identity(InternalComponent::Agent).unwrap();
        assert_eq!(report.identity.source_commit.len(), 40);
        assert!(
            report.identity.source_tree.len() == 40
                || (report.identity.source_tree.len() == 71
                    && report.identity.source_tree.starts_with("sha256:"))
        );
        assert_eq!(
            report.identity.target.package_profile,
            PackageProfile::Workspace
        );
    }

    #[test]
    fn accepts_the_shared_typescript_rust_wire_fixture() {
        let report: super::ComponentIdentityReport = serde_json::from_str(include_str!(
            "../../../test/fixtures/unified-product-identity.json"
        ))
        .unwrap();
        report.validate().unwrap();
        assert_eq!(report.component, InternalComponent::Gateway);
    }

    #[test]
    fn every_internal_component_shares_the_exact_product_identity() {
        let agent = compiled_component_identity(InternalComponent::Agent).unwrap();
        let gateway = compiled_component_identity(InternalComponent::Gateway).unwrap();
        let command = compiled_component_identity(InternalComponent::CommandMode).unwrap();
        assert_eq!(agent.identity, gateway.identity);
        assert_eq!(agent.identity, command.identity);
    }

    #[test]
    fn target_matrix_rejects_unsupported_products() {
        let windows_arm = ProductTarget {
            os: TargetOperatingSystem::Windows,
            architecture: TargetArchitecture::Aarch64,
            profile: RuntimeProfile::Native,
            package_profile: PackageProfile::Setup,
        };
        let docker_macos = ProductTarget {
            os: TargetOperatingSystem::Macos,
            architecture: TargetArchitecture::Aarch64,
            profile: RuntimeProfile::Docker,
            package_profile: PackageProfile::Oci,
        };
        assert!(!windows_arm.is_supported());
        assert!(!docker_macos.is_supported());
    }
}
