//! Shared Rust foundations for the CMClient Agent.

pub mod access;
pub mod secrets;
pub mod web;

use crate::access::{LanAccessConfig, ManagementAccessController};
use cmclient_runtime_primitives::{
    DocumentError, DocumentFormat, DurableDocument, ExclusiveFileLock, LockError, TypedDocument,
};
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fmt::{Display, Formatter},
    fs,
    net::IpAddr,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

/// Stable workspace identity for the Agent core boundary.
pub const COMPONENT: &str = "agent-core";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePaths {
    pub data_dir: PathBuf,
    pub config_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub log_dir: PathBuf,
}

impl RuntimePaths {
    /// The single mutable root used by every native runtime.
    pub fn root_dir(&self) -> &Path {
        &self.data_dir
    }

    pub fn config_file(&self) -> PathBuf {
        self.data_dir.join("config.toml")
    }

    pub fn state_dir(&self) -> PathBuf {
        self.data_dir.join("state")
    }

    pub fn setup_state_file(&self) -> PathBuf {
        self.state_dir().join("setup.json")
    }

    pub fn database_file(&self) -> PathBuf {
        self.data_dir.join("cmclient.db")
    }

    pub fn run_dir(&self) -> PathBuf {
        self.data_dir.join("run")
    }

    pub fn lock_dir(&self) -> PathBuf {
        self.run_dir()
    }

    pub fn agent_lock_file(&self) -> PathBuf {
        self.run_dir().join("agent.lock")
    }

    pub fn secrets_file(&self) -> PathBuf {
        self.data_dir.join("secrets.json")
    }

    pub fn backups_dir(&self) -> PathBuf {
        self.data_dir.join("backups")
    }

    pub fn updates_dir(&self) -> PathBuf {
        self.data_dir.join("updates")
    }

    pub fn migration_state_file(&self) -> PathBuf {
        self.state_dir().join("migration.json")
    }

    pub fn managed_directories(&self) -> [PathBuf; 7] {
        [
            self.data_dir.clone(),
            self.state_dir(),
            self.run_dir(),
            self.cache_dir.clone(),
            self.log_dir.clone(),
            self.backups_dir(),
            self.updates_dir(),
        ]
    }

    pub fn from_environment(environment: &BTreeMap<String, String>) -> Result<Self, ConfigError> {
        for name in [
            "CMCLIENT_DATA_DIR",
            "CMCLIENT_CONFIG_DIR",
            "CMCLIENT_CACHE_DIR",
            "CMCLIENT_LOG_DIR",
        ] {
            reject_path_override(environment, name)?;
        }

        let root = if is_docker_profile(environment) {
            PathBuf::from("/home/cmclient/.cmclient")
        } else {
            home_directory(environment)?.join(".cmclient")
        };
        Ok(Self {
            data_dir: root.clone(),
            config_dir: root.clone(),
            cache_dir: root.join("cache"),
            log_dir: root.join("logs"),
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentConfig {
    pub paths: RuntimePaths,
    pub config_file: PathBuf,
    pub gateway_command: Option<Vec<String>>,
    pub callmesh: Option<CallMeshConfig>,
    pub meshtastic: Option<MeshtasticConfig>,
    pub aprs: Option<AprsConfig>,
    pub proxy: Option<ProxyConfig>,
    pub management_web_enabled: bool,
    pub management_lan: Option<ManagementLanConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallMeshConfig {
    pub url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MeshtasticConfig {
    pub mesh_network_id: String,
    pub gateway_id: String,
    pub connection: MeshtasticConnectionConfig,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MeshtasticConnectionConfig {
    Tcp { host: String, port: u16 },
    Serial { path: String, baud_rate: u32 },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AprsConfig {
    /// Optional operator endpoint override; identity comes from CallMesh.
    pub host: Option<String>,
    /// Optional operator endpoint override; the Gateway owns the default.
    pub port: Option<u16>,
    /// Optional operator destination override; the Gateway owns the default.
    pub destination: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProxyConfig {
    pub host: String,
    pub port: u16,
    pub upstream_host: String,
    pub upstream_port: u16,
    pub mode: String,
    pub allow_lan: bool,
    pub allowlist: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ManagementLanConfig {
    pub bind: IpAddr,
    pub port: u16,
    pub access: LanAccessConfig,
    pub certificate_path: PathBuf,
    pub private_key_path: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct AgentState {
    pub schema_version: u8,
    pub pid: u32,
    pub started_at_unix_seconds: u64,
}

impl DurableDocument for AgentState {
    const FORMAT: DocumentFormat = DocumentFormat::Json;
    const MAX_BYTES: usize = 4 * 1024;

    fn validate(&self) -> bool {
        self.schema_version == 1 && self.pid != 0 && self.started_at_unix_seconds != 0
    }
}

pub struct AgentLease {
    _lock: ExclusiveFileLock,
    state_file: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    HomeDirectoryUnavailable,
    RelativePathOverride { name: &'static str },
    ForeignPathOverride { name: &'static str },
    ReadConfig { path: PathBuf },
    InvalidConfig,
    EmptyGatewayCommand,
    InvalidCallMesh,
    InvalidMeshtastic,
    InvalidAprs,
    InvalidProxy,
    InvalidManagementLan,
}

#[derive(Debug)]
pub enum InstanceError {
    AlreadyRunning,
    StateInvalid,
    Io,
}

impl InstanceError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::AlreadyRunning => "AGENT_INSTANCE_ALREADY_RUNNING",
            Self::StateInvalid => "AGENT_INSTANCE_STATE_INVALID",
            Self::Io => "AGENT_INSTANCE_IO_FAILED",
        }
    }
}

impl Display for InstanceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for InstanceError {}

impl ConfigError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::HomeDirectoryUnavailable => "AGENT_CONFIG_HOME_UNAVAILABLE",
            Self::RelativePathOverride { .. } => "AGENT_CONFIG_PATH_NOT_ABSOLUTE",
            Self::ForeignPathOverride { .. } => "AGENT_CONFIG_FOREIGN_PATH_FORBIDDEN",
            Self::ReadConfig { .. } => "AGENT_CONFIG_READ_FAILED",
            Self::InvalidConfig => "AGENT_CONFIG_INVALID",
            Self::EmptyGatewayCommand => "AGENT_CONFIG_GATEWAY_COMMAND_EMPTY",
            Self::InvalidCallMesh => "AGENT_CONFIG_CALLMESH_INVALID",
            Self::InvalidMeshtastic => "AGENT_CONFIG_MESHTASTIC_INVALID",
            Self::InvalidAprs => "AGENT_CONFIG_APRS_INVALID",
            Self::InvalidProxy => "AGENT_CONFIG_PROXY_INVALID",
            Self::InvalidManagementLan => "AGENT_CONFIG_MANAGEMENT_LAN_INVALID",
        }
    }
}

impl Display for ConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.code())
    }
}

impl std::error::Error for ConfigError {}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct FileConfig {
    agent: Option<AgentSection>,
    callmesh: Option<CallMeshSection>,
    meshtastic: Option<MeshtasticSection>,
    aprs: Option<AprsSection>,
    proxy: Option<ProxySection>,
    management_lan: Option<ManagementLanSection>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CallMeshSection {
    url: String,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct MeshtasticSection {
    transport: String,
    mesh_network_id: String,
    gateway_id: String,
    tcp_host: Option<String>,
    tcp_port: Option<u16>,
    serial_path: Option<String>,
    serial_baud_rate: Option<u32>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct AprsSection {
    host: Option<String>,
    port: Option<u16>,
    destination: Option<String>,
    #[serde(rename = "login_callsign")]
    _legacy_login_callsign: Option<String>,
    #[serde(rename = "symbol_table")]
    _legacy_symbol_table: Option<String>,
    #[serde(rename = "symbol_code")]
    _legacy_symbol_code: Option<String>,
    #[serde(rename = "comment")]
    _legacy_comment: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProxySection {
    upstream_host: String,
    upstream_port: u16,
    host: Option<String>,
    port: Option<u16>,
    mode: Option<String>,
    allow_lan: Option<bool>,
    allowlist: Option<Vec<String>>,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentSection {
    gateway_command: Option<Vec<String>>,
    management_web_enabled: Option<bool>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ManagementLanSection {
    bind: IpAddr,
    port: u16,
    password_hash: String,
    allowed_origins: Vec<String>,
    session_ttl_seconds: Option<u64>,
    audit_capacity: Option<usize>,
    certificate_path: PathBuf,
    private_key_path: PathBuf,
}

impl AgentConfig {
    pub fn load() -> Result<Self, ConfigError> {
        let environment = env::vars().collect::<BTreeMap<_, _>>();
        Self::from_environment(&environment)
    }

    pub fn from_environment(environment: &BTreeMap<String, String>) -> Result<Self, ConfigError> {
        let paths = RuntimePaths::from_environment(environment)?;
        let config_file = config_file_override(environment, &paths)?;

        let file_config = if config_file.exists() {
            let contents =
                fs::read_to_string(&config_file).map_err(|_| ConfigError::ReadConfig {
                    path: config_file.clone(),
                })?;
            toml::from_str::<FileConfig>(&contents).map_err(|_| ConfigError::InvalidConfig)?
        } else {
            FileConfig::default()
        };
        let agent = file_config.agent.unwrap_or_default();
        if agent.gateway_command.as_ref().is_some_and(|command| {
            command.is_empty() || command.iter().any(|argument| argument.is_empty())
        }) {
            return Err(ConfigError::EmptyGatewayCommand);
        }
        let management_lan = file_config
            .management_lan
            .map(|lan| {
                if lan.bind.is_loopback()
                    || lan.port == 0
                    || !lan.certificate_path.is_absolute()
                    || !lan.private_key_path.is_absolute()
                {
                    return Err(ConfigError::InvalidManagementLan);
                }
                let access = LanAccessConfig {
                    password_hash: lan.password_hash,
                    allowed_origins: lan.allowed_origins.into_iter().collect::<BTreeSet<_>>(),
                    session_ttl_seconds: lan.session_ttl_seconds.unwrap_or(3_600),
                    audit_capacity: lan.audit_capacity.unwrap_or(512),
                };
                ManagementAccessController::new(access.clone())
                    .map_err(|_| ConfigError::InvalidManagementLan)?;
                Ok(ManagementLanConfig {
                    bind: lan.bind,
                    port: lan.port,
                    access,
                    certificate_path: lan.certificate_path,
                    private_key_path: lan.private_key_path,
                })
            })
            .transpose()?;
        let callmesh = file_config
            .callmesh
            .map(|callmesh| {
                let url = callmesh.url.trim();
                if !is_https_origin(url) {
                    return Err(ConfigError::InvalidCallMesh);
                }
                Ok(CallMeshConfig {
                    url: url.to_owned(),
                })
            })
            .transpose()?;
        let meshtastic = file_config
            .meshtastic
            .map(parse_meshtastic_config)
            .transpose()?;
        let aprs = file_config.aprs.map(parse_aprs_config).transpose()?;
        let proxy = file_config.proxy.map(parse_proxy_config).transpose()?;

        Ok(Self {
            paths,
            config_file,
            gateway_command: agent.gateway_command,
            callmesh,
            meshtastic,
            aprs,
            proxy,
            management_web_enabled: agent.management_web_enabled.unwrap_or(true),
            management_lan,
        })
    }
}

fn parse_meshtastic_config(section: MeshtasticSection) -> Result<MeshtasticConfig, ConfigError> {
    if !is_bounded_text(&section.mesh_network_id, 128) || !is_bounded_text(&section.gateway_id, 128)
    {
        return Err(ConfigError::InvalidMeshtastic);
    }
    let connection = match section.transport.as_str() {
        "tcp"
            if section.serial_path.is_none()
                && section.serial_baud_rate.is_none()
                && section.tcp_port.unwrap_or(4_403) != 0 =>
        {
            let host = section
                .tcp_host
                .unwrap_or_else(|| String::from("127.0.0.1"));
            if !is_endpoint_host(&host) {
                return Err(ConfigError::InvalidMeshtastic);
            }
            MeshtasticConnectionConfig::Tcp {
                host,
                port: section.tcp_port.unwrap_or(4_403),
            }
        }
        "serial"
            if section.tcp_host.is_none()
                && section.tcp_port.is_none()
                && section.serial_baud_rate.unwrap_or(115_200) > 0 =>
        {
            let path = section.serial_path.unwrap_or_default();
            if !is_bounded_text(&path, 4_096) {
                return Err(ConfigError::InvalidMeshtastic);
            }
            MeshtasticConnectionConfig::Serial {
                path,
                baud_rate: section.serial_baud_rate.unwrap_or(115_200),
            }
        }
        _ => return Err(ConfigError::InvalidMeshtastic),
    };
    Ok(MeshtasticConfig {
        mesh_network_id: section.mesh_network_id,
        gateway_id: section.gateway_id,
        connection,
    })
}

fn parse_aprs_config(section: AprsSection) -> Result<AprsConfig, ConfigError> {
    if section
        .host
        .as_ref()
        .is_some_and(|host| !is_endpoint_host(host))
        || section.port.is_some_and(|port| port == 0)
        || section
            .destination
            .as_ref()
            .is_some_and(|destination| !is_aprs_destination(destination))
    {
        return Err(ConfigError::InvalidAprs);
    }
    Ok(AprsConfig {
        host: section.host,
        port: section.port,
        destination: section.destination,
    })
}

fn parse_proxy_config(section: ProxySection) -> Result<ProxyConfig, ConfigError> {
    let host = section.host.unwrap_or_else(|| String::from("127.0.0.1"));
    let port = section.port.unwrap_or(4_403);
    let mode = section.mode.unwrap_or_else(|| String::from("monitor"));
    let allow_lan = section.allow_lan.unwrap_or(false);
    let allowlist = section.allowlist.unwrap_or_default();
    if !is_endpoint_host(&host)
        || port == 0
        || !is_endpoint_host(&section.upstream_host)
        || section.upstream_port == 0
        || !matches!(mode.as_str(), "monitor" | "message" | "full")
        || (!allow_lan && !allowlist.is_empty())
        || allowlist.iter().any(|address| {
            address.parse::<IpAddr>().is_err()
                || address.len() > 64
                || address.contains(char::is_whitespace)
        })
    {
        return Err(ConfigError::InvalidProxy);
    }
    Ok(ProxyConfig {
        host,
        port,
        upstream_host: section.upstream_host,
        upstream_port: section.upstream_port,
        mode,
        allow_lan,
        allowlist,
    })
}

fn is_bounded_text(value: &str, maximum_length: usize) -> bool {
    !value.is_empty() && value.len() <= maximum_length && !value.chars().any(char::is_control)
}

fn is_endpoint_host(value: &str) -> bool {
    is_bounded_text(value, 255) && !value.contains(char::is_whitespace)
}

fn is_aprs_destination(value: &str) -> bool {
    (1..=6).contains(&value.len())
        && value
            .chars()
            .all(|character| character.is_ascii_uppercase() || character.is_ascii_digit())
}

fn is_https_origin(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("https://") else {
        return false;
    };
    let boundary = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let (authority, suffix) = rest.split_at(boundary);
    !authority.is_empty()
        && !authority.contains('@')
        && !authority.contains('\\')
        && !authority.contains(char::is_whitespace)
        && (suffix.is_empty() || suffix == "/")
}

fn home_directory(environment: &BTreeMap<String, String>) -> Result<PathBuf, ConfigError> {
    let name = if cfg!(target_os = "windows") {
        "USERPROFILE"
    } else {
        "HOME"
    };
    environment
        .get(name)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or(ConfigError::HomeDirectoryUnavailable)
}

fn is_docker_profile(environment: &BTreeMap<String, String>) -> bool {
    environment
        .get("CMCLIENT_RUNTIME_PROFILE")
        .is_some_and(|profile| profile.trim().eq_ignore_ascii_case("docker"))
        || environment
            .get("CMCLIENT_PACKAGE_PROFILE")
            .is_some_and(|profile| profile.trim().eq_ignore_ascii_case("oci"))
}

fn reject_path_override(
    environment: &BTreeMap<String, String>,
    name: &'static str,
) -> Result<(), ConfigError> {
    match environment.get(name) {
        Some(value) if !value.trim().is_empty() => {
            let path = PathBuf::from(value);
            if path.is_absolute() {
                Err(ConfigError::ForeignPathOverride { name })
            } else {
                Err(ConfigError::RelativePathOverride { name })
            }
        }
        _ => Ok(()),
    }
}

fn config_file_override(
    environment: &BTreeMap<String, String>,
    paths: &RuntimePaths,
) -> Result<PathBuf, ConfigError> {
    reject_path_override(environment, "CMCLIENT_AGENT_CONFIG")?;
    Ok(paths.config_file())
}

pub fn ensure_runtime_directories(paths: &RuntimePaths) -> Result<(), ConfigError> {
    for path in paths.managed_directories() {
        ensure_runtime_directory(&path)?;
    }
    Ok(())
}

fn ensure_runtime_directory(path: &Path) -> Result<(), ConfigError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(_) => {
            return Err(ConfigError::ReadConfig {
                path: path.to_owned(),
            });
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(|_| ConfigError::ReadConfig {
                path: path.to_owned(),
            })?;
        }
        Err(_) => {
            return Err(ConfigError::ReadConfig {
                path: path.to_owned(),
            });
        }
    }
    if !fs::symlink_metadata(path)
        .map_err(|_| ConfigError::ReadConfig {
            path: path.to_owned(),
        })?
        .file_type()
        .is_dir()
    {
        return Err(ConfigError::ReadConfig {
            path: path.to_owned(),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|_| {
            ConfigError::ReadConfig {
                path: path.to_owned(),
            }
        })?;
    }
    Ok(())
}

impl AgentLease {
    pub fn acquire(paths: &RuntimePaths) -> Result<(Self, AgentState), InstanceError> {
        ensure_runtime_directory(&paths.data_dir).map_err(|_| InstanceError::Io)?;
        let canonical_data_dir =
            fs::canonicalize(&paths.data_dir).map_err(|_| InstanceError::Io)?;
        let lock_dir = canonical_data_dir.join("run");
        ensure_runtime_directory(&lock_dir).map_err(|_| InstanceError::Io)?;
        let lock_path = lock_dir.join("agent.lock");
        let lock = ExclusiveFileLock::try_acquire(&lock_path).map_err(map_lock_error)?;

        let state = AgentState {
            schema_version: 1,
            pid: std::process::id(),
            started_at_unix_seconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| InstanceError::Io)?
                .as_secs(),
        };
        let state_dir = canonical_data_dir.join("state");
        ensure_runtime_directory(&state_dir).map_err(|_| InstanceError::Io)?;
        let state_file = state_dir.join("agent.json");
        TypedDocument::<AgentState>::new(&state_file)
            .and_then(|document| document.store(&state))
            .map_err(map_document_write_error)?;
        Ok((
            Self {
                _lock: lock,
                state_file,
            },
            state,
        ))
    }

    pub fn read_state(paths: &RuntimePaths) -> Result<Option<AgentState>, InstanceError> {
        let state_file = paths.state_dir().join("agent.json");
        TypedDocument::<AgentState>::new(state_file)
            .and_then(|document| document.load_optional())
            .map_err(map_document_read_error)
    }
}

impl Drop for AgentLease {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.state_file);
    }
}

fn map_lock_error(error: LockError) -> InstanceError {
    match error {
        LockError::Contended => InstanceError::AlreadyRunning,
        _ => InstanceError::Io,
    }
}

fn map_document_read_error(error: DocumentError) -> InstanceError {
    match error {
        DocumentError::Malformed | DocumentError::SchemaInvalid | DocumentError::TooLarge => {
            InstanceError::StateInvalid
        }
        _ => InstanceError::Io,
    }
}

fn map_document_write_error(_error: DocumentError) -> InstanceError {
    InstanceError::Io
}

pub fn is_config_file(path: &Path) -> bool {
    path.file_name().is_some_and(|name| name == "config.toml")
}

#[cfg(test)]
mod tests {
    use super::{
        AgentConfig, AgentLease, AprsConfig, ConfigError, InstanceError, MeshtasticConfig,
        MeshtasticConnectionConfig, ProxyConfig, RuntimePaths, ensure_runtime_directories,
        is_config_file,
    };
    use std::{
        collections::BTreeMap,
        fs,
        path::{Path, PathBuf},
    };
    use uuid::Uuid;

    fn fixture_home() -> PathBuf {
        #[cfg(windows)]
        {
            PathBuf::from(r"C:\fixture\home")
        }
        #[cfg(not(windows))]
        {
            PathBuf::from("/fixture/home")
        }
    }

    fn environment_for_home(home: &Path) -> BTreeMap<String, String> {
        let mut environment =
            BTreeMap::from([(String::from("HOME"), home.to_string_lossy().into_owned())]);
        #[cfg(windows)]
        environment.insert(
            String::from("USERPROFILE"),
            home.to_string_lossy().into_owned(),
        );
        environment
    }

    fn environment() -> BTreeMap<String, String> {
        environment_for_home(&fixture_home())
    }

    fn config_fixture(label: &str) -> (PathBuf, BTreeMap<String, String>, PathBuf) {
        let directory =
            std::env::temp_dir().join(format!("cmclient-agent-config-{label}-{}", Uuid::new_v4()));
        let home = directory.join("home");
        let config_file = home.join(".cmclient/config.toml");
        fs::create_dir_all(config_file.parent().expect("config parent should exist"))
            .expect("configuration root should exist");
        (directory, environment_for_home(&home), config_file)
    }

    #[test]
    fn uses_standard_platform_paths() {
        let paths = RuntimePaths::from_environment(&environment()).expect("paths should load");
        let home = fixture_home();
        assert_eq!(paths.data_dir, home.join(".cmclient"));
        assert_eq!(paths.config_dir, home.join(".cmclient"));
        assert_eq!(paths.cache_dir, home.join(".cmclient/cache"));
        assert_eq!(paths.log_dir, paths.data_dir.join("logs"));
        assert_eq!(paths.database_file(), home.join(".cmclient/cmclient.db"));
        assert_eq!(paths.secrets_file(), home.join(".cmclient/secrets.json"));
        assert_eq!(paths.config_file(), home.join(".cmclient/config.toml"));
        assert_eq!(
            paths.setup_state_file(),
            home.join(".cmclient/state/setup.json")
        );
        assert_eq!(
            paths.migration_state_file(),
            home.join(".cmclient/state/migration.json")
        );
        assert_eq!(
            paths.agent_lock_file(),
            home.join(".cmclient/run/agent.lock")
        );
    }

    #[test]
    fn docker_paths_are_fixed_and_ignore_host_home() {
        let mut environment = environment();
        environment.insert(
            String::from("CMCLIENT_RUNTIME_PROFILE"),
            String::from("docker"),
        );
        environment.insert(String::from("HOME"), String::from("C:\\foreign"));
        let paths = RuntimePaths::from_environment(&environment).expect("docker paths should load");
        assert_eq!(paths.data_dir, PathBuf::from("/home/cmclient/.cmclient"));
        assert_eq!(
            paths.config_file(),
            PathBuf::from("/home/cmclient/.cmclient/config.toml")
        );
    }

    #[test]
    fn rejects_legacy_and_foreign_runtime_path_overrides() {
        let mut legacy_environment = environment();
        legacy_environment.insert(String::from("CMCLIENT_DATA_DIR"), String::from("relative"));
        assert_eq!(
            RuntimePaths::from_environment(&legacy_environment),
            Err(ConfigError::RelativePathOverride {
                name: "CMCLIENT_DATA_DIR"
            })
        );

        let mut foreign_environment = environment();
        foreign_environment.insert(
            String::from("CMCLIENT_CONFIG_DIR"),
            fixture_home()
                .join("foreign-config")
                .to_string_lossy()
                .into_owned(),
        );
        assert_eq!(
            RuntimePaths::from_environment(&foreign_environment),
            Err(ConfigError::ForeignPathOverride {
                name: "CMCLIENT_CONFIG_DIR"
            })
        );

        let mut config_environment = environment();
        config_environment.insert(
            String::from("CMCLIENT_AGENT_CONFIG"),
            fixture_home()
                .join("foreign-config.toml")
                .to_string_lossy()
                .into_owned(),
        );
        assert_eq!(
            AgentConfig::from_environment(&config_environment),
            Err(ConfigError::ForeignPathOverride {
                name: "CMCLIENT_AGENT_CONFIG"
            })
        );
    }

    #[test]
    fn environment_snapshot_is_immutable() {
        let environment = environment();
        let snapshot = environment.clone();
        RuntimePaths::from_environment(&environment).expect("paths should load");
        assert_eq!(environment, snapshot);
    }

    #[test]
    fn creates_only_canonical_runtime_directories_and_rejects_foreign_entries() {
        let directory =
            std::env::temp_dir().join(format!("cmclient-runtime-paths-{}", Uuid::new_v4()));
        let home = directory.join("home");
        fs::create_dir_all(&home).expect("fixture home should exist");
        let paths = RuntimePaths::from_environment(&environment_for_home(&home))
            .expect("runtime paths should resolve");

        ensure_runtime_directories(&paths).expect("runtime directories should be created");
        for path in paths.managed_directories() {
            assert!(path.is_dir(), "{} should be a directory", path.display());
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                assert_eq!(
                    fs::metadata(&path)
                        .expect("runtime directory should have metadata")
                        .permissions()
                        .mode()
                        & 0o7777,
                    0o700,
                    "{} should be user-private",
                    path.display()
                );
            }
        }

        fs::remove_dir_all(paths.run_dir()).expect("run directory should be removable");
        fs::write(paths.run_dir(), b"not-a-directory").expect("blocking file should be written");
        assert!(matches!(
            ensure_runtime_directories(&paths),
            Err(ConfigError::ReadConfig { path }) if path == paths.run_dir()
        ));
        fs::remove_dir_all(directory).expect("runtime fixture should be removed");
    }

    #[test]
    fn loads_a_strict_agent_file() {
        let (directory, environment, config_file) = config_fixture("strict");
        fs::write(
            &config_file,
            "[agent]\ngateway_command = [\"gateway\", \"serve\"]\nmanagement_web_enabled = false\n",
        )
        .expect("configuration should be written");

        let config =
            AgentConfig::from_environment(&environment).expect("configuration should load");
        assert_eq!(
            config.gateway_command,
            Some(vec![String::from("gateway"), String::from("serve")])
        );
        assert!(!config.management_web_enabled);
        assert!(is_config_file(&config.config_file));
        fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[test]
    fn rejects_the_removed_gateway_port_setting() {
        let (directory, environment, config_file) = config_fixture("removed-port");
        fs::write(
            &config_file,
            ["[agent]\n", "gateway_", "port = 4810\n"].concat(),
        )
        .expect("configuration should be written");

        assert_eq!(
            AgentConfig::from_environment(&environment),
            Err(ConfigError::InvalidConfig)
        );
        fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[test]
    fn accepts_only_an_https_callmesh_url_without_a_secret_in_configuration() {
        let (directory, environment, config_file) = config_fixture("callmesh");
        fs::write(
            &config_file,
            "[callmesh]\nurl = \"https://callmesh.example.invalid/\"\n",
        )
        .expect("configuration should be written");

        let config = AgentConfig::from_environment(&environment)
            .expect("HTTPS CallMesh configuration should load");
        assert_eq!(
            config
                .callmesh
                .expect("CallMesh configuration should exist")
                .url,
            "https://callmesh.example.invalid/"
        );
        fs::write(
            &config_file,
            "[callmesh]\nurl = \"https://callmesh.example.invalid/v1\"\n",
        )
        .expect("configuration should be written");
        assert_eq!(
            AgentConfig::from_environment(&environment),
            Err(ConfigError::InvalidCallMesh)
        );
        fs::write(
            &config_file,
            "[callmesh]\nurl = \"https:///missing-host\"\n",
        )
        .expect("configuration should be written");
        assert_eq!(
            AgentConfig::from_environment(&environment),
            Err(ConfigError::InvalidCallMesh)
        );
        fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[test]
    fn loads_gateway_transport_aprs_and_proxy_without_inline_secrets() {
        let (directory, environment, config_file) = config_fixture("gateway-runtime");
        fs::write(
            &config_file,
            r#"[meshtastic]
transport = "tcp"
mesh_network_id = "fixture-network"
gateway_id = "fixture-gateway"
tcp_host = "192.0.2.10"
tcp_port = 4403

[aprs]
host = "asia.aprs2.net"
port = 14580
destination = "APCM20"

[proxy]
upstream_host = "192.0.2.10"
upstream_port = 4403
host = "127.0.0.1"
port = 4404
mode = "message"
allow_lan = true
allowlist = ["192.0.2.20"]
"#,
        )
        .expect("configuration should be written");
        let config = AgentConfig::from_environment(&environment)
            .expect("gateway runtime configuration should load");
        assert_eq!(
            config.meshtastic,
            Some(MeshtasticConfig {
                mesh_network_id: String::from("fixture-network"),
                gateway_id: String::from("fixture-gateway"),
                connection: MeshtasticConnectionConfig::Tcp {
                    host: String::from("192.0.2.10"),
                    port: 4_403,
                },
            })
        );
        assert_eq!(
            config.aprs,
            Some(AprsConfig {
                host: Some(String::from("asia.aprs2.net")),
                port: Some(14_580),
                destination: Some(String::from("APCM20")),
            })
        );
        assert_eq!(
            config.proxy,
            Some(ProxyConfig {
                host: String::from("127.0.0.1"),
                port: 4_404,
                upstream_host: String::from("192.0.2.10"),
                upstream_port: 4_403,
                mode: String::from("message"),
                allow_lan: true,
                allowlist: vec![String::from("192.0.2.20")],
            })
        );

        fs::write(&config_file, "[aprs]\n").expect("Gateway-owned APRS defaults should be written");
        assert_eq!(
            AgentConfig::from_environment(&environment)
                .expect("empty APRS override section should load")
                .aprs,
            Some(AprsConfig {
                host: None,
                port: None,
                destination: None,
            })
        );

        fs::write(
            &config_file,
            r#"[aprs]
host = "asia.aprs2.net"
login_callsign = "N0CALL-7"
symbol_table = "/"
symbol_code = ">"
comment = "legacy comment"
"#,
        )
        .expect("legacy-compatible configuration should be written");
        assert_eq!(
            AgentConfig::from_environment(&environment)
                .expect("known legacy APRS fields should be ignored")
                .aprs,
            Some(AprsConfig {
                host: Some(String::from("asia.aprs2.net")),
                port: None,
                destination: None,
            })
        );

        for field in ["passcode", "unknown_field"] {
            fs::write(&config_file, format!("[aprs]\n{field} = \"fixture\"\n"))
                .expect("unknown configuration should be written");
            assert_eq!(
                AgentConfig::from_environment(&environment),
                Err(ConfigError::InvalidConfig),
                "unknown APRS field {field} must not be accepted",
            );
        }
        fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[test]
    fn rejects_ambiguous_meshtastic_and_unsafe_proxy_configuration() {
        let (directory, environment, config_file) = config_fixture("runtime-invalid");
        fs::write(
            &config_file,
            "[meshtastic]\ntransport = \"tcp\"\nmesh_network_id = \"mesh\"\ngateway_id = \"gateway\"\nserial_path = \"/dev/ttyUSB0\"\n",
        )
        .expect("invalid configuration should be written");
        assert_eq!(
            AgentConfig::from_environment(&environment),
            Err(ConfigError::InvalidMeshtastic)
        );
        fs::write(
            &config_file,
            "[proxy]\nupstream_host = \"192.0.2.10\"\nupstream_port = 4403\nallowlist = [\"192.0.2.20\"]\n",
        )
        .expect("invalid configuration should be written");
        assert_eq!(
            AgentConfig::from_environment(&environment),
            Err(ConfigError::InvalidProxy)
        );
        fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[test]
    fn accepts_only_a_complete_non_loopback_management_lan_configuration() {
        let (directory, environment, config_file) = config_fixture("management-lan");
        let certificate_path = directory.join("certificate.pem");
        let private_key_path = directory.join("private-key.pem");
        fs::write(
            &config_file,
            format!(
                r#"[management_lan]
bind = "127.0.0.1"
port = 7443
password_hash = "$argon2id$v=19$m=19456,t=2,p=1$Y21jbGllbnQtYWNjZXNzLWZpeHR1cmU$mlUCFMgY1I8EWPxA0OXMpw"
allowed_origins = ["https://cmclient.example"]
session_ttl_seconds = 3600
audit_capacity = 32
certificate_path = '{}'
private_key_path = '{}'
"#,
                certificate_path.display(),
                private_key_path.display(),
            ),
        )
        .expect("configuration should be written");
        assert!(AgentConfig::from_environment(&environment).is_err());
        fs::write(
            &config_file,
            format!(
                r#"[management_lan]
bind = "192.168.1.10"
port = 7443
password_hash = "$argon2id$v=19$m=19456,t=2,p=1$Y21jbGllbnQtYWNjZXNzLWZpeHR1cmU$dpMi7KyBMZbZy6JnUqumeIrRr43snfWb1zJ6H5D2myg"
allowed_origins = ["https://cmclient.example"]
session_ttl_seconds = 3600
audit_capacity = 32
certificate_path = '{}'
private_key_path = '{}'
"#,
                certificate_path.display(),
                private_key_path.display(),
            ),
        )
        .expect("configuration should be written");
        let config =
            AgentConfig::from_environment(&environment).expect("LAN configuration should load");
        assert_eq!(
            config
                .management_lan
                .expect("LAN configuration should exist")
                .port,
            7443
        );
        fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[test]
    fn writes_and_clears_diagnostic_state_with_the_lease() {
        let directory =
            std::env::temp_dir().join(format!("cmclient-agent-lease-{}", std::process::id()));
        let _ = fs::remove_dir_all(&directory);
        let paths = RuntimePaths {
            data_dir: directory.clone(),
            config_dir: directory.join("config"),
            cache_dir: directory.join("cache"),
            log_dir: directory.join("logs"),
        };
        let (lease, state) = AgentLease::acquire(&paths).expect("lease should be acquired");
        assert_eq!(
            AgentLease::read_state(&paths).expect("state should load"),
            Some(state)
        );
        assert!(matches!(
            AgentLease::acquire(&paths),
            Err(InstanceError::AlreadyRunning)
        ));
        drop(lease);
        assert_eq!(
            AgentLease::read_state(&paths).expect("state should load"),
            None
        );
        let (replacement, _) =
            AgentLease::acquire(&paths).expect("dropped lease should release process guard");
        drop(replacement);
        fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[test]
    fn releases_the_process_guard_when_lease_initialization_fails() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-lease-failure-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).expect("temporary directory should exist");
        let paths = RuntimePaths {
            data_dir: directory.clone(),
            config_dir: directory.join("config"),
            cache_dir: directory.join("cache"),
            log_dir: directory.join("logs"),
        };
        let blocked_state = directory.join("state").join("agent.json");
        fs::create_dir_all(directory.join("state")).expect("state directory should exist");
        fs::create_dir(&blocked_state).expect("blocked state fixture should exist");

        assert!(matches!(
            AgentLease::acquire(&paths),
            Err(InstanceError::Io)
        ));

        fs::remove_dir(&blocked_state).expect("blocked state fixture should remove");
        let (lease, _) = AgentLease::acquire(&paths)
            .expect("failed initialization must release process and file locks");
        drop(lease);
        fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[test]
    fn malformed_or_invalid_diagnostic_state_fails_with_a_stable_error() {
        let directory = std::env::temp_dir().join(format!(
            "cmclient-agent-state-invalid-{}",
            std::process::id()
        ));
        let _ = fs::remove_dir_all(&directory);
        fs::create_dir_all(&directory).expect("state fixture should exist");
        let paths = RuntimePaths {
            data_dir: directory.clone(),
            config_dir: directory.join("config"),
            cache_dir: directory.join("cache"),
            log_dir: directory.join("logs"),
        };

        fs::create_dir_all(directory.join("state")).expect("state directory should exist");
        fs::write(directory.join("state/agent.json"), b"{").expect("malformed state should write");
        let error = AgentLease::read_state(&paths).expect_err("malformed state must fail closed");
        assert!(matches!(error, InstanceError::StateInvalid));
        assert_eq!(error.code(), "AGENT_INSTANCE_STATE_INVALID");

        fs::write(
            directory.join("state/agent.json"),
            br#"{"schemaVersion":2,"pid":1,"startedAtUnixSeconds":1}"#,
        )
        .expect("invalid schema state should write");
        assert!(matches!(
            AgentLease::read_state(&paths),
            Err(InstanceError::StateInvalid)
        ));
        fs::remove_dir_all(directory).expect("state fixture should remove");
    }
}
