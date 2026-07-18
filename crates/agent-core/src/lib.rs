//! Shared Rust foundations for the CMClient Agent.

pub mod access;
pub mod secrets;
pub mod web;

use crate::access::{LanAccessConfig, ManagementAccessController};
use fs2::FileExt;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet},
    env,
    fmt::{Display, Formatter},
    fs,
    fs::OpenOptions,
    net::IpAddr,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

/// Stable workspace identity for the Agent core boundary.
pub const COMPONENT: &str = "agent-core";

const APP_NAME: &str = "CMClient";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimePaths {
    pub data_dir: PathBuf,
    pub config_dir: PathBuf,
    pub cache_dir: PathBuf,
    pub log_dir: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AgentConfig {
    pub paths: RuntimePaths,
    pub config_file: PathBuf,
    pub gateway_command: Option<Vec<String>>,
    pub gateway_port: u16,
    pub callmesh: Option<CallMeshConfig>,
    pub management_web_enabled: bool,
    pub management_lan: Option<ManagementLanConfig>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CallMeshConfig {
    pub url: String,
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
pub struct AgentState {
    pub schema_version: u8,
    pub pid: u32,
    pub started_at_unix_seconds: u64,
}

pub struct AgentLease {
    lock_file: fs::File,
    state_file: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    HomeDirectoryUnavailable,
    RelativePathOverride { name: &'static str },
    ReadConfig { path: PathBuf },
    InvalidConfig,
    EmptyGatewayCommand,
    InvalidGatewayPort,
    InvalidCallMesh,
    InvalidManagementLan,
}

#[derive(Debug)]
pub enum InstanceError {
    AlreadyRunning,
    Io,
}

impl InstanceError {
    pub const fn code(&self) -> &'static str {
        match self {
            Self::AlreadyRunning => "AGENT_INSTANCE_ALREADY_RUNNING",
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
            Self::ReadConfig { .. } => "AGENT_CONFIG_READ_FAILED",
            Self::InvalidConfig => "AGENT_CONFIG_INVALID",
            Self::EmptyGatewayCommand => "AGENT_CONFIG_GATEWAY_COMMAND_EMPTY",
            Self::InvalidGatewayPort => "AGENT_CONFIG_GATEWAY_PORT_INVALID",
            Self::InvalidCallMesh => "AGENT_CONFIG_CALLMESH_INVALID",
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
    management_lan: Option<ManagementLanSection>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct CallMeshSection {
    url: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct AgentSection {
    gateway_command: Option<Vec<String>>,
    gateway_port: Option<u16>,
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
        let config_file = path_override(
            environment,
            "CMCLIENT_AGENT_CONFIG",
            paths.config_dir.join("agent.toml"),
        )?;

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
        let gateway_port = agent.gateway_port.unwrap_or(4810);
        if gateway_port == 0 {
            return Err(ConfigError::InvalidGatewayPort);
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
                if !is_https_url(url) {
                    return Err(ConfigError::InvalidCallMesh);
                }
                Ok(CallMeshConfig {
                    url: url.to_owned(),
                })
            })
            .transpose()?;

        Ok(Self {
            paths,
            config_file,
            gateway_command: agent.gateway_command,
            gateway_port,
            callmesh,
            management_web_enabled: agent.management_web_enabled.unwrap_or(true),
            management_lan,
        })
    }
}

fn is_https_url(value: &str) -> bool {
    let Some(rest) = value.strip_prefix("https://") else {
        return false;
    };
    let authority = rest.split(['/', '?', '#']).next().unwrap_or_default();
    !authority.is_empty() && !authority.contains('@') && !authority.contains(char::is_whitespace)
}

impl RuntimePaths {
    pub fn from_environment(environment: &BTreeMap<String, String>) -> Result<Self, ConfigError> {
        let home = home_directory(environment)?;
        let (data_dir, config_dir, cache_dir) = if cfg!(target_os = "macos") {
            let library = home.join("Library");
            (
                library.join("Application Support").join(APP_NAME),
                library.join("Application Support").join(APP_NAME),
                library.join("Caches").join(APP_NAME),
            )
        } else if cfg!(target_os = "windows") {
            let app_data = environment
                .get("APPDATA")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join("AppData").join("Roaming"));
            let local_app_data = environment
                .get("LOCALAPPDATA")
                .map(PathBuf::from)
                .unwrap_or_else(|| home.join("AppData").join("Local"));
            (
                app_data.join(APP_NAME),
                app_data.join(APP_NAME),
                local_app_data.join(APP_NAME).join("cache"),
            )
        } else {
            (
                xdg_path(environment, "XDG_DATA_HOME", home.join(".local/share")).join("cmclient"),
                xdg_path(environment, "XDG_CONFIG_HOME", home.join(".config")).join("cmclient"),
                xdg_path(environment, "XDG_CACHE_HOME", home.join(".cache")).join("cmclient"),
            )
        };

        let data_dir = path_override(environment, "CMCLIENT_DATA_DIR", data_dir)?;
        let config_dir = path_override(environment, "CMCLIENT_CONFIG_DIR", config_dir)?;
        let cache_dir = path_override(environment, "CMCLIENT_CACHE_DIR", cache_dir)?;
        let log_dir = path_override(environment, "CMCLIENT_LOG_DIR", data_dir.join("logs"))?;

        Ok(Self {
            data_dir,
            config_dir,
            cache_dir,
            log_dir,
        })
    }
}

fn home_directory(environment: &BTreeMap<String, String>) -> Result<PathBuf, ConfigError> {
    environment
        .get("HOME")
        .or_else(|| environment.get("USERPROFILE"))
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .ok_or(ConfigError::HomeDirectoryUnavailable)
}

fn xdg_path(environment: &BTreeMap<String, String>, name: &str, fallback: PathBuf) -> PathBuf {
    environment
        .get(name)
        .map(PathBuf::from)
        .filter(|path| path.is_absolute())
        .unwrap_or(fallback)
}

fn path_override(
    environment: &BTreeMap<String, String>,
    name: &'static str,
    fallback: PathBuf,
) -> Result<PathBuf, ConfigError> {
    match environment.get(name) {
        Some(value) if !value.trim().is_empty() => {
            let path = PathBuf::from(value);
            if path.is_absolute() {
                Ok(path)
            } else {
                Err(ConfigError::RelativePathOverride { name })
            }
        }
        _ => Ok(fallback),
    }
}

pub fn ensure_runtime_directories(paths: &RuntimePaths) -> Result<(), ConfigError> {
    for path in [
        &paths.data_dir,
        &paths.config_dir,
        &paths.cache_dir,
        &paths.log_dir,
    ] {
        fs::create_dir_all(path).map_err(|_| ConfigError::ReadConfig { path: path.clone() })?;
    }
    Ok(())
}

impl AgentLease {
    pub fn acquire(paths: &RuntimePaths) -> Result<(Self, AgentState), InstanceError> {
        fs::create_dir_all(&paths.data_dir).map_err(|_| InstanceError::Io)?;
        let lock_path = paths.data_dir.join("agent.lock");
        let lock_file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(false)
            .open(lock_path)
            .map_err(|_| InstanceError::Io)?;
        match lock_file.try_lock_exclusive() {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                return Err(InstanceError::AlreadyRunning);
            }
            Err(_) => return Err(InstanceError::Io),
        }

        let state = AgentState {
            schema_version: 1,
            pid: std::process::id(),
            started_at_unix_seconds: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_err(|_| InstanceError::Io)?
                .as_secs(),
        };
        let state_file = paths.data_dir.join("agent-state.json");
        write_state(&state_file, &state)?;
        Ok((
            Self {
                lock_file,
                state_file,
            },
            state,
        ))
    }

    pub fn read_state(paths: &RuntimePaths) -> Result<Option<AgentState>, InstanceError> {
        let state_file = paths.data_dir.join("agent-state.json");
        if !state_file.exists() {
            return Ok(None);
        }
        let contents = fs::read_to_string(state_file).map_err(|_| InstanceError::Io)?;
        serde_json::from_str(&contents)
            .map(Some)
            .map_err(|_| InstanceError::Io)
    }
}

impl Drop for AgentLease {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.state_file);
        let _ = FileExt::unlock(&self.lock_file);
    }
}

fn write_state(path: &Path, state: &AgentState) -> Result<(), InstanceError> {
    let temporary_path = path.with_extension(format!("{}.tmp", std::process::id()));
    let serialized = serde_json::to_vec(state).map_err(|_| InstanceError::Io)?;
    fs::write(&temporary_path, serialized).map_err(|_| InstanceError::Io)?;
    fs::rename(temporary_path, path).map_err(|_| InstanceError::Io)
}

pub fn is_config_file(path: &Path) -> bool {
    path.file_name().is_some_and(|name| name == "agent.toml")
}

#[cfg(test)]
mod tests {
    use super::{
        AgentConfig, AgentLease, ConfigError, InstanceError, RuntimePaths, is_config_file,
    };
    use std::{collections::BTreeMap, fs, path::PathBuf};

    fn environment() -> BTreeMap<String, String> {
        BTreeMap::from([(String::from("HOME"), String::from("/fixture/home"))])
    }

    #[test]
    fn uses_standard_platform_paths() {
        let paths = RuntimePaths::from_environment(&environment()).expect("paths should load");
        if cfg!(target_os = "macos") {
            assert_eq!(
                paths.data_dir,
                PathBuf::from("/fixture/home/Library/Application Support/CMClient")
            );
            assert_eq!(
                paths.config_dir,
                PathBuf::from("/fixture/home/Library/Application Support/CMClient")
            );
        } else if cfg!(target_os = "windows") {
            assert_eq!(
                paths.data_dir,
                PathBuf::from("/fixture/home/AppData/Roaming/CMClient")
            );
        } else {
            assert_eq!(
                paths.data_dir,
                PathBuf::from("/fixture/home/.local/share/cmclient")
            );
            assert_eq!(
                paths.config_dir,
                PathBuf::from("/fixture/home/.config/cmclient")
            );
        }
        assert_eq!(paths.log_dir, paths.data_dir.join("logs"));
    }

    #[test]
    fn rejects_relative_runtime_path_overrides() {
        let mut environment = environment();
        environment.insert(String::from("CMCLIENT_DATA_DIR"), String::from("relative"));
        assert_eq!(
            RuntimePaths::from_environment(&environment),
            Err(ConfigError::RelativePathOverride {
                name: "CMCLIENT_DATA_DIR"
            })
        );
    }

    #[test]
    fn loads_a_strict_agent_file() {
        let directory =
            std::env::temp_dir().join(format!("cmclient-agent-core-{}", std::process::id()));
        fs::create_dir_all(&directory).expect("temporary directory should exist");
        let config_file = directory.join("agent.toml");
        fs::write(
            &config_file,
            "[agent]\ngateway_command = [\"gateway\", \"serve\"]\ngateway_port = 4811\nmanagement_web_enabled = false\n",
        )
        .expect("configuration should be written");
        let mut environment = environment();
        environment.insert(
            String::from("CMCLIENT_AGENT_CONFIG"),
            config_file.display().to_string(),
        );

        let config =
            AgentConfig::from_environment(&environment).expect("configuration should load");
        assert_eq!(
            config.gateway_command,
            Some(vec![String::from("gateway"), String::from("serve")])
        );
        assert_eq!(config.gateway_port, 4811);
        assert!(!config.management_web_enabled);
        assert!(is_config_file(&config.config_file));
        fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[test]
    fn rejects_an_ephemeral_gateway_port() {
        let directory =
            std::env::temp_dir().join(format!("cmclient-agent-port-{}", std::process::id()));
        fs::create_dir_all(&directory).expect("temporary directory should exist");
        let config_file = directory.join("agent.toml");
        fs::write(&config_file, "[agent]\ngateway_port = 0\n")
            .expect("configuration should be written");
        let mut environment = environment();
        environment.insert(
            String::from("CMCLIENT_AGENT_CONFIG"),
            config_file.display().to_string(),
        );

        assert_eq!(
            AgentConfig::from_environment(&environment),
            Err(ConfigError::InvalidGatewayPort)
        );
        fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }

    #[test]
    fn accepts_only_an_https_callmesh_url_without_a_secret_in_configuration() {
        let directory =
            std::env::temp_dir().join(format!("cmclient-agent-callmesh-{}", std::process::id()));
        fs::create_dir_all(&directory).expect("temporary directory should exist");
        let config_file = directory.join("agent.toml");
        let mut environment = environment();
        environment.insert(
            String::from("CMCLIENT_AGENT_CONFIG"),
            config_file.display().to_string(),
        );
        fs::write(
            &config_file,
            "[callmesh]\nurl = \"https://api.example.invalid/v1\"\n",
        )
        .expect("configuration should be written");

        let config = AgentConfig::from_environment(&environment)
            .expect("HTTPS CallMesh configuration should load");
        assert_eq!(
            config
                .callmesh
                .expect("CallMesh configuration should exist")
                .url,
            "https://api.example.invalid/v1"
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
    fn accepts_only_a_complete_non_loopback_management_lan_configuration() {
        let directory =
            std::env::temp_dir().join(format!("cmclient-agent-lan-{}", std::process::id()));
        fs::create_dir_all(&directory).expect("temporary directory should exist");
        let config_file = directory.join("agent.toml");
        fs::write(
            &config_file,
            r#"[management_lan]
bind = "127.0.0.1"
port = 7443
password_hash = "$argon2id$v=19$m=19456,t=2,p=1$Y21jbGllbnQtYWNjZXNzLWZpeHR1cmU$mlUCFMgY1I8EWPxA0OXMpw"
allowed_origins = ["https://cmclient.example"]
session_ttl_seconds = 3600
audit_capacity = 32
certificate_path = "/fixture/certificate.pem"
private_key_path = "/fixture/private-key.pem"
"#,
        )
        .expect("configuration should be written");
        let mut environment = environment();
        environment.insert(
            String::from("CMCLIENT_AGENT_CONFIG"),
            config_file.display().to_string(),
        );

        assert!(AgentConfig::from_environment(&environment).is_err());
        fs::write(
            &config_file,
            r#"[management_lan]
bind = "192.168.1.10"
port = 7443
password_hash = "$argon2id$v=19$m=19456,t=2,p=1$Y21jbGllbnQtYWNjZXNzLWZpeHR1cmU$dpMi7KyBMZbZy6JnUqumeIrRr43snfWb1zJ6H5D2myg"
allowed_origins = ["https://cmclient.example"]
session_ttl_seconds = 3600
audit_capacity = 32
certificate_path = "/fixture/certificate.pem"
private_key_path = "/fixture/private-key.pem"
"#,
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
        fs::remove_dir_all(directory).expect("temporary directory should be removed");
    }
}
