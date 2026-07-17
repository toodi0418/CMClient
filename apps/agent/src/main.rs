use cmclient_agent_core::web::{ManagementWebConfig, ManagementWebListener, gateway_health};
use cmclient_agent_core::{AgentConfig, AgentLease, ensure_runtime_directories};
use cmclient_control_api::{
    ControlCommand, ControlEndpoint, ControlError, ControlHandler, ControlServer, ControlStatus,
    GatewayControlStatus, default_unix_socket,
};
use cmclient_supervisor::{BackoffPolicy, GatewayCommand, GatewayStatus, GatewaySupervisor};
use std::sync::{Arc, Mutex};
use std::{collections::BTreeMap, net::SocketAddr, process::ExitCode, thread};

const EX_USAGE: u8 = 2;
const EX_CONFIG: u8 = 5;

struct AgentController {
    supervisor: Mutex<Option<GatewaySupervisor>>,
    gateway: SocketAddr,
}

impl AgentController {
    fn from_config(config: &AgentConfig) -> Result<Self, ControlError> {
        let supervisor = config
            .gateway_command
            .as_ref()
            .map(|command| {
                let mut supervisor = GatewaySupervisor::new(
                    GatewayCommand {
                        program: command.first().cloned().unwrap_or_default(),
                        arguments: command.iter().skip(1).cloned().collect(),
                    },
                    BackoffPolicy::default(),
                )
                .map_err(|_| ControlError::CommandFailed)?;
                supervisor.set_environment(BTreeMap::from([
                    (
                        String::from("CMCLIENT_GATEWAY_HOST"),
                        String::from("127.0.0.1"),
                    ),
                    (
                        String::from("CMCLIENT_GATEWAY_PORT"),
                        config.gateway_port.to_string(),
                    ),
                    (
                        String::from("CMCLIENT_DATA_DIR"),
                        config.paths.data_dir.to_string_lossy().into_owned(),
                    ),
                ]));
                Ok(supervisor)
            })
            .transpose()?;
        Ok(Self {
            supervisor: Mutex::new(supervisor),
            gateway: gateway_address(config.gateway_port),
        })
    }

    fn status(&self) -> Result<ControlStatus, ControlError> {
        let mut supervisor = self
            .supervisor
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        let lifecycle = match supervisor.as_mut() {
            Some(supervisor) => {
                let _ = supervisor
                    .poll_heartbeat()
                    .map_err(|_| ControlError::CommandFailed)?;
                match supervisor.status() {
                    GatewayStatus::Stopped => GatewayControlStatus::Stopped,
                    GatewayStatus::Running { .. } => GatewayControlStatus::Running,
                    GatewayStatus::Backoff { .. } => GatewayControlStatus::Backoff,
                }
            }
            None => GatewayControlStatus::Stopped,
        };
        drop(supervisor);
        let gateway = match lifecycle {
            GatewayControlStatus::Running if gateway_health(self.gateway) => {
                GatewayControlStatus::Running
            }
            GatewayControlStatus::Running => GatewayControlStatus::Degraded,
            status => status,
        };
        Ok(ControlStatus {
            schema_version: 1,
            agent: String::from("running"),
            gateway,
        })
    }
}

impl ControlHandler for AgentController {
    fn handle(&self, command: ControlCommand) -> Result<ControlStatus, ControlError> {
        match command {
            ControlCommand::Status => self.status(),
            ControlCommand::Start => {
                let mut supervisor = self
                    .supervisor
                    .lock()
                    .map_err(|_| ControlError::CommandFailed)?;
                supervisor
                    .as_mut()
                    .ok_or(ControlError::CommandFailed)?
                    .start()
                    .map_err(|_| ControlError::CommandFailed)?;
                drop(supervisor);
                self.status()
            }
            ControlCommand::Stop => {
                let mut supervisor = self
                    .supervisor
                    .lock()
                    .map_err(|_| ControlError::CommandFailed)?;
                supervisor
                    .as_mut()
                    .ok_or(ControlError::CommandFailed)?
                    .stop()
                    .map_err(|_| ControlError::CommandFailed)?;
                drop(supervisor);
                self.status()
            }
            ControlCommand::Restart => {
                {
                    let mut supervisor = self
                        .supervisor
                        .lock()
                        .map_err(|_| ControlError::CommandFailed)?;
                    let supervisor = supervisor.as_mut().ok_or(ControlError::CommandFailed)?;
                    supervisor.stop().map_err(|_| ControlError::CommandFailed)?;
                    supervisor
                        .start()
                        .map_err(|_| ControlError::CommandFailed)?;
                }
                self.status()
            }
        }
    }
}

fn main() -> ExitCode {
    let mut arguments = std::env::args().skip(1);
    match arguments.next().as_deref() {
        Some("--serve") => serve(),
        Some("--serve-web-once") => serve_web_once(),
        None | Some("--check-config") | Some("--check-instance") => match AgentConfig::load() {
            Ok(config) => {
                if let Err(error) = ensure_runtime_directories(&config.paths) {
                    eprintln!("{}", error.code());
                    return ExitCode::from(EX_CONFIG);
                }
                if std::env::args()
                    .skip(1)
                    .any(|argument| argument == "--check-instance")
                {
                    match AgentLease::acquire(&config.paths) {
                        Ok((_lease, _state)) => println!("agent instance lock valid"),
                        Err(error) => {
                            eprintln!("{}", error.code());
                            return ExitCode::from(EX_CONFIG);
                        }
                    }
                } else {
                    println!("agent configuration valid");
                }
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("{}", error.code());
                ExitCode::from(EX_CONFIG)
            }
        },
        Some("--version") => {
            println!("cmclient-agent {}", env!("CARGO_PKG_VERSION"));
            ExitCode::SUCCESS
        }
        Some(_) => {
            eprintln!("AGENT_USAGE_INVALID_ARGUMENT");
            ExitCode::from(EX_USAGE)
        }
    }
}

fn serve_web_once() -> ExitCode {
    let config = match AgentConfig::load() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    let web_config = ManagementWebConfig {
        enabled: config.management_web_enabled,
        gateway: gateway_address(config.gateway_port),
        ..Default::default()
    };
    let listener = match ManagementWebListener::bind(&web_config) {
        Ok(listener) => listener,
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    listener.serve_once().map_or_else(
        |error| {
            eprintln!("{}", error.code());
            ExitCode::from(EX_CONFIG)
        },
        |_| ExitCode::SUCCESS,
    )
}

fn serve() -> ExitCode {
    let config = match AgentConfig::load() {
        Ok(config) => config,
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    if let Err(error) = ensure_runtime_directories(&config.paths) {
        eprintln!("{}", error.code());
        return ExitCode::from(EX_CONFIG);
    }
    let _lease = match AgentLease::acquire(&config.paths) {
        Ok((lease, _)) => lease,
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    let controller = match AgentController::from_config(&config) {
        Ok(controller) => Arc::new(controller),
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    if config.management_web_enabled {
        let web_config = ManagementWebConfig {
            enabled: true,
            gateway: gateway_address(config.gateway_port),
            ..Default::default()
        };
        let listener = match ManagementWebListener::bind(&web_config) {
            Ok(listener) => listener,
            Err(error) => {
                eprintln!("{}", error.code());
                return ExitCode::from(EX_CONFIG);
            }
        };
        if thread::Builder::new()
            .name(String::from("cmclient-management-web"))
            .spawn(move || {
                if let Err(error) = listener.serve() {
                    eprintln!("{}", error.code());
                }
            })
            .is_err()
        {
            eprintln!("MANAGEMENT_WEB_THREAD_START_FAILED");
            return ExitCode::from(EX_CONFIG);
        }
    }
    let endpoint = match default_unix_socket(&config.paths.data_dir) {
        ControlEndpoint::UnixSocket(path) => ControlEndpoint::unix(path),
        endpoint => endpoint,
    };
    let server = match ControlServer::bind(endpoint, controller) {
        Ok(server) => server,
        Err(error) => {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    };
    loop {
        if let Err(error) = server.serve_once() {
            eprintln!("{}", error.code());
            return ExitCode::from(EX_CONFIG);
        }
    }
}

fn gateway_address(port: u16) -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], port))
}

#[cfg(test)]
mod tests {
    #[cfg(not(target_os = "windows"))]
    use super::{
        AgentConfig, AgentController, ControlCommand, ControlHandler, GatewayControlStatus,
    };
    #[cfg(not(target_os = "windows"))]
    use cmclient_agent_core::RuntimePaths;
    #[cfg(not(target_os = "windows"))]
    use std::{
        io::{Read, Write},
        net::TcpListener,
        path::PathBuf,
        thread,
    };

    #[cfg(not(target_os = "windows"))]
    #[test]
    fn reports_running_only_after_gateway_health_succeeds() {
        let gateway = TcpListener::bind("127.0.0.1:0").expect("gateway should bind");
        let port = gateway
            .local_addr()
            .expect("gateway address should load")
            .port();
        let gateway_thread = thread::spawn(move || {
            let (mut stream, _) = gateway.accept().expect("gateway should accept");
            let mut request = [0_u8; 4096];
            let _ = stream
                .read(&mut request)
                .expect("health request should read");
            stream
                .write_all(
                    b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 15\r\nconnection: close\r\n\r\n{\"status\":\"ok\"}",
                )
                .expect("health response should write");
        });
        let config = AgentConfig {
            paths: RuntimePaths {
                data_dir: PathBuf::from("/tmp/cmclient-agent-health"),
                config_dir: PathBuf::from("/tmp/cmclient-agent-health"),
                cache_dir: PathBuf::from("/tmp/cmclient-agent-health/cache"),
                log_dir: PathBuf::from("/tmp/cmclient-agent-health/logs"),
            },
            config_file: PathBuf::from("/tmp/cmclient-agent-health/agent.toml"),
            gateway_command: Some(vec![
                String::from("sh"),
                String::from("-c"),
                String::from("sleep 30"),
            ]),
            gateway_port: port,
            management_web_enabled: false,
        };
        let controller = AgentController::from_config(&config).expect("controller should build");

        let status = controller
            .handle(ControlCommand::Start)
            .expect("gateway should start");
        assert_eq!(status.gateway, GatewayControlStatus::Running);
        controller
            .handle(ControlCommand::Stop)
            .expect("gateway should stop");
        gateway_thread.join().expect("gateway should join");
    }
}
