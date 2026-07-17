use cmclient_agent_core::web::{ManagementWebConfig, ManagementWebListener};
use cmclient_agent_core::{AgentConfig, AgentLease, ensure_runtime_directories};
use cmclient_control_api::{
    ControlCommand, ControlEndpoint, ControlError, ControlHandler, ControlServer, ControlStatus,
    GatewayControlStatus, default_unix_socket,
};
use cmclient_supervisor::{BackoffPolicy, GatewayCommand, GatewayStatus, GatewaySupervisor};
use std::process::ExitCode;
use std::sync::{Arc, Mutex};

const EX_USAGE: u8 = 2;
const EX_CONFIG: u8 = 5;

struct AgentController {
    supervisor: Mutex<Option<GatewaySupervisor>>,
}

impl AgentController {
    fn from_config(config: &AgentConfig) -> Result<Self, ControlError> {
        let supervisor = config
            .gateway_command
            .as_ref()
            .map(|command| {
                GatewaySupervisor::new(
                    GatewayCommand {
                        program: command.first().cloned().unwrap_or_default(),
                        arguments: command.iter().skip(1).cloned().collect(),
                    },
                    BackoffPolicy::default(),
                )
                .map_err(|_| ControlError::CommandFailed)
            })
            .transpose()?;
        Ok(Self {
            supervisor: Mutex::new(supervisor),
        })
    }

    fn status(&self) -> Result<ControlStatus, ControlError> {
        let mut supervisor = self
            .supervisor
            .lock()
            .map_err(|_| ControlError::CommandFailed)?;
        let gateway = match supervisor.as_mut() {
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
