use cmclient_agent_core::{AgentConfig, ensure_runtime_directories};
use std::process::ExitCode;

const EX_USAGE: u8 = 2;
const EX_CONFIG: u8 = 5;

fn main() -> ExitCode {
    let mut arguments = std::env::args().skip(1);
    match arguments.next().as_deref() {
        None | Some("--check-config") => match AgentConfig::load() {
            Ok(config) => {
                if let Err(error) = ensure_runtime_directories(&config.paths) {
                    eprintln!("{}", error.code());
                    return ExitCode::from(EX_CONFIG);
                }
                println!("agent configuration valid");
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
