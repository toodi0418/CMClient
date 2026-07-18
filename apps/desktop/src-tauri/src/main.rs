use cmclient_agent_core::AgentConfig;
use cmclient_control_api::{ControlClient, ControlCommand, ControlStatus, default_unix_socket};

#[tauri::command]
fn agent_status() -> Result<ControlStatus, String> {
    control(ControlCommand::Status)
}

#[tauri::command]
fn agent_command(command: String) -> Result<ControlStatus, String> {
    control(parse_command(&command)?)
}

fn parse_command(command: &str) -> Result<ControlCommand, String> {
    match command {
        "start" => Ok(ControlCommand::Start),
        "stop" => Ok(ControlCommand::Stop),
        "restart" => Ok(ControlCommand::Restart),
        _ => Err(String::from("DESKTOP_AGENT_COMMAND_INVALID")),
    }
}

fn control(command: ControlCommand) -> Result<ControlStatus, String> {
    let config =
        AgentConfig::load().map_err(|_| String::from("DESKTOP_AGENT_CONFIG_UNAVAILABLE"))?;
    let client = ControlClient::new(default_unix_socket(&config.paths.data_dir))
        .map_err(|error| error.code().to_owned())?;
    match command {
        ControlCommand::Status => client.status(),
        ControlCommand::Start => client.start(),
        ControlCommand::Stop => client.stop(),
        ControlCommand::Restart => client.restart(),
    }
    .map_err(|error| error.code().to_owned())
}

fn main() {
    tauri::Builder::default()
        .invoke_handler(tauri::generate_handler![agent_status, agent_command])
        .run(tauri::generate_context!())
        .expect("TAURI_RUNTIME_FAILED");
}

#[cfg(test)]
mod tests {
    use super::parse_command;
    use cmclient_control_api::ControlCommand;

    #[test]
    fn maps_only_desktop_control_commands() {
        assert_eq!(parse_command("start"), Ok(ControlCommand::Start));
        assert_eq!(parse_command("stop"), Ok(ControlCommand::Stop));
        assert_eq!(parse_command("restart"), Ok(ControlCommand::Restart));
        assert_eq!(
            parse_command("delete"),
            Err(String::from("DESKTOP_AGENT_COMMAND_INVALID"))
        );
    }
}
