use cmclient_agent_core::AgentConfig;
use cmclient_control_api::{
    ControlClient, ControlCommand, ControlStatus, UpdateControlStatus, default_local_endpoint,
};
use std::{thread, time::Duration};
use tauri::{
    AppHandle, Emitter, Manager, Runtime, WindowEvent,
    menu::{MenuBuilder, MenuItemBuilder},
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
};
use tauri_plugin_opener::OpenerExt;

mod service_status;

use service_status::DesktopServiceStatus;

const MAIN_WINDOW_LABEL: &str = "main";
const TRAY_OPEN_ID: &str = "open";
const TRAY_EXIT_ID: &str = "exit";
const UPDATE_STATUS_EVENT: &str = "agent-update-status";

#[tauri::command]
fn agent_status() -> Result<ControlStatus, String> {
    control(ControlCommand::Status)
}

#[tauri::command]
fn agent_command(command: String) -> Result<ControlStatus, String> {
    control(parse_command(&command)?)
}

#[tauri::command]
fn agent_update_status() -> Result<UpdateControlStatus, String> {
    control_client()?
        .update_status()
        .map_err(|error| error.code().to_owned())
}

#[tauri::command]
fn agent_service_status() -> Result<DesktopServiceStatus, String> {
    let config =
        AgentConfig::load().map_err(|_| String::from("DESKTOP_AGENT_CONFIG_UNAVAILABLE"))?;
    Ok(service_status::load(default_local_endpoint(
        &config.paths.run_dir(),
    )))
}

#[tauri::command]
fn open_management_web(app: AppHandle) -> Result<(), String> {
    let status = control(ControlCommand::Status)?;
    let url = status
        .management_web_url
        .ok_or_else(|| String::from("DESKTOP_MANAGEMENT_WEB_DISABLED"))?;
    app.opener()
        .open_url(url, None::<&str>)
        .map_err(|_| String::from("DESKTOP_MANAGEMENT_WEB_OPEN_FAILED"))
}

#[tauri::command]
fn exit_desktop(app: AppHandle) {
    app.exit(0);
}

fn parse_command(command: &str) -> Result<ControlCommand, String> {
    match command {
        "start" => Ok(ControlCommand::Start),
        "stop" => Ok(ControlCommand::Stop),
        "restart" => Ok(ControlCommand::Restart),
        "enable_web" => Ok(ControlCommand::EnableManagementWeb),
        "disable_web" => Ok(ControlCommand::DisableManagementWeb),
        _ => Err(String::from("DESKTOP_AGENT_COMMAND_INVALID")),
    }
}

fn control(command: ControlCommand) -> Result<ControlStatus, String> {
    let client = control_client()?;
    match command {
        ControlCommand::Status => client.status(),
        ControlCommand::Start => client.start(),
        ControlCommand::Stop => client.stop(),
        ControlCommand::Restart => client.restart(),
        ControlCommand::EnableManagementWeb => client.enable_management_web(),
        ControlCommand::DisableManagementWeb => client.disable_management_web(),
        ControlCommand::ShutdownAgent => {
            return Err(String::from("DESKTOP_AGENT_COMMAND_INVALID"));
        }
    }
    .map_err(|error| error.code().to_owned())
}

fn control_client() -> Result<ControlClient, String> {
    let config =
        AgentConfig::load().map_err(|_| String::from("DESKTOP_AGENT_CONFIG_UNAVAILABLE"))?;
    ControlClient::new(default_local_endpoint(&config.paths.run_dir()))
        .map_err(|error| error.code().to_owned())
}

fn start_update_event_forwarder(app: AppHandle) {
    let _ = thread::Builder::new()
        .name(String::from("cmclient-desktop-update-events"))
        .spawn(move || {
            loop {
                let Ok(client) = control_client() else {
                    thread::sleep(Duration::from_millis(250));
                    continue;
                };
                let Ok(mut events) = client.subscribe_update_events() else {
                    thread::sleep(Duration::from_millis(250));
                    continue;
                };
                while let Ok(Some(event)) = events.next_event() {
                    if let Ok(status) = String::from_utf8(event.data) {
                        let _ = app.emit(UPDATE_STATUS_EVENT, status);
                    }
                }
                thread::sleep(Duration::from_millis(250));
            }
        });
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrayMenuAction {
    Open,
    Exit,
}

fn tray_menu_action(id: &str) -> Option<TrayMenuAction> {
    match id {
        TRAY_OPEN_ID => Some(TrayMenuAction::Open),
        TRAY_EXIT_ID => Some(TrayMenuAction::Exit),
        _ => None,
    }
}

fn show_main_window<R: Runtime>(app: &AppHandle<R>) -> tauri::Result<()> {
    if let Some(window) = app.get_webview_window(MAIN_WINDOW_LABEL) {
        window.show()?;
        window.set_focus()?;
    }
    Ok(())
}

fn setup_tray<R: Runtime>(app: &AppHandle<R>) -> tauri::Result<()> {
    let open = MenuItemBuilder::with_id(TRAY_OPEN_ID, "Open CMClient").build(app)?;
    let exit = MenuItemBuilder::with_id(TRAY_EXIT_ID, "Exit CMClient").build(app)?;
    let menu = MenuBuilder::new(app).items(&[&open, &exit]).build()?;
    let builder = TrayIconBuilder::with_id("cmclient-tray")
        .menu(&menu)
        .tooltip("CMClient")
        .show_menu_on_left_click(false)
        .on_menu_event(|app, event| match tray_menu_action(event.id().as_ref()) {
            Some(TrayMenuAction::Open) => {
                let _ = show_main_window(app);
            }
            Some(TrayMenuAction::Exit) => app.exit(0),
            None => {}
        })
        .on_tray_icon_event(|tray, event| {
            if matches!(
                event,
                TrayIconEvent::Click {
                    button: MouseButton::Left,
                    button_state: MouseButtonState::Up,
                    ..
                }
            ) {
                let _ = show_main_window(tray.app_handle());
            }
        });
    let builder = match app.default_window_icon() {
        Some(icon) => builder.icon(icon.clone()),
        None => builder,
    };
    builder.build(app)?;
    Ok(())
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _, _| {
            let _ = show_main_window(app);
        }))
        .plugin(tauri_plugin_opener::init())
        .setup(|app| {
            setup_tray(app.handle())?;
            start_update_event_forwarder(app.handle().clone());
            Ok(())
        })
        .on_window_event(|window, event| {
            if window.label() == MAIN_WINDOW_LABEL {
                if let WindowEvent::CloseRequested { api, .. } = event {
                    api.prevent_close();
                    let _ = window.hide();
                }
            }
        })
        .invoke_handler(tauri::generate_handler![
            agent_status,
            agent_command,
            agent_update_status,
            agent_service_status,
            open_management_web,
            exit_desktop
        ])
        .run(tauri::generate_context!())
        .expect("TAURI_RUNTIME_FAILED");
}

#[cfg(test)]
mod tests {
    use super::{TrayMenuAction, UPDATE_STATUS_EVENT, parse_command, tray_menu_action};
    use cmclient_control_api::ControlCommand;

    #[test]
    fn maps_only_desktop_control_commands() {
        assert_eq!(parse_command("start"), Ok(ControlCommand::Start));
        assert_eq!(parse_command("stop"), Ok(ControlCommand::Stop));
        assert_eq!(parse_command("restart"), Ok(ControlCommand::Restart));
        assert_eq!(
            parse_command("enable_web"),
            Ok(ControlCommand::EnableManagementWeb)
        );
        assert_eq!(
            parse_command("disable_web"),
            Ok(ControlCommand::DisableManagementWeb)
        );
        assert_eq!(
            parse_command("delete"),
            Err(String::from("DESKTOP_AGENT_COMMAND_INVALID"))
        );
        assert_eq!(
            parse_command("shutdown_agent"),
            Err(String::from("DESKTOP_AGENT_COMMAND_INVALID"))
        );
    }

    #[test]
    fn routes_only_known_tray_menu_actions() {
        assert_eq!(tray_menu_action("open"), Some(TrayMenuAction::Open));
        assert_eq!(tray_menu_action("exit"), Some(TrayMenuAction::Exit));
        assert_eq!(tray_menu_action("restart"), None);
    }

    #[test]
    fn keeps_the_agent_update_event_name_stable_for_the_webview() {
        assert_eq!(UPDATE_STATUS_EVENT, "agent-update-status");
    }
}
