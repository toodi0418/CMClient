use atomic_write_file::AtomicWriteFile;
#[cfg(target_os = "windows")]
use cmclient_agent_core::windows_process_identity;
use cmclient_agent_core::{DesktopProcessIdentity, RuntimePaths};
use cmclient_control_api::{
    ControlClient, ControlCommand, ControlStatus, UpdateControlStatus, default_local_endpoint,
};
use std::{
    collections::BTreeMap,
    fs,
    io::{self, Write},
    path::{Path, PathBuf},
    thread,
    time::Duration,
};
use tauri::{AppHandle, Emitter, Manager, Runtime, WindowEvent};
use tauri_plugin_notification::NotificationExt;
use tauri_plugin_opener::OpenerExt;

mod service_status;

use service_status::DesktopServiceStatus;

const MAIN_WINDOW_LABEL: &str = "main";
const UPDATE_STATUS_EVENT: &str = "agent-update-status";

struct DesktopProcessRegistration {
    path: PathBuf,
    identity: DesktopProcessIdentity,
}

impl Drop for DesktopProcessRegistration {
    fn drop(&mut self) {
        if read_desktop_process_identity(&self.path).as_ref() == Some(&self.identity) {
            let _ = fs::remove_file(&self.path);
        }
    }
}

fn register_desktop_process() -> io::Result<DesktopProcessRegistration> {
    let paths = runtime_paths().map_err(|_| io::Error::other("desktop paths unavailable"))?;
    register_desktop_process_at(&paths.desktop_process_file(), current_process_identity()?)
}

fn runtime_paths() -> Result<RuntimePaths, String> {
    let environment = std::env::vars().collect::<BTreeMap<_, _>>();
    RuntimePaths::from_environment(&environment)
        .map_err(|_| String::from("DESKTOP_AGENT_CONFIG_UNAVAILABLE"))
}

fn register_desktop_process_at(
    path: &Path,
    identity: DesktopProcessIdentity,
) -> io::Result<DesktopProcessRegistration> {
    if !identity.is_valid() {
        return Err(io::Error::other("desktop process identity is invalid"));
    }
    let parent = path
        .parent()
        .ok_or_else(|| io::Error::other("desktop process path has no parent"))?;
    fs::create_dir_all(parent)?;
    let mut encoded = serde_json::to_vec(&identity)
        .map_err(|_| io::Error::other("desktop process identity is invalid"))?;
    encoded.push(b'\n');
    let mut output = AtomicWriteFile::open(path)?;
    output.write_all(&encoded)?;
    output.commit()?;
    Ok(DesktopProcessRegistration {
        path: path.to_path_buf(),
        identity,
    })
}

fn read_desktop_process_identity(path: &Path) -> Option<DesktopProcessIdentity> {
    let contents = fs::read(path).ok()?;
    if contents.len() > 512 {
        return None;
    }
    let identity = serde_json::from_slice::<DesktopProcessIdentity>(&contents).ok()?;
    identity.is_valid().then_some(identity)
}

#[cfg(target_os = "windows")]
fn current_process_identity() -> io::Result<DesktopProcessIdentity> {
    windows_process_identity(std::process::id())
}

#[cfg(not(target_os = "windows"))]
fn current_process_identity() -> io::Result<DesktopProcessIdentity> {
    let creation_time = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(io::Error::other)?
        .as_nanos()
        .try_into()
        .map_err(|_| io::Error::other("desktop process identity overflow"))?;
    Ok(DesktopProcessIdentity::new(
        std::process::id(),
        creation_time,
        0,
    ))
}

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
    let paths = runtime_paths()?;
    let endpoint =
        default_local_endpoint(paths.root_dir()).map_err(|error| error.code().to_owned())?;
    Ok(service_status::load(endpoint))
}

#[tauri::command]
fn open_management_web(app: AppHandle) -> Result<(), String> {
    open_management_web_with_app(&app)
}

fn open_management_web_with_app<R: Runtime>(app: &AppHandle<R>) -> Result<(), String> {
    let status = control(ControlCommand::Status)?;
    let url = status
        .management_web_url
        .ok_or_else(|| String::from("DESKTOP_MANAGEMENT_WEB_DISABLED"))?;
    app.opener()
        .open_url(url, None::<&str>)
        .map_err(|_| String::from("DESKTOP_MANAGEMENT_WEB_OPEN_FAILED"))
}

fn setup_management_web_url<'a>(
    latest_error_code: Option<&str>,
    management_web_url: Option<&'a str>,
) -> Option<&'a str> {
    (latest_error_code == Some("SETUP_REQUIRED"))
        .then_some(management_web_url)
        .flatten()
}

fn open_setup_wizard(app: &AppHandle) {
    let app = app.clone();
    let _ = thread::Builder::new()
        .name(String::from("cmclient-desktop-setup-wizard"))
        .spawn(move || {
            let Ok(paths) = runtime_paths() else {
                return;
            };
            let Ok(endpoint) = default_local_endpoint(paths.root_dir()) else {
                return;
            };
            let Ok(client) = ControlClient::new_with_timeout(endpoint, Duration::from_secs(2))
            else {
                return;
            };
            let Ok(status) = client.status() else {
                return;
            };
            let Some(url) = setup_management_web_url(
                status.latest_error_code.as_deref(),
                status.management_web_url.as_deref(),
            ) else {
                return;
            };
            let _ = app.opener().open_url(url, None::<&str>);
        });
}

#[tauri::command]
fn exit_desktop(app: AppHandle) {
    // This exits only the Desktop shell. The resident Agent remains owned by
    // its own lifecycle and continues through the local Control boundary.
    app.exit(0);
}

fn notify<R: Runtime>(app: &AppHandle<R>, title: &str, body: &str) {
    let _ = app.notification().builder().title(title).body(body).show();
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
        ControlCommand::OpenDesktop
        | ControlCommand::OperationalReset
        | ControlCommand::ShutdownAgent => {
            return Err(String::from("DESKTOP_AGENT_COMMAND_INVALID"));
        }
    }
    .map_err(|error| error.code().to_owned())
}

fn control_client() -> Result<ControlClient, String> {
    let paths = runtime_paths()?;
    let endpoint =
        default_local_endpoint(paths.root_dir()).map_err(|error| error.code().to_owned())?;
    ControlClient::new(endpoint).map_err(|error| error.code().to_owned())
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
                    match String::from_utf8(event.data) {
                        Ok(status) => {
                            let _ = app.emit(UPDATE_STATUS_EVENT, status);
                        }
                        Err(_) => notify(&app, "CMClient", "DESKTOP_UPDATE_EVENT_INVALID"),
                    }
                }
                thread::sleep(Duration::from_millis(250));
            }
        });
}

fn show_main_window<R: Runtime>(app: &AppHandle<R>) -> tauri::Result<()> {
    if let Some(window) = app.get_webview_window(MAIN_WINDOW_LABEL) {
        window.show()?;
        window.set_focus()?;
    }
    Ok(())
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_single_instance::init(|app, _, _| {
            let _ = show_main_window(app);
            open_setup_wizard(app);
        }))
        .plugin(tauri_plugin_opener::init())
        .plugin(tauri_plugin_notification::init())
        .setup(|app| {
            let registration = register_desktop_process().inspect_err(|_| {
                eprintln!("DESKTOP_PROCESS_REGISTRATION_UNAVAILABLE");
            })?;
            app.manage(registration);
            start_update_event_forwarder(app.handle().clone());
            open_setup_wizard(app.handle());
            Ok(())
        })
        .on_window_event(|window, event| {
            if window.label() == MAIN_WINDOW_LABEL {
                if let WindowEvent::CloseRequested { api, .. } = event {
                    api.prevent_close();
                    // The Agent owns the resident tray; closing Desktop only
                    // hides this control surface and leaves Agent running.
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
    use super::{
        UPDATE_STATUS_EVENT, parse_command, read_desktop_process_identity,
        register_desktop_process_at, setup_management_web_url,
    };
    use cmclient_agent_core::DesktopProcessIdentity;
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
    fn setup_required_opens_only_the_agent_advertised_management_web() {
        assert_eq!(
            setup_management_web_url(Some("SETUP_REQUIRED"), Some("http://127.0.0.1:7080"),),
            Some("http://127.0.0.1:7080"),
        );
        assert_eq!(
            setup_management_web_url(None, Some("http://127.0.0.1:7080")),
            None,
        );
        assert_eq!(setup_management_web_url(Some("SETUP_REQUIRED"), None), None);
    }

    #[test]
    fn keeps_the_agent_update_event_name_stable_for_the_webview() {
        assert_eq!(UPDATE_STATUS_EVENT, "agent-update-status");
    }

    #[test]
    fn desktop_process_registration_removes_only_its_own_pid() {
        let root = std::env::temp_dir().join(format!(
            "cmclient-desktop-registration-{}",
            std::process::id()
        ));
        let path = root.join("run/desktop.pid");
        let identity = DesktopProcessIdentity::new(41, 100, 2);
        let replacement = DesktopProcessIdentity::new(42, 101, 2);
        let registration =
            register_desktop_process_at(&path, identity).expect("registration should write");
        assert_eq!(read_desktop_process_identity(&path), Some(identity));
        std::fs::write(&path, serde_json::to_vec(&replacement).unwrap())
            .expect("new primary should replace registration");
        drop(registration);
        assert_eq!(read_desktop_process_identity(&path), Some(replacement));
        std::fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[test]
    fn desktop_process_registration_atomically_replaces_partial_identity() {
        let root = std::env::temp_dir().join(format!(
            "cmclient-desktop-registration-partial-{}",
            std::process::id()
        ));
        let path = root.join("run/desktop.pid");
        std::fs::create_dir_all(path.parent().expect("path should have a parent"))
            .expect("fixture parent should create");
        std::fs::write(&path, b"{\"schemaVersion\":1").expect("partial identity should store");
        assert_eq!(read_desktop_process_identity(&path), None);

        let identity = DesktopProcessIdentity::new(43, 102, 2);
        let registration =
            register_desktop_process_at(&path, identity).expect("registration should replace");
        assert_eq!(read_desktop_process_identity(&path), Some(identity));
        assert_eq!(
            std::fs::read_dir(path.parent().expect("path should have a parent"))
                .expect("registration directory should read")
                .count(),
            1,
            "atomic registration must not leave a partial or temporary identity",
        );
        drop(registration);
        assert!(!path.exists());
        std::fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[test]
    fn desktop_process_registration_fails_when_identity_parent_is_not_a_directory() {
        let root = std::env::temp_dir().join(format!(
            "cmclient-desktop-registration-failure-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("fixture root should create");
        let run = root.join("run");
        std::fs::write(&run, b"not-a-directory").expect("blocking file should store");
        let path = run.join("desktop.pid");

        assert!(
            register_desktop_process_at(&path, DesktopProcessIdentity::new(44, 103, 2)).is_err(),
            "Desktop setup must fail instead of continuing without a registered identity",
        );
        assert!(!path.exists());
        std::fs::remove_dir_all(root).expect("fixture should clean up");
    }
}
