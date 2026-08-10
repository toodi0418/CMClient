//! Agent-owned native tray lifecycle.
//!
//! The Agent is the resident owner, so the tray must outlive the optional
//! Desktop process. Platforms without a native tray (including headless Linux)
//! use the no-op implementation below and continue normally.

use std::{
    path::PathBuf,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
};

#[cfg(target_os = "windows")]
use cmclient_agent_core::{
    DesktopProcessIdentity, terminate_windows_process, windows_process_identity,
};

#[cfg(target_os = "windows")]
use std::process::{Child, Command};
#[cfg(target_os = "windows")]
use std::{fs, path::Path};

#[cfg(target_os = "windows")]
use sysinfo::{Pid, ProcessesToUpdate, System};

#[cfg(not(target_os = "windows"))]
use cmclient_control_api::ControlEndpoint;
#[cfg(target_os = "windows")]
use cmclient_control_api::{ControlClient, ControlEndpoint, GatewayControlStatus};

/// A best-effort resident tray worker. Failure to create a tray is never an
/// Agent startup failure: the same Agent remains usable through Web and CLI.
pub struct AgentTray {
    stop: Arc<AtomicBool>,
    worker: Option<JoinHandle<()>>,
}

/// Returns whether this build has the resident tray implementation enabled.
/// Windows owns the native event loop from the Agent worker; macOS and
/// Linux/Docker remain graceful headless fallbacks until their main-thread or
/// desktop-host launchers are wired in.
#[allow(dead_code)]
pub const fn native_tray_supported() -> bool {
    cfg!(target_os = "windows")
}

impl AgentTray {
    pub fn start(endpoint: ControlEndpoint, desktop_process_file: PathBuf) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        #[cfg(target_os = "windows")]
        if !current_windows_session_id().is_some_and(session_is_interactive) {
            eprintln!("AGENT_TRAY_UNAVAILABLE");
            return Self { stop, worker: None };
        }
        let worker_stop = Arc::clone(&stop);
        let worker = thread::Builder::new()
            .name(String::from("cmclient-agent-tray"))
            .spawn(move || run(worker_stop, endpoint, desktop_process_file));
        let worker = match worker {
            Ok(worker) => Some(worker),
            Err(_) => {
                eprintln!("AGENT_TRAY_UNAVAILABLE");
                None
            }
        };
        Self { stop, worker }
    }

    pub fn stop(&mut self) {
        self.stop.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl Drop for AgentTray {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(not(target_os = "windows"))]
fn run(_stop: Arc<AtomicBool>, _endpoint: ControlEndpoint, _desktop_process_file: PathBuf) {
    eprintln!("AGENT_TRAY_UNAVAILABLE");
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone)]
enum TrayEvent {
    Icon(tray_icon::TrayIconEvent),
    Menu(tray_icon::menu::MenuEvent),
}

#[cfg(target_os = "windows")]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TrayMenuAction {
    OpenDesktop,
    ExitDesktop,
    ShutdownProduct,
}

#[cfg(target_os = "windows")]
fn tray_menu_action(id: &str) -> Option<TrayMenuAction> {
    match id {
        "open" => Some(TrayMenuAction::OpenDesktop),
        "exit" => Some(TrayMenuAction::ExitDesktop),
        "shutdown" => Some(TrayMenuAction::ShutdownProduct),
        _ => None,
    }
}

#[cfg(target_os = "windows")]
fn run(stop: Arc<AtomicBool>, endpoint: ControlEndpoint, desktop_process_file: PathBuf) {
    use tao::{
        event::Event,
        event_loop::{ControlFlow, EventLoopBuilder},
    };
    use tray_icon::{
        Icon, TrayIconBuilder, TrayIconEvent,
        menu::{Menu, MenuEvent, MenuItem},
    };

    #[cfg(target_os = "windows")]
    use tao::platform::{run_return::EventLoopExtRunReturn, windows::EventLoopBuilderExtWindows};

    let mut builder = EventLoopBuilder::<TrayEvent>::with_user_event();
    // The Agent has no window and normally starts from a worker/service host.
    // Windows supports an event loop on this thread; macOS is kept as a
    // graceful no-tray fallback because AppKit requires the main thread.
    #[cfg(target_os = "windows")]
    builder.with_any_thread(true);
    let mut event_loop = builder.build();
    let proxy = event_loop.create_proxy();
    TrayIconEvent::set_event_handler(Some(move |event| {
        let _ = proxy.send_event(TrayEvent::Icon(event));
    }));
    let proxy = event_loop.create_proxy();
    MenuEvent::set_event_handler(Some(move |event| {
        let _ = proxy.send_event(TrayEvent::Menu(event));
    }));

    let open = MenuItem::with_id("open", "Open CMClient Desktop", true, None);
    let exit_desktop = MenuItem::with_id("exit", "Exit CMClient Desktop", true, None);
    let shutdown = MenuItem::with_id("shutdown", "Shut Down CMClient", true, None);
    let menu = match Menu::with_items(&[&open, &exit_desktop, &shutdown]) {
        Ok(menu) => menu,
        Err(_) => {
            eprintln!("AGENT_TRAY_UNAVAILABLE");
            return;
        }
    };
    let mut tray_builder = TrayIconBuilder::new()
        .with_menu(Box::new(menu))
        .with_tooltip(tray_tooltip(&endpoint))
        .with_menu_on_left_click(false);
    if let Ok(icon) = Icon::from_rgba(tray_rgba(), TRAY_ICON_SIZE, TRAY_ICON_SIZE) {
        tray_builder = tray_builder.with_icon(icon);
    }
    let tray = match tray_builder.build() {
        Ok(tray) => tray,
        Err(_) => {
            eprintln!("AGENT_TRAY_UNAVAILABLE");
            return;
        }
    };

    let mut next_status_refresh = std::time::Instant::now();
    let mut owned_desktop: Option<Child> = None;
    #[allow(clippy::empty_loop)]
    let _ = event_loop.run_return(move |event, _, control_flow| {
        let now = std::time::Instant::now();
        if stop.load(Ordering::Acquire) {
            close_owned_desktop_for_tray_stop(&mut owned_desktop);
            *control_flow = ControlFlow::Exit;
            return;
        }
        *control_flow = ControlFlow::WaitUntil(now + std::time::Duration::from_millis(250));
        if now >= next_status_refresh {
            let _ = tray.set_tooltip(Some(tray_tooltip(&endpoint)));
            next_status_refresh = now + std::time::Duration::from_secs(1);
        }
        if owned_desktop
            .as_mut()
            .is_some_and(|desktop| desktop.try_wait().is_ok_and(|status| status.is_some()))
        {
            owned_desktop = None;
        }
        let Event::UserEvent(event) = event else {
            return;
        };
        match event {
            TrayEvent::Icon(tray_icon::TrayIconEvent::Click {
                button: tray_icon::MouseButton::Left,
                button_state: tray_icon::MouseButtonState::Up,
                ..
            }) => show_desktop_error(
                &tray,
                &mut next_status_refresh,
                &desktop_process_file,
                &mut owned_desktop,
            ),
            TrayEvent::Menu(event)
                if tray_menu_action(event.id().as_ref()) == Some(TrayMenuAction::OpenDesktop) =>
            {
                show_desktop_error(
                    &tray,
                    &mut next_status_refresh,
                    &desktop_process_file,
                    &mut owned_desktop,
                )
            }
            TrayEvent::Menu(event)
                if tray_menu_action(event.id().as_ref()) == Some(TrayMenuAction::ExitDesktop) =>
            {
                if close_desktop(&desktop_process_file, &mut owned_desktop).is_err() {
                    let _ = tray.set_tooltip(Some("CMClient - Desktop exit failed"));
                    next_status_refresh =
                        std::time::Instant::now() + std::time::Duration::from_secs(5);
                }
            }
            TrayEvent::Menu(event)
                if tray_menu_action(event.id().as_ref())
                    == Some(TrayMenuAction::ShutdownProduct) =>
            {
                let result =
                    request_agent_shutdown(&endpoint, &desktop_process_file, &mut owned_desktop);
                if fence_after_shutdown_request(&stop, &result) {
                    *control_flow = ControlFlow::Exit;
                } else {
                    let _ = tray.set_tooltip(Some("CMClient - Shutdown failed"));
                    next_status_refresh =
                        std::time::Instant::now() + std::time::Duration::from_secs(5);
                }
            }
            _ => {}
        }
    });
}

#[cfg(target_os = "windows")]
fn fence_after_shutdown_request(stop: &AtomicBool, result: &Result<(), ()>) -> bool {
    if result.is_err() {
        return false;
    }
    stop.store(true, Ordering::Release);
    true
}

#[cfg(target_os = "windows")]
fn tray_tooltip(endpoint: &ControlEndpoint) -> &'static str {
    let status =
        ControlClient::new_with_timeout(endpoint.clone(), std::time::Duration::from_millis(500))
            .and_then(|client| client.status())
            .ok();
    status
        .as_ref()
        .map_or("CMClient - Agent unavailable", |status| {
            tooltip_for_status(&status.gateway, status.latest_error_code.as_deref())
        })
}

#[cfg(target_os = "windows")]
fn tooltip_for_status(
    gateway: &GatewayControlStatus,
    latest_error_code: Option<&str>,
) -> &'static str {
    if latest_error_code == Some("SETUP_REQUIRED") {
        return "CMClient - Setup required";
    }
    if latest_error_code.is_some_and(|code| code.starts_with("CALLMESH_")) {
        return "CMClient - CallMesh degraded";
    }
    match gateway {
        GatewayControlStatus::Running if latest_error_code.is_none() => "CMClient - Ready",
        GatewayControlStatus::Starting => "CMClient - Gateway starting",
        GatewayControlStatus::Stopped | GatewayControlStatus::Backoff => {
            "CMClient - Gateway offline"
        }
        GatewayControlStatus::Degraded | GatewayControlStatus::Running => {
            "CMClient - Gateway degraded"
        }
    }
}

#[cfg(target_os = "windows")]
const TRAY_ICON_SIZE: u32 = 32;

// The tray uses the same transparent logo as the Desktop bundle. The pixels
// are embedded so the headless Agent does not need a filesystem asset.
#[cfg(target_os = "windows")]
const TRAY_ICON_RGBA: &[u8; 32 * 32 * 4] = include_bytes!("../assets/tray-icon-32.rgba");

#[cfg(target_os = "windows")]
fn tray_rgba() -> Vec<u8> {
    TRAY_ICON_RGBA.to_vec()
}

#[cfg(target_os = "windows")]
fn desktop_paths() -> Vec<PathBuf> {
    std::env::current_exe()
        .ok()
        .map_or_else(Vec::new, |agent| desktop_paths_from_agent(&agent))
}

#[cfg(target_os = "windows")]
fn desktop_paths_from_agent(agent: &Path) -> Vec<PathBuf> {
    let Some(bin) = agent.parent() else {
        return Vec::new();
    };
    let mut candidates = Vec::with_capacity(2);
    let runtime = bin.parent();
    if path_has_name(bin, "bin")
        && runtime.is_some_and(|path| path_has_name(path, "cmclient-runtime"))
    {
        if let Some(install_root) = runtime.and_then(Path::parent) {
            let installed = install_root.join("cmclient-desktop.exe");
            if installed.is_file() {
                candidates.push(installed);
            }
        }
    }
    let adjacent = bin.join("cmclient-desktop.exe");
    if adjacent.is_file() {
        candidates.push(adjacent);
    }
    candidates
}

#[cfg(target_os = "windows")]
fn path_has_name(path: &Path, expected: &str) -> bool {
    path.file_name()
        .is_some_and(|name| name.to_string_lossy().eq_ignore_ascii_case(expected))
}

#[cfg(target_os = "windows")]
const DESKTOP_ENV_ALLOWLIST: &[&str] = &[
    "APPDATA",
    "COMSPEC",
    "HOME",
    "HOMEDRIVE",
    "HOMEPATH",
    "LOCALAPPDATA",
    "PATH",
    "PATHEXT",
    "PROGRAMDATA",
    "SYSTEMROOT",
    "TEMP",
    "TMP",
    "USERPROFILE",
    "WINDIR",
];

#[cfg(target_os = "windows")]
fn desktop_command(path: &Path) -> Command {
    let mut command = Command::new(path);
    command.env_clear();
    for name in DESKTOP_ENV_ALLOWLIST {
        if let Some(value) = std::env::var_os(name) {
            command.env(name, value);
        }
    }
    command
}

#[cfg(target_os = "windows")]
fn show_desktop_error(
    tray: &tray_icon::TrayIcon,
    next_status_refresh: &mut std::time::Instant,
    desktop_process_file: &Path,
    owned_desktop: &mut Option<Child>,
) {
    if let Err(message) = launch_desktop(desktop_process_file, owned_desktop) {
        let _ = tray.set_tooltip(Some(message));
        *next_status_refresh = std::time::Instant::now() + std::time::Duration::from_secs(5);
    }
}

#[cfg(target_os = "windows")]
fn launch_desktop(
    desktop_process_file: &Path,
    owned_desktop: &mut Option<Child>,
) -> Result<(), &'static str> {
    let paths = desktop_paths();
    let Some(path) = paths.first() else {
        eprintln!("AGENT_TRAY_DESKTOP_UNAVAILABLE");
        return Err("CMClient - Desktop unavailable");
    };
    let registered = registered_desktop(desktop_process_file, &paths).is_some();
    if !registered
        && owned_desktop
            .as_mut()
            .is_some_and(|desktop| desktop.try_wait().is_ok_and(|status| status.is_none()))
    {
        return Ok(());
    }
    let child = desktop_command(path).spawn().map_err(|_| {
        eprintln!("AGENT_TRAY_DESKTOP_LAUNCH_FAILED");
        "CMClient - Desktop launch failed"
    })?;
    if !registered {
        *owned_desktop = Some(child);
    }
    Ok(())
}

#[cfg(target_os = "windows")]
struct DesktopProcess {
    identity: DesktopProcessIdentity,
}

#[cfg(target_os = "windows")]
impl DesktopProcess {
    fn open(identity: DesktopProcessIdentity, expected_paths: &[PathBuf]) -> Option<Self> {
        if !identity.is_valid()
            || current_windows_session_id()? != identity.session_id
            || !sysinfo_image_matches(identity.pid, expected_paths)
        {
            return None;
        }
        if windows_process_identity(identity.pid).ok()? != identity {
            return None;
        }
        Some(Self { identity })
    }

    fn terminate(&self) -> bool {
        terminate_windows_process(self.identity).unwrap_or(false)
    }
}

#[cfg(target_os = "windows")]
fn sysinfo_image_matches(pid: u32, expected_paths: &[PathBuf]) -> bool {
    let mut system = System::new();
    let pid = Pid::from_u32(pid);
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    system
        .process(pid)
        .and_then(|process| process.exe())
        .is_some_and(|actual| {
            let actual = normalized_windows_path(actual);
            expected_paths
                .iter()
                .any(|expected| actual == normalized_windows_path(expected))
        })
}

#[cfg(target_os = "windows")]
fn current_windows_session_id() -> Option<u32> {
    windows_process_identity(std::process::id())
        .ok()
        .map(|identity| identity.session_id)
}

#[cfg(target_os = "windows")]
const fn session_is_interactive(session_id: u32) -> bool {
    session_id != 0
}

#[cfg(target_os = "windows")]
fn normalized_windows_path(path: &Path) -> String {
    let canonical = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
    canonical
        .to_string_lossy()
        .trim_start_matches(r"\\?\")
        .to_ascii_lowercase()
}

#[cfg(target_os = "windows")]
fn registered_desktop(path: &Path, expected_paths: &[PathBuf]) -> Option<DesktopProcess> {
    let contents = fs::read(path).ok()?;
    if contents.len() > 512 {
        return None;
    }
    let identity = serde_json::from_slice::<DesktopProcessIdentity>(&contents).ok()?;
    DesktopProcess::open(identity, expected_paths)
}

#[cfg(target_os = "windows")]
fn remove_process_file_for(path: &Path, identity: DesktopProcessIdentity) {
    if fs::read(path)
        .ok()
        .filter(|contents| contents.len() <= 512)
        .and_then(|contents| serde_json::from_slice::<DesktopProcessIdentity>(&contents).ok())
        == Some(identity)
    {
        let _ = fs::remove_file(path);
    }
}

#[cfg(target_os = "windows")]
fn close_registered_desktop(process_file: &Path, desktops: &[PathBuf]) -> Result<bool, ()> {
    let Some(process) = registered_desktop(process_file, desktops) else {
        return Ok(false);
    };
    if !process.terminate() {
        return Err(());
    }
    remove_process_file_for(process_file, process.identity);
    Ok(true)
}

#[cfg(target_os = "windows")]
fn close_owned_desktop(
    owned_desktop: &mut Option<Child>,
    registered_closed: bool,
) -> Result<(), ()> {
    close_owned_desktop_with_timeout(
        owned_desktop,
        registered_closed,
        std::time::Duration::from_secs(5),
    )
}

#[cfg(target_os = "windows")]
fn close_owned_desktop_with_timeout(
    owned_desktop: &mut Option<Child>,
    registered_closed: bool,
    timeout: std::time::Duration,
) -> Result<(), ()> {
    let Some(mut owned) = owned_desktop.take() else {
        return Ok(());
    };
    if !registered_closed {
        let _ = owned.kill();
    }
    let deadline = std::time::Instant::now() + timeout;
    loop {
        match owned.try_wait() {
            Ok(Some(_)) => return Ok(()),
            Ok(None) if std::time::Instant::now() < deadline => {
                std::thread::sleep(std::time::Duration::from_millis(50));
            }
            _ => break,
        }
    }
    eprintln!("AGENT_TRAY_OWNED_DESKTOP_CLOSE_TIMEOUT");
    let _ = owned.kill();
    let _ = owned.wait();
    Err(())
}

#[cfg(target_os = "windows")]
fn close_owned_desktop_for_tray_stop(owned_desktop: &mut Option<Child>) {
    if close_owned_desktop(owned_desktop, false).is_err() {
        eprintln!("AGENT_TRAY_OWNED_DESKTOP_STOP_FAILED");
    }
}

#[cfg(target_os = "windows")]
fn close_desktop(desktop_process_file: &Path, owned_desktop: &mut Option<Child>) -> Result<(), ()> {
    let paths = desktop_paths();
    let registered_closed = close_registered_desktop(desktop_process_file, &paths)?;
    close_owned_desktop(owned_desktop, registered_closed)
}

#[cfg(target_os = "windows")]
fn request_agent_shutdown(
    endpoint: &ControlEndpoint,
    desktop_process_file: &Path,
    owned_desktop: &mut Option<Child>,
) -> Result<(), ()> {
    close_desktop(desktop_process_file, owned_desktop)?;
    let result =
        ControlClient::new_with_timeout(endpoint.clone(), std::time::Duration::from_secs(2))
            .and_then(|client| client.shutdown_agent());
    if result.is_err() {
        eprintln!("AGENT_TRAY_SHUTDOWN_FAILED");
        return Err(());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    fn test_endpoint() -> (cmclient_control_api::ControlEndpoint, std::path::PathBuf) {
        let sequence = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should follow epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cmclient-agent-tray-test-{}-{sequence}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("test root should create");
        let endpoint = cmclient_control_api::default_local_endpoint(&root)
            .expect("test endpoint should be valid");
        (endpoint, root)
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn tray_icon_uses_the_transparent_logo() {
        let pixels = super::tray_rgba();
        assert_eq!(pixels.len(), 32 * 32 * 4);
        assert!(
            pixels.chunks_exact(4).any(|pixel| pixel[3] == 0),
            "logo corners should remain transparent",
        );
        assert!(
            pixels
                .chunks_exact(4)
                .any(|pixel| pixel[1] > pixel[0] && pixel[1] > pixel[2]),
            "logo should retain its green network mark",
        );
        assert!(
            pixels
                .chunks_exact(4)
                .any(|pixel| pixel[2] > pixel[0] && pixel[1] > pixel[0]),
            "logo should retain its cyan ring",
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn desktop_children_receive_only_the_safe_environment_allowlist() {
        let command = super::desktop_command(std::path::Path::new("cmclient-desktop.exe"));
        let environment = command
            .get_envs()
            .filter_map(|(name, value)| value.map(|_| name.to_string_lossy().into_owned()))
            .collect::<Vec<_>>();
        assert!(environment.iter().all(|name| {
            super::DESKTOP_ENV_ALLOWLIST
                .iter()
                .any(|allowed| name.eq_ignore_ascii_case(allowed))
        }));
        assert!(
            environment
                .iter()
                .all(|name| !name.to_ascii_uppercase().contains("KEY"))
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn desktop_exit_terminates_an_agent_owned_unregistered_desktop() {
        let child = std::process::Command::new("powershell.exe")
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Start-Sleep -Seconds 30",
            ])
            .spawn()
            .expect("owned process fixture should start");
        let pid = child.id();
        let mut owned = Some(child);
        super::close_owned_desktop_with_timeout(
            &mut owned,
            false,
            std::time::Duration::from_millis(50),
        )
        .expect("owned process should close");
        assert!(owned.is_none());
        let mut system = sysinfo::System::new();
        let pid = sysinfo::Pid::from_u32(pid);
        system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
        assert!(system.process(pid).is_none());
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn external_tray_stop_terminates_and_reaps_an_agent_owned_desktop() {
        let child = std::process::Command::new("powershell.exe")
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Start-Sleep -Seconds 30",
            ])
            .spawn()
            .expect("owned process fixture should start");
        let pid = child.id();
        let mut owned = Some(child);

        super::close_owned_desktop_for_tray_stop(&mut owned);

        assert!(owned.is_none());
        let mut system = sysinfo::System::new();
        let pid = sysinfo::Pid::from_u32(pid);
        system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[pid]), true);
        assert!(
            system.process(pid).is_none(),
            "AgentTray::stop must not orphan the Desktop it launched",
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn desktop_exit_terminates_a_registered_desktop_not_owned_by_the_tray() {
        let powershell = std::path::PathBuf::from(
            std::env::var_os("SYSTEMROOT")
                .map(std::path::PathBuf::from)
                .expect("SystemRoot should exist"),
        )
        .join("System32/WindowsPowerShell/v1.0/powershell.exe");
        let mut child = std::process::Command::new(&powershell)
            .args([
                "-NoLogo",
                "-NoProfile",
                "-NonInteractive",
                "-Command",
                "Start-Sleep -Seconds 30",
            ])
            .spawn()
            .expect("external Desktop fixture should start");
        let identity = cmclient_agent_core::windows_process_identity(child.id())
            .expect("external Desktop identity should resolve");
        let root = std::env::temp_dir().join(format!(
            "cmclient-agent-tray-external-desktop-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("fixture root should create");
        let process_file = root.join("desktop.pid");
        std::fs::write(
            &process_file,
            serde_json::to_vec(&identity).expect("identity should encode"),
        )
        .expect("identity should store");

        assert_eq!(
            super::close_registered_desktop(&process_file, &[powershell]),
            Ok(true),
        );
        assert!(!process_file.exists());
        assert!(
            child
                .try_wait()
                .expect("external Desktop status should read")
                .is_some(),
            "registered Desktop must be gone before Agent shutdown",
        );
        std::fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn malformed_registered_identity_is_ignored_without_terminating_a_process() {
        let current = std::env::current_exe().expect("test executable should resolve");
        let root = std::env::temp_dir().join(format!(
            "cmclient-agent-tray-malformed-desktop-{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&root).expect("fixture root should create");
        let process_file = root.join("desktop.pid");
        std::fs::write(&process_file, b"{\"pid\":").expect("malformed identity should store");

        assert_eq!(
            super::close_registered_desktop(&process_file, &[current]),
            Ok(false),
        );
        assert!(process_file.exists());
        std::fs::remove_dir_all(root).expect("fixture should clean up");
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn successful_product_shutdown_fences_new_tray_events() {
        let stop = std::sync::atomic::AtomicBool::new(false);
        assert!(super::fence_after_shutdown_request(&stop, &Ok(())));
        assert!(stop.load(std::sync::atomic::Ordering::Acquire));

        let stop = std::sync::atomic::AtomicBool::new(false);
        assert!(!super::fence_after_shutdown_request(&stop, &Err(())));
        assert!(!stop.load(std::sync::atomic::Ordering::Acquire));
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn tray_desktop_exit_is_distinct_from_full_product_shutdown() {
        assert_eq!(
            super::tray_menu_action("open"),
            Some(super::TrayMenuAction::OpenDesktop),
        );
        assert_eq!(
            super::tray_menu_action("exit"),
            Some(super::TrayMenuAction::ExitDesktop),
        );
        assert_eq!(
            super::tray_menu_action("shutdown"),
            Some(super::TrayMenuAction::ShutdownProduct),
        );
        assert_eq!(super::tray_menu_action("quit"), None);
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn registered_identity_requires_exact_executable_creation_and_session() {
        let current = std::env::current_exe().expect("test executable path should resolve");
        let pid = std::process::id();
        let identity = cmclient_agent_core::windows_process_identity(pid)
            .expect("current process identity should resolve");
        assert!(super::DesktopProcess::open(identity, std::slice::from_ref(&current)).is_some());
        assert!(
            super::DesktopProcess::open(
                identity,
                &[std::path::PathBuf::from("not-cmclient-desktop.exe")],
            )
            .is_none()
        );
        assert!(
            super::DesktopProcess::open(
                identity,
                &[std::path::PathBuf::from("preferred.exe"), current.clone()],
            )
            .is_some(),
            "the packaged resource copy may be the existing single-instance primary",
        );
        let mut stale = identity;
        stale.creation_time = stale.creation_time.saturating_add(1);
        assert!(super::DesktopProcess::open(stale, std::slice::from_ref(&current)).is_none());
        let mut foreign_session = identity;
        foreign_session.session_id = foreign_session.session_id.saturating_add(1);
        assert!(
            super::DesktopProcess::open(foreign_session, std::slice::from_ref(&current)).is_none()
        );
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn session_zero_is_a_headless_tray_fallback() {
        assert!(!super::session_is_interactive(0));
        assert!(super::session_is_interactive(1));
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn packaged_agent_prefers_the_install_root_desktop_and_debug_uses_adjacent() {
        let sequence = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should follow epoch")
            .as_nanos();
        let root = std::env::temp_dir().join(format!(
            "cmclient-agent-tray-layout-{}-{sequence}",
            std::process::id(),
        ));
        let packaged_bin = root.join("cmclient-runtime/bin");
        std::fs::create_dir_all(&packaged_bin).expect("packaged bin should create");
        let packaged_agent = packaged_bin.join("cmclient-agent.exe");
        let embedded_desktop = packaged_bin.join("cmclient-desktop.exe");
        let installed_desktop = root.join("cmclient-desktop.exe");
        std::fs::write(&embedded_desktop, b"embedded").expect("embedded Desktop should create");
        std::fs::write(&installed_desktop, b"installed").expect("installed Desktop should create");
        assert_eq!(
            super::desktop_paths_from_agent(&packaged_agent),
            vec![installed_desktop.clone(), embedded_desktop.clone()],
        );

        std::fs::remove_file(&installed_desktop).expect("installed Desktop should remove");
        assert_eq!(
            super::desktop_paths_from_agent(&packaged_agent),
            vec![embedded_desktop],
        );

        let debug_bin = root.join("debug");
        std::fs::create_dir_all(&debug_bin).expect("debug bin should create");
        let debug_desktop = debug_bin.join("cmclient-desktop.exe");
        std::fs::write(&debug_desktop, b"debug").expect("debug Desktop should create");
        assert_eq!(
            super::desktop_paths_from_agent(&debug_bin.join("cmclient-agent.exe")),
            vec![debug_desktop],
        );
        std::fs::remove_dir_all(root).expect("layout fixture should remove");
    }

    #[test]
    fn headless_start_and_stop_are_safe() {
        let (endpoint, root) = test_endpoint();
        let mut tray = super::AgentTray::start(endpoint, root.join("run/desktop.pid"));
        tray.stop();
        std::fs::remove_dir_all(root).expect("test root should clean up");
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn tooltip_reports_only_bounded_operational_state() {
        use cmclient_control_api::GatewayControlStatus;

        assert_eq!(
            super::tooltip_for_status(&GatewayControlStatus::Stopped, Some("SETUP_REQUIRED")),
            "CMClient - Setup required"
        );
        assert_eq!(
            super::tooltip_for_status(
                &GatewayControlStatus::Running,
                Some("CALLMESH_NETWORK_UNAVAILABLE")
            ),
            "CMClient - CallMesh degraded"
        );
        assert_eq!(
            super::tooltip_for_status(&GatewayControlStatus::Running, None),
            "CMClient - Ready"
        );
        assert_eq!(
            super::tooltip_for_status(&GatewayControlStatus::Backoff, None),
            "CMClient - Gateway offline"
        );
    }

    #[test]
    fn support_flag_matches_the_platform_fallback_policy() {
        assert_eq!(super::native_tray_supported(), cfg!(target_os = "windows"));
    }
}
