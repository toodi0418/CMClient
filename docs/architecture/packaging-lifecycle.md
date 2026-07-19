# Packaging Lifecycle Matrix

CMClient treats executable releases and user state as different ownership
domains. A clean install may create a service registration, release directory,
or Docker volume, but it must not synthesize credentials. Upgrade replaces only
the executable target. Uninstall removes the deployment registration or release
directory while retaining user-owned configuration, data, cache, and logs.

| Mode | Clean install | Upgrade / refresh | Uninstall retention | Automated gate |
| --- | --- | --- | --- | --- |
| Portable archive | Extract a signed archive into a new release directory | Extract the next archive into a separate directory and select it externally | Removing the prior release leaves adjacent user data unchanged | Archive fixture extracts, refreshes, and removes a release directory |
| Native Desktop package | Install Tauri plus the complete portable runtime under `cmclient-runtime/`; service registration remains explicit | Install the next package, then point the independently managed Agent service at its validated runtime or matching portable release | Package removal must not delete external Agent configuration, data, cache, or logs | CI extracts every package, validates the embedded composition, and bounded-launches Tauri without claiming service auto-registration |
| systemd | Install `cmclient-agent.service` for a non-login account | Re-run installer with the next Agent path; it regenerates the unit | Unit removal retains `/etc/cmclient`, `/var/lib/cmclient`, cache, and bounded application logs | Installer fixture verifies V1 to V2 unit rewrite and retained state; log tests prove JSONL priority and stable-code-only journal fallback |
| launchd | Install a per-user `io.cmclient.agent` plist with unmanaged stdout/stderr routed to `/dev/null` | Re-run installer with the next Agent path; it replaces only the plist | Plist removal retains Application Support, cache, and bounded application logs | Installer fixture verifies V1 to V2 plist rewrite, retained state, JSONL log selection, and no launchd-owned unbounded files |
| Windows Service | Register the fixed singleton `CMClientAgent` SCM service running the service host as LocalService | Re-run installer with the next host path; custom service names are not supported | SCM removal retains ProgramData and bounded `service-host.jsonl`, `agent.jsonl`, and `gateway.jsonl` logs | Native Windows CI reconfigures and starts the canonical service, validates its named-pipe Agent status, then removes it with retained state; manager tests fix the ProgramData log paths and bounded line range, while one reusable smoke also exercises staged and final-ZIP Service Hosts, adjacent Agent, external-Node Gateway identity, and CLI health |
| Docker | Create the named data volume and internal Gateway/Web deployment | Recreate containers from the next immutable image | `down` without `--volumes` retains Gateway data; final explicit volume removal is destructive | Native Docker smoke writes a sentinel, force-recreates, and reads it back |

The matrix deliberately does not treat Docker `down --volumes`, a user-requested
data-directory deletion, or external release asset deletion as ordinary
uninstall. Those are destructive operator actions and require separate explicit
approval. Signed Agent update backup/rollback remains the update transaction
documented in [Update Installation](./update-installation.md).
