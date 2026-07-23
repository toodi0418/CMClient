# Packaging Lifecycle Matrix

CMClient treats executable releases and user state as different ownership
domains. A clean install may create current-user integration, a release
directory, or a Docker volume, but it must not synthesize credentials. Every
native mutable path derives from the effective startup user's `~/.cmclient`;
Docker uses `/home/cmclient/.cmclient`. Upgrade replaces only the executable
target. Uninstall removes program/integration files while retaining the state
root.

| Mode | Clean install | Upgrade / refresh | Uninstall retention | Automated gate |
| --- | --- | --- | --- | --- |
| Internal portable runtime | Extract a candidate into an immutable build/test directory outside the state root | Select a separately verified candidate | Removing the runtime leaves `~/.cmclient` unchanged | Fixture removes the runtime and proves state retention |
| Native unified package | Install graphical/command modes, Agent, Gateway, Web, and private Node plus reversible user integration | Replace program files, preserve and transactionally migrate state | Package removal retains all of `~/.cmclient` | Per-target package smoke launches GUI and CLI without system Node and verifies retained state |
| systemd headless fallback | Register Agent for one account with that account's HOME | Regenerate only the unit and executable path | Unit removal retains that account's `~/.cmclient` | Fixture proves one root, private Control socket, bounded logs, and no credential mount or vault |
| launchd / SMAppService | Register the current user's resident Agent | Repair the signed application path | Removal retains the current user's `~/.cmclient` | Fixture proves idempotent registration and retained state |
| Windows login startup | Register `cmclient --background` in the current-user context without SCM or routine UAC | Repair the current-user executable path | Removal retains `%USERPROFILE%\.cmclient` | Native Windows smoke starts from a clean user profile and validates named-pipe Control plus retained state |
| Docker | Create the `/home/cmclient/.cmclient` named volume for the one `cmclient` service | Recreate from the next immutable image/index | `down` without `--volumes` retains all state; final explicit volume removal is destructive | Native Docker smoke writes a sentinel, force-recreates, and reads it back |

The matrix deliberately does not treat Docker `down --volumes`, a user-requested
state-root deletion, or external release asset deletion as ordinary
uninstall. Those are destructive operator actions and require separate explicit
approval. Signed Agent update backup/rollback remains the update transaction
documented in [Update Installation](./update-installation.md). Backups and
rollback media exclude `secrets.json`, `run/`, logs, and update staging.
