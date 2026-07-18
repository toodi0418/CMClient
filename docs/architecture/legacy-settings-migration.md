# Legacy Settings Migration

`cmclient-migrate` is a one-shot offline migration tool. It reads a Legacy
`client-preferences.json` file (or an Electron response wrapper containing a
`preferences` object), produces a machine-readable dry-run report, and is the
only CMClient 2.0 code that understands this legacy shape. The Agent, Gateway,
Desktop, and normal `cmclient` control client do not parse or retain it.

```bash
cmclient-migrate settings \
  --source /absolute/path/client-preferences.json \
  --dry-run
```

The report includes stable codes and field names, never legacy values. It is
bounded to 64 KiB and rejects malformed JSON or a non-object root. The one
currently safe mapping is deliberately small:

| Legacy field | CMClient 2.0 result |
| --- | --- |
| `webDashboardEnabled` boolean | `[agent] management_web_enabled` candidate |
| TCP/serial host, path, baud, mode | `LEGACY_SETTINGS_TRANSPORT_REQUIRES_REVIEW` |
| APRS endpoint/interval | `LEGACY_SETTINGS_APRS_REQUIRES_REVIEW` |
| `shareWithTenmanMap` | `LEGACY_SETTINGS_REMOVED_TENMAN` |
| Legacy traceroute controls | `LEGACY_SETTINGS_REMOVED_LEGACY_RUNTIME_FEATURE` |
| Keys/tokens/passcodes/passwords/secrets | `LEGACY_SETTINGS_SECRET_SKIPPED` |

No legacy credential is moved to the OS credential store automatically. An
operator must add a current credential through `cmclient secret set` after
reviewing the report. Transport and APRS fields require current CMClient 2.0
configuration because their legacy semantics cannot be trusted or mapped to a
runtime compatibility layer.

After review, write mode creates a new absolute Agent config path only:

```bash
cmclient-migrate settings \
  --source /absolute/path/client-preferences.json \
  --write-agent-config /absolute/path/new-config/agent.toml
```

Write mode never merges or overwrites an existing file, touches no SQLite data,
and uses a synced same-directory temporary file followed by an atomic
no-clobber target creation. Existing runtime configuration is therefore
protected from accidental migration changes.
