# Tracked Runtime Artifact Removal Record

## Scope

This is a metadata-only record of runtime and generated artifacts that were
tracked by the Legacy `dev` baseline. The migration never read, printed,
parsed, copied, or restored their potentially sensitive contents.

| Path | Baseline blob size | Classification | Final disposition |
| --- | ---: | --- | --- |
| `.env` | 496 bytes | Potential secret-bearing runtime configuration | Removed. Runtime secrets enter only supported OS storage or deployment injection; no environment file is tracked. |
| `callmesh-data.sqlite` | 0 bytes | Runtime database artifact | Removed. Real user databases use the verified offline migration workflow. |
| `callmesh-client.tar` | 92,791,808 bytes | Generated archive artifact | Removed. Reproducible release archives and images are assembled by CI. |
| `nohup.out` | 30,320 bytes | Runtime log | Removed. Operational logs use bounded, redacted runtime storage. |

## Completed Boundaries

- Generic ignore rules and the repository removal scanner reject environment
  files, databases, logs, archives, and other generated output under any name.
- P11 removed the Legacy root runtime, manual tests, build/debug helpers, both
  Meshtastic gitlinks, and the unlicensed offline tile/font corpus.
- The locked `proto/meshtastic/` source corpus remains. Gateway decoding,
  Docker, release staging, and compatibility tests all consume that exact
  versioned input.
- Historical Git commits remain intact. No history rewrite or force push was
  used to hide the baseline evidence.

## Continuous Verification

- `pnpm run test:legacy-removal` scans every tracked path and file, rejects any
  gitlink, and permits removal evidence only at exact reviewed locations.
- The same gate rejects retired direct dependencies and package scripts while
  allowing the production `protobufjs` and `serialport` integrations.
- The workspace secret scan checks every added or modified path before each
  checkpoint without reopening deleted sensitive artifacts.
- Release composition tests prove source/runtime data is not included in
  Desktop, Headless, CLI, Service, or Docker product surfaces.
