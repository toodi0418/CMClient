# Unified Product Identity And Capability Contract

`@cmclient/contracts` and `cmclient-product-identity` define the same wire
contract for Agent, Gateway, Web, graphical mode, command mode, and updater.

```text
ReleaseIdentity v1
  product = CMClient
  version = strict SemVer
  sourceCommit = lowercase 40-character Git commit ID
  sourceTree = Git tree ID, or sha256 content identity for a dirty workspace
  channel = dev | candidate | stable

ProductTarget
  profile = native | docker
  os / architecture / packageProfile = one supported exact tuple

ComponentIdentityReport v1
  component + ProductIdentity
```

Supported targets are closed: Windows x86-64 workspace/Setup; macOS x86-64 or
ARM64 workspace and Universal DMG; Linux x86-64 or ARM64 workspace/AppImage;
and Linux x86-64 or ARM64 Docker OCI. Windows ARM64, macOS Docker, native OCI,
and mismatched package profiles fail validation. A Universal DMG component
reports `universal/dmg`; the CPU that executed it belongs in qualification
evidence, not product identity.

The capability keys are `managementWeb`, `commandMode`, `graphicalMode`,
`loginAutostart`, `serial`, `nativeUpdate`, `dockerPullRecreateUpdate`,
`localControl`, and `remoteDispatch`. False states accept only the closed reason
set from the schema, including the exact Docker reason
`unavailable_in_docker`. Unknown keys, unknown reasons, and incomplete false
states fail validation.

Clients use the identity nested in status/capabilities and do not maintain a
second platform or profile field. That prevents a payload from claiming, for
example, a native identity and Docker capabilities at the same time.
