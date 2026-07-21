# License And Source Provenance

## Approved Client Route

The CMClient client is distributed as `GPL-3.0-only`. The root `LICENSE` is the
exact GNU GPL version 3 text carried by the verified Meshtastic protobuf source;
its SHA-256 is
`3972dc9744f6499f0f9b2dbf76696f2ae7ad8af9b23dde66d6af86c9dfb36986`.
`NOTICE` retains Meshtastic provenance, Apache ECharts notice text, contributor
history, and the rule that every other dependency keeps its own terms.

The official CallMesh server is a separately operated HTTPS service. No server
implementation is linked into or distributed with CMClient, and the client has
no self-hosted server mode. The official service at
`https://callmesh.tmmarc.org` is the sole production provision and mapping
authority. This records the product boundary and is not a general legal opinion.

## Meshtastic Corpus

The 41 tracked files in `proto/meshtastic` were compared by Git blob identity
and bytes with the official source:

| Field | Exact value |
| --- | --- |
| Repository | `https://github.com/meshtastic/protobufs` |
| Commit | `7f1110dd7737c7884012cc899862f9d7427b9c51` |
| Tree | `760145a5f860ebd521f574d54caba0f39a7a64d6` |
| Upstream path | `meshtastic` |
| Local import commit | `b8036a42db1264648f66ccdd3fdb534541ecde67` |
| Files | 41 total: 23 `.proto`, 18 `.options`, 198,635 bytes |
| Match result | 41 of 41 unmodified |
| Runtime `.proto` fingerprint | `762fc01e0e6520b03487c6cc7b4afbafeadc39f10a66fa17def966e9ea428602` |
| Full inventory SHA-256 | `ce3d3f9376b9a2552fc22c7d962ee9b25ebeda9e748301284be730fbff21b8f1` |
| Upstream LICENSE blob | `f288702d2fa16d3cdf0035b15a9fcbc552cd88e7` |

Upstream labels the corpus GPL-3.0 and supplies the GPLv3 text, but does not
state whether that revision is `-only` or `-or-later`. Treating it as
GPL-3.0-only is compatible with either interpretation and matches the approved
CMClient client route.

## Dependency Inventory

The promotion baseline is CMClient commit
`b3fec2b344bda3b11d0a15d2bad381f7d926114e`, tree
`0a647c1f814a98b563bf9ebc1dcd26219afffaaa`.

- Baseline `Cargo.lock` SHA-256:
  `91d9a6b87f834b20edde0341f97c20eac8deab044ed4fdd82b81fe402d1d73e1`.
- Cargo license inventory: 586 registry packages, 12 workspace packages, zero
  Git sources; inventory SHA-256
  `b62d86a78088d0ec37f0a409ca2435e7d26ba4833fdd030ee89db39cbb09cc7a`.
- `pnpm-lock.yaml` SHA-256:
  `99207257e14da5b216e65b9863c11dfcde7fdb58403be094cd93a9ec66fdbca3`.
- The current Windows production install contains 183 package versions;
  inventory SHA-256
  `1a46ec827117d651b449faf536c353763f642de4550537361358a93b5a22b281`.
- No GPL-2.0-only, AGPL, proprietary, Git-sourced, or unknown dependency license
  expression was found. MPL-2.0 dependencies carry no Exhibit-B incompatible
  notice; their file-level source and notice obligations remain in force.
- The Apache ECharts 6.1.0 notice SHA-256 is
  `d491d358344f842685c1b1585970999db65fe30ecf7ef3867af8814f4016c016`
  and its text is retained in the root `NOTICE`.

## Invalidation And Release Gate

`state/LICENSE_PROVENANCE.json` is the workspace approval record. The committed
task graph locks its required schema and route. A change to any Meshtastic file,
`Cargo.lock`, `pnpm-lock.yaml`, license metadata, notice, or bundled dependency
invalidates the corresponding approval until the inventory is regenerated.

Each final Windows, macOS, Linux, and Docker stage must run its own Syft,
license, and notice scan over the exact packaged bytes. The Windows-only Node
inventory above does not qualify absent target-specific packages, private Node,
WebView2, or later dependency changes. Production signing creates different
bytes and requires the final exact-subject scans and bindings again.
