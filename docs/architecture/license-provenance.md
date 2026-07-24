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

- Current `Cargo.lock` SHA-256:
  `5afcf2d027cc345772f20c72d6acac29a4e9da489820f7d28b7fad03d31764b4`.
- Cargo license inventory: 574 registry packages, 13 workspace packages, zero
  Git sources and zero unknown licenses; inventory SHA-256
  `0da4c2e9cc7770b7cccc23e7ce60799d4f43d21998676db9efebcc67f279f5a3`.
  The digest is canonical JSON sorted by package name, version, source,
  checksum, and declared license.
  P13-T08 removed the Rust SQLite and obsolete Windows ACL migration chain and
  made the already-locked `libc` and `winapi-util` packages exact direct
  platform dependencies. They provide no-follow existing-file opens on Unix
  and read-only hard-link count inspection on Windows for migration and staged
  Agent configuration validation.
- `pnpm-lock.yaml` SHA-256:
  `9b234eb100e287daaf76e9cb13cd07a47205e50ea08adadcfe0e97b933fdc5ab`.
- The current Windows production install contains 183 package versions;
  inventory SHA-256
  `632b3ae7d319aede117c8113b1961e23a331d66972f6b59ff2b548b5fc10ca5f`.
  Its canonical input is one `name@version<TAB>SPDX-expression` row per
  production version, JavaScript-lexically sorted and encoded as UTF-8 LF with
  a terminal LF. The previous literal backslash-tab/backslash-newline digest is
  not retained as evidence.
- No GPL-2.0-only, AGPL, proprietary, Git-sourced, or unknown dependency license
  expression was found. MPL-2.0 dependencies carry no Exhibit-B incompatible
  notice; their file-level source and notice obligations remain in force.
- The Apache ECharts 6.1.0 notice SHA-256 is
  `d491d358344f842685c1b1585970999db65fe30ecf7ef3867af8814f4016c016`
  and its text is retained in the root `NOTICE`.

### Adopted Runtime Primitives

P13 adopts the following exact crates from the crates.io registry. The digest
is the SHA-256 checksum of the published `.crate` archive recorded by crates.io
and Cargo. Each declared license is compatible with CMClient's
`GPL-3.0-only` distribution route; the dependency's own license text and
notices remain part of the target-specific packaging inventory.

| Crate | Exact version | Upstream | License | Crate SHA-256 | Declared Rust floor |
| --- | --- | --- | --- | --- | --- |
| `tokio` | `1.53.1` | `https://github.com/tokio-rs/tokio` | MIT | `202caea871b69668250d242070849eb495be178ed697a3e98aebce5bc81a0bed` | 1.71 |
| `tokio-util` | `0.7.18` | `https://github.com/tokio-rs/tokio` | MIT | `9ae9cec805b01e8fc3fd2fe289f89149a9b66dd16786abd8b19cfa7b48cb0098` | 1.71 |
| `atomic-write-file` | `0.3.0` | `https://github.com/andreacorbellini/rust-atomic-write-file` | BSD-3-Clause | `84790c55b5704b0d35130bf16a4ce22a8e70eb0ea773522557524d9a4852663d` | 1.85 |
| `fs4` | `1.1.0` | `https://github.com/al8n/fs4` | MIT OR Apache-2.0 | `7e72ed92b67c146290f88e9c89d60ca163ea417a446f61ffd7b72df3e7f1dfd5` | 1.75.0 |
| `libc` | `0.2.186` | `https://github.com/rust-lang/libc` | MIT OR Apache-2.0 | `68ab91017fe16c622486840e4c83c9a37afeff978bd239b5293d61ece587de66` | 1.65 |
| `same-file` | `1.0.6` | `https://github.com/BurntSushi/same-file` | Unlicense/MIT | `93fc1dc3aaa9bfed95e02e6eadabb4baf7e3078b0bd1b4d7b6b0b68378900502` | Not declared |
| `time` | `0.3.45` | `https://github.com/time-rs/time` | MIT OR Apache-2.0 | `f9e442fc33d7fdb45aa9bfeb312c095964abdf596f7567261062b2a7107aaabd` | 1.83.0 |
| `tracing` | `0.1.44` | `https://github.com/tokio-rs/tracing` | MIT | `63e71662fa4b2a2c3a26f570f037eb95bb1f85397f3cd8076caed2f026a6d100` | 1.65.0 |
| `tracing-appender` | `0.2.5` | `https://github.com/tokio-rs/tracing` | MIT | `050686193eb999b4bb3bc2acfa891a13da00f79734704c4b8b4ef1a10b368a3c` | 1.63.0 |
| `winapi-util` | `0.1.11` | `https://github.com/BurntSushi/winapi-util` | Unlicense OR MIT | `c2a7b1c03c876122aa43f3020e6c3c3ee5c05081c9a00739faf7503aeba10d22` | not declared |

P13-T08 adopts one exact npm archive reader and its sole transitive dependency
for bounded private-Node staging. Both are MIT licensed and pinned by registry
integrity in `pnpm-lock.yaml`; neither is shipped as the private Node runtime.
Target package license/SBOM scans in P15 remain authoritative for shipped
bytes.

| Package | Exact version | Upstream | License | npm registry integrity |
| --- | --- | --- | --- | --- |
| `yauzl` | `3.4.0` | `https://github.com/thejoshwolfe/yauzl` | MIT | `sha512-jIH9yLR9wqr0wOS0TpBvo/g/2UgZH5qePVbjgRliiF0BYvOZyaBknKsF+x9Iht0O6sqgnB93rCICdOZFecJuDw==` |
| `pend` | `1.2.0` | `https://github.com/andrewrk/node-pend` | MIT | `sha512-F3asv42UuXchdzt+xXqfW1OGlVBe+mxa2mqI0pg5yAHZPvFmY3Y6drSf/GQ1A86WgWEN9Kzh/WrgKa6iGcHXLg==` |

The workspace MSRV is Rust 1.87.0 and the pinned current toolchain is Rust
1.96.0. CI checks every workspace target at 1.87 and runs the full workspace
test and build at 1.96. The official Rust 1.87.0 channel manifest is
`https://static.rust-lang.org/dist/channel-rust-1.87.0.toml`, with SHA-256
`2949b5ea91e3f9c45e75ff2fc6cfc7776616c693ab599ed43abc2120b7522415`.
The MSRV workflow pins the `dtolnay/rust-toolchain` 1.87.0 action commit
`c4743642b206695ff6aa863032b1037759ee95ea`. Rust is distributed under MIT or
Apache-2.0 terms with its bundled third-party notices.

The current Windows qualification installed the official minimal
`1.87.0-x86_64-pc-windows-msvc` toolchain in current-user campaign tooling and
ran `cargo check --workspace --all-targets --locked` successfully. The exact
`rustc` commit is `17067e9ac6d7ecb70e50f92c1944e545188d2359`; the installed
`rustc.exe` SHA-256 is
`31219ec9fefef647623ca50fb119c36ecc737f80f06863b550a88bfaac85c193` and
`cargo.exe` SHA-256 is
`4ec4e44523bc28667db1e1a3febfa450938d8f6f50667b06218849f0a9d6dd4e`.
This is current-host Windows x86-64 evidence only; CI remains responsible for
the independently pinned Linux MSRV job.

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
