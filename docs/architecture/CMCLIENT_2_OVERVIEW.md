# CMClient 2.0 Architecture Overview

CMClient 2.0 由 Vue Web、Tauri Supervisor、Rust CLI、Rust Agent 與 TypeScript Gateway 組成。Agent 管理程序、控制 API、Web listener、更新與回滾；Gateway 處理 Meshtastic、Position/APRS、CallMesh、TCP Proxy、Persistence、Jobs 與 Domain Events。

Desktop 的本機 Agent client 邊界見 `docs/architecture/desktop-supervisor.md`。

更新與回滾由 Agent 執行。Release manifest 的簽章、bundle 與 trust boundary 見
`docs/api/update-manifest.md` 與 `docs/architecture/update-manifest.md`。

本文件是 Repository 內架構入口。詳細契約應隨實作逐步補充到 `docs/api`、`docs/events`、`docs/position-aprs`、`docs/update`、`docs/testing`。
