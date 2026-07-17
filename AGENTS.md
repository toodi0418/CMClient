# CMClient 2.0 Repository Agent Rules

本 Repository 的主要開發分支是 `dev`，目前正在從 Legacy 全面重構為 CMClient 2.0。

## 必須遵守

- 回覆與交接使用繁體中文；程式識別字、API、schema 使用英文。
- 先閱讀 Repository 內 `docs/architecture/`、`docs/api/`、`docs/events/`、`docs/position-aprs/` 與目前任務相關文件。
- 不直接搬 Legacy Electron、raw HTTP、TENMAN/TENMAP、舊 updater 或舊 `@cm` Bot。
- Web、Desktop、CLI 都透過 Rust Agent；Gateway 專注 Meshtastic/APRS domain。
- 所有長操作使用 persistent async Job，所有即時狀態使用 SSE。
- 每個 Mesh Node／APRS callsign 獨立維護位置狀態。
- 位置主要依可信 GPS event time 排序，不能用到達順序取代。
- 相同 Mesh event 必須產生相同 APRS Data；Gateway observation 不得進 Data。
- 不確定位置新舊時不上傳 APRS。
- 只在 `precision_bits === 32` 時上傳位置。
- TCP Proxy 必須 protocol-aware，禁止 raw socket pipe。
- 不提交 secrets、`.env`、資料庫、log、binary、tar 或本機產物。
- 每個變更必須有測試，lint/typecheck/test 通過。
- 禁止 force push、禁止直接推 main、禁止空 commit。

## 目標目錄

```text
apps/web
apps/desktop
apps/gateway
apps/agent
apps/cli
packages/ui
packages/theme
packages/contracts
packages/api-client
packages/event-client
packages/config
packages/validation
packages/i18n
packages/testing
crates/agent-core
crates/control-api
crates/cli-client
crates/updater
crates/supervisor
```

完整跨工作階段計畫位於本機外層 AI Workspace，不應複製進 Repository。
