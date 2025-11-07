# Meshtastic Monitor / CallMesh Client 交接手冊

> 本文件彙整專案背景、架構、模組行為與作業流程，供後續接手者快速上手。請在進行任何修改前完整閱讀，並依照文內指引操作。

---

## 0. 目前狀態更新（2025-11）

- **Web Dashboard 事件精簡**：`callmesh` SSE payload 僅保留前端使用欄位（狀態、Mapping 摘要、Provision、APRS 狀態），敏感資訊如 `verifiedKey`、agent 字串已移除。
- **CallMesh 狀態顯示調整**：GUI/Web 均以「正常／異常」兩態對外呈現；異常代表未驗證 Key 或處於 degraded。
- **遙測圖表**：Chart.js Tooltip 會依實際最小差距動態套用小數位數，避免相同值因四捨五入看似變化；Y 軸／最新值同步採用此精度。
- **Web Telemetry 初始同步**：Electron 啟動以及 API Key 驗證流程完成後，都會將最新遙測快照播送給 Web Server，確保頁面立即有資料。
- **SSE 管線**：Electron 端每當遙測 `append/reset` 出現時，會同時廣播給 Renderer 與 Web Server，兩側資料來源一致。
- **節點資料庫**：新增 `src/nodeDatabase.js`，集中維護 Mesh 節點的長名稱、模型、角色與最後出現時間；CLI、Electron、Web 均透過 Bridge 發佈 `node` / `node-snapshot` 事件使用同一份資料。
- **節點清單座標顯示**：Electron / Web 節點頁新增「座標」欄，會顯示緯度、經度與高度（若可用），並支援以座標字串搜尋；同時過濾 `!abcd` 前綴暫存 ID。
- **遙測統計**：Bridge 會回傳遙測筆數、節點數及 `telemetry-records.jsonl` 檔案大小。Electron Telemetry 頁與 Web Dashboard 均顯示最新統計。
- **遙測 CSV 下載**：Electron 與 Web 遙測頁新增「下載 CSV」按鈕，依目前節點與範圍匯出遙測資料。
- **遙測最後轉發與跳數**：Telemetry 紀錄同步保存最後轉發節點與跳數資訊，桌面／Web UI 及 CSV 皆可檢視（含推測提示）。
- **GUI 訊息頻道持久化**：桌面版新增「訊息」分頁，將 CH0~CH3 文字封包以 `message-log.jsonl` （`app.getPath('userData')/callmesh/`）保存並自動復原，預設每頻道保留 200 筆，並顯示來源節點、跳數與最後轉發節點。
- **訊息來源名稱對齊節點資料庫**：儲存的文字訊息會帶入節點 Mesh ID，重新載入時會回查節點資料庫補齊長短名，避免僅顯示 Mesh ID。
- **最後轉發推測升級**：`meshtasticClient` 會比對 `relay-link-stats.json` 與節點資料庫，若韌體僅回傳尾碼則使用歷史 SNR/RSSI 推測完整節點並產生說明字串。
- **Relay 提示 UI**：CLI/Electron/Web 均以圓形 `?` 按鈕提示推測結果；桌面與 Web 啟用半透明 Modal 顯示推測原因、候選節點與 Mesh ID。
- **TENMANMAP 轉發管線**：`CallMeshAprsBridge` 會以 WebSocket 將位置封包上傳至 TENMANMAP 服務，預設全數節點皆轉發，可透過環境變數 `TENMAN_DISABLE=1` 或 CLI 旗標 `--no-share-with-tenmanmap` 全域停用；桌面版設定頁亦提供低調的分享開關；佇列、驗證與自動重連機制維持啟用。
- **訊息距離顯示**：訊息分頁會根據節點資料庫座標與最後更新時間，顯示距離（km／m）與位置更新時間差（例如 `22.9 km (3 分鐘前)`）。
- **遙測時間戳統一**：所有 Telemetry 紀錄寫入時都會以收包當下的時間 (`timestampMs`) 為準，同步更新 `sampleTime*` 與 `telemetry.time*` 欄位，避免裝置 RTC 漂移造成前端區間掛零。
- **CLI 旗標**：預設關閉 Web UI；若需啟動可加上 `--web-ui`。Electron 亦可透過設定頁切換，或以 `TMAG_WEB_DASHBOARD` 強制指定。
- **連線設定即時套用**：Electron 設定頁調整 TCP/Serial 模式、主機位址或 Serial 裝置後，會即時觸發重連並沿用更新後參數，無需手動點擊「連線」。
- **Serial 自動重連對齊**：Serial 連線現在使用與 TCP 相同的自動重連、閒置檢測與錯誤回復流程；中斷時會進入同一套 `startReconnectLoop()` 機制。
- **TenManMap 分享偏好持久化**：CLI (`--no-share-with-tenmanmap`)、環境變數 (`TENMAN_DISABLE=1`) 與 GUI 勾選會寫入偏好檔並呼叫 `setTenmanShareEnabled()`，即時控制資料是否同步至 TenManMap 與其合作夥伴。

## 1. 專案定位與核心價值

- 以 **Node.js** 打造的 **Meshtastic TCP Stream 監控工具**，同時提供 **CLI**、**Electron 桌面應用**與 **輕量級 Web Dashboard**。
- 整合 **CallMesh 後台**：API Key 驗證、Heartbeat、Mapping/Provision 同步，以及 APRS Gateway。
- 核心精神：「**CallMesh API Key 是唯一開機鑰匙**」。未通過驗證時，CLI / GUI / Web 介面一律鎖定、不進行任何連動。

整體資料流：

```
Meshtastic 裝置 (TCP) ──► MeshtasticClient
                            │
                            ▼
                     CallMeshAprsBridge ──► CallMesh API
                            │                  │
                            │                  └─► Heartbeat / Mapping / Provision
                            ▼
             Electron Renderer / CLI / Web Dashboard
                            │
                            └─► APRS-IS (uplink / telemetry)
```

---

## 2. 目錄結構與角色

```
CMClient/
├── src/
│   ├── index.js               # CLI 入口，包裝 Meshtastic ↔ CallMesh ↔ APRS 流程
│   ├── meshtasticClient.js    # Meshtastic TCP 監聽/解碼，輸出 summary 與 fromRadio
│   ├── callmesh/
│   │   ├── client.js          # CallMesh API 包裝：heartbeat、mapping、provision 等
│   │   └── aprsBridge.js      # 共用橋接層：CallMesh ↔ APRS ↔ Meshtastic，CLI 與 GUI/Web 共用
│   ├── aprs/client.js         # APRS-IS TCP Client：登入、keepalive、重試機制
│   ├── discovery.js           # mDNS 掃描 `_meshtastic._tcp`
│   └── electron/
│       ├── main.js            # Electron 主行程：初始化、IPC、啟動 Web Dashboard
│       ├── preload.js         # Renderer 可用之安全 API
│       ├── renderer.js        # 桌面儀表板：封包表、遙測、Mapping Flow、設定、Log UI
│       ├── index.html         # 桌面 UI 結構
│       └── styles.css         # 桌面 UI 樣式
├── src/nodeDatabase.js        # 節點資料庫：統一儲存 Mesh 節點長短名、型號、角色、最後出現時間
├── src/web/
│   ├── server.js              # 輕量 Web 服務 (HTTP + Server-Sent Events)
│   └── public/
│       ├── index.html         # Web Dashboard UI
│       ├── main.js            # Web 前端邏輯：SSE 訂閱、封包/狀態展示
│       └── styles.css         # Web UI 樣式
├── docs/
│   ├── callmesh-client.md     # CallMesh API 規格與流程說明
│   └── handover.md            # 交接文件（本檔）
├── README.md                  # 快速使用說明（CLI / Electron）
├── package.json               # 專案設定、腳本（版本號維護於此）
└── package-lock.json
```

---

## 3. 模組詳解

### 3.1 Meshtastic 客戶端 (`src/meshtasticClient.js`)
- 使用 `net.createConnection` 與 Meshtastic 節點進行 TCP 連線，解碼 Protobuf (`proto/meshtastic/*.proto`)。
- 對外事件：
  - `connected` / `disconnected`
  - `summary`：桌面與 Web 儀表板使用的封包摘要
  - `fromRadio`：原始 Protobuf 轉 JSON
  - `myInfo`：自家節點資訊 (`meshId`、使用者設定)
- 內建：
  - 重複封包檢查 (`_seenPacketKeys`)
  - Heartbeat / WantConfig，確保連線穩定
  - 解析 Telemetry payload，將 `batteryLevel`、`voltage`、`channelUtilization`、`airUtilTx` 等量測轉換為 `summary.telemetry`（包含 `kind`、時間戳與原始 `metrics`），供 `CallMeshAprsBridge` 及前端儲存與顯示。
  - `_formatNode()` 會同時輸出 `shortName`、`longName`、`hwModel`、`role`，並交由 Bridge 回寫到節點資料庫，確保 CLI / GUI / Web 呈現一致的節點資訊。
  - 針對 `relay_node` 僅回傳尾碼的情境，`_normalizeRelayNode()` 會整合 `nodeMap`、節點資料庫 (`nodeDatabase.list()`)、`relay-link-stats.json` 的歷史 SNR/RSSI：
    1. 先彙整所有尾碼符合的候選節點；
    2. 以歷史樣本計算差距，選出最接近者；
    3. 回傳 `guessed=true` 並產生 `relayGuessReason`（缺樣本時列出候選清單），供前端顯示問號提示。
  - 補上一系列工具函式（`formatRelativeAge()`、`formatHexId()` 等），協助在 CLI/UI 呈現推測原因與時間差。

### 3.2 CallMesh ↔ APRS Bridge (`src/callmesh/aprsBridge.js`)
- **單一事實來源**：CLI、Electron、Web 端都透過此 Bridge 操作 CallMesh、APRS 與節點資料。
- 功能：
  - 驗證 API Key → Heartbeat → Mapping / Provision 同步
  - 持久化 artifacts (`~/<userData>/callmesh/` / `~/.config/callmesh/`)
  - APRS-IS 連線管理：登入、keepalive、斷線重試
  - Beacon/Telemetry 排程、CallMesh degraded 模式
  - Flow 管理：為每個 `summary` 製作 `flowId`，APR S 上傳後透過 `aprs-uplink` 事件回報
  - 節點資料庫整合：
    - 所有 `nodeInfo`、`myInfo` 與 `summary` 內的節點欄位會寫入 `nodeDatabase`，統一記錄長短名、型號、角色與最後出現時間；
    - 透過 `node`、`node-snapshot` 事件推播給 Electron / Web Dashboard，確保多個介面共用同一份節點資訊。
  - 遙測資料庫：
    - 所有含 `summary.telemetry` 的封包均寫入 `telemetry-records.jsonl`（一行一筆 JSON），並同步更新記憶體快取；
    - 事件透過 `bridge.emit('telemetry')` 推播給 Electron / Web Dashboard，類型分為 `append` 與 `reset`；
    - 預設每節點僅保留 500 筆最新紀錄（避免佔用過多記憶體），但 JSONL 會完整累積，以便跨重啟保留歷史；
    - 同步回傳 `stats`（筆數、節點數、JSONL 檔案大小），前端可直接顯示。

### 3.3 節點資料庫 (`src/nodeDatabase.js`)
- 採用 `Map` 快取 Mesh 節點資訊，索引鍵為正規化後的 `meshId`（`!xxxxxxxx`）。
- 儲存欄位：`shortName`、`longName`、`hwModel`、`role`、原始 Mesh ID、最後出現時間；同時生成 `label` 供 UI 直接顯示。
- `CallMeshAprsBridge` 為唯一寫入入口，會在收到 `nodeInfo`、`myInfo` 或 `summary` 時更新節點並廣播 `node` 事件。
- 提供 `getNodeSnapshot()` 供 CLI/Electron/Web 取得一次性列表；Electron 啟動、Web SSE 初始連線時會送出 `node-snapshot`。
- 若需持久化，可自行在 Bridge 初始化時擴充序列化流程，目前預設為記憶體常駐。

### 3.4 CLI (`src/index.js`)
- 指令：
  - `node src/index.js` (或 `npm start`) → 監看 Meshtastic，需先設定 `CALLMESH_API_KEY`
  - `node src/index.js discover` → mDNS 掃描 `_meshtastic._tcp`
- 功能與桌面版一致，使用 Shared Bridge:
  - Session 內自動 Heartbeat & Mapping 同步
  - APRS 上傳與計數
- 重要環境變數：
  - `CALLMESH_API_KEY`、`CALLMESH_ARTIFACTS_DIR`、`CALLMESH_VERIFICATION_FILE`
  - `TMAG_WEB_DASHBOARD=0`（禁用 Web Dashboard，CLI 情境可忽略）
- 指令列旗標：
  - `--web-ui` 可強制啟用內建 Web Dashboard；若無帶入則沿用偏好與環境變數設定。
  - `--no-share-with-tenmanmap` 可停用 TenManMap 分享，會覆寫環境變數與 GUI 設定。

### 3.5 Electron 主行程 (`src/electron/main.js`)
- 建立 `BrowserWindow`、註冊 IPC handler、啟動 MeshtasticClient 與 CallMeshAprsBridge。
- 自動啟動 **Web Dashboard Server (`WebDashboardServer`)**：
  - 預設 `http://0.0.0.0:7080` (可用 `TMAG_WEB_HOST` / `TMAG_WEB_PORT` 調整)
  - `TMAG_WEB_DASHBOARD=0` 可禁用
- `meshtastic:*`、`callmesh:*`、`aprs:*`、`app:*` IPC 入口都集中於此。
- 注意：關閉應用或 IPC 錯誤時，務必呼叫 `cleanupMeshtasticClient()`、`shutdownWebDashboard()` 避免殘留連線。
- `callmesh/bridge` 會在背景將節點快照持久化至 `CALLMESH_ARTIFACTS_DIR/node-database.json`，採 Debounce 寫入；清除 node DB 時記得同時刪除該檔案並重新推播節點快照。
- 文字訊息封包（`summary.type === 'Text'`）會透過 `persistMessageSummary()` 寫入 `message-log.jsonl`（路徑：`<userData>/callmesh/message-log.jsonl`），每個頻道最多保留 200 筆；紀錄內容包含當下節點快照（長短名、Mesh ID），啟動時呼叫 `loadMessageLog()` 會回查節點資料庫補齊顯示名稱。前端可透過 `messages:get-snapshot` IPC 取得整份快照。
- 寫入採用同步序列排程（`messageWritePromise`），確保大量訊息時仍會依序刷新檔案；結束應用前（`before-quit`）會嘗試 flush 一次，避免遺失最後訊息。
- `updateClientPreferences()` 現已接受 `shareWithTenmanMap`，會即時呼叫 `bridge.setTenmanShareEnabled()`；若設定為 `null` 則回復環境變數預設。

### 3.6 Electron Renderer (`src/electron/renderer.js`)
- 主要分頁：
  1. **監視**：封包表與計數（10 分鐘封包 / APRS 上傳 / Mapping 節點）。節點名稱會套用節點資料庫資料；若最後轉發為推測結果，欄位會顯示圓形 `?` 按鈕，點擊後使用內建 Modal 呈現推測原因、候選節點與 Mesh ID。
  2. **訊息**：左側列出 CH0~CH3 頻道，支援未讀標記與快速切換；右側顯示訊息內容、來源節點、跳數與最後一跳摘要。初始化會呼叫 `getMessageSnapshot()` 載入 `message-log.jsonl` 的快取，並對每筆文字封包進行去重（以 `flowId` 為主）與上限裁切（預設 200 筆／頻道）；來源欄位會優先顯示節點長名稱／短名稱，若僅有 Mesh ID 會回查節點資料庫再填入。
     - 若節點資料庫有座標資訊，訊息尾端會顯示距離與最後更新時間差，例如 `22.9 km (3 分鐘前)`；距離以 Provision 座標為基準計算。
  3. **遙測數據**：Chart.js 畫面與資料表，可依節點、時間範圍、指標模式切換；節點輸入框整合了 datalist 與搜尋，鍵入 Mesh ID、暱稱或任意關鍵字即可切換節點或直接套用全域篩選，輸入清空時會自動還原到最近選取節點並顯示完整資料；頁面右上角顯示「筆數 / 節點 / 檔案大小」統計並提供「清空遙測數據」按鈕。
  4. **Mapping 封包追蹤**：具 Mapping 的位置封包列表，支援搜尋、狀態篩選與 CSV 匯出；節點資訊與 APRS 狀態會即時更新。
  5. **設定**：設定 Meshtastic Host、CallMesh API Key、APRS Server、信標間隔，並可切換是否啟用 Web UI。
     - 連線模式、主機欄位與 Serial 裝置現在具備 **即時套用**；修改後會透過 `scheduleConnectionApply()` 觸發重連，不需再按「連線」。
     - 「允許與 TenManMap 及合作夥伴分享資料」為預設開啟的低調開關；取消勾選時會呼叫 `savePreferences()` 與 `updateClientPreferences()`，進而更新 `CallMeshAprsBridge.setTenmanShareEnabled(false)`。
  6. **資訊**：顯示 CallMesh Provision 詳細資料（呼號、座標、PHG 等）。
  7. **Log**：顯示 Meshtastic / CallMesh / APRS / APP 事件，支援搜尋與匯出。
- **節點資料庫分頁**：顯示目前快取節點、座標（緯度／經度／高度）、線上狀態與距離資訊。
  - 表格以最後出現時間由新到舊排序，線上統計以「一小時內更新」為準並同步顯示「符合條件 / 總線上」。
  - 搜尋框支援名稱、Mesh ID、型號、角色與 Label 模糊匹配；結果會同步影響統計與表格內容。
  - 座標欄位會顯示 `lat, lon[, 高度]`；可直接以座標片段或 Mesh ID 搜尋對應節點。
- 節點事件：Renderer 會接收 `node`、`node-snapshot` 事件，更新本地節點快取並同步至封包表、Flow 追蹤與 Telemetry 表格。
- 遙測處理：
  - 初始化時透過 `getTelemetrySnapshot()` 與 `getNodeSnapshot()` 建立快取；
  - `telemetry:update` 事件附帶統計數據，直接反映在頁面統計區；
  - 每個節點最多保留 500 筆資料，Chart/Table 依快取渲染，並提供單一指標模式。
- Flow 追蹤資料與 APRS 狀態會與節點資料庫共用節點名稱與型號資訊。
- 圖表使用 `chart.js`（`node_modules/chart.js/dist/chart.umd.js`），若升級版本請重新打包並驗證。

### 3.7 Web Dashboard (`src/web`)
- **伺服器 (`src/web/server.js`)**
  - HTTP 靜態資產 + `GET /api/events` (Server-Sent Events)。
  - 推播事件：`status`、`callmesh`、`summary` / `summary-batch`、`log`、`log-batch`、`aprs`、`metrics`、`self`、`telemetry-*`、`node`、`node-snapshot`。
  - 伺服器端維護節點資料庫：收到 `node` 事件或 Telemetry/summary 內含節點資訊時即時更新，初次連線會先送完整快照。
  - 遙測資料庫同樣保留 500 筆/節點，並在 `telemetry-snapshot/append/reset` 事件中附帶統計（筆數、節點數、磁碟大小）。
  - 計數邏輯與 Electron 對齊：`packetLast10Min`、`aprsUploaded`、`mappingCount`。
- **前端 (`src/web/public/main.js`)**
  - 監視頁一次呈現 CallMesh Provision、封包表與 APRS 狀態，節點欄位會套用節點資料庫資訊。若最後轉發為推測，欄位會顯示圓形 `?` 按鈕，點擊後以自訂 Modal 顯示推測原因、候選節點與 Mesh ID。
  - 遙測頁提供節點篩選、時間範圍、圖表模式切換，以及「筆數 / 節點 / 檔案大小」統計；清空遙測資料會觸發 `telemetry-reset`。
  - Mapping 封包追蹤支援搜尋、狀態篩選與節點 metadata（長名稱、模型、角色）；APRS 上傳狀態與 CLI 保持一致，Flow 列表同樣支援點擊 `?` 按鈕呼出 Modal 說明。
  - 訊息分頁與桌面版同步：會從節點資料庫補齊「來自」欄位的長名稱，歷史訊息也會在載入時回查 Mesh ID 映射。
  - `node`、`node-snapshot` 事件會同步更新節點暱稱與型號，Flow/Telemetry/封包表都會受益。
  - 節點頁同樣新增座標欄與距離欄，搜尋可使用座標片段或節點暱稱。
- Web Dashboard 可獨立瀏覽 (`npm run desktop` 後開 `http://localhost:7080`)，但不另提供設定 UI，所有設定仍在 Electron/CLI。

### 3.8 遙測資料庫（Telemetry Archive）
- 遙測摘要（`summary.telemetry`）會由 `CallMeshAprsBridge` 寫入 `storageDir/telemetry-records.jsonl`，採 **JSON Lines** 形式持久化，重啟後仍能恢復歷史紀錄。
- 新資料會同步保留在記憶體的節點快取中，每個節點最多保存 **500** 筆最新紀錄，超出時會淘汰最舊資料；刪除資料或按下「重置所有資料」時會一併移除 JSONL 檔。
- Telemetry 事件會透過 IPC/SSE 推播至 Electron Renderer 與 Web Dashboard：
  - `telemetry:type=append`：即時新增單筆並附上節點資訊；
  - `telemetry:type=reset`：資料被清除時，通知前端刷新。
- 所有遙測封包（包含自家節點）都會寫入資料庫，並以 **收到封包當下的時間** 標記 `timestampMs` / `sampleTimeMs` / `telemetry.timeMs`，避免裝置 RTC 漂移導致前端找不到資料。
- 目前支援的量測包含 `batteryLevel`、`voltage`、`channelUtilization`、`airUtilTx`、`temperature`、`relativeHumidity`、`barometricPressure` 等（其餘欄位會以 `metrics.*` 原樣保留）。
- 事件 payload 會附帶統計資訊：`totalRecords`、`totalNodes`、`diskBytes`。GUI 與 Web 直接顯示，不需自行計算。
- Telemetry 記錄內的節點欄位會與節點資料庫合併，確保長名稱及模型資訊一致。
- GUI 端節點輸入框同時充當搜尋欄位：輸入任意關鍵字時會保留原先節點選擇並使用文字篩選 Chart/Table；選取 datalist 項目則會直接切換節點並重置搜尋。
- 若需校正舊檔（早期使用裝置回報時間的紀錄），可執行 `node scripts/fix-telemetry-timestamps.js <path/to/telemetry-records.jsonl>` 將所有 `sampleTime*` / `telemetry.time*` 欄位同步到相對應的 `timestampMs`，原檔會留下 `.bak` 備份。

### 3.9 節點資料庫（Node Database）
- 透過 `src/nodeDatabase.js` 單例集中管理節點資訊，Electron / Web / CLI 共用。
- CallMeshAprsBridge 會將節點快照持久化至 `storageDir/node-database.json`（同 `CALLMESH_ARTIFACTS_DIR`），重啟後會還原舊資料並自動清洗無效座標或 `unknown` 名稱。
- 持久化內容包含：
  - `meshId` / `meshIdOriginal` / 長短名稱 / Label
  - 解析後的硬體型號與角色（使用 proto enum 映射）
  - 最後出現時間（毫秒）、最後一次位置資訊（緯度/經度/高度）
  - 透過 CallMesh Provision 座標可計算與本地的距離；若座標無效或為 (0,0) 會自動忽略。
- 會忽略 `!abcd****` 這類暫存 Mesh ID，避免測試節點被納入統計；前端節點表亦會排除。
- `node-database.json` 以及遙測 JSONL 均可透過節點資料庫分頁的「清除節點資料庫」按鈕、或 `callmesh:clear` 流程一併刪除。

---

## 4. GUI / Web 儀表板計數方式

| 指標             | 計算方式                                                         |
| ---------------- | ---------------------------------------------------------------- |
| **10 分鐘封包** | 以 60 秒桶 (`PACKET_BUCKET_MS`) 統計 10 分鐘 (`PACKET_WINDOW_MS`) 內非自家節點封包數 |
| **已上傳 APRS** | FlowId 去重；每次 APRS uplink 事件 (`aprs-uplink` / SSE `aprs`) 會累加一次 |
| **Mapping 節點**| Mapping items 依 `mesh_id` 正規化後去重                           |

以上邏輯在 Electron 與 Web 端均對齊；若 GUI 與 Web 數值不同，請確認：
- 是否同時啟動多個實例（會競爭 APRS uplink flowId）
- Web 是否被 `TMAG_WEB_DASHBOARD=0` 禁用
- Meshtastic 節點是否重複推送自家封包（GUI/Web 均會濾掉）

---

## 5. 設定與環境變數

| 變數                         | 預設值 / 說明                                                   |
| ---------------------------- | ---------------------------------------------------------------- |
| `CALLMESH_API_KEY`           | CallMesh 驗證用。未設定時 CLI/GUI/WEB 一律鎖定                   |
| `CALLMESH_ARTIFACTS_DIR`     | CLI 模式下的 CallMesh artifacts 目錄 (`~/.config/callmesh/`)       |
| `CALLMESH_VERIFICATION_FILE` | CLI 驗證快取檔；未指定時預設 `~/.config/callmesh/monitor.json`     |
| `TMAG_WEB_HOST` / `PORT`     | Web Dashboard 服務位置，預設 `0.0.0.0:7080`                      |
| `TMAG_WEB_DASHBOARD`         | 設為 `0` 可禁用 Web Dashboard（不啟動 HTTP/SSE）                 |
| `MESHTASTIC_HOST` / `PORT`   | 若未提供 CLI 參數，Electron 設定頁或 CLI Flag 需手動輸入           |

Electron 會將 CallMesh 驗證資訊與 artifacts 存於 `~/Library/Application Support/<app>/callmesh/`（macOS），Windows/Linux 對應 OS 預設資料夾；同一目錄下的 `message-log.jsonl` 為桌面版訊息紀錄檔，每個頻道預設最多保留 200 筆。

---

## 6. 常用操作

### 6.1 開發環境啟動

```bash
npm install
npm run desktop        # 啟動 Electron + Web Dashboard
# 或
npm start              # CLI 模式
```

### 6.2 Web Dashboard

1. 啟動 `npm run desktop`
2. 開瀏覽器至 `http://localhost:7080`
3. 若需禁用：`TMAG_WEB_DASHBOARD=0 npm run desktop`

### 6.3 CLI

```bash
export CALLMESH_API_KEY="xxxxx"
node src/index.js --host 192.168.1.50 --port 4403

node src/index.js discover           # 掃描 Meshtastic 裝置

# 啟動內建 Web Dashboard（預設關閉）
node src/index.js --host 192.168.1.50 --port 4403 --web-ui

# Serial 連線（以 /dev/ttyUSB0 為例，預設 115200 bps）
node src/index.js --connection serial --serial-path /dev/ttyUSB0 --serial-baud 115200
# 或可直接透過 host 指定 serial:// 路徑（同樣採 115200 bps，若需其他速率再帶 --serial-baud）
node src/index.js --host serial:///dev/ttyUSB0 --web-ui
```

- Electron 設定頁的「連線模式」可切換 TCP/Serial，Serial 模式可從清單選擇偵測到的裝置或手動輸入 `serial:///` 路徑。

### 6.4 遙測與節點資料

- Electron 遙測頁整合了節點輸入與搜尋：在輸入框選擇 datalist 項目可直接切換節點，輸入其他關鍵字則會即時篩選圖表與資料表；搜尋欄位清空時會回到最近選取節點（無紀錄時回退到第一筆）並展示全部資料。
- Web Dashboard 遙測頁提供相同的節點快照與統計資訊；首次連線會先收到節點快照與最新遙測資料。
- 節點資料庫分頁支援模糊搜尋與線上節點統計（預設視為 1 小時內更新），距離會以 Provision 座標為基準計算；表格顯示的筆數與線上數會在使用搜尋時同步標示「符合 / 總數」。座標欄會顯示 `lat, lon[, 高度]`，可直接用座標片段搜尋。
- 節點長名稱、型號等資訊由 `nodeDatabase` 推播，CLI / GUI / Web 顯示一致；若需要擴充欄位，請從 Bridge emit 的 `node` 事件開始串接。
- 所有節點快照會持久化於 `CALLMESH_ARTIFACTS_DIR/node-database.json`，同 `telemetry-records.jsonl` 一樣可透過節點分頁的「清除節點資料庫」或 `callmesh:clear` IPC 重新初始化。

### 6.5 訊息頻道

- Electron 「訊息」分頁會顯示 CH0~CH3 文字封包，並記錄在 `message-log.jsonl`（`~/Library/Application Support/<app>/callmesh/`，Windows/Linux 依 OS 對應路徑）。
- 每個頻道預設保留最近 200 筆，若需清空可在離線狀態下刪除 `message-log.jsonl` 後重新啟動桌面程式。
- 支援未讀標記：切換頻道後會清除該頻道的未讀狀態；訊息清空或檔案刪除後會自動重建。
- 來源欄會優先顯示節點長名稱／短名稱；若僅存 Mesh ID，載入時會回查節點資料庫補齊暱稱（失敗時才退回 Mesh ID）。

---

## 7. 專案架構與模組細節

以下整理專案組成、資料流與模組責任，提供維護時的快速索引。

### 7.1 整體資料流

```
Meshtastic（TCP / Serial）
        │
        ▼
MeshtasticClient（封包解碼、節點快取、relay 估算）
        │ summary / myInfo / fromRadio
        ▼
CallMeshAprsBridge ──► CallMesh API（Heartbeat / Provision / Mapping）
        │                               │
        │                               └─► APRS-IS 上傳（含 TENMANMAP 佇列）
        │
        ├─► CLI 輸出（表格 / JSON）
        ├─► Electron 主行程（IPC）──► Renderer UI（設定、監控、訊息、遙測）
        └─► WebDashboardServer（SSE）─► 瀏覽器端 Dashboard
```

- 所有通路（CLI / Electron / Web）都透過 `CallMeshAprsBridge` 共享狀態，確保資料一致。
- `MeshtasticClient` 支援 TCP 與 Serial；Serial 採 `serialport` 套件，預設鮑率 115200，可由 CLI flag 或 GUI 設定覆寫。
- Relay 尾碼補全：若韌體僅回傳 `relay_node` 尾碼（常見情況），會藉由 routing 封包、節點資料庫與歷史 RSSI/SNR 建立尾碼對應表，必要時標示為 `relayGuess`。

### 7.2 主要檔案與職責

| 檔案 / 資料夾 | 職責與重點 |
| ------------ | ---------- |
| `src/index.js` | CLI 入口；處理命令列參數（連線方式、API Key、Web UI 等），負責初始化 `MeshtasticClient` 與 `CallMeshAprsBridge`，並管理重連、訊號處理（SIGINT）。 |
| `src/meshtasticClient.js` | Meshtastic 客戶端，負責：連線（TCP/Serial）、封包解析（protobuf）、節點快取、重複封包過濾、relay 猜測與尾碼映射、心跳與 keep-alive、summary 事件發出。 |
| `src/callmesh/client.js` | CallMesh API 包裝：驗證、心跳、mapping、provision、TENMANMAP 追蹤等；對服務降級與認證失敗提供統一錯誤處理。 |
| `src/callmesh/aprsBridge.js` | 負責整合 Meshtastic summary 與 CallMesh／APRS 流程，包含：Mapping／Provision 資料同步、APRS/TENMANMAP 上傳節流、telemetry append/reset、節點資料庫維護、flow tracking。 |
| `src/nodeDatabase.js` | Mesh 節點資料庫單例：提供增刪查改、持久化 (`node-database.json`)、查詢距離／角色／硬體等功能。 |
| `src/web/server.js` | 簡易 Express + SSE 伺服器：對外提供儀表板頁面 (`src/web/public`) 與即時事件。 |
| `src/electron/main.js` | Electron 主行程：建立視窗、IPC handler（連線、偏好、節點、遙測、Web Dashboard 控制）、啟動 `WebDashboardServer`，並串接訊息持久化。 |
| `src/electron/renderer.js` | Renderer 腳本：UI 互動、Auto reconnect、Serial 裝置列表、偏好儲存、節點/遙測/訊息渲染、Log 管理。檔案較大，擴充時請留意共用 helper。 |
| `scripts/` | `run-electron.js`、`build-*.js`、`fix-telemetry-timestamps.js` 等開發/維運腳本。 |
| `meshtastic/` | 韌體參考資料（config、board 定義等），主要作為測試與範例。 |

### 7.3 重要事件與資料結構

- `MeshtasticClient` 事件：
  - `connected` / `disconnected`：連線狀態。
  - `summary`：封包摘要（時間、節點、relay、SNR/RSSI、hop、detail）。
  - `fromRadio`：原始 `FromRadio` protobuf。
  - `myInfo`：本機節點資訊。
- `CallMeshAprsBridge` 事件：
  - `state`：API 驗證、心跳狀態（含 degraded）。
  - `node`、`telemetry`：節點／遙測更新。
  - `aprs-uplink`：APRS 上傳回報。
  - `log`：橋接層內部 log，Electron/Web 會呈現於 Log 頁。
- Electron IPC：
  - `meshtastic:connect`／`disconnect`、`discover`、`list-serial`。
  - `callmesh:save-key`／`reset`、`nodes:get-snapshot`、`telemetry:get-snapshot`、`web:set-enabled` 等。
- SSE Topic（WebDashboardServer）：
  - `status`、`summary`、`node`、`telemetry`、`aprs`、`log`。

### 7.4 連線模式與偏好

- 連線模式儲存在 localStorage 與 `client-preferences.json`，欄位包括 `host`、`connectionMode`、`serialPath`、`webDashboardEnabled`。
- Serial 模式：
  - 預設鮑率 115200；可自 CLI `--serial-baud` 或 Electron 設定覆寫。
  - Electron 會列舉 Serial 裝置 (`SerialPort.list()`)，選擇後自動帶入 `serial:///` 路徑。
  - `MeshtasticClient` 會將 routing 封包中的完整節點 ID 與 `relay_node` 尾碼對照，避免 Summary 一律顯示本機。
- TCP 模式：
  - `host` 預設 127.0.0.1、port 4403；可於 CLI / GUI 設定。
  - Keep-alive、idle timeout 依 CLI / UI 預設（15 秒心跳、90 秒 idle）。

### 7.5 Relay 尾碼推測

- 韌體常回傳尾碼（`relay_node=0xfc`），需要補全：
  1. `routing` 封包中的 `route` / `routeBack` 會先記錄完整 meshId → 尾碼對應。
  2. `nodeDatabase` 及歷史 relays (`_relayLinkStats`) 會視情況補足候選。
  3. 若仍無法唯一判定，summary 會標示 `relayGuess=true`，在 CLI / GUI 顯示推測說明。
- 若尾碼只對應到本機，會直接視為「直收」並清空 `summary.relay`。

### 7.6 資料持久化

- `CALLMESH_ARTIFACTS_DIR`（預設 `~/.config/callmesh/`）：
  - `monitor.json`：CallMesh API 驗證結果、心跳時間。
  - `node-database.json`：節點快照。
  - `telemetry-records.jsonl`：遙測資料。
  - `relay-link-stats.json`：relay 猜測歷史（SNR/RSSI）。
  - `message-log.jsonl`：桌面版訊息紀錄。
- WebDashboard Snapshots：啟動時會以 `seed*` 方法注入節點、遙測、訊息快照；重新整理頁面也會取得最新資料。

### 7.7 建議維運檢查表

1. **連線流程**
   - 驗證 API Key 成功（橋接層 log 顯示 `verify success`）。
   - Meshtastic 連線後有 `summary` log，SNR/RSSI 等資訊如期更新。
   - Serial → TCP 切換時，偏好資料與 UI placeholder 是否更新。
2. **Relay 顯示**
   - Serial 模式不應一直顯示本機 ID；若出現 `relayGuess`，應有合理原因。
   - 封包 `hops=0` 要顯示「直收」。
3. **節點資料庫**
   - 節點清單搜尋與線上統計（1 小時內）是否準確。
   - 距離計算需有 Provision 座標，若為 (0,0) 應被忽略。
4. **遙測／APRS**
   - Telemetry append/reset 是否同步至 GUI/Web。
   - APRS 上傳需觀察 `aprs-uplink` log 與 flow 狀態，確認速率與節流。
5. **Logs / 儀表板**
   - Electron Log 頁是否能依 Tag / 關鍵字篩選。
   - Web Dashboard SSE 是否收到 `status`、`summary` 等事件（可透過瀏覽器 DevTools 檢視）。

---

## 8. Build / Release 流程

### 8.1 GitHub Actions

分支 `main`/`dev` push 或 PR → 自動觸發：

| Workflow                     | 產物                                               |
| ---------------------------- | -------------------------------------------------- |
| Build macOS & Linux Targets | macOS、Linux GUI/CLI ZIP（各自打包）                |
| Build Windows App           | Windows GUI/CLI ZIP                                |

發版步驟：

1. `npm version <patch|minor>` → 推送 tag (ex: `v0.2.10`)
2. 推送 tag 後，GitHub Actions `Build & Publish Release` workflow 會自動：
   - 針對 macOS / Linux / Windows 編譯 GUI 與 CLI；
   - 壓縮成對應 ZIP；
   - 若 Release 尚未存在則建立，並直接上傳所有產物。
3. 若需手動觸發（例如補傳舊 tag），可在 Actions → **Build & Publish Release** 使用 `Run workflow`，輸入 `release_tag`（例如 `v0.2.18`）；必要時可使用 `gh workflow run "Build & Publish Release" --ref main -f release_tag=v0.2.18`。
4. 若要額外附檔或修改說明，再使用 `gh release edit` 或 GitHub UI 調整即可。

### 8.2 本地打包

僅作備援（仍以 Actions 產物為主）：

```bash
npm install
npm run build:mac-cli
npx electron-packager . "TMAG Monitor" --platform=darwin --arch=x64 ...
npm run build:win
npx electron-packager . "TMAG Monitor" --platform=linux --arch=x64 ...
npx pkg src/index.js --targets node18-linux-x64
```

---

## 9. 開發注意事項與 QA

1. **API Key 流程**
   - 首次啟動需在設定頁輸入 Key；驗證成功才會解鎖 UI 與 CLI。
   - CallMesh 回傳 degraded 仍允許使用快取 Provision 與 Mapping。
   - `Reset Data` 會清除 API Key、Mapping/Provision artifacts 及 localStorage。

2. **Web Dashboard / Electron UI**
   - 需確保 `src/web/server.js`、`src/web/public/main.js` 與 `src/electron/renderer.js` 的節點快照、遙測統計與計數邏輯保持同步。
   - 若新增事件或 UI 欄位，請同步擴充 Bridge (`telemetry` / `node` 事件 payload) 及三端 renderer 的處理函式。
   - 遙測節點輸入框同時負責下拉選單與搜尋，調整時需維護 `refreshTelemetrySelectors()` / `handleTelemetryNodeInputChange()` 的對應行為與 datalist 更新。

3. **APR S Flow**
   - Flow 以 `flowId` 辨識（`summary.timestampMs + random`）；APR S uplink 後 `aprs-uplink` 事件會附帶 flowId，GUI/Web 用以染色、顯示呼號。
   - 避免同時啟動多個實例向同一 Meshtastic 上傳，避免 flowId 重複。

4. **計數驗證**
   - 若 web 與 GUI 計數不一致（特別是 10 分鐘封包），檢查是否有自家節點封包、時間戳異常（未同步 NTP 時可能造成桶計算漂移）。

5. **程式碼風格**
   - JS/Node 皆採現有程式風格（無額外 lint 工具）。若要導入 ESLint/Prettier，請先告知團隊。
   - `renderer.js` 體積較大，新增功能請盡量模組化為 helper，注意避免產生循環引用。

6. **版本號**
   - 每個功能變更需更新 `package.json` 版本號；確保 `package-lock.json` 同步。

7. **文件 / Log**
   - `docs/callmesh-client.md` 為 CallMesh API 參考文件，請保持更新。
   - `docs/handover.md`（本文件）需隨架構調整更新，避免資訊落差。

8. **測試建議**
   - 目前無自動化測試；請手動測試以下情境：
     - CallMesh Key 驗證成功 / 失敗 / degraded
     - Meshtastic 斷線 → 自動重連
     - APRS 成功登入、keepalive、上傳封包
     - Web Dashboard 開啟後是否與 GUI 數值一致
      - 節點資料庫搜尋、線上統計與距離是否合理（特別是 Provision 座標缺失或為 0,0 情境）
      - 遙測節點輸入框在「選取節點」與「純搜尋」兩種使用方式下渲染結果是否一致
9. **遙測資料**
  - 若需清空歷史記錄，請使用 GUI/Web 的「清空遙測數據」或手動刪除 `callmesh/telemetry-records.jsonl`；
  - 若需保留舊檔但將舊資料的時間戳對齊收包時間，可在專案根目錄執行 `node scripts/fix-telemetry-timestamps.js ~/.config/callmesh/telemetry-records.jsonl`（會產生 `.bak` 備份後再覆寫原檔）；
  - 調整每節點快取上限可修改 `CallMeshAprsBridge` 建構子參數 `telemetryMaxEntriesPerNode`；
  - 若要顯示額外統計欄位，請從 Bridge `getTelemetryStats()` 擴充，並同步更新 Electron/Web 的顯示邏輯。

---

## 9. 常見問題 (FAQ)

| 問題 | 說明 |
| ---- | ---- |
| Web UI 與 GUI 計數不同 | 確認是否濾掉自家節點、時間設定是否正確，或是否同時啟動多個實例。 |
| APRS 已上傳但未變色 | 檢查 Flow 是否有 `flowId`、`aprs-uplink` 是否回報。同時確認 `flowAprsCallsigns` 是否被清空。 |
| Web Dashboard port 被占用 | 調整 `TMAG_WEB_PORT`，或先停用其它應用；若需完全關閉 Web Dashboard，可設 `TMAG_WEB_DASHBOARD=0`。 |
| CallMesh Key 被鎖 | 使用設定頁「重置本地資料」或刪除 `~/<userData>/callmesh/` 目錄，再重新輸入 Key。 |
| 遙測圖表沒有資料 | 確認選擇的時間區間內有紀錄；若僅存在歷史資料，請切換至符合的日/週/月/年或自訂時間，或使用 `scripts/fix-telemetry-timestamps.js` 將舊紀錄對齊收包時間。 |
| Telemetry JSONL 長期成長 | 可定期歸檔或壓縮 `telemetry-records.jsonl`，必要時調整 `telemetryMaxEntriesPerNode` 限制。 |
| Chart.js 未載入 | 確保 `node_modules/chart.js/dist/chart.umd.js` 存在；若 `desktop` 包裝成可攜版，記得把整個 `node_modules/chart.js` 併入發佈資產。 |
| 節點名稱仍顯示 MeshID | 確認 `node` / `node-snapshot` 事件是否正常送達；若 API Key 尚未驗證或節點尚未回報 `nodeInfo`，會暫時只顯示 MeshID。 |
| 遙測統計未更新 | Bridge `telemetry` 事件未觸發或被攔截，可檢查 Renderer Console；統計資料由 Bridge 計算，前端僅顯示。 |

---

## 10. 後續建議

- 建立測試用 Meshtastic 模擬器（自動發送位置、訊息封包），便於 CI 或單機測試。
- 引入 ESLint/Prettier / TypeScript，降低 `renderer.js` 體積與維護成本。
- Web Dashboard 可增設登入驗證（目前無權限控管），避免網段內任意人存取。
- 規畫 REST / WebSocket（非 SSE）接口，讓第三方服務能重複利用 summary / mapping 資料。

---

## 版本註記

- **v0.2.13**：導入節點資料庫 (`nodeDatabase`)、推播 `node`/`node-snapshot` 事件，並在 GUI/Web 顯示遙測統計（筆數 / 節點 / 檔案大小）。
- **v0.2.10**：新增 Web Dashboard、同步 GUI 計數邏輯。
- 更早版本請參考 Git history。

--- 

交接到此，如需更多資訊請參考程式碼註解或 `docs/callmesh-client.md`。祝接手順利！  
