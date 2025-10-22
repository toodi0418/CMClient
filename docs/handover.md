# Meshtastic Monitor & CallMesh Client 整合總覽

> 本文件用來交接專案背景、架構與作業指引，讓下一位開發者（Codex）可以「開箱即上手」。請務必完整閱讀，避免踩雷。

---

## 1. 專案定位

- 這是一個 **Meshtastic TCP Stream 監控器**，以 Node.js 撰寫，涵蓋 **CLI** 與 **Electron 桌面應用**。
- 在 Meshtastic 之外，還整合了 **CallMesh 管理後台**：
  - Heartbeat / Mapping API
  - API Key 驗證（同步於 CLI 與 Electron）
  - Key 未驗證時，整個系統必須鎖定。

一句話交接話術：
> 「這個工具是 Meshtastic 資料監控的瑞士刀，CallMesh API Key 就是唯一的開機鑰匙。沒有 Key，所有功能一律鎖死。幫我看緊這顆鑰匙。」

---

## 2. Repo 架構速覽

```
CMClient/
├── src/
│   ├── index.js             # CLI 入口（watch/discover/callmesh 三種模式）
│   ├── meshtasticClient.js  # TCP 封包解析與解碼 (protobuf 轉 JSON)
│   ├── callmesh/
│   │   ├── client.js        # CallMesh API 封裝（偵測平台 + Agent + heartbeat/mappings）
│   │   └── aprsBridge.js    # 共用的 CallMesh ↔ APRS 橋接模組（Electron/CLI 共用）
│   ├── aprs/client.js       # APRS-IS 客戶端（登入、keepalive、重連）
│   ├── discovery.js         # mDNS 搜尋 Meshtastic 裝置
│   └── electron/
│       ├── main.js          # Electron 主行程，負責 IPC、Key 驗證、Meshtastic 連線
│       ├── preload.js       # 提供 renderer 可調用的 API
│       ├── renderer.js      # 前端邏輯（表格、CallMesh overlay 等）
│       ├── index.html       # UI layout
│       └── styles.css       # UI 樣式
├── docs/
│   ├── callmesh-client.md   # CallMesh API 技術規範（agent、流程）
│   └── handover.md          # ← 本文件
└── README.md                # 使用說明（CLI/Electron/CallMesh）
```

---

## 3. 模組與元件細項

### 3.1 CLI (`src/index.js`)
- 使用 `yargs` 提供兩個指令：
  - 預設（`tmag-cli`）：連線監看 Meshtastic TCP 封包，並自動進行 CallMesh heartbeat、mapping/provision 同步與 APRS uplink。
  - `discover`：透過 mDNS (`discovery.js`) 掃描 `_meshtastic._tcp` 裝置。
- 透過環境變數或 CLI 參數取得 `CALLMESH_API_KEY` 與連線資訊；未驗證前會拒絕進入監看模式。
- 監控流程透過共用的 `CallMeshAprsBridge` 模組執行 CallMesh heartbeat、mapping/provision 落地、APRS uplink 與 Telemetry，行為與桌面版一致。CLI 預設將 artifacts 寫入 `~/.config/callmesh/`，可用 `CALLMESH_ARTIFACTS_DIR` 覆寫。

### 3.2 Meshtastic 客戶端 (`src/meshtasticClient.js`)
- 以 `net.createConnection` 建立 TCP 連線，解碼 protobuf (`proto/meshtastic/*.proto`)。
- 事件：
  - `connected` / `disconnected`：連線狀態改變。
  - `summary`：整理過的封包摘要（供 Electron UI 表格、圖表）。
  - `fromRadio`：原始 protobuf 轉 JSON 後的資料。
- 抗抖機制：維護 `_seenPacketKeys` 以避免重複訊息；自動處理 heartbeat、handshake。

### 3.3 CallMesh API (`src/callmesh/client.js`)
- 封裝 `fetch` 呼叫，統一處理 user-agent 與逾時。
- 提供 `heartbeat()`、`fetchMappings()` 等 API；同時解析 OS/arch 資訊。
- 回傳資料會被主行程記錄在 `~/<userData>/callmesh/mappings.json`、`provision.json`。

### 3.4 APRS 客戶端 (`src/aprs/client.js`)
- 純 TCP 實作，負責登入、keepalive（30 秒）、自動 reconnect。
- 對外事件：`connected`、`disconnected`、`line`（伺服器訊息）。
- `updateConfig()` 允許不關閉 UI 即時調整伺服器、呼號等設定。
- 連線成功會先送一筆 keepalive，再以固定間隔維持連線；若伺服器尚未回 `logresp … verified`，訊標與狀態封包都會暫停排隊，避免尚未驗證就上行。

### 3.5 CallMesh ↔ APRS Bridge (`src/callmesh/aprsBridge.js`)
- 將原本散落於 Electron 主行程的 CallMesh 狀態管理、artifact 持久化、APRS uplink 與 Telemetry 定時器封裝成 `CallMeshAprsBridge`。
- 透過事件介面 (`state` / `log` / `aprs-uplink`) 提供 UI 與 CLI 監聽：Electron 用來更新 renderer，CLI 則直接輸出到終端。
- 內建 heartbeat 迴圈、mapping/provision 同步、APRS 連線管理、beacon/telemetry 排程，以及 CallMesh degraded fallback（會回退到快取的 provision 並維持 APRS 線路）。
- 可自訂 artifacts 目錄：Electron 使用 `app.getPath('userData')/callmesh`，CLI 預設 `~/.config/callmesh/`，亦可由 `CALLMESH_ARTIFACTS_DIR` 覆寫。

### 3.6 Electron 主行程 (`src/electron/main.js`)
- 啟動流程：
  - 建立 `BrowserWindow`（`autoHideMenuBar=true`，同步移除預設選單）。
  - 根據 sentinel `.skip-env-key` 判斷是否讀取環境變數的 API key。
  - 初始化 `CallMeshAprsBridge` 並呼叫 `bridge.init()`（決定是否復原 artifacts）。
- CallMesh 驗證：
  - `callmesh:save-key`：透過 bridge 執行驗證，成功後清除 sentinel、寫入新的 artifacts 並啟動 heartbeat。
  - `callmesh:reset`：呼叫 bridge 清理 artifacts、刪除 sentinel、重置狀態。
- Meshtastic 連線：透過 IPC 指令 `meshtastic:connect` / `disconnect` 控制；失敗時有 30 秒節流的背景重試與 manual session。
- APRS Bridge：委派給 `CallMeshAprsBridge`，`state` 更新會同步到 renderer，`log` 與 `aprs-uplink` 事件亦直接轉發至 UI。

### 3.7 Electron Renderer (`src/electron/renderer.js`)
- 主要負責 UI 狀態：
- CallMesh overlay、設定頁、Log、封包表格與 10 分鐘分析圖。
- 監視頁開頭提供簡易儀表板：顯示 10 分鐘內封包總數、Mapping 節點已上傳 APRS 與目前 Mapping 節點數量，狀態異常時會以顏色提醒。監控表格新增「最後轉發」欄位，會依 hop 判斷顯示 `直收`、`Self`（本站台轉發）或實際節點名稱（缺完整 ID 時會以 `?` 提醒）。
- 「Mapping 封包追蹤」頁面：僅顯示具 mapping 的位置封包，可依搜尋欄即時篩選，並可用「全部 / 已上傳 APRS / 待上傳」下拉篩選；若封包同步至 APRS 會附上上傳內容，並保留 mapping 標籤、跳數資訊與「最後轉發節點」。
- Log 分頁提供 Tag 篩選與全文搜尋，可快速鎖定 CALLMESH、APRS 等特定訊息；複製按鈕會抓取已過濾的內容，方便匯出。
  - Reset 流程會清空 `localStorage`、通知主行程刪除 sentinel。
  - 初始啟動會詢問 `shouldAutoValidateKey()` 判斷是否自動帶入 localStorage 中的 API Key。
- 連線邏輯：
  - 自動連線：啟動後延遲 500ms 嘗試，失敗 3 次後停止。
  - 重連節奏：失敗時記錄在 rolling window，2 分鐘內連續 3 次失敗即暫停背景重試並提示使用者。

### 3.8 Windows 打包工具 (`scripts/build-win.js`)
- 純 Node 腳本，主要步驟：
  1. 下載對應版本的 Electron Windows runtime。
  2. 整理 staging 目錄，複製 `package.json` / `package-lock.json` / `src` / `proto`。
  3. 執行 `npm install --omit=dev` 安裝 production 依賴。
  4. 將 staging 的內容移入 `resources/app/`。
  5. 壓縮輸出為 `TMAG_Monitor-win32-x64.zip`，同時保留解壓後資料夾。

---

## 4. 執行流程

### 4.1 Electron 啟動
1. 主行程初始化 `callmeshState`（若存在 `.skip-env-key` 或環境變數自動判斷為禁止自動驗證）。
2. 載入 `index.html` 後，renderer 啟動 UI、載入 `localStorage` 偏好並決定是否自動驗證。
3. 若已有驗證紀錄：
   - 呼叫 `callmesh:save-key` → heartbeat → mapping → provision。
   - Meshtastic 連線成功後開始轉發封包。

### 4.2 CallMesh 驗證 & Heartbeat
1. 使用者在 overlay 或設定頁輸入 Key。
2. 主行程即刻送 `heartbeat` 驗證；成功後：
   - 寫入 `mappings.json`、`provision.json`。
   - 計算 APRS passcode，啟動 APRS 客戶端。
3. Heartbeat 每 60 秒執行一次，依序處理 mapping 更新、provision 更新與 APRS 同步。
4. 若 Heartbeat 失敗：
   - 若先前已成功驗證 → 進入 degraded 狀態，保留快取並持續 Meshtastic/APRS 功能。
   - 若屬於授權錯誤 → 清除 Key，立即鎖定 UI。

### 4.3 Meshtastic → APRS 橋接
1. `meshtasticClient` 接收封包後透過 `summary` 事件送往 renderer 與主行程。
2. 主行程依 mapping 決定呼號、符號、註解，以及 30 秒內的去重策略。
3. 組合 APRS payload（支援 overlay、table、code、heading/speed/altitude）；成功送出即寫回 log。

### 4.4 重置／離線行為
1. 使用者按「重置本地資料」：
   - Renderer 清空 `localStorage`，並呼叫 `callmesh:reset`。
   - 主行程刪除 `mappings.json`、`provision.json`、建立 `.skip-env-key`。
2. 關閉再啟動時，由於 sentinel 存在，系統不會讀取環境變數或舊 artifacts，保持鎖定狀態。
3. 離線時若曾有驗證紀錄，可保留快取（不要重置）；Heartbeat 失敗會優先搬出 `cachedProvision` 讓 APRS/Renderer 繼續使用。

---

## 5. 核心流程總結

### 5.1 Meshtastic TCP Monitoring
- 透過 `MeshtasticClient` 連線 `host:4403`，解碼 Meshtastic protobuf。
- CLI 預設 (`node src/index.js`) 顯示 summary 表格；可改 `--format json`。
- 每次接收到 `FromRadio` 封包 → 解碼 (Position/Telemetry/NodeInfo…等) → 避免重複輸出（依 `from+id` 去重）。

### 5.2 CallMesh API & APRS 流程
- `CallMeshClient`：
  - 建構 agent 字串 `callmesh-client/version (OS; arch)`。
  - `heartbeat()` / `fetchMappings()` 透過 fetch 實作，預設逾時分別為 10s / 15s。
  - 串 OS detection（Windows/Mac/Linux；自動抓 `os-release`）。

- 驗證時機：
  1. Electron 啟動：若無已驗證紀錄 → overlay 遮罩整個 UI，要輸入 Key。
  2. 按「驗證並儲存」或 CLI `callmesh` → 立刻送 `heartbeat` 驗證。成功才拿掉遮罩並啟動連線循環。
  3. CLI `startMonitor` / `callmesh` 子指令＆Electron 背景服務：若伺服器暫時無回應但先前已驗證成功，允許「降級模式（degraded）」繼續用，並提示使用者。
  4. Key 失敗 / 清空時 → 即刻鎖定整個功能，heartbeat 亦會停止。

- 驗證結果快取：
  - Electron：localStorage + IPC 共享狀態 (`callmeshState`)，並把 mapping / provision 寫入 `~/Library/Application Support/<app>/callmesh/`。
  - CLI：`~/.config/callmesh/monitor.json`（可透過 `CALLMESH_VERIFICATION_FILE` 指定）。

- **Heartbeat → Mapping → Provision → APRS 完整流程（60 秒循環）**
  1. 背景排程每 60 秒觸發一次 heartbeat，攜帶當前 mapping hash（若有）。
  2. Heartbeat 回傳 `needs_update=true` 或首次啟動 → 呼叫 `fetchMappings()`，把新 mapping 寫入 `mappings.json`，並同步 `callmeshState.mappingItems`。
  3. Heartbeat 回傳 `provision` → 存成 `provision.json`，廣播到前端（資訊頁即時更新呼號、Symbol、座標、評論、最後同步時間）。
  4. 當 provision 包含呼號（base + SSID）時，主行程計算 APRS passcode，更新 APRS 連線設定並觸發登入。

- **APRS 連線要點**
  - `src/aprs/client.js` 會在 `connect()` 時建立 TCP 連線，送出 `user <callsign> pass <passcode> vers TMAG <version>` 與連線訊息。
  - 每 30 秒送一次 `# keepalive`；若寫入失敗或 socket 中斷，就會在 30 秒後自動重連。
  - `updateConfig()` 可在不中斷 UI 的情況下調整 server/port/callsign/passcode，若連線中則會自動斷線＋重連。
  - 驗證成功後會立即送出一筆 APRS 位置信標（預設 10 分鐘週期），資料包含最新 provision 的呼號、Symbol、座標與 comment；設定頁可自訂 1–1440 分鐘的間隔。
  - 每 1 小時還會送出系統狀態封包（`>TMAG Client vX.Y.Z`），確認機器仍在線。
- Meshtastic 位置封包會即時轉換成 APRS `Position` 封包：frame source 僅使用 mapping 對應的呼號（含 SSID）；無 mapping 的節點（包含本站台自身）不會再上傳，以避免重複呼號造成衝突。Path 固定為 `APTMAG,MESHD*,qAR,<登入呼號>`；Symbol / Comment 取自 mapping，海拔若存在會轉換為 `/A=xxxxxx`（英尺），若封包帶有 Heading/Speed 亦會轉成 `ccc/sss`（度 / 節）附在座標後方。
  - 主畫面右上角會顯示 APRS 伺服器與連線狀態；Log 頁會記錄 connect/disconnect、keepalive、beacon、錯誤與 reconnect 事件。
- **TCP 連線管理**
  - 啟動時會自動嘗試連線 Meshtastic TCP API，最多 3 次、每次間隔 5 秒。若全部失敗，會暫停背景重試並等待使用者介入。
  - 手動按下「連線」時，UI 會進入手動重試流程（最多 3 次，間隔 5 秒），並支援「取消連線」立即終止；成功才重新開啟背景重連。
  - 背景 reconnect loop 使用單次排程機制，失敗後才會在 30 秒後再次嘗試，並受 `allowReconnectLoop` 控制，避免無限快速重試。

### 5.3 mDNS 自動搜尋
- 使用 `bonjour-service` 搜尋 `_meshtastic._tcp`。
- Electron UI 提供「掃描」按鈕 → 更新下拉選單 → 套用 Host/Port。
- CLI `node src/index.js discover` 也會列出所有裝置。

---

## 6. 使用者流程（話術版）

1. **第一次開啟**  
   - Overlay 直接蓋住畫面：「請輸入 API Key 才能使用」。  
   - 你只要提醒：「Copy 後 paste 到欄位，按下『驗證並儲存』就會解除鎖定。」

2. **輸入錯誤 / 伺服器無回應**  
   - Overlay 下方狀態訊息會說明「驗證失敗」或「暫時無回應」。  
   - 如果是暫時無回應、但 Key 曾經驗證過 → 使用者依然可以進入（畫面顯示警示文字）。  
   - 話術：「伺服器暫時失聯，不用緊張，稍後再按一次『重新驗證』即可。」

3. **CLI 劇本**  
   - `export CALLMESH_API_KEY="xxxx"`  
   - `node src/index.js --host 172.16.8.91 --port 4403`（會自動驗證、同步 mapping/provision、登入 APRS）  
   - 如果 Key 或伺服器有問題 → CLI 會直接停止，提醒調整。
   - 話術：「CLI 版本同樣有保護，沒設定 Key 或 Key 失效，根本不讓你往下走。」

4. **Meshtastic 監看**  
   - `npm start` 即可看封包摘要表格。  
  - `node src/index.js --format json --show-raw` 可轉 raw Hex。  
  - Electron UI 分成四個分頁：
    - **監視**：封包表格、10 分鐘平均圖表、CallMesh / APRS 狀態（右上角同步顯示 APRS 實際連線伺服器與連線結果）。
    - **資訊**：顯示最新 provision（Callsign、Symbol、座標、Comment、更新時間），座標會從 provision 的 `latitude` / `longitude` 取值並寫入 Log。
    - **Log**：列出 heartbeat、mapping、provision、APRS keepalive / beacon / status / uplink / reconnect、自身節點更新、重置等事件並自動捲動，並提供「複製全部日誌」按鈕快速複製紀錄。
    - **設定**：設定 Meshtastic Host、CallMesh API Key、APRS server、APRS 信標間隔（1-1440 分，預設 10 分鐘），並提供「重置本地資料」按鈕（會清掉 key、mapping、provision 與 localStorage）。
  - UI 中「連線」按鈕支援手動重試（最多 3 次、間隔 5 秒）以及「取消連線」，取消後狀態會回到 idle 並停止所有背景重連。
  - 話術：「抓到 LoRa 封包後，直接同步 CallMesh 和 APRS，整個 TMAG 生態都看得到。」

---

## 7. 重要設定／環境變數

| 環境變數 | 用途 |
| --- | --- |
| `CALLMESH_API_KEY` | CLI 預設的 API Key（必填） |
| `CALLMESH_VERIFICATION_FILE` | CLI 驗證快取檔路徑，預設 `~/.config/callmesh/monitor.json` |
| `CALLMESH_ARTIFACTS_DIR` | 覆寫 CallMesh artifacts 資料夾（CLI 預設 `~/.config/callmesh/`，Electron 預設 `userData/callmesh`） |
| `CALLMESH_BASE_URL` | 可覆寫 CallMesh API base URL（預設 `https://callmesh.tmmarc.org`） |
| `APRS_SERVER` | 可覆寫 APRS-IS 伺服器（預設 `asia.aprs2.net`） |

Electron 端會透過 IPC 顯示目前狀態（`callmeshState.lastStatus`），方便 debug。

> **Sentinel 行為 (`.skip-env-key`)**  
> - 按「重置本地資料」或清空 API Key 時，主行程會在 `~/Library/Application Support/Electron/callmesh/.skip-env-key` 寫入記號。  
> - 下次啟動時如偵測到記號，或環境變數 `CALLMESH_API_KEY` / `CALLMESH_VERIFICATION_FILE` 存在，即不會自動帶入金鑰；需使用者手動再次驗證。  
> - 驗證成功後會移除此記號，離線模式可延續使用本地快取。

---

## 8. 常見情境處理

1. **Key 設錯**  
   - Electron：遮罩一直在 → hint「請輸入 API Key」。  
   - CLI：終端顯示 `CallMesh API Key 驗證失敗`，請重新設定環境變數。

2. **伺服器暫時掛掉**  
   - 若 Key 曾驗證過 → 顯示警告，但允許繼續；下次會再驗一次。  
   - 若從未驗證 → 直接鎖住功能。

3. **Meshtastic 連不上**  
   - 檢查裝置是否開啟 TCP API（預設 port 4403）。  
   - 可先用 `node src/index.js discover` 確認 mDNS 有抓到。  
   - Electron 下拉選單有列出所有裝置。
4. **APRS 連線異常**  
   - 確認 provision 是否包含呼號與 SSID。  
   - 設定頁可手動切換 APRS 伺服器（例：`noam.aprs2.net`）。  
   - Log 會顯示 `APRS` keepalive、beacon、status、uplink、disconnect、reconnect 訊息，方便定位。
5. **需要重置設定/換人接手**  
   - 設定頁點「重置本地資料」即可清除 API Key、mapping、provision、偏好設定與 APRS server，回到初始狀態。  
   - CLI 端如需同步清空，可手動刪除 `~/.config/callmesh/monitor.json`。
6. **調整 APRS 信標頻率**  
   - 進入「設定」頁修改「APRS 信標間隔（分鐘）」即可；最小 1 分鐘、最大 1440 分鐘。  
   - 儲存後系統會立即套用新的間隔並重新排程下一筆 Beacon。
7. **TCP 連線重試策略**  
   - 自動連線失敗三次後會停止背景重試並等待使用者手動介入。  
   - 手動按「連線」時最多重試三次（每次間隔 5 秒），期間可隨時按「取消連線」中止流程。  
   - 背景 reconnect loop 以 30 秒冷卻排程執行，若狀態切換成手動或取消會立即停下，避免無限快速重連。
---

## 9. Handover Talk Script（交接話術）

> 「Codex，這套系統就是 Meshtastic 的雷達站。  
> CallMesh API Key 是唯一的解鎖碼，流程我都接好：  
> ① 開啟時馬上驗證，過不了就沒 UI；  
> ② heartbeat 會自動抓 mapping、套用 provision；  
> ③ 有了呼號就自動登入 APRS，主畫面立刻看得到狀態。  
> 你只要顧好 `CALLMESH_API_KEY`、APRS server 需要時再換，其他我都鋪好。皮要繃緊，這是 TMAG 後台的命脈。」 

---

## 10. 下一步建議

- 若要擴充：可考慮新增自動同步排程（目前 CLI 可自行排 cron）。  
- 若要導入更多 CallMesh API：直接在 `callmesh/client.js` 延伸即可。  
- Electron UI 如需顯示更多狀態，可參考 `summary` 事件資料。

---

## 11. Windows / Linux 客戶端打包

macOS 上已配置好跨平台打包流程，可直接產出可攜式的 Windows / Linux x64 版本：

1. 安裝依賴（第一次）：`npm install`
2. Windows 打包：`npm run build:win`
   - 可直接攜帶的資料夾：`dist/TMAG Monitor-win32-x64/`
   - 封裝壓縮檔：`dist/TMAG_Monitor-win32-x64.zip`
3. Linux 打包：`npm run build:linux`
   - 可直接攜帶的資料夾：`dist/TMAG Monitor-linux-x64/`
   - 封裝壓縮檔：`dist/TMAG_Monitor-linux-x64.zip`

打包腳本會：
- 自動下載對應版 Electron Windows runtime（快取於 `dist/electron-v<version>-win32-x64.zip`）
- 自動下載對應版 Electron Linux runtime（快取於 `dist/electron-v<version>-linux-x64.zip`）
- 安裝專案的 production 依賴（不含 devDependencies）
- 將應用程式置入 `resources/app`
- 自動壓縮成 ZIP，方便交付或遠端部署

> 備註：若要自訂圖示或產生安裝程式，可在 Windows 環境中再進一步加工。現有流程無需安裝 Wine，適合快速產出可攜式執行檔。


祝你接手順利，有問題就從這份文件往對應的原始碼找起即可。
