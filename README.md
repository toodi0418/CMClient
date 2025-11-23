# Meshtastic APRS Gateway (TMAG)

TMAG 是一套使用 Node.js 打造的 **Meshtastic → APRS Gateway**，整合了 CallMesh 平台驗證、Mapping 同步、APRS uplink 與遙測統計。專案同時提供：

- **CLI**：適合在 macOS / Linux / Windows（或 Raspberry Pi）上以無頭模式執行，能透過 Meshtastic **TCP 或 Serial** 連線將封包轉換成 APRS uplink 並回報 CallMesh。
- **Electron 桌面版**：提供即時儀表板、封包/節點追蹤、APRS 狀態檢視等 GUI 功能，並支援 TCP / Serial 模式與裝置清單選取。

> ✅ 只要準備 CallMesh API Key 與 Meshtastic 連線（TCP 或 Serial），就能在任何支援 Node.js 的平台啟動整個 Gateway。

---

## 1. 主要功能

- **Meshtastic 監控**：解析 Meshtastic TCP / Serial 流中的 protobuf 封包，摘要顯示節點、SNR/RSSI、Hops、座標等資訊。
- **CallMesh 整合**：透過 Heartbeat/Mapping/Provision API 取得節點配置與 APRS 參數，並在 Key 驗證失敗時鎖定系統。
- **APRS Uplink**：依 Mapping 決定呼號、符號與註解，上傳位置/狀態/遙測資料到 APRS-IS，具備 Beacon、Status、Telemetry 定時器與互斥上傳控制。
- **TenManMap 轉發**：驗證成功後，會將 Meshtastic 摘要同步到 TenManMap 及其合作夥伴，佇列與重試機制常駐執行。
- **@cm 自動回覆機器人**：在 CH2 `Signal Test` 頻道輸入 `@cm ...` 會觸發機器人，由最近（含本機）節點自動回覆測試訊息；需保持 TenManMap 分享為啟用狀態，停用分享時該機器人無法運作。
- **遙測統計**：計算近 10 分鐘的封包數量、上傳比率、類型分佈（Position / Message / Control）。
- **彈性連線模式**：CLI 與 GUI 皆支援 `tcp://` 與 `serial://` 目標，可自動判讀 `serial:///dev/ttyUSB0` 類型字串並套用鮑率。
- **跨平台部署**：可自行打包 macOS / Windows / Linux x64 CLI 或 Desktop 版；CLI 同時支援自動重連。
- **穩定時間戳**：Telemetry 紀錄寫入時會使用收包當下的時間戳，避免裝置 RTC 漂移造成前端區間掛零。
- **節點資料庫共用**：內建 `nodeDatabase` 集中維護節點長短名、模型、座標等資訊，CLI / Electron / Web 透過 `node`、`node-snapshot` 事件共享同一份資料，節點清單支援座標搜尋與距離顯示。
- **訊息與 Relay 體驗**：GUI/Web 會持久化 CH0~CH3 文字訊息（含節點暱稱、最後轉發資訊），同時提供 Relay 推測提示 UI，能追蹤候選節點與推測理由。
- **APRS 去重**：橋接層內建三層快取（feed 30 分鐘、本地與座標 30 秒），即使 Meshtastic 網路延遲 30 秒～10 分鐘，也能避免不同站重複 uplink 造成位置回朔或浪費 APRS-IS 配額。

---

## 2. 環境需求

- Node.js 22 以上（需支援內建 `node:sqlite`）
- Meshtastic 裝置或 Gateway（TCP API 或 Serial 連線）
- CallMesh 平台有效的 API Key（環境變數 `CALLMESH_API_KEY`）
- （若使用 APRS）穩定的網際網路連線
- （若使用 Serial）系統需對裝置節點具有讀寫權限，例如將使用者加入 `dialout` 或 `uucp` 群組

---

## 3. 安裝與初始設定

```bash
# 1. 取得專案原始碼
git clone https://github.com/toodi0418/CMClient.git
cd CMClient

# 2. 安裝相依套件（CLI / GUI 共用）
npm install
```

### 沒有安裝 Node.js / npm？

1. **macOS / Linux**  
   建議安裝 [nvm](https://github.com/nvm-sh/nvm)：  
   ```bash
   curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.5/install.sh | bash
   source ~/.nvm/nvm.sh
   nvm install 22
   nvm use 22
   ```  
   完成後 `node -v`、`npm -v` 應該能顯示版本號。

2. **Windows**  
   前往 [Node.js 官方網站](https://nodejs.org/en/download) 下載 Node.js 22 LTS 安裝程式，依指示完成安裝後重新開啟終端機（PowerShell / CMD）。  
   驗證：  
   ```powershell
   node -v
   npm -v
   ```

### 如何升級 Node.js 到最新版本

- **使用 nvm（macOS / Linux）**  
  ```bash
  nvm install 22 --latest-npm           # 安裝或更新 22.x 最新版
  nvm alias default 22                  # （可選）設為預設版本
  nvm use 22
  node -v && npm -v                     # 確認版本
  ```
- **使用 Homebrew（macOS）**  
  ```bash
  brew update
  brew upgrade node@22
  echo 'export PATH="/usr/local/opt/node@22/bin:$PATH"' >> ~/.zshrc
  source ~/.zshrc
  node -v && npm -v
  ```
- **Windows**  
  - 重新下載並執行 Node.js 22 LTS 安裝程式；或  
  - 若使用 [nvm-windows](https://github.com/coreybutler/nvm-windows)，可執行：  
    ```powershell
    nvm install 22.21.1
    nvm use 22.21.1
    node -v
    npm -v
    ```

### 必備環境變數

```bash
export CALLMESH_API_KEY="你的 CallMesh API Key"
# 可選：變更快取與驗證檔案位置
export CALLMESH_VERIFICATION_FILE=~/.config/callmesh/monitor.json
export CALLMESH_ARTIFACTS_DIR=~/.config/callmesh/

# TenManMap 開關（預設啟用）
export TENMAN_DISABLE=1   # 設為 1 / true / yes / on 時停用 TenManMap 推播
```

---

### TenManMap 推播與隱私聲明

驗證成功後，TMAG 會沿用 CallMesh 的授權資訊，將 Meshtastic 摘要資料同步至 TenManMap 及其合作夥伴。以下為資料使用的概述：

- 同步範圍：與節點狀態與現地觀測相關的摘要資訊（例如位置、遙測與訊息之摘要欄位）。
- 使用目的：服務提供與維運、效能與可靠度監測、資安維護、統計分析、研究，以及依據商業策略所需之用途。
- 分享對象：TenManMap 與其合作夥伴；依法令要求時，可能提供予主管機關或其他有權單位。
- 保留期間：為達成前述目的所需之期間，或依法律／合約要求延長。
- 你的選擇：預設同步為開啟；如需停用，請在啟動前設定環境變數 `TENMAN_DISABLE=1`（支援 `true` / `yes` / `on`），於 CLI 加上 `--no-share-with-tenmanmap`，或於桌面版設定頁底部調整分享開關。
- 資料安全：我們採取合理的技術與管理措施降低風險，但無法保證絕對安全。
- 跨區處理：資料可能在不同法域之間處理與儲存，適用當地法規。
- 權利行使：若你欲查詢、請求刪除或停止同步，請依你的部署流程聯絡維運窗口。
- 條款更新：本段內容可能依實際作業調整，更新後恕不另行通知。

若你的環境對資料揭露有額外規範，請在啟動前評估是否需要停用同步。更多結構與欄位示意請參考 `new-api.md`。

---

### 更新到最新版本

#### 透過原始碼部署（git clone）
```bash
cd CMClient
git pull          # 取得最新程式
npm install       # 套件若有更新會同步安裝
```
更新完成後即可照原流程啟動 `npm start` / `npm run desktop` 或重新執行 `npm run build:*` 產出新版套裝。

#### 使用 Release 可攜版
1. 前往最新 Release：<https://github.com/toodi0418/CMClient/releases>
2. 下載目標平台的 ZIP（GUI 或 CLI）。
3. 解壓後直接覆蓋舊版本或以全新資料夾啟動；設定檔（CallMesh API Key、Mapping artifacts）仍沿用原本位置。

> 提醒：更新前請確認既有執行中的 CLI / GUI 已中止，避免 APRS uplink flowId 重複。

---

### Raspberry Pi 快速部署

樹莓派預設沒有 Node.js，可依下列步驟一次完成：

```bash
# 系統更新 + 安裝 git / curl
sudo apt update && sudo apt install -y git curl

# 安裝 nvm 並使用 Node.js 22（建議 LTS）
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.5/install.sh | bash
source ~/.nvm/nvm.sh
nvm install 22
nvm use 22

# 取得 TMAG 並安裝依賴
git clone https://github.com/toodi0418/CMClient.git
cd CMClient
npm install

# 匯入 CallMesh API Key 後直接啟動 CLI
export CALLMESH_API_KEY="你的 Key"
node src/index.js --host <節點ip> --port 4403
```

若想在 Pi 上打包成單一執行檔，可再執行：

```bash
npx pkg src/index.js \
  --config package.json \
  --targets node22-linux-armv7 \
  --compress Brotli --public \
  --output tmag-cli-linux-armv7
```

---

## 4. CLI 快速上手

CLI 預設會驗證 CallMesh API Key、啟動 heartbeat 與 Mapping 同步，再連線 Meshtastic 目標（TCP / Serial）並自動轉發到 APRS。

```bash
node src/index.js --host 172.16.8.91 --port 4403
```

若裝置透過 USB / Serial 連線，也可以：

```bash
node src/index.js --connection serial --serial-path /dev/ttyUSB0 --serial-baud 115200
# 或直接使用 host=serial:/// 路徑（鮑率可搭配查詢字串或 @ 參數覆寫）
node src/index.js --host serial:///dev/ttyUSB0
```

使用 `pkg` 打包後的 CLI 執行檔也支援完整的指令列參數，可透過 `--help` 查看：

```bash
./tmag-cli --help

Commands:
  tmag-cli discover  自動搜尋區網內的 Meshtastic TCP 裝置
  tmag-cli           連線並監看 Meshtastic 封包                        [default]

Options:
      --version     Show version number                                [boolean]
      --help        Show help                                          [boolean]
  -K, --api-key     CallMesh API Key（若未帶入將使用環境變數 CALLMESH_API_KEY）
                                                                        [string]
  -H, --host        Meshtastic TCP 伺服器主機位置[string] [default: "127.0.0.1"]
  -P, --port        Meshtastic TCP 伺服器埠號           [number] [default: 4403]
  -C, --connection  Meshtastic 連線方式（未指定時會依 host 判斷）
                                                     [choices: "tcp", "serial"]
      --serial-path Serial 連線時的裝置路徑（例如 /dev/ttyUSB0）        [string]
      --serial-baud Serial 連線時的鮑率             [number] [default: 115200]
  -m, --max-length  允許的最大封包大小 (位元組)          [number] [default: 512]
  -r, --show-raw    在摘要輸出時同時列印 payload 十六進位
                                                      [boolean] [default: false]
      --web-ui      啟用內建 Web Dashboard（預設關閉）
                                                      [boolean] [default: false]
  -f, --format      輸出格式：summary 顯示表格，json 顯示完整資料
                               [choices: "summary", "json"] [default: "summary"]
  -p, --pretty      搭配 --format json 時使用縮排輸出  [boolean] [default: true]
      --no-share-with-tenmanmap  停用 TenManMap 分享         [boolean] [default: false]
```

常用選項：

| 參數 | 預設 | 說明 |
| ---- | ---- | ---- |
| `--connection, -C` | 自動偵測 | 指定 `tcp` 或 `serial`；未設定時會依 `--host` 內容判斷。 |
| `--host, -H` | `127.0.0.1` | Meshtastic 目標，可填入 IP（如 `192.168.1.50`）或 `serial:///dev/ttyUSB0`。 |
| `--port, -P` | `4403` | TCP 模式使用的連接埠。 |
| `--serial-path` | - | Serial 模式的裝置路徑（若 `--host` 已使用 `serial://` 可省略）。 |
| `--serial-baud` | `115200` | Serial 模式鮑率。 |
| `--format, -f` | `summary` | `summary` 表格、`json` 輸出完整 protobuf 內容。 |
| `--show-raw, -r` | `false` | 在 summary 下同時顯示十六進位 payload。 |
| `--web-ui` | `false` | 啟用內建 Web Dashboard（HTTP + SSE 伺服器）。 |
| `--no-share-with-tenmanmap` | `false` | 停用 TenManMap 分享。 |
| `--discover` | - | 搜尋區網內 `_meshtastic._tcp` 裝置。 |

CLI 具備自動重連：若 TCP 連線中斷會每 30 秒重試一次，直到 `Ctrl+C` 終止。

### CallMesh CLI 工具

仍可透過 CLI 單獨執行 CallMesh API：

```bash
# 同步 Heartbeat + Mapping
node src/index.js callmesh sync \
  --state-file ~/.config/callmesh/state.json \
  --mappings-output ~/.config/callmesh/mappings.json
```

指令詳情可見 `docs/callmesh-client.md`。

### APRS 去重與偵錯

- **改善動機**：Meshtastic 網路偶爾塞住，封包可能延遲 30 秒到 10 分鐘才送到另一個站台。若每站都再次 uplink，同一筆位置會在 APRS-IS 上「倒退」，也浪費配額。
- **三層快取**（皆為記憶體資料，重啟即清空）  
  1. `aprsPacketCache` / `aprsCallsignSummary`：記錄 30 分鐘內 APRS-IS feed 已出現的 payload／呼號，只要再看到相同呼號＋payload，就標記 `seen-on-feed` 並跳過上傳。  
  2. `aprsLocalTxHistory`：保留本地 uplink 的 payload 30 秒，用來擋掉 UI/排程誤觸造成的重送。  
  3. `aprsLastPositionDigest`：同一 Mesh ID 30 秒內座標＋符號＋註解完全相同就不再上傳，避免 GPS 靜止時不停重複。  
- **使用方式**：任何實例都可開 `http://<host>:7080/debug` 檢視 `aprsDedup`，快速判斷某筆為何被擋。例如 `packetCache` 命中代表 feed 已有、`localTxHistory` 命中代表 30 秒內剛由本機上傳。  
- **自訂與除錯**：  
  - `TMAG_APRS_FEED_FILTER` / `TMAG_APRS_FEED_RADIUS_KM` 用來覆寫監聽範圍，未設定時會依 Provision 座標自動套用 `#filter r/<lat>/<lon>/300`。  
  - `TMAG_APRS_LOG_VERBOSE=1` 可恢復完整的 APRS tx/rx/keepalive log，預設靜音避免噴 log。  

---

## 5. Electron 桌面版

```bash
npm run desktop
```

GUI 提供：

- 連線狀態、APRS 伺服器與登錄顯示
- 封包列表與 Mapping 封包追蹤（含「已上傳／待上傳」狀態）
- CallMesh Log、APRS Log 與遙測統計圖表
- 設定頁：輸入 CallMesh API Key、調整 APRS Beacon 間隔、掃描 Meshtastic 裝置，並可切換 TCP / Serial 模式與選擇 Serial 裝置

首次啟動需輸入並驗證 CallMesh API Key，通過後即可連線 Meshtastic。

---

## 6. 維護工具

- **遙測時間校正（升級前／JSONL）**：若早期資料受裝置時間影響，可在升級至 SQLite 版本前於專案根目錄執行  
  ```bash
  node scripts/fix-telemetry-timestamps.js ~/.config/callmesh/telemetry-records.jsonl
  ```  
  腳本會將每筆紀錄的 `timestampMs`／`sampleTimeMs`／`telemetry.timeMs` 對齊收包時刻，並保留 `.bak` 備份。
- **遙測資料儲存**：自 v0.2.23 起改採 `~/.config/callmesh/telemetry-records.sqlite`（SQLite）；首次啟動會自動匯入舊版 JSONL 並將原檔更名為 `.migrated`。
- **遙測歷史復原**：若 SQLite 已寫入新資料、但仍要再次匯入 `telemetry-records.jsonl.migrated`，只要在程式關閉後把它改回 `telemetry-records.jsonl`（CLI 路徑 `~/.config/callmesh/`，GUI 路徑 `~/Library/Application Support/TMAG Monitor/callmesh/`），下次啟動會再次把整份 JSON 匯入資料庫，完成後檔案會自動改回 `.migrated` 備份。
- **共用資料庫**：節點快照、Mapping/Provision 快取、訊息紀錄與 Relay 統計統一儲存在 `~/.config/callmesh/callmesh-data.sqlite`，升級時會自動匯入舊版 `node-database.json`、`message-log.jsonl`、`relay-link-stats.json`。
- **打包工具**：`scripts/pack-cli.sh`、`scripts/pack-desktop.sh` 可快速產出 CLI / GUI 可攜版（需安裝 `pkg`、`electron-packager`）。

### 清除節點資料庫

- **CLI 旗標**  
  ```bash
  node src/index.js --clear-nodedb
  ```  
  在不啟動監控流程的情況下，會直接清空 `callmesh-data.sqlite` 的 `nodes` 與 `relay_stats` 表，並移除舊版 `node-database.json` / `relay-link-stats.json`，完成後立即結束程式。指令會同時掃描 CLI 預設資料夾與 Electron（含開發模式）使用的 `userData` 路徑，macOS 例如 `~/Library/Application Support/Electron/callmesh/`。
- **Electron 桌面版**  
  1. 切換到「節點資料庫」分頁。  
  2. 點擊右上角「清除節點資料庫」，會同時清空記憶體快取、`callmesh-data.sqlite` 中的 `nodes` 表，以及 relay link-state（`relay_stats` / `relay-link-stats.json`）。
- **CLI / 服務模式**  
  1. 停止 TMAG 程式。  
  2. 執行下列指令（若有自訂 `CALLMESH_ARTIFACTS_DIR`，請換成對應路徑）：  
     ```bash
     sqlite3 ~/.config/callmesh/callmesh-data.sqlite "DELETE FROM nodes; VACUUM;"
     ```  
     或直接刪除 `callmesh-data.sqlite`，下次啟動時會自動重建並重新同步 Mapping / Provision。

--- 

## 7. 打包指令

| 目標 | 指令 | 輸出位置 |
| ---- | ---- | -------- |
| macOS CLI | `npm run build:mac-cli` | `dist/cli/tmag-cli` |
| Windows x64 GUI | `npm run build:win` | `dist/TMAG Monitor-win32-x64/` |
| Linux x64 GUI | `npm run build:linux` | `dist/TMAG Monitor-linux-x64/` |

### Linux / Raspberry Pi 版 CLI

macOS 無法交叉編譯 Linux/ARM 執行檔，請在目標 Linux 主機（例如樹莓派）上執行：

```bash
git clone https://github.com/toodi0418/CMClient.git
cd CMClient
npm install

npx pkg src/index.js \
  --config package.json \
  --targets node22-linux-armv7 \
  --compress Brotli --public \
  --output tmag-cli-linux-armv7
```
或針對 64 位元 Pi OS 使用 `--targets node22-linux-arm64`。

### Docker 佈署

GitHub Actions 會自動執行 **Build & Publish Docker Image** workflow，並把映像推送到 GitHub Container Registry（GHCR）。本專案的映像位置是 `ghcr.io/toodi0418/cmclient:<tag>`（例如 `ghcr.io/toodi0418/cmclient:latest`）。常見流程如下：

1. **取得映像**
   - 從 GHCR 下載：  
     ```bash
     docker pull ghcr.io/toodi0418/cmclient:latest
     ```
   - 或在原始碼目錄自行建置：  
     ```bash
     docker build -t callmesh-client .
     ```
   - 若已透過 `docker save callmesh-client:latest -o callmesh-client.tar` 匯出，可在其他主機使用 `docker load -i callmesh-client.tar` 匯入。

2. **準備環境與 compose**
   - 根目錄已有 `.env` 範本，填入 `CALLMESH_API_KEY`、`MESHTASTIC_HOST`、`TMAG_WEB_PORT` 等參數。
   - `docker-compose.yml` 會：
     - 以 `.env` 中的參數建置/啟動 `callmesh-client`；
     - 將 `/data/callmesh` 透過 volume `callmesh-data` 持久化 CallMesh 驗證、歷史遙測與訊息記錄；
     - 預設開啟 Web Dashboard（7080 埠），如需停用可把 `TMAG_WEB_DASHBOARD` 設為 `0`。

3. **啟動**
   ```bash
   docker compose up -d --build
   ```
   - 變更設定後重新載入：`docker compose up -d`。
   - 查看日誌：`docker compose logs -f`.
   - 需改用 Serial 裝置時，可在 `docker-compose.yml` 新增：
     ```yaml
     devices:
       - /dev/ttyUSB0:/dev/ttyUSB0
     command:
       - npm
       - start
       - --
       - --connection
       - serial
       - --serial-path
       - /dev/ttyUSB0
       - --serial-baud
       - "115200"
     ```

4. **群暉 NAS 提示**
   - 在 DSM「Container Manager」建立專案時，直接匯入 repo 內的 `docker-compose.yml`，並把 `.env` 一併上傳。
   - 若想把資料存進共享資料夾，可將 compose 內的 volume 改為 `./data:/data/callmesh`，確保資料夾具有讀寫權限。
   - Serial 連線需要在 Container Manager → 編輯容器 → 裝置中勾選 `/dev/ttyUSB*`，同時於 compose 增加 `devices`。
   - 開放 Web Dashboard 時，務必在 DSM 防火牆放行 `TMAG_WEB_PORT`（預設 7080）。

#### `.env` 範本與環境變數

```env
CALLMESH_API_KEY=cm_xxxxx               # CallMesh 發放的 API Key，沒有就會一直卡在 locked
MESHTASTIC_HOST=meshtastic.local        # 目標 Meshtastic TCP host，可填 IP 或 mDNS 名稱
MESHTASTIC_PORT=4403                    # Meshtastic TCP 監聽 port，預設 4403
TMAG_WEB_PORT=7080                      # Web Dashboard 映射到主機的 port
TMAG_WEB_DASHBOARD=1                    # 設為 0 可完全關閉 Web Dashboard
TENMAN_DISABLE=0                        # 1=停用 TenManMap 上傳
AUTO_UPDATE=1                           # 1=啟動時自動 git pull 指定分支（main/dev）
AUTO_UPDATE_BRANCH=main                 # main（預設）或 dev，其他值會回退 main
AUTO_UPDATE_WORKDIR=/data/callmesh/app-src
AUTO_UPDATE_REPO=https://github.com/toodi0418/CMClient.git
AUTO_UPDATE_POLL_SECONDS=300            # 多少秒檢查一次遠端更新
```

- `.env` 會被 `docker-compose.yml` 自動讀取；如果部署在會commit 的環境，建議以 `cp .env .env.local` 再編輯以免誤傳敏感資訊。
- `CALLMESH_API_KEY` 必填；`MESHTASTIC_HOST/PORT` 需能被容器解析與連線，若 Meshtastic 裝置在同一主機可直接填 `host.docker.internal` 或本機 IP。
- 修改 `.env` 內容後重新套用 `docker compose up -d`.

#### 常見操作流程

1. **建置/更新映像**：在 repo 根目錄執行 `docker compose build`（或 `docker compose pull` 直接拉 GHCR 版本）。
2. **啟動服務**：`docker compose up -d --build`（會依 `AUTO_UPDATE` 決定是否在 entrypoint 重新 git pull）。
3. **查看狀態/日誌**：`docker compose logs -f callmesh-client`，確認 CallMesh 驗證、Meshtastic 連線與 APRS 流程是否正常。
4. **重啟或變更設定**：更新 `.env` → `docker compose up -d`；如需乾淨重建可先 `docker compose down` 再 `up -d`.
5. **除錯**：`docker exec -it callmesh-client /bin/bash` 進入容器，可檢查 `/data/callmesh` 內的 artifacts 或執行 `npm start -- --help`。

#### 連線方式

- **Web Dashboard**：啟動後在瀏覽器輸入 `http://<部署主機IP>:<TMAG_WEB_PORT>`（預設 7080），首次載入會要求 CallMesh API Key 通過驗證後才顯示資料。
- **Meshtastic TCP**：容器會依 `.env` 的 `MESHTASTIC_HOST/PORT` 直接連線，請確保該 host/port 對 Docker network 可達；若目標在同一臺主機，可填 `host.docker.internal` 或實際 IP。
- **Meshtastic Serial**：在 `docker-compose.yml` 的 service 內加入
  ```yaml
  devices:
    - /dev/ttyUSB0:/dev/ttyUSB0
  command:
    - npm
    - start
    - --
    - --connection
    - serial
    - --serial-path
    - /dev/ttyUSB0
    - --serial-baud
    - "115200"
  ```
  並確認宿主機已授權 Docker 存取該裝置（Linux 可能需要將使用者加入 `dialout`）。
- **CallMesh / TenManMap**：容器會將 artifacts 寫入 volume `/data/callmesh`，可把 `callmesh-data` 綁到實體資料夾以便備份或跨版本沿用，確保 Key/Mapping/遙測都能持續。

#### 開機自動啟動（systemd）

1. 複製範例 unit 檔並依實際路徑調整 `WorkingDirectory=`（需指向含 `docker-compose.yml` 的資料夾）：  
   ```bash
   sudo cp systemd/callmesh-client.service.example /etc/systemd/system/callmesh-client.service
   sudo sed -i 's#/opt/CMClient#/home/<user>/CMClient#g' /etc/systemd/system/callmesh-client.service
   ```
   （或以 `nano`/`vim` 編輯，並確認 `docker compose up -d` 指令沒有其他自訂參數）
2. 重新載入 systemd 並啟用服務：  
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now callmesh-client.service
   ```
3. 之後主機開機時會先啟動 Docker，再由 systemd 執行 `docker compose up -d`。若需暫停服務可 `sudo systemctl stop callmesh-client`，並於調整設定後 `sudo systemctl restart callmesh-client`。

> 提醒：compose 內已設定 `restart: unless-stopped`，可確保 Docker daemon 重啟時自動復原；systemd unit 則負責在 OS 開機時立即執行 compose，適合長期部署。

### Docker 自動更新（main / dev）

- `docker-entrypoint.sh` 會先把指定分支同步到 `/data/callmesh/app-src`，執行 `npm ci --omit=dev` 後啟動程式，並持續以 `AUTO_UPDATE_POLL_SECONDS`（預設 300 秒）輪詢遠端。偵測到 `main` / `dev` 有新 commit 時，會優雅地停止目前的 `npm start`、重新 `git pull + npm ci`，再自動重啟服務，整個流程不需重建容器。
- 預設 `AUTO_UPDATE=1`、`AUTO_UPDATE_BRANCH=main`。若希望跟進開發版可把 `.env` 設成 `AUTO_UPDATE_BRANCH=dev`；若要完全依賴發佈映像，則把 `AUTO_UPDATE=0`，entrypoint 會直接執行 `/app` 內的打包版本。
- 相關環境變數：
  - `AUTO_UPDATE_REPO`：Git repository URL（需可匿名讀取），預設 `https://github.com/toodi0418/CMClient.git`。
  - `AUTO_UPDATE_BRANCH`：同步分支（僅接受 `main` 或 `dev`，其餘值會回退 `main`）。
  - `AUTO_UPDATE_WORKDIR`：同步與安裝依賴的目錄，預設 `/data/callmesh/app-src`，與 artifacts 共用 volume 以節省重複下載。
  - `AUTO_UPDATE_REMOTE`：git remote 名稱，預設 `origin`。
  - `AUTO_UPDATE_POLL_SECONDS`：背景輪詢間隔秒數，調越小更新越即時但 `git fetch` 會更頻繁。
- 當環境無法連網或使用私有 repo 時，可把 `AUTO_UPDATE=0` 並手動執行 `docker pull` / `docker compose up -d --build` 來更新。

---

## 8. 專案結構概覽

```
CMClient/
├── src/
│   ├── index.js             # CLI 入口（Meshtastic/CallMesh 桥接）
│   ├── callmesh/            # CallMesh API 與 APRS 桥接核心
│   ├── aprs/client.js       # APRS-IS TCP 客戶端
│   ├── meshtasticClient.js  # Meshtastic TCP 流解析
│   └── electron/            # Electron 主行程、Renderer 與 UI
├── proto/meshtastic         # 官方 protobuf 定義
├── docs/                    # handover 與 CallMesh 技術說明
└── scripts/                 # Windows / Linux 打包腳本
```

核心邏輯集中在 `src/callmesh/aprsBridge.js`，CLI 與 Electron 共用同一套橋接流程，確保兩邊行為一致。

---

## 9. 開發與測試

1. 更動程式後可直接執行 `npm start`（CLI summary 模式）。
2. 若要觀察 APRS 上行內容，可同時開啟 Electron GUI 的「Mapping 封包追蹤」頁面，或在 CLI 監看 log。
3. 修改 comment 等站台資訊時，Bridge 只會送出更新後的 Beacon，不會重連或干擾定時器；Mapping 中的「已上傳」狀態會在收到對應 flow 的 APRS uplink 後自動更新。

---

## 10. 常見問題

- **為何 GitHub Repo 只有原始碼沒有 `dist/`？**  
  build 產物會破百 MB，已排除在 git 外。先 `npm install` 後再執行 `npm run build:*` 可在本機重新產出。

- **Meshtastic 斷線會自動重連嗎？**  
  CLI 會自動重連；Electron 版由 GUI 控制，可手動重試或啟用背景重連。

- **APRS 不停重連怎麼辦？**  
  通常是同一呼號在不同實例上登入或 comment 更新後頻繁重連；目前我們只在呼號/SSID 變更時才 teardown APRS，若仍被踢請確認無其他裝置使用相同呼號。

---

## 11. 授權

此專案依原作者設定的授權條款釋出；若需商用或替代授權，請聯絡 maintainer。
