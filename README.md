# Meshtastic APRS Gateway (TMAG)

TMAG 是一套使用 Node.js 打造的 **Meshtastic → APRS Gateway**，整合了 CallMesh 平台驗證、Mapping 同步、APRS uplink 與遙測統計。專案同時提供：

- **CLI**：適合在 macOS / Linux / Windows（或 Raspberry Pi）上以無頭模式執行，固定將 Meshtastic TCP 流轉換成 APRS 封包並回報至 CallMesh。
- **Electron 桌面版**：提供即時儀表板、封包/節點追蹤、APR S 狀態檢視等 GUI 功能。

> ✅ 只要準備 CallMesh API Key 與 Meshtastic TCP Access（預設埠 4403），就能在任何支援 Node.js 的平台啟動整個 Gateway。

---

## 1. 主要功能

- **Meshtastic 監控**：解析 TCP Stream 內的 protobuf 封包，摘要顯示節點、SNR/RSSI、Hops、座標等資訊。
- **CallMesh 整合**：透過 Heartbeat/Mapping/Provision API 取得節點配置與 APRS 參數，並在 Key 驗證失敗時鎖定系統。
- **APRS Uplink**：依 Mapping 決定呼號、符號與註解，上傳位置/狀態/遙測資料到 APRS-IS，具備 Beacon、Status、Telemetry 定時器與互斥上傳控制。
- **遙測統計**：計算近 10 分鐘的封包數量、上傳比率、類型分佈（Position / Message / Control）。
- **跨平台部署**：可自行打包 macOS / Windows / Linux x64 CLI 或 Desktop 版；CLI 同時支援自動重連。

---

## 2. 環境需求

- Node.js 18 以上（建議使用 LTS 版本）
- Meshtastic 裝置或 Gateway，且啟用 TCP API（預設 `tcp://<裝置 IP>:4403`）
- CallMesh 平台有效的 API Key（環境變數 `CALLMESH_API_KEY`）
- （若使用 APRS）穩定的網際網路連線

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
   nvm install 18
   nvm use 18
   ```  
   完成後 `node -v`、`npm -v` 應該能顯示版本號。

2. **Windows**  
   前往 [Node.js 官方網站](https://nodejs.org/en/download) 下載 LTS 安裝程式，依指示完成安裝後重新開啟終端機（PowerShell / CMD）。  
   驗證：  
   ```powershell
   node -v
   npm -v
   ```

### 必備環境變數

```bash
export CALLMESH_API_KEY="你的 CallMesh API Key"
# 可選：變更快取與驗證檔案位置
export CALLMESH_VERIFICATION_FILE=~/.config/callmesh/monitor.json
export CALLMESH_ARTIFACTS_DIR=~/.config/callmesh/
```

---

### Raspberry Pi 快速部署

樹莓派預設沒有 Node.js，可依下列步驟一次完成：

```bash
# 系統更新 + 安裝 git / curl
sudo apt update && sudo apt install -y git curl

# 安裝 nvm 並使用 Node.js 18（建議 LTS）
curl -o- https://raw.githubusercontent.com/nvm-sh/nvm/v0.39.5/install.sh | bash
source ~/.nvm/nvm.sh
nvm install 18
nvm use 18

# 取得 TMAG 並安裝依賴
git clone https://github.com/toodi0418/CMClient.git
cd CMClient
npm install

# 匯入 CallMesh API Key 後直接啟動 CLI
export CALLMESH_API_KEY="你的 Key"
node src/index.js --host 127.0.0.1 --port 4403
```

若想在 Pi 上打包成單一執行檔，可再執行：

```bash
npx pkg src/index.js \
  --config package.json \
  --targets node18-linux-armv7 \
  --compress Brotli --public \
  --output tmag-cli-linux-armv7
```

---

## 4. CLI 快速上手

CLI 預設會驗證 CallMesh API Key、啟動 heartbeat 與 Mapping 同步，再連線 Meshtastic TCP 並自動轉發到 APRS。

```bash
node src/index.js --host 172.16.8.91 --port 4403
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
  -m, --max-length  允許的最大封包大小 (位元組)          [number] [default: 512]
  -r, --show-raw    在摘要輸出時同時列印 payload 十六進位
                                                      [boolean] [default: false]
  -f, --format      輸出格式：summary 顯示表格，json 顯示完整資料
                               [choices: "summary", "json"] [default: "summary"]
  -p, --pretty      搭配 --format json 時使用縮排輸出  [boolean] [default: true]
```

常用選項：

| 參數 | 預設 | 說明 |
| ---- | ---- | ---- |
| `--host, -H` | `127.0.0.1` | Meshtastic TCP 伺服器 |
| `--port, -P` | `4403` | TCP 連接埠 |
| `--format, -f` | `summary` | `summary` 表格、`json` 輸出完整 protobuf 內容 |
| `--show-raw, -r` | `false` | 在 summary 下同時顯示十六進位 payload |
| `--discover` | - | 搜尋區網內 `_meshtastic._tcp` 裝置 |

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

---

## 5. Electron 桌面版

```bash
npm run desktop
```

GUI 提供：

- 連線狀態、APR S 伺服器與登錄顯示
- 封包列表與 Mapping 封包追蹤（含「已上傳／待上傳」狀態）
- CallMesh Log、APRS Log 與遙測統計圖表
- 設定頁：輸入 CallMesh API Key、調整 APRS Beacon 間隔、掃描 Meshtastic 裝置

首次啟動需輸入並驗證 CallMesh API Key，通過後即可連線 Meshtastic。

---

## 6. 打包指令

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
  --targets node18-linux-armv7 \
  --compress Brotli --public \
  --output tmag-cli-linux-armv7
```
或針對 64 位元 Pi OS 使用 `--targets node18-linux-arm64`。

---

## 7. 專案結構概覽

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

## 8. 開發與測試

1. 更動程式後可直接執行 `npm start`（CLI summary 模式）。
2. 若要觀察 APRS 上行內容，可同時開啟 Electron GUI 的「Mapping 封包追蹤」頁面，或在 CLI 監看 log。
3. 修改 comment 等站台資訊時，Bridge 只會送出更新後的 Beacon，不會重連或干擾定時器；Mapping 中的「已上傳」狀態會在收到對應 flow 的 APRS uplink 後自動更新。

---

## 9. 常見問題

- **為何 GitHub Repo 只有原始碼沒有 `dist/`？**  
  build 產物會破百 MB，已排除在 git 外。先 `npm install` 後再執行 `npm run build:*` 可在本機重新產出。

- **Meshtastic 斷線會自動重連嗎？**  
  CLI 會自動重連；Electron 版由 GUI 控制，可手動重試或啟用背景重連。

- **APR S 不停重連怎麼辦？**  
  通常是同一呼號在不同實例上登入或 comment 更新後頻繁重連；目前我們只在呼號/SSID 變更時才 teardown APRS，若仍被踢請確認無其他裝置使用相同呼號。

---

## 10. 授權

此專案依原作者設定的授權條款釋出；若需商用或替代授權，請聯絡 maintainer。
