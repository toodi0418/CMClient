# Meshtastic TCP 封包擷取工具

這是一個使用 Node.js 實作的簡易 CLI，能夠連線到 Meshtastic 裝置的 TCP API（預設埠 `4403`），並解析以 protobuf 編碼的 `FromRadio` 封包後輸出成 JSON。若需要檢查原始位元組，也可同時顯示十六進位字串。

## 環境需求

- Node.js 16 以上版本（建議 18+）
- Meshtastic 裝置或 gateway，且已開啟 TCP API（預設在 `tcp://<裝置 IP>:4403`）

## 安裝

```bash
npm install
```

這會安裝 `protobufjs` 與 `yargs`，並保留 `proto/meshtastic` 目錄下所需的 `.proto` 定義。

## 使用方式

最簡單的用法（連到本機的 Meshtastic TCP 伺服器）：

```bash
npm start
```

或直接使用 `node` 執行：

```bash
node src/index.js --host <裝置 IP> --port 4403
```

常用參數：

- `--host, -H`：目標 Meshtastic TCP 伺服器（預設 `127.0.0.1`）
- `--port, -P`：連接埠（預設 `4403`）
- `--max-length, -m`：允許的最大封包大小，依 Meshtastic 協定預設為 `512` bytes，若韌體更新可自行調整
- `--pretty, -p`：搭配 `--format json` 使用，是否以縮排輸出 JSON（預設開啟）
- `--show-raw, -r`：在摘要模式下，同時列印 payload 的十六進位字串
- `--format, -f`：切換輸出格式，`summary`（預設）為表格、`json` 為完整訊息
- `--handshake, -k` / `--no-handshake`：預設會在連線後送出 `want_config` 請求以觸發裝置回傳節點/設定資訊，可依需求關閉
- `--heartbeat, -b`：指定秒數（例如 `--heartbeat 30`）即可定期送出 heartbeat 維持連線，預設為 `0` 表示不傳送
- 所有指令執行前需設定 `CALLMESH_API_KEY` 環境變數，確保系統已綁定合法的 CallMesh 憑證

若想列出區域網路內的 Meshtastic 裝置，可執行：

```bash
node src/index.js discover
```

程式會透過 mDNS (`_meshtastic._tcp`) 掃描裝置並顯示 Host/Port 及節點資訊。

CLI 內建 `--help` 可隨時查看支援選項：

```bash
node src/index.js --help
```

## Electron 桌面應用

若希望以 GUI 方式即時監控封包，可啟動內建的 Electron 應用程式：

```bash
npm run desktop
```

打開後輸入 Meshtastic TCP API 的 `Host` 與 `Port`（預設 127.0.0.1:4403），按下「連線」即可看到與 CLI 相同的資料表。節點欄位同樣會附上 Mesh ID，點選「中斷」即可關閉連線；應用預設會記住上一個連線設定。

此外，點擊「掃描」可自動搜尋網路中的 Meshtastic 裝置（同樣依賴 mDNS），搜尋結果能直接套用到輸入欄位。
※ 首次使用前請於畫面中的「CallMesh API Key」欄位輸入後按下「儲存」。系統會立即驗證 Key 是否有效，未通過驗證將無法建立 Meshtastic 連線。

## CallMesh 客戶端整合

專案內含 `callmesh` 指令，可依 [CallMesh 客戶端整合指南](docs/callmesh-client.md) 執行心跳與 mapping 更新。

```bash
# 送一次心跳並必要時同步 mapping，狀態會記錄在 ./callmesh-state.json
export CALLMESH_API_KEY="<API_KEY>"
node src/index.js callmesh sync \
  --state-file ~/.config/callmesh/state.json \
  --mappings-output ~/.config/callmesh/mappings.json
```

- `sync`：先送心跳，若伺服器要求更新（或以 `--force` 指定）則下載 mapping。
- `heartbeat`：只送心跳並更新本地 `hash`／`provision`（仍需 `CALLMESH_API_KEY`）。
- `mappings`：忽略心跳直接抓取最新 mapping。
- `--product`、`--client-version`、`--platform`、`--agent` 可客製 Agent 字串；未指定時會依系統自動推算。

指令會先驗證 API Key 是否有效，成功後才送出心跳，並將 hash、provision、mapping 等資料寫入 `--state-file` 指定的 JSON，方便排程器每 60 秒週期性呼叫。

## 摘要輸出

預設的 `summary` 模式會即時列出每個封包，欄位依序為日期、節點、頻道、SNR、RSSI、封包類型、Hops 與額外資訊。節點欄位會顯示暱稱（或表情符號）以及 Mesh ID，方便辨識來源。例如：

```
10/12 18:44:16 | 4cdc -> !c576aa1b          |  0 |  -4.25 |  -114 | Position     | 2/7    | (24.269, 120.493) 17m asl
```

目前針對 `Position`、`Routing` 與 `Telemetry` 封包會進一步解碼，顯示座標、路徑或電量等細節；其他封包則顯示 portnum 名稱。若改用 `--format json`，即回到原始的 JSON 輸出。

## 內部實作概要

- 程式會先載入 `proto/meshtastic/mesh.proto` 以及其引用的其他 `.proto` 檔案，並使用 `meshtastic.FromRadio` 訊息定義解碼。
- Meshtastic TCP 封包使用 4 bytes 標頭：前兩個 bytes 固定為 `0x94C3`，後兩個 bytes 是 payload 長度（big-endian）。程式會持續監聽資料流並依此 framing 做切割，遇到異常長度時會重新尋找同步標記。
- 解析後的結果預設以摘要表格呈現，並針對常見的 `Position` / `Routing` / `Telemetry` 封包進行額外解碼；若需要原始資料可切到 JSON 模式。
- 預設會在連線後送出一個 `ToRadio.want_config_id` 請求，與官方 App 類似，可確保裝置立即回傳最新的節點與設定資料。必要時也可以加上 heartbeat 以保持連線活躍。

## 注意事項

- 這個工具僅用來接收並解析 Meshtastic 裝置送出的 `FromRadio` 封包，若需要送出 `ToRadio` 指令需額外擴充。
- 連線中斷時會顯示提示訊息，如需自動重連可在外部用 `systemd`, `supervisord` 或 shell script 包裝。
- 若裝置端同時輸出其他非 Meshtastic 封包資料，請確認已關閉額外的 serial debug，以免造成 framing 無法同步。
