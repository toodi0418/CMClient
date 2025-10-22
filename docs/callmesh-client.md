# CallMesh 客戶端整合指南

本文整理平台客戶端與伺服器互動流程，涵蓋 Agent 規範與 API 呼叫方式。達成以下目標後，管理後台即可正確顯示客戶端狀態、作業系統與原始 Agent，並同步最新的 Mapping。

若使用本專案的 Electron 介面，可在「CallMesh API Key」欄位輸入並儲存 Key，程式會立即呼叫後端驗證：通過驗證後系統才允許連線；失敗則拒絕後續操作。右上角狀態列會顯示目前驗證/同步狀態。CLI 則可透過 `node src/index.js callmesh` 操作，並在執行前匯出 `CALLMESH_API_KEY` 以便驗證。

## Agent 命名規範

每次呼叫心跳 API 時，需要傳送 `X-Client-Agent` header（以及 body 的 `agent` 欄位）。請遵循格式：

```
<產品識別>/<版本> (<平台資訊>)
```

- `<產品識別>`：自訂代號，例如 `callmesh-client`、`callmesh-agent`，僅使用英數與 `-`。
- `<版本>`：建議使用語義化版本 `major.minor.patch`（例：`1.4.2`）。
- `<平台資訊>`：括號內描述作業系統 / 發行版 / 架構，可視需要加入 build 資訊。

### Windows 範例
| 作業系統 | 推薦平台字串 | Agent 範例 |
| --- | --- | --- |
| Windows 11 / 10 | `Windows NT 10.0` | `callmesh-client/1.4.2 (Windows NT 10.0; x64)` |
| Windows Server 2019 | `Windows Server 2019` | `callmesh-client/1.5.0 (Windows Server 2019)` |

### macOS 範例
| 系統版本 | 推薦平台字串 | Agent 範例 |
| --- | --- | --- |
| macOS 14 Sonoma | `macOS 14.0` | `callmesh-client/1.4.2 (macOS 14.0; arm64)` |
| macOS 13 Ventura | `macOS 13.0` | `callmesh-client/1.3.5 (macOS 13.0; x86_64)` |

### Linux 範例
| 發行版 | 推薦平台字串 | Agent 範例 |
| --- | --- | --- |
| Ubuntu 22.04 LTS | `Ubuntu 22.04` | `callmesh-client/1.4.2 (Ubuntu 22.04; x86_64)` |
| Ubuntu 21.04 | `Ubuntu 21.04` | `callmesh-client/1.4.2 (Ubuntu 21.04; amd64)` |
| Debian 12 | `Debian 12` | `callmesh-client/1.5.0 (Debian 12; arm64)` |
| CentOS Stream 9 | `CentOS Stream 9` | `callmesh-client/1.4.0 (CentOS Stream 9)` |
| Raspbian Bullseye | `Raspbian Bullseye` | `callmesh-client/1.3.0 (Raspbian Bullseye)` |

若無法取得精確版本，至少提供主作業系統（例：`Linux`、`Windows`、`macOS`）。

> 專案內建的 `callmesh` 指令會自動依上述規範產生 Agent。若需要客製，可透過 `--agent` 或 `--platform` 選項覆寫。

## API 呼叫流程

以下以 Windows 10 客戶端為例，其他平台流程相同：

1. **準備**：
   - 保存後台建立客戶端時收到的 API Key（僅顯示一次）。
   - 記錄上一輪的 mapping hash（第一次啟動為 `null`）。
   - 準備 Agent 字串，例如 `callmesh-client/1.0.0 (Windows NT 10.0; x64)`。

2. **送出心跳** `POST /api/v1/client/heartbeat`
   ```bash
   curl -s -X POST https://callmesh.tmmarc.org/api/v1/client/heartbeat \
     -H 'Content-Type: application/json' \
     -H 'X-API-Key: <API_KEY>' \
     -H 'X-Client-Agent: callmesh-client/1.0.0 (Windows NT 10.0; x64)' \
     -d '{"local_hash": null, "agent": "callmesh-client/1.0.0 (Windows NT 10.0; x64)"}'
   ```
 - 回應會告知最新 `hash`、`needs_update`、`provision` 以及 `server_time`。

3. **下載 Mapping（必要時）** `POST /api/v1/client/mappings`
   - 若 `needs_update = true` 或首次啟動（`local_hash = null`），呼叫：
     ```bash
     curl -s -X POST https://callmesh.tmmarc.org/api/v1/client/mappings \
       -H 'Content-Type: application/json' \
       -H 'X-API-Key: <API_KEY>' \
       -d '{"known_hash": null}'
     ```
   - 回傳的 `items` 為目前所有 `enabled=true` 的 Mapping；請儲存新的 `hash`，下次心跳時帶入 `local_hash`。

4. **自動派發設定**：
   - 若後台啟用「自動下發」功能，心跳回應會帶出 `provision` 物件，範例如下：

     ```json
     "provision": {
       "callsign_base": "BM2OBM",
       "ssid": -8,
       "symbol_table": "\\",
       "symbol_code": "&",
       "symbol_overlay": "P",
       "comment": "TMMARC Meshtastic APRS Gateway https://callmesh.tmmarc.org/",
       "tx_power_w": 25,
       "antenna_gain_dbi": 2,
       "antenna_height_m": 24.384
     }
     ```

   - 客戶端接收到 `provision` 時應立即更新：
     - 呼號核心：`callsign_base`
     - SSID：`ssid`（負數代表負號 SSID；`-1` 表示 `-1`、`-8` 表示 `-8`）
     - Symbol：`symbol_table` + `symbol_code`
     - Comment：`comment`，若後端未設定則預設為 `TMMARC Meshtastic APRS Gateway https://callmesh.tmmarc.org/`
     - Symbol Overlay：`symbol_overlay`（值為 `null` 代表關閉 overlay）
     - 其餘欄位（如 `latitude`、`longitude`、`tx_power_w`、`antenna_gain_dbi`、`antenna_height_m`）僅在後台勾選對應選項時才會下發；若未下發則維持本地設定。
   - ✔️ 後台帳號 `tony_xd` 的客戶端 `123` 實測可收到上述設定；建議將採用的值記錄於 log 或狀態檔，方便稽核。
   - 若 `provision` 為 `null`，表示目前採用手動配置，可忽略此欄位。

5. **排程與重試**：
   - 固定每 60 秒送一次 heartbeat（Windows 可用 Task Scheduler，macOS/Linux 可用 cron/systemd timer）。
   - 心跳失敗時應立即重試並記錄錯誤原因，避免超過 3 分鐘未報到導致後台顯示離線。

6. **關閉 / 離線**：
   - 若客戶端即將停止，最後再送一次 heartbeat（帶最新 hash），使後台更新「離線」時間。

> 注意：所有 API 呼叫都必須帶正確的 `X-API-Key`，否則會回傳 `401 invalid_client_key`。 API Key 遺失時，只能在後台刪除重建。

## CLI 自動化

本專案提供 `node src/index.js callmesh` 指令協助上述流程：

```bash
node src/index.js callmesh sync \
  --api-key "<API_KEY>" \
  --state-file ~/.config/callmesh/state.json \
  --mappings-output ~/.config/callmesh/mappings.json
```

- `sync`：預設模式，會送出 heartbeat 並在需要時下載最新 mapping。
- `heartbeat`：只送心跳並更新本地 hash。
- `mappings`：忽略 heartbeat，直接下載 mapping。
- `--force`：即使伺服器回報 `needs_update = false` 仍強制重新下載。
- `--product`、`--client-version`、`--platform`、`--agent` 可覆寫 Agent 各欄位。

指令會將 `hash`、`provision`、`mappings` 等資料儲存在 `--state-file` 指定的 JSON，方便排程器週期性呼叫。

## 參考指令
把上述流程整理成簡易指令範本：

```bash
# 1) 設定變數
API_KEY="<你的 API Key>"
AGENT="callmesh-client/1.0.0 (Windows NT 10.0; x64)"
HASH="null"   # 首次啟動為 null，之後換成上一輪回應的 hash

# 2) 心跳
curl -s -X POST https://callmesh.tmmarc.org/api/v1/client/heartbeat \
  -H 'Content-Type: application/json' \
  -H "X-API-Key: $API_KEY" \
  -H "X-Client-Agent: $AGENT" \
  -d '{"local_hash":'"$HASH"',"agent":"'$AGENT'"}'

# 3) 需要更新時抓 mapping
curl -s -X POST https://callmesh.tmmarc.org/api/v1/client/mappings \
  -H 'Content-Type: application/json' \
  -H "X-API-Key: $API_KEY" \
  -d '{"known_hash":'"$HASH"'}'
```

依上述規範實作後，CallMesh 管理後台即可正確顯示：
- 在線／離線狀態
- 解析後的作業系統（`Windows`、`macOS`、`Linux` 等）
- 原始 Agent 字串
- 最近一次心跳時間
- （如啟用）自動派發配置

請開發團隊在各平台客戶端實作時，統一遵守此文件，以便風險控管與維運監控。
