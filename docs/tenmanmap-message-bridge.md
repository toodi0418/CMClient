# TenManMap 訊息互通規格

> 版本：2025-02-XX（同步 `CMClient` TenManMap 擴充功能）

TenManMap 目前透過 WebSocket 取得位置封包。本文件說明 `CMClient` 內建的「訊息全量轉發」與「TenManMap 下行指令」設計，協助 TenManMap 團隊調整伺服器與前端邏輯。內容以現行 `CallMeshAprsBridge` 行為為基礎，並延伸其佇列、驗證與節流機制。

---

## 1. WebSocket 通道總覽

- **Endpoint**：`wss://tenmanmap.yakumo.tw/ws`
- **驗證**：連線後立即送出

  ```json
  {
    "action": "authenticate",
    "api_key": "<CallMesh API Key>",
    "suppress_ack": true    // 選填；預設 true，欲接收 ack 則設為 false
  }
  ```

- **伺服器回應**
  - 驗證成功：`{"action":"authenticate","status":"pass","gateway_id":"<GW>","node_id":"<NODE>"}`（欄位名稱可能為 `result` / `ok`）
  - 驗證失敗：`{"action":"authenticate","status":"fail","error": "..."}`
- **心跳 / 重連**：由 `CallMeshAprsBridge` 管理，連線中斷時會於 5 秒後自動重試。
- **佇列限制**：最多保留 64 筆尚未送出的訊息（位置 + 訊息共用），超量時最舊資料會被捨棄並記錄 `TENMAN` log。

---

## 2. 位置資料（現行行為）

為避免破壞既有整合，位置發佈仍採既有格式：

```json
{
  "action": "publish",
  "payload": {
    "device_id": "<mesh 或 gateway ID>",
    "timestamp": "2025-02-08T15:04:23+08:00",
    "latitude": 25.033964,
    "longitude": 121.564468,
    "altitude": 23.4,
    "speed": 12.5,
    "heading": 270,
    "node_name": "台北中繼-01",
    "extra": {
      "source": "TMAG",
      "mesh_id": "!abcd1234",
      "short_name": "TPE1"
    }
  }
}
```

TenManMap 可繼續沿用既有處理流程。

---

## 3. 上行：Meshtastic → TenManMap 文字訊息

### 3.1 觸發來源

- `MeshtasticClient.summary` 中 `type = "Text"`（含壓縮文字）或 `detail` 為文字內容的封包。
- 預設涵蓋 CH0~CH3 的廣播訊息；未來若加入點對點或特殊 Port，將在 `payload.scope` 標示。

### 3.2 WebSocket 負載

```json
{
  "action": "message.publish",
  "payload": {
    "message_id": "1739017465123-a1b2c3d4",     // 對應 summary.flowId
    "received_at": "2025-02-08T15:04:25+08:00", // summary.timestamp + UTC+8
    "channel": 0,
    "text": "QTH OK",
    "encoding": "utf-8",
    "scope": "broadcast",                       // 目前僅支援 broadcast
    "from": {
      "mesh_id": "!abcd1234",
      "short_name": "NODE1",
      "long_name": "中和測試節點",
      "last_seen_at": "2025-02-08T15:03:59Z"
    },
    "to": {
      "mesh_id": "!efef9876",
      "short_name": "NODE2"
    },
    "relay": {
      "mesh_id": "!11223344",
      "guessed": false
    },
    "hops": {
      "used": 1,
      "limit": 3
    },
    "rssi": -87,
    "snr": 9.75,
    "raw_hex": "515448204f4b",
    "raw_length": 6,
    "mesh_packet_id": 305419896,                  // 新增：原始 mesh packet id，可用於 reply_id
    "reply_id": 123456789,                        // 若原封包為 Meshtastic 回覆，會填入原始 reply_id
    "reply_to": "!efef9876"                       // 若為群組回覆（broadcast + destination），會提供回覆目標
  }
}
```

### 3.3 拓展注意

- `scope` 目前僅會傳 `broadcast`，但預留點對點（`directed`）與群組（`group`）。
- `text` 為 UTF-8，若後續需要傳遞純位元資料，可追加 `encoding: "base64"` 與 `raw_base64` 欄位。
- `relay.guessed = true` 代表透過歷史資料推測最後轉發節點，TenManMap 顯示時可附註。
- 若伺服器啟用 `suppress_ack = false`，會收到 `{"type":"ack","action":"message.publish","status":"ok","message_id":"..."}`。

---

## 4. 下行：TenManMap → Meshtastic 指令

TenManMap 可透過同一 WebSocket 送出文字訊息，由 `CallMeshAprsBridge` 轉交 Meshtastic。

### 4.1 指令格式

```json
{
  "action": "send_message",
  "payload": {
    "client_message_id": "tenman-20250208-00001", // 選填；ack 會回傳
    "channel": 0,
    "text": "ALL OK",
    "encoding": "utf-8",                           // 預設 utf-8
    "scope": "broadcast",                          // broadcast / directed
    "destination": "!efef9876",                    // broadcast=群組回覆時可填寫對象；scope=directed 時必填
    "reply_id": 305419896,                         // 選填；若要讓 Meshtastic 原生 UI 顯示回覆指標，請帶入原訊息的 mesh_packet_id
    "want_ack": false                              // 選填；預設 false
  }
}
```

### 4.2 成功回應

```json
{
  "type": "ack",
  "action": "send_message",
  "status": "accepted",
  "client_message_id": "tenman-20250208-00001",
  "flow_id": "1739017468890-5f5e4d3c",
  "scope": "broadcast",
  "mesh_destination": "broadcast",
  "reply_to": "!efef9876",                         // broadcast + destination 時帶回對象
  "mesh_packet_id": 412345678,                    // 新送出的封包 ID，可供後續回覆使用
  "channel": 0,
  "queued_at": "2025-02-08T15:05:05+08:00"
}
```

- `flow_id`：`CallMeshAprsBridge` 生成的內部識別碼，可用於日後網頁對照。
- 回應 `status` 可能值：
  - `accepted`：已排入送出佇列；
  - `delivered`：成功交給 Meshtastic stack（需後續韌體確認）；
  - `failed`：傳送失敗，會附帶 `error_code`。
- 傳送時固定套用 hop limit = 6，確保訊息可跨節點中繼。
- `scope=directed` 時必須提供 `destination`（`!` 開頭 Mesh ID），橋接層會自動轉為點對點傳送。
- 若仍以 `broadcast` 廣播但希望標示「回覆對象」，可在 `destination` 帶入 Mesh ID；bridge 會保留廣播行為，並在 ACK 加入 `reply_to`。
- Meshtastic 官方 App 需要 `reply_id` 才會顯示回覆指標。請將原訊息 `message.publish.payload.mesh_packet_id` 回填為 `reply_id`（橋接層也會在 ACK 的 `reply_id` / `mesh_packet_id` 提供）。

### 4.3 可能錯誤碼

| `error_code`       | 說明                                                   | 常見修正                                         |
| ------------------ | ------------------------------------------------------ | ------------------------------------------------ |
| `UNAUTHORIZED`     | 未通過驗證或 API Key 失效                               | 重新驗證 `authenticate`                          |
| `INVALID_PAYLOAD`  | 缺少必要欄位或欄位型態錯誤                             | 檢查 `channel`/`text`/`destination`              |
| `UNSUPPORTED_SCOPE`| 指定了尚未支援的 `scope`                              | 僅使用 `broadcast` 或 `directed`                 |
| `MESSAGE_TOO_LONG` | 文本 > 200 Bytes（Meshtastic 限制）                    | 請裁切或改以多則訊息                             |
| `ROUTING_UNAVAILABLE` | 指定 Mesh ID 不在 CallMesh 節點列表中或處於離線 | 確認節點是否在線／提供備援流程                  |
| `RATE_LIMITED`     | TenManMap 短時間內送出過多訊息                         | 稍待 5 秒再重試（預設每 5 秒允許 1 則）          |
| `INTERNAL_ERROR`   | 其他未捕捉錯誤                                         | 參考 `TENMAN` log，並回報 CallMesh 團隊         |
| `INVALID_DESTINATION` | `scope=directed` 缺少或填錯 `destination` Mesh ID | 填入 `!` 開頭的 8 位十六進位 Mesh ID             |

### 4.4 節流與排程

- 佇列採 FIFO，單次傳送完成後才會處理下一筆。
- 為避免刷屏，預設對 TenManMap 下行訊息套用 **5 秒 1 則** 的節流；後續可調。
- `want_ack = true` 時，若 Meshtastic 韌體回覆 ACK/NAK，會轉成對應 `status` (`delivered` 或 `failed` + `ACK_TIMEOUT`)。

---

## 5. 啟用與控管

- CLI / Electron 可透過偏好設定取消 TenManMap 分享：`--no-share-with-tenmanmap`、環境變數 `TENMAN_DISABLE=1` 或 GUI 勾選。
- 下行功能僅在分享開啟時啟用；若使用者手動停用分享，伺服器會收到 `{"type":"error","action":"send_message","status":"disabled"}`。
- 若需於「僅收不發」情境運作，可討論新增 `TENMAN_RX_ONLY=1` 環境變數（預計後續補強）。

---

## 6. 驗證步驟建議

1. 使用測試 API Key 連線並驗證成功。
2. 從 Meshtastic 傳送文本，確認收到 `message.publish` JSON，內容包含 `message_id`。
3. 從 TenManMap 發送 `send_message`，確認：
   - 得到 `status=accepted` ack；
   - Meshtastic 端收到文字；
   - CallMesh UI 的訊息分頁出現 `auto summary synthetic=false`。
4. 測試長度限制（>200 bytes 應回 `MESSAGE_TOO_LONG`）。
5. 測試停用分享後的錯誤回覆。

---

## 7. 後續延伸

- 若 TenManMap 需要影像或檔案類型，可擴充 `encoding` 與 `payload.kind`。
- 佇列與節流參數可視實際流量調整。
- 如需跨組織共用，可考慮在 WebSocket 再加上子帳號授權欄位（例如 `tenant_id`）。

---

如有任何欄位命名或流程調整需求，請直接在專案 issue 留言，或透過 CallMesh 團隊聯繫。前三週間內文件仍視為草案，實作時若有重大差異會再同步更新。
