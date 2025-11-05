# TenManMap WebSocket API（新版驗證流程）

本文說明新版 WebSocket 介面，特別是採用「連線後先驗證，再傳資料」的流程。所有範例皆使用 JSON 格式。

## 連線與傳輸協定

- **端點**：`wss://<host>/ws`
- **協定**：WebSocket over TLS（Hypercorn/ASGI）
- **編碼**：所有訊息為 UTF-8 JSON 物件

## 基本訊息格式

```jsonc
{
  "action": "<命令名稱>",
  // 其他鍵視 action 而定
}
```

伺服器的回覆亦為 JSON，通常會帶上 `type` 或 `status` 方便判斷。

## 流程總覽

1. **建立 WebSocket 連線**。
2. 連線成功後，客戶端必須先送出 `authenticate` 訊息並帶上 Cellmesh API Key。
3. 伺服器回覆 `auth` → `status: pass` 時，代表已授權，可以開始上傳位置資料。
4. 在同一個 WebSocket 連線內，後續的 `publish` 不需再帶 API Key。
5. 連線中斷後重新連線，需要重新走一次驗證流程。

## 驗證（authenticate）

### 請求

```json
{
  "action": "authenticate",
  "api_key": "YOUR_CELL_MESH_API_KEY"
}
```

> `api_key` 為必填字串，伺服器會以此向資料庫查詢合法的 gateway 註冊資訊。

### 成功回應

```json
{
  "type": "auth",
  "status": "pass",
  "gateway_id": "GW1234",
  "node_id": "NODE-001"
}
```

- `status: pass` 代表驗證通過。
- `gateway_id` / `node_id` 是伺服器留存的註冊資訊，後續上傳位置時會自動套用。

### 失敗回應

```json
{
  "type": "auth",
  "status": "fail",
  "action": "authenticate"
}
```

常見原因：API Key 錯誤、網路延遲、該 Gateway 被停用等。必須修正後再次發送驗證。

## 上傳位置資料（publish）

驗證成功後即可傳送位置封包。格式沿用原本的 `publish` action，但 **不用** 再附 `api_key`。伺服器會把驗證時取得的 gateway 資訊填入 `gateway_id` 儲存。

### 請求

```json
{
  "action": "publish",
  "payload": {
    "device_id": "N0C12345",
    "node_name": "隨身節點",
    "timestamp": "2025-11-04T04:45:06.123456+00:00",
    "latitude": 25.033964,
    "longitude": 121.564468,
    "altitude": 15.2,
    "speed": 12.5,
    "heading": 182.3,
    "extra": {
      "battery": 87
    }
  }
}
```

欄位限制與舊版相同：
- `timestamp` 必須是含時區的 ISO 8601 字串。
- `latitude` / `longitude` 為合法的 WGS84 座標。
- `extra` 可選，為自定義 JSON 物件。
- `node_name` 可選，建議提供使用者能理解的節點名稱；若缺省則介面會 fallback 到 `device_id`。

### 成功回應

```json
{
  "type": "ack",
  "device_id": "N0C12345",
  "node_name": "隨身節點",
  "timestamp": "2025-11-04T04:45:06.123456+00:00",
  "latitude": 25.033964,
  "longitude": 121.564468,
  "altitude": 15.2,
  "speed": 12.5,
  "heading": 182.3,
  "gateway_id": "NODE-001",
  "extra": {
    "battery": 87
  }
}
```

伺服器會在 ACK 與推播的 `point` 訊息中回傳相同的 `node_name`，地圖前端會優先使用此名稱顯示節點標籤。

若上傳前未先驗證，伺服器會回覆：

```json
{
  "error": "authentication required",
  "action": "publish"
}
```

## 訂閱功能（subscribe / unsubscribe）

此部分與舊版本相同，僅影響地圖介面資料推播，與位置上傳無關：

- `subscribe`：`{"action":"subscribe","device_id":"*","track":false}`
- `unsubscribe`：`{"action":"unsubscribe","device_id":"*"}`

建議在驗證成功後再訂閱，以確保連線是合法的。

## 斷線與重連

- WebSocket 連線中斷後，伺服器即清除該連線的驗證狀態與訂閱資料。
- 客戶端重連後必須「重新送 authenticate」才可繼續上傳位置。

## 錯誤代碼（摘要）

| 錯誤內容                  | 時機                     | 解決方法                         |
|---------------------------|--------------------------|----------------------------------|
| `api_key is required`     | `authenticate` 缺少欄位  | 補上合法 API Key                 |
| `auth` → `status: fail`   | API Key 不合法或被停用   | 重新確認 Portal 上的設定        |
| `authentication required` | 未驗證就呼叫 `publish`   | 先送 `authenticate`              |
| `invalid payload`         | `publish` 內容不合格式   | 依欄位限制修正資訊              |

## 範例時序圖

```
客戶端                        伺服器
   | --- (WebSocket 握手) -->  |
   | <--- 101 Switching ---    |
   |                           |
   | -- authenticate(api_key) ->|
   | <- auth:pass -------------|
   |                           |
   | ---- publish(payload) --->|
   | <- ack -------------------|
   |                           |
（連線中斷）
   | --- 重新握手 -------->    |
   | <- 101 Switching -----    |
   | -- authenticate --------> |  // 必須重新驗證
```

## 注意事項

- 整個流程假設資料庫已預先註冊合法的 gateway，並分配對應 API Key。
- 若伺服器啟用 TLS，請確保客戶端使用 `wss://` 並信任該憑證。
- 建議實作重試機制：若連線或驗證失敗，等待一段時間後重新嘗試。毎次重連都要重送 `authenticate`。
