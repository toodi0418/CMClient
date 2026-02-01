<!-- TMAG-RELAY Traceroute 解碼指南 -->
# TMAG-RELAY Traceroute 解碼指南（詳細版）

本文件說明 TMAG-RELAY 端如何解碼 **去程 / 回程** traceroute 路徑、SNR 值的正確轉換方式，以及常見缺值情境的判斷與處理。

---

## 1. 來源封包類型

TMAG-RELAY 會收到 Meshtastic 原始 protobuf（`fromRadio` / `toRadioRaw`）。  
Traceroute 的回應通常出現在 **Routing App**（`PortNum.ROUTING_APP`）的 `Routing.route_reply`。

Proto 定義（`proto/meshtastic/mesh.proto`）：

```
message Routing {
  oneof variant {
    RouteDiscovery route_request = 1;
    RouteDiscovery route_reply = 2;
    Error error_reason = 3;
  }
}

message RouteDiscovery {
  repeated fixed32 route = 1;
  repeated int32 snr_towards = 2;  // dB * 4
  repeated fixed32 route_back = 3;
  repeated int32 snr_back = 4;     // dB * 4
}
```

---

## 2. 基本解碼流程

1. 解碼 `FromRadio`。
2. 檢查 `decoded.data.portnum` 是否為 `ROUTING_APP`。
3. 解碼 `Routing`，取 `route_reply`（或 `route_request`，但 traceroute 回應是 `route_reply`）。
4. 取得：
   - `route`（去程中途節點）
   - `route_back`（回程中途節點）
   - `snr_towards`（去程 SNR）
   - `snr_back`（回程 SNR）

---

## 3. 必備欄位速查

下列欄位是 TMAG-RELAY 解碼時最常用的：

- `FromRadio.packet.from`：來源節點（source）
- `FromRadio.packet.to`：目的節點（destination）
- `Routing.route_reply.route`：去程中途節點列表
- `Routing.route_reply.route_back`：回程中途節點列表
- `Routing.route_reply.snr_towards`：去程 SNR（dB * 4）
- `Routing.route_reply.snr_back`：回程 SNR（dB * 4）

---

## 4. NodeNum 轉 Mesh ID

`route` / `route_back` 內的值是 **uint32 nodenum**，需轉為 Mesh ID：

```
meshId = "!" + toHex8(nodenum)
```

範例：

```
1491251277 -> 0x58e2b04d -> !58e2b04d
```

### 4.1 節點 ID 正規化規則

建議統一以下格式（避免混用）：

- `!` + 8 位 16 進位小寫（必要時補 0）
- 例如：
  - `0x58E2B04D` → `!58e2b04d`
  - `58e2b04d` → `!58e2b04d`

---

## 5. 去程 / 回程的完整路徑

`route` 與 `route_back` **只包含中途節點**。  
若要組成完整路徑：

- 去程（forward）：
  ```
  [source, ...route, destination]
  ```
- 回程（return）：
  ```
  [destination, ...route_back, source]
  ```

其中：
- `source` = 封包來源節點（FromRadio 內 `from`）
- `destination` = traceroute 目的節點（通常是 `to` 或 route_reply 的對端）

---

## 6. SNR 轉換規則（重要）

`snr_towards` / `snr_back` 是 **dB * 4** 的整數，需要除以 4：

```
snr_db = raw / 4
```

特殊值：
- `-128` 代表未知，應忽略（不顯示）。

### 6.1 SNR 對應關係

一般情況：SNR 陣列長度 ≈ route 長度。  
建議對應方式（與目前 UI 一致）：

- 去程：
  - 不顯示在**第一個節點**（source）
  - 從第二個節點開始依序對應 `snr_towards[0]`、`snr_towards[1]`...
- 回程：
  - 不顯示在**第一個節點**（destination）
  - 從第二個節點開始依序對應 `snr_back[0]`、`snr_back[1]`...

也就是「**頭不顯示，尾要顯示**」。

### 6.2 SNR 缺值情境（常見）

以下情況屬於正常：

- **只回 snr_back 或只回 snr_towards**  
  韌體可能只提供單邊 SNR。
- **SNR 陣列長度比 route 短**  
  後段節點無 SNR（顯示為無值即可）。
- **完全沒有 snr_towards**  
  UI 會顯示去程路徑，但 SNR 空白。

---

## 7. 範例

假設解碼結果：

```
from = 0x58e2b04d
to   = 0x03919375
route = [0x3fcdc072]
route_back = [0x06aa0416]
snr_towards = [ -47 ]    // raw
snr_back = [  44 ]       // raw
```

轉換：

```
route (hex) = [!3fcdc072]
route_back  = [!06aa0416]

SNR towards = -47 / 4  = -11.75 dB
SNR back    =  44 / 4  = 11.00 dB
```

完整路徑：

```
forward: !58e2b04d -> !3fcdc072 -> !03919375
return:  !03919375 -> !06aa0416 -> !58e2b04d
```

SNR 顯示（頭不顯示、尾顯示）：

```
forward: !58e2b04d, !3fcdc072(-11.75dB), !03919375
return:  !03919375, !06aa0416(11.00dB), !58e2b04d
```

---

## 8. 建議的 TMAG-RELAY 輸出格式（選用）

若 TMAG-RELAY 需要提供給上游服務使用，建議輸出以下 JSON 結構：

```
{
  "from": "!58e2b04d",
  "to": "!03919375",
  "route": ["!3fcdc072"],
  "routeBack": ["!06aa0416"],
  "snrTowards": [-47],
  "snrBack": [44],
  "forward": ["!58e2b04d", "!3fcdc072", "!03919375"],
  "return": ["!03919375", "!06aa0416", "!58e2b04d"],
  "snrTowardsDb": [-11.75],
  "snrBackDb": [11.0]
}
```

---

## 9. Debug Checklist

1. `portnum` 是否為 `ROUTING_APP`
2. 是否存在 `route_reply`
3. `route` / `route_back` 是否為 uint32
4. `snr_*` 是否為 int32（需除以 4）
5. `-128` 是否被忽略
6. `route` 空但 `snr_*` 有值是否照常處理

---

## 10. 注意事項

- 有些韌體回覆 **只有 snr_back 或只有 snr_towards**，屬正常情況。
- `route` 可能為空，但 `snr_towards` 有值（或反之）。
- `route_back` 空時並不代表直回；可能是韌體未回傳。
- `!ffffffff` 應視為未知節點。
