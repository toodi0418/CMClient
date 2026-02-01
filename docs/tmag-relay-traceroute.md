<!-- TMAG-RELAY Traceroute 解碼指南 -->
# TMAG-RELAY Traceroute 解碼指南

本文件說明 TMAG-RELAY 端如何解碼 **去程 / 回程** traceroute 路徑，以及 SNR 值的正確轉換方式。

## 1. 來源封包類型

TMAG-RELAY 會收到 Meshtastic 原始 protobuf（`fromRadio`/`toRadioRaw`）。  
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

## 2. 基本解碼流程

1. 解碼 `FromRadio`。
2. 檢查 `decoded.data.portnum` 是否為 `ROUTING_APP`。
3. 解碼 `Routing`，取 `route_reply`（或 `route_request`，但 traceroute 回應是 `route_reply`）。
4. 取得：
   - `route`（去程中途節點）
   - `route_back`（回程中途節點）
   - `snr_towards`（去程 SNR）
   - `snr_back`（回程 SNR）

## 3. NodeNum 轉 Mesh ID

`route` / `route_back` 內的值是 **uint32 nodenum**，需轉為 Mesh ID：

```
meshId = "!" + toHex8(nodenum)   // 例如 1491251277 -> !58e2b04d
```

## 4. 去程 / 回程的完整路徑

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

## 5. SNR 轉換規則（重要）

`snr_towards` / `snr_back` 是 **dB * 4** 的整數，需要除以 4：

```
snr_db = raw / 4
```

特殊值：
- `-128` 代表未知，應忽略（不顯示）。

### 對應關係

一般情況：SNR 陣列長度 ≈ route 長度。  
建議對應方式（與目前 UI 一致）：

- 去程：
  - 不顯示在**第一個節點**（source）
  - 從第二個節點開始依序對應 `snr_towards[0]`、`snr_towards[1]`...
- 回程：
  - 不顯示在**第一個節點**（destination）
  - 從第二個節點開始依序對應 `snr_back[0]`、`snr_back[1]`...

也就是「**頭不顯示，尾要顯示**」。

## 6. 範例

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

## 7. 注意事項

- 有些韌體回覆 **只有 snr_back 或只有 snr_towards**，屬正常情況。
- `route` 可能為空，但 `snr_towards` 有值（或反之）。
- `route_back` 空時並不代表直回；可能是韌體未回傳。
- `!ffffffff` 應視為未知節點。

