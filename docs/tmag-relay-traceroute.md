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

## 9. TMAG-RELAY 提問回覆（routeBack 解讀與拓樸建議）

以下回覆對應 TMAG-RELAY 團隊 2026-02-01 的提問內容，說明 CMClient 目前的解讀方式與建議。

### 9.1 routeBack 是否為完整回程路徑？

**不保證**。實務觀察（CMClient 端與社群回報）顯示：

- `routeBack` 可能比 forward 路徑短，甚至缺漏數個中繼節點。
- `routeBack` 有時像是回程路徑的片段 / 抽樣，而不是完整 hop 序列。
- `snr_back` 通常與 `route_back` 長度相符，但不代表回程 hop 全部可見。

因此若強行假設 `routeBack` 等同完整回程路徑，會產生「不可能的直連」。

### 9.2 CMClient 如何解讀 routeBack？

CMClient 的做法（與 UI 顯示一致）是：

- 去程完整路徑：  
  `forward = [from, ...route, to]`

- 回程完整路徑（僅依 routeBack）：  
  `return = [to, ...routeBack, from]`

- 若 `routeBack` 為空，回程視為未知（顯示「直回」或空白）。
- 不會補齊 forward 中缺失的中途節點，也不會嘗試對齊 forward。

### 9.3 你們信任 routeBack 來畫拓樸嗎？

**有限度信任**。實際策略如下：

- forward 路徑是主要結構（可信度較高）。
- return 路徑僅作為「可見片段」繪製，不假設完整性。
- 若 `routeBack` 造成不可能的直連，那是 `routeBack` 本身缺節點的結果，不是解碼錯誤。

### 9.4 snrBack 如何對應到 edges？

規則是按照 return 陣列的節點順序一一對應：

```
return nodes = [to, r1, r2, ..., from]
snrBack[i] 對應 return nodes[i] → return nodes[i+1]
```

且 `snr_back` 需先除以 4 才是 dB。

### 9.5 拓樸避免「不可能 edge」的建議

若你們想降低錯誤邊的視覺干擾，可採用以下策略：

**建議 A（保守、穩定）**
- 拓樸只用 forward 畫結構。
- 回程只顯示在卡片或線上標籤，不進圖。

**建議 B（折衷）**
- forward 畫結構。
- return 僅在 `routeBack.length >= 2` 且 `snrBack.length` 足夠時才畫線。

**建議 C（現況）**
- forward + return 都畫，但標註為「回程片段」。
- UI 文案提醒回程路徑可能不完整（避免誤解）。

---

### 9.6 與你們提供的例子對齊

```
from: !43396ec0
to:   !0406c85c
route:     [!58e2b04d, !03919375, !ac7f3e7c, !23b6043a]
routeBack: [!23b6043a, !58e2b04d]
```

CMClient 會解讀為：

```
forward: !0406c85c → !58e2b04d → !03919375 → !ac7f3e7c → !23b6043a → !43396ec0
return:  !43396ec0 → !23b6043a → !58e2b04d → !0406c85c
```

其中 `!23b6043a → !58e2b04d` 看起來跨跳，  
但這是 `routeBack` 本身缺少中途節點所致。

---

## 10. 結論（給 TMAG-RELAY 的一句話）

`routeBack` 在實務上常是「回程片段」，不可視為完整 hop；  
建議以 forward 作拓樸骨幹，return 只當補充資訊或品質提示。

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

---

## 11. 路徑鄰居圖（Route Neighbors）結構說明

這個「路徑鄰居圖 (Route Neighbors)」的結構設計是用來幫助您一眼看懂 Mesh 網路的骨幹與訊號品質。以下是詳細的結構說明：

1. 佈局邏輯 (Layout)
當您選擇 "Pyramid (Hubs Top)" 模式時：

頂端 (Apex): 程式會自动找出連接數最多的節點（Hub / 核心中繼）。它是目前網路上最忙碌的樞紐，被放置在金字塔的頂端。
層級 (Layers): 其他節點會根據與核心節點的「跳數距離」依序向下排列。
意義: 越上面的節點越重要，通常是位置優越的中繼站或 Gateway；越下面的通常是終端用戶。
2. 連線 (Edges)
圖上的連線代表實際成功傳輸過的 Traceroute 路徑：

線條樣式 (方向):
實線 (Solid): 代表 去程 (Request)，即發起端送到目標端的路徑。
虛線 (Dashed): 代表 回程 (Reply)，即目標端回傳給發起端的路徑。
注意：Traceroute 的去程與回程路徑經常不同（網路的不對稱性）。
顏色 (訊號品質 SNR):
🟢 綠色: 訊號極佳 (SNR > 5 dB)。
🟡 黃色: 訊號普通 (0 ~ 5 dB)。
🔴 紅色: 訊號微弱 (SNR < 0 dB)，隨時可能斷線。
標籤: 線上的數字即為該段路徑的 SNR 值，直接反映鏈路品質。
3. 節點 (Nodes)
標示: 顯示格式為 長名稱 (短名稱)，例如 台北小可愛 (ef18)。
點擊互動: 點擊節點或連線，右側會滑出詳細面板，顯示最後連線時間、封包計數等細節。
這張圖表只會顯示最近 3 小時內（預設）有發生過 Traceroute 互動的節點，因此它反映的是「即時的網路拓撲」，而非歷史累積的靜態地圖。
