# Meshtastic → APRS-IS 防回朔（Anti-Backtrack / Anti-Out-of-Order）機制規格（搭配既有去重）

> 適用情境：你目前只有「去重」(duplicate suppression)；但遇到 **不同內容的新位置包** 因為 **RF/mesh 延遲、亂序、晚到** 而造成 aprs.fi（或其他顯示端）「位置倒退/瞬移」的問題。  
> 限制條件：**不動硬體、不依賴 tracker 內部時鐘正確**（只用你 gateway/伺服器收到包的時間）。

---

## 1. 問題定義

### 1.1 你現有的去重能解什麼
- 去重能解：**同一個 RF 包被多個站台收到了** → 最快上傳者先送，其他站看到「伺服器已經有」就不再送。
- 去重不能解：**不是同一個包**，而是「不同座標的新包」**晚到**或**亂序**到達 → 內容不同、所以去重一定放行 → 造成回朔。

### 1.2 回朔的典型形狀（你貼的 log 就是這種）
- 位置沿著軌跡正常前進（A）
- 突然插入一顆「離當前軌跡很遠」的點（B）
- 下一包又回到原本軌跡附近（A'）
- A → B → A' 會讓 aprs.fi 的「最新位置」倒退或抖動

---

## 2. 目標與非目標

### 2.1 目標
1. **不讓孤立的瞬移/回朔點污染 APRS-IS**
2. 保持一般移動（含迴轉）正常上傳
3. **高鐵友善**：允許 300 km/h 等級的移動，不要整路被擋
4. 與既有去重機制並存：先去重、再防回朔

### 2.2 非目標（務實一點）
- 不追求「100% 不漏點」且「100% 不回朔」同時成立  
  （在不知道裝置真實發包時間且 mesh 會亂序的前提下，這是互相衝突的願望）
- 不嘗試把晚到點「補點回填」到 aprs.fi  
  （aprs.fi 本身不支援你任意回填歷史點；你只能選擇送或不送）

---

## 3. 介面與整體流程（你要改的地方）

你現有流程（簡化）：
1. 收到封包 → 抽 callsign + content
2. 去重 cache 查詢（你現有）
3. 不重複 → 直接上傳 APRS-IS

你要改成：
1. 收到封包 → 抽 callsign + content + lat/lon
2. **Gate 0：去重（維持你現有）**
3. **Gate 1：防回朔（新增）**
4. Gate 1 放行才上傳 APRS-IS
5. **只有「成功上傳」才更新 last_uploaded 狀態**（超重要）

> 放置位置：**就在「準備 send APRS-IS」之前**。  
> 這樣你不用改你的 RF 接收與解析，只改輸出決策。

---

## 4. 核心觀念：兩套狀態（別踩坑）

每個 callsign（例如 BU2GE-7）都要維護兩種狀態：

### 4.1 `last_uploaded_*`（只記「你真的上傳成功」的最後狀態）
- `last_uploaded_time`：你最後一次**成功上傳**的時間（用你伺服器時間）
- `last_uploaded_pos`：最後一次**成功上傳**的座標
- `prev_uploaded_pos`：倒數第二次成功上傳的座標（用來做方向/穩定性）

> **規則：任何被你判為 outlier / pending 的點，絕對不能更新 last_uploaded。**  
> 否則你會把一顆鬼點當成基準，接下來正常點反而被判「跳太快」，整段邏輯崩壞。

### 4.2 `pending_outlier`（暫存「可疑大跳點」，等第二包確認）
- `pending_pos`
- `pending_first_seen_time`
- `pending_reason`（optional：例如 speed_exceeded / far_from_cluster / reverse_jump）

---

## 5. Gate 1：防回朔演算法（高鐵友善版）

### 5.1 參數（建議初始值）
你先照這組上線，之後再用實際 log 微調：

- `V_CAR = 200 km/h`  
  一般道路移動合理上限（含國道）
- `V_HSR = 380 km/h`  
  高鐵友善上限（留誤差）
- `HSR_ENTER = 240 km/h`  
  進入「高鐵模式」的門檻
- `HSR_EXIT = 180 km/h`  
  退出「高鐵模式」門檻（做 hysteresis 避免跳來跳去）
- `R_CONFIRM = 5~8 km`  
  兩次確認半徑：第二包若落在 pending 附近才承認
- `T_CONFIRM = 3~5 分鐘`  
  兩次確認時間窗：太久就當不可信
- `PENDING_TIMEOUT = 20 分鐘`  
  pending 放太久就清掉
- `DT_MIN = 10 秒`  
  dt 太小速度會爆炸，直接當 outlier 或進 pending
- `CLUSTER_K = 5`  
  用最近 5 個「已上傳點」做穩定性判斷（可選但很有效）
- `CLUSTER_RADIUS = 10~15 km`  
  與最近軌跡群的距離超過這個也算 outlier（視你的點精度調整）

> 你不用一次全開；最小可用組合是：`V_* + pending 二次確認 + last_uploaded only`。

---

## 6. 模式判定：車模式 vs 高鐵模式（可選但建議）

### 6.1 為什麼需要模式
你若只用一個速度上限：
- 設太低：高鐵整路被擋
- 設太高：回朔點更容易穿過

### 6.2 判定方式（不依賴 tracker 時鐘）
用你最近的 **已上傳**點估算速度（以伺服器時間）：
- 若最近連續 2~3 次速度 > `HSR_ENTER` → 進入 HSR 模式
- 若在 HSR 模式下連續 3 次速度 < `HSR_EXIT` → 退出 HSR 模式

在不同模式下採用不同 `Vmax`：
- 車模式：`Vmax = V_CAR`
- 高鐵模式：`Vmax = V_HSR`

---

## 7. Outlier 判斷（什麼情況要進 pending）

收到新位置點 `P`（已通過去重）後，先做 outlier 檢測：

### 7.1 基本速度檢測（必備）
以 `last_uploaded_*` 為基準：
- `dt = now - last_uploaded_time`
- 若 `dt < DT_MIN`：直接當 outlier（進 pending）
- `dist = distance(last_uploaded_pos, P)`
- `v = dist / dt`

若 `v > Vmax(當前模式)` → outlier（進 pending）

### 7.2 軌跡群距離檢測（建議，專治「孤立瞬移回舊點」）
取最近 `CLUSTER_K` 個 **已上傳**點（含 last、prev...）的中心（可用中位數/平均）得到 `C`：
- `d_cluster = distance(C, P)`
若 `d_cluster > CLUSTER_RADIUS` → outlier（進 pending）

> 你貼的 log 那種「突然跳到固定舊點、下一包又回來」，通常 `d_cluster` 會很大，一次就抓住。

### 7.3 反向回跳檢測（可選加強，處理 A→B→A 類型）
用 `prev_uploaded_pos`、`last_uploaded_pos`、`P` 做簡易判斷：
- 若 `distance(P, prev_uploaded_pos)` 明顯比 `distance(P, last_uploaded_pos)` 小很多  
  （例如靠近 prev 而遠離 last，且 dist(last, P) > 某門檻）  
  → 可能是「回跳」→ outlier（進 pending）

---

## 8. Pending 二次確認（把孤立鬼點擋掉）

### 8.1 第一次看到 outlier：不送、只暫存
若判定 outlier，且 `pending_outlier` 不存在：
- `pending_pos = P`
- `pending_first_seen_time = now`
- **不送 APRS-IS**
- **不更新 last_uploaded**

### 8.2 第二次看到相近點：承認位置真的變了，才送
若 `pending_outlier` 已存在：
- 若 `now - pending_first_seen_time > T_CONFIRM`：清掉 pending，重新判斷本次 P（視為第一次）
- 否則：
  - `d2 = distance(P, pending_pos)`
  - 若 `d2 <= R_CONFIRM`：
    - 承認這不是孤立鬼點，而是「真的已到該區域」
    - **送 APRS-IS（送這次 P）**
    - 更新 `prev_uploaded_pos = last_uploaded_pos`
    - 更新 `last_uploaded_pos = P`
    - 更新 `last_uploaded_time = now`
    - 清掉 pending
  - 若 `d2 > R_CONFIRM`：
    - 仍然可疑（亂跳）
    - 策略 A（建議）：更新 pending_pos = P（跟著最新可疑點走），但仍不送  
    - 策略 B：保持第一次 pending 不動，直到逾時（較保守）

### 8.3 Pending 逾時
若 `now - pending_first_seen_time > PENDING_TIMEOUT`：
- 清掉 pending（避免卡死）
- 不影響 last_uploaded

---

## 9. Gate 1 放行條件（什麼時候真的上傳）

「非 outlier」或「已通過 pending 二次確認」才上傳。

上傳成功後，**唯一**會更新 `last_uploaded_*` 的時機是：
- 你確定已把該 APRS frame 寫出（或你把它丟給 APRS-IS client 並視為成功）

> 你不需要等待 aprs.fi 採用（你也等不到），但你要確保你自己「輸出決策」一致。

---

## 10. 與你現有去重的整合方式

### 10.1 Gate 順序
1. Gate 0 去重：擋同包多站重送（你現有）
2. Gate 1 防回朔：擋亂序/晚到新包造成倒退（新增）

### 10.2 Cache 範圍建議（去重）
- 去重 cache 目的：擋同包多站上傳
- 時間窗建議：**2~10 分鐘**通常就夠（依你網路/站台數量調整）
- 你現在 3 小時也能用，但要注意：若裝置在同地點發完全相同內容很久，可能被你誤擋（看你 content 是否包含時間/序號）

> 去重 cache 跟 pending/last_uploaded 是兩套系統，別混在一起。

---

## 11. 多站台（多 gateway）情境：狀態要不要共享？

你目前每站台都會抓 APRS-IS 內容來去重。防回朔也有同樣問題：  
**如果多個 gateway 都可能上傳同一 callsign 的位置**，那每個 gateway 各自維護 `last_uploaded` 會不一致。

### 11.1 最佳方案（推薦）：集中出口
- 所有站台把解包結果送到一個「中央上傳器」  
- 由中央上傳器做 Gate 0 + Gate 1 + 上傳
- 優點：狀態一致、最乾淨  
- 缺點：要改架構

### 11.2 次佳方案：共享狀態存放（例如 Redis / DB）
- `last_uploaded` 與 `pending_outlier` 以 callsign 為 key 存到共享存放
- 每個 gateway 做決策前先讀取、決策後寫回（要注意並發）

### 11.3 最簡方案：一致性哈希（不用共享存放）
- 用 callsign 做 hash，固定由某一台 gateway 負責該 callsign 的上傳  
- 其他 gateway 只收包、不上傳（或只做備援）
- 優點：不用共享狀態  
- 缺點：容錯要設計

> 如果你現在已經是「每站都可能上傳同一 callsign」：強烈建議至少做 11.2 或 11.3，不然回朔機制在多站下會打折。

---

## 12. 記錄與觀測（你要能驗證它真的在擋）

建議你在 Gateway/上傳器端記錄以下事件（log/metrics）：
- `DROP_DUP`：被去重擋掉
- `HOLD_OUTLIER`：判 outlier，進 pending（附原因：speed/cluster/reverse）
- `ACCEPT_PENDING`：pending 二次確認放行
- `DROP_PENDING_TIMEOUT`：pending 逾時丟掉
- `UPLOAD_OK`：實際上傳

這樣你才知道「倒退點」是被你擋住了，而不是碰巧沒出現。

---

## 13. 測試方法（用你貼的案例驗證）

用你貼的回朔片段（抽出時間與座標）做單元測試或回放測試：

### 13.1 A→B→A 模式必須被擋
例：
- A：`25°06N 121°42E`
- B：`24°59N 121°14E`（孤立大跳）
- A'：`25°06N 121°43E`

期望結果：
- B 進 pending 不上傳
- A' 上傳（因為和 last_uploaded 連續且合理）
- pending 最後逾時清掉（或被後續點覆蓋）

### 13.2 真正到達新區域必須能放行（第二點確認）
例：
- A：台北
- P1：台中（第一顆）→ pending
- P2：台中附近（第二顆）→ 允許上傳

期望結果：
- 不會因為「一顆」就倒退
- 但真的到台中後，第二顆開始會更新到台中

---

## 14. 常見邊界情況與處理

1. **剛啟動沒有 last_uploaded**  
   - 第一顆直接上傳並初始化 last_uploaded
2. **靜止 GPS 抖動**  
   - `dist` 很小，速度合理，正常放行  
   - 若你想降噪，可加「最小位移門檻」(例如 < 30m 不送) 但非必要
3. **迴轉/掉頭**  
   - 只要速度合理，不會被擋；不需要你判斷“方向”
4. **長時間離線後再次上線**  
   - `dt` 很大 → 速度判斷通常會變得寬鬆  
   - 建議：若 `dt > 2 小時`（自訂），可直接接受第一顆並重置 pending（避免莫名卡住）
5. **dt 太小（連續包秒到）**  
   - 用 `DT_MIN` 避免速度爆炸造成誤判

---

## 15. 實作落點（你要改什麼函式/模組）

你要在「準備送 APRS-IS」之前，新增一個決策函式（概念上）：

- 輸入：`callsign, lat, lon, content, now`
- 輸出：`decision = {UPLOAD, DROP_DUP, HOLD_PENDING}` + `reason`

整合：
1. 先跑你現有的去重判斷（DROP_DUP 就結束）
2. 否則跑防回朔 Gate（UPLOAD 才真的送）
3. 只有 UPLOAD 才更新 last_uploaded（並清 pending）

---

## 16. 你可以先做的最小可行版本（MVP）

如果你不想一開始就弄 cluster/模式判定：

**MVP 只做這三件事就很有感：**
1. `last_uploaded` 與 `pending_outlier` 兩套狀態分離
2. 速度上限（車 200 / 高鐵 380）+ `DT_MIN`
3. outlier 二次確認（R_CONFIRM + T_CONFIRM）

這就能把你 log 那種「插一顆瞬移點」大量擋掉。

---

## 17. 附錄：為什麼 APRS 端 log 出現 “Location changes too fast”
你看到的 `[Location changes too fast (adaptive limit)]` 多半是顯示端/服務端在做自己的 anti-teleport 過濾。  
但你不能依賴它替你保證「不倒退」，因為：
- 不同下游系統採用規則不一
- APRS-IS 是廣播總線，你送出去就已經污染了別人
- 你真正能控制的是「你要不要送」

所以防回朔要在你送之前做，才有意義。

---

## 18. 版本資訊
- 文件版本：v1.0
- 目的：提供可落地的防回朔規格，讓你把既有去重系統擴充成「去重 + 亂序濾波」。

