# Meshwaya 軌跡防回朔（含補點）作業指引

## 目的
- 避免因 RF/mesh 亂序、晚到造成「最新位置回朔/瞬移」。
- Meshwaya 支援補點（寫入歷史軌跡），可以同時「保護即時點」又「不漏掉晚到點」。
- 以現行 anti-backtrack Gate 概念為基礎，增加「live 更新」與「歷史補點」雙通路。

## 核心概念
- **Live Gate**：用 anti-backtrack 兩階段門檻（速度/cluster + pending 二次確認）判斷是否可以更新「最新位置」。
- **補點通路**：晚到/亂序但仍想保存的點，可在不更新最新位置的前提下，寫入歷史軌跡（backfill），並攜帶事件時間戳。
- **兩套狀態**：`last_uploaded/prev_uploaded`（只在 live 上傳成功後更新）與 `pending`（暫存可疑點）。補點不會動到 `last/prev`。

## 資料欄位建議
- `track_id`：用來識別節點／裝置（Mesh ID、裝置序號或平台使用的唯一 ID）。
- `lat/lon/alt`：座標。
- `rx_time_ms`：Gateway 收到時間（必備，伺服器時間）。
- `sample_time_ms`：若封包內有原始時間/序號，可填（沒有就與 `rx_time_ms` 相同）。
- `is_live`：是否更新最新位置；`is_backfill`：是否僅寫入歷史。
- （可選）`flow_id`/`mesh_packet_id`/`hops`/`relay` 供追蹤。

## 決策流程（概略）
1) **Gate 0：去重**（既有機制，防多站重送）。  
2) **Gate 1：anti-backtrack**  
   - 速度上限：車 200 km/h、HSR 380 km/h，240/180 km/h 門檻切換模式；`DT_MIN`=10s，cluster 半徑 12 km。  
   - outlier → pending，需 5 分鐘內第二顆落在 8 km 內才放行。  
   - 長時間間隔 >2h → 視為重新開機，直接接受並清 pending。  
3) **輸出策略**  
   - `decision=upload`（含 pending 確認通過）：送 Meshwaya **live** 更新，並更新 `last/prev`。  
   - `decision=hold`/`pending`：不送 live，不更新 `last/prev`。可選：  
     - 若 Meshwaya 允許 backfill，將該點以 `is_backfill=1` + `rx_time_ms`（或 `sample_time_ms`）寫入歷史，不影響 live。  
   - `pending 逾時`：丟棄 live，若要補點可選擇用 backfill 寫入歷史，再清除 pending。

## Meshwaya 端建議
- **Live API/Topic**：套用「最後一筆即時位置」邏輯，只吃 `is_live=1` 的點。  
- **Backfill API/Topic**：允許按事件時間插入歷史，不覆寫 live。延遲點可寫入此通路。  
- **前端呈現**：live 位置取最新 `is_live`；軌跡線可同時繪 live + backfill，並以時間排序。

## 推薦參數（可微調）
- `V_CAR=200 km/h`、`V_HSR=380 km/h`、`HSR_ENTER=240/HSR_EXIT=180 km/h`  
- `DT_MIN=10s`、`LONG_GAP_RESET=2h`  
- `CLUSTER_K=5`、`CLUSTER_RADIUS=12 km`  
- `PENDING_CONFIRM_RADIUS=8 km`、`PENDING_CONFIRM_WINDOW=5 min`、`PENDING_TIMEOUT=20 min`

## 範例時序
- A→B→A（B 晚到）：B 判為 outlier → pending，不上傳 live；A' 回到原軌跡，live 持續在 A 附近。B 可選擇用 backfill 寫入歷史，不動 live。  
- 真正移動到新區域：第一顆 P1 進 pending，P2 落在 8 km/5 分內 → live 更新至新區域，pending 清空；歷史軌跡包含 P1/P2。

## 實作重點
- 狀態持久化：Gateway 端的 backtrack state（last/prev/pending/history）會寫入 SQLite，重啟不會忘記基準。  
- 保持 `last/prev` 只在 live 成功上傳後更新；backfill 不改基準。  
- Log/Debug：當前實作會在 /debug 顯示 anti-backtrack 狀態，並對 hold/confirm/timeout 打 log，便於追蹤。
