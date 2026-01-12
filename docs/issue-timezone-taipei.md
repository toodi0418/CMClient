# 問題：Mapping 封包追蹤與多處時間顯示未固定台灣時區

## 問題描述
目前 UI 顯示時間會依賴執行環境的本地時區，當 OS/容器為 UTC 或其他時區時，Mapping Flow 與多處時間欄位顯示非台灣時間。

## 影響範圍
- Web Dashboard：`src/web/public/main.js`
- Electron UI：`src/electron/renderer.js`

## 重現步驟
1. 將系統時區設定為 UTC（或在容器中執行）
2. 開啟 Web Dashboard 或 Electron
3. 進入 Mapping 封包追蹤頁並觀察時間顯示

## 預期結果
時間顯示為台灣時間（Asia/Taipei, UTC+8）。

## 實際結果
時間顯示為系統/瀏覽器本地時區。

## 建議修正
- 建立統一時間格式化 helper，指定 `timeZone: 'Asia/Taipei'`
- Mapping Flow 與 `toLocaleString()` 顯示改為固定台灣時區
- 若需彈性，可新增 `TMAG_TIMEZONE` 設定供覆寫

## 驗收標準
- 在 UTC 環境下顯示仍為台灣時間
- Web / Electron 顯示時間一致
- Mapping Flow 與其他時間欄位皆為台灣時間
