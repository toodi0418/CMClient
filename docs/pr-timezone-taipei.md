# PR：固定 UI 顯示為台灣時區

## 變更摘要
- 新增統一時間格式化 helper，固定 `Asia/Taipei`
- Mapping Flow、遙測、節點與 tooltip 等顯示改用固定時區
- Web 與 Electron 顯示時間一致

## 變更檔案
- `src/web/public/main.js`
- `src/electron/renderer.js`

## 測試
- 未執行（僅前端時間格式化調整）
