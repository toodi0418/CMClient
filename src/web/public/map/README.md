# 本地向量圖磚

本資料夾提供 Web Dashboard 的本地向量圖磚與樣式檔。

## 檔案
- `style.json`：MapLibre style 定義。
- `land.geojson`：當本地圖磚不存在時的簡易備援底圖來源。

## 使用自備圖磚
1. 將圖磚放到 `src/web/public/map/tiles/{z}/{x}/{y}.pbf`。
2. 或設定環境變數 `TMAG_VECTOR_TILES_DIR` 指向圖磚根目錄。

## 使用 MBTiles（建議）
- 將 MBTiles 放到 `src/web/public/map/tiles.mbtiles`，或設定環境變數 `TMAG_VECTOR_MBTILES` 指向檔案。
- MBTiles 會優先於目錄式圖磚讀取。
- 若檔案不存在，Web Server 會自動下載（可用 `TMAG_VECTOR_AUTO_DOWNLOAD=0` 關閉）。
- 自動下載來源可用 `TMAG_VECTOR_MBTILES_URL` 覆寫。

## 台灣 Shortbread 範例
- Geofabrik 提供台灣 Shortbread 向量圖磚（約 170MB），可直接下載到 `tiles.mbtiles`。
- 下載位置：`https://download.geofabrik.de/asia/taiwan-shortbread-1.0.mbtiles`

## 字型（地名標籤）
- 需要 MapLibre glyphs 才能顯示地名，預設路徑：`/map/fonts/{fontstack}/{range}.pbf`。
- 本專案已放入 `Noto Sans Regular`（來自 openmaptiles 字型包），若需要完整中文可改用含 CJK 的 glyphs。

## 地形陰影（山形）
- 本專案使用 Mapzen Terrarium DEM tiles，已下載台灣範圍 z0~z10 到 `src/web/public/map/dem/`。
- 若要重新下載，可參考 `https://s3.amazonaws.com/elevation-tiles-prod/terrarium/{z}/{x}/{y}.png`。

## 使用自備 GeoJSON
- 設定 `TMAG_VECTOR_GEOJSON` 指向 GeoJSON 檔案，伺服器會即時轉為向量圖磚。

> 注意：`land.geojson` 為極簡 placeholder，建議替換成正式地圖資料。
