# Meshtastic TCP Proxy

這個資料夾提供獨立的 Meshtastic TCP Proxy 服務，可單獨部署，不依賴 CallMesh / APRS / Electron 主流程。

## 功能

- 連接上游 Meshtastic（TCP 或 Serial）
- 對外提供一個 TCP Proxy port
- 將上游收到的 raw bytes 廣播給所有連線的 client
- 將 client 傳入的 raw bytes 寫回上游
- 支援上游斷線自動重連

## 啟動方式

### 1. 直接用 Node.js 啟動

#### TCP 上游
```bash
node tcp-proxy/index.js \
  --connection tcp \
  --host 192.168.1.50 \
  --port 4403 \
  --listen-host 0.0.0.0 \
  --listen-port 4403
```

#### Serial 上游
```bash
node tcp-proxy/index.js \
  --connection serial \
  --serial-path /dev/ttyUSB0 \
  --serial-baud 115200 \
  --listen-host 0.0.0.0 \
  --listen-port 4403
```

---

### 2. 透過環境變數啟動

```bash
export MESHTASTIC_CONNECTION=tcp
export MESHTASTIC_HOST=192.168.1.50
export MESHTASTIC_PORT=4403
export MESHTASTIC_PROXY_HOST=0.0.0.0
export MESHTASTIC_PROXY_PORT=4403
export PROXY_RECONNECT_DELAY_MS=5000
export PROXY_CONNECT_TIMEOUT_MS=15000

node tcp-proxy/index.js
```

---

## Docker 建置

```bash
docker build -f tcp-proxy/Dockerfile -t meshtastic-tcp-proxy .
```

### Docker 執行（TCP 上游）

```bash
docker run -d \
  --name meshtastic-tcp-proxy \
  -p 4403:4403 \
  -e MESHTASTIC_CONNECTION=tcp \
  -e MESHTASTIC_HOST=192.168.1.50 \
  -e MESHTASTIC_PORT=4403 \
  -e MESHTASTIC_PROXY_HOST=0.0.0.0 \
  -e MESHTASTIC_PROXY_PORT=4403 \
  meshtastic-tcp-proxy
```

### Docker 執行（Serial 上游）

```bash
docker run -d \
  --name meshtastic-tcp-proxy \
  --device /dev/ttyUSB0:/dev/ttyUSB0 \
  -p 4403:4403 \
  -e MESHTASTIC_CONNECTION=serial \
  -e MESHTASTIC_SERIAL_PATH=/dev/ttyUSB0 \
  -e MESHTASTIC_SERIAL_BAUD=115200 \
  -e MESHTASTIC_PROXY_HOST=0.0.0.0 \
  -e MESHTASTIC_PROXY_PORT=4403 \
  meshtastic-tcp-proxy
```

## 重要環境變數

| 變數 | 說明 |
| ---- | ---- |
| `MESHTASTIC_CONNECTION` | `tcp` 或 `serial` |
| `MESHTASTIC_HOST` | TCP 上游主機 |
| `MESHTASTIC_PORT` | TCP 上游埠號 |
| `MESHTASTIC_SERIAL_PATH` | Serial 裝置路徑 |
| `MESHTASTIC_SERIAL_BAUD` | Serial 鮑率 |
| `MESHTASTIC_PROXY_HOST` | Proxy 監聽位址 |
| `MESHTASTIC_PROXY_PORT` | Proxy 監聽埠號 |
| `PROXY_RECONNECT_DELAY_MS` | 上游斷線重連間隔 |
| `PROXY_CONNECT_TIMEOUT_MS` | 上游連線逾時時間 |

## 提供給其他應用的連線方式

假設 Proxy 主機 IP 是 `192.168.1.10`，監聽埠是 `4403`，則其他應用請連線到：

```text
192.168.1.10:4403
```

如果 Proxy 只給本機用，可把監聽位址設成：

```text
127.0.0.1
```

則其他本機應用請連線到：

```text
127.0.0.1:4403
```

## 注意事項

1. 若上游 Meshtastic 本身就在本機 `4403`，建議將 Proxy 改用其他 port，例如 `14403`，避免埠衝突。
2. 若使用 Serial，Docker 需提供裝置映射與權限。
3. 這個 Proxy 目前是透明轉發，不含認證機制，建議只在可信任的內網環境使用。
