#!/bin/bash
set -e

# --- 顏色設定 ---
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

log() { printf "${GREEN}[deploy]${NC} %s\n" "$*"; }
warn() { printf "${YELLOW}[warn]${NC} %s\n" "$*"; }
error() { printf "${RED}[error]${NC} %s\n" "$*" >&2; }

# --- 檢查環境 ---
if [[ $EUID -ne 0 ]]; then
   error "此腳本需要使用 sudo 權限執行。"
   exit 1
fi

if ! command -v docker &> /dev/null; then
    error "找不到 docker，請先安裝 Docker。"
    exit 1
fi

# --- 自我更新檢查 (保證不迴圈) ---
check_self_update() {
    # 1. 核心保護：如果環境變數顯示已經重啟過，絕對不再跑第二次
    if [ -n "$DEPLOY_SH_UPDATED" ]; then
        return
    fi

    if [ -d .git ]; then
        log "檢查部署腳本是否有更新..."
        git fetch origin main >/dev/null 2>&1 || true
        
        UPSTREAM_CHANGES=$(git rev-list --count HEAD..origin/main -- deploy.sh 2>/dev/null || echo 0)
        
        if [ "$UPSTREAM_CHANGES" -gt 0 ]; then
            warn "偵測到遠端有新版 deploy.sh，為確保執行順利，即將自動同步程式碼並重啟..."
            
            # 如果有本地修改，先備份以免被覆蓋
            if ! git diff-index --quiet HEAD --; then
                git stash save "deploy_auto_stash_$(date +%s)" >/dev/null 2>&1
                STASHED=true
            else
                STASHED=false
            fi
            
            git reset --hard origin/main >/dev/null 2>&1
            
            if [ "$STASHED" = true ]; then
                git stash pop >/dev/null 2>&1 || true
            fi
            
            # 3. 設置環境變數並重啟
            export DEPLOY_SH_UPDATED=1
            log "腳本已自動同步為最新版，重新啟動中..."
            exec bash "$0" "$@"
        fi
    fi
}

# --- 啟動自我更新流程 ---
check_self_update

# --- 互動函式 ---
prompt_api_key() {
    local key=""
    while [[ -z "$key" ]]; do
        read -rp "請輸入 CallMesh API Key: " key
        [[ -z "$key" ]] && error "API Key 不可為空。"
    done
    echo "$key"
}

select_serial_device() {
    local devices=()
    for p in /dev/ttyACM* /dev/ttyUSB* /dev/ttyS* /dev/ttyAMA*; do
        [[ -e "$p" ]] && devices+=("$p")
    done

    if [[ ${#devices[@]} -eq 0 ]]; then
        warn "未偵測到常見的 Serial 裝置。"
        read -rp "請手動輸入裝置路徑 [例如 /dev/ttyACM0]: " selected
        echo "${selected:-/dev/ttyACM0}"
    else
        echo "偵測到以下 Serial 裝置：" >&2
        for i in "${!devices[@]}"; do
            echo "  $((i+1))) ${devices[$i]}" >&2
        done
        local choice
        read -rp "請選擇裝置編號 (預設 1): " choice
        choice=${choice:-1}
        if [[ "$choice" =~ ^[0-9]+$ ]] && (( choice >= 1 && choice <= ${#devices[@]} )); then
            echo "${devices[$((choice-1))]}"
        else
            echo "${devices[0]}"
        fi
    fi
}

# --- 主程式面 ---
cat << "EOF"
  ____ __  __  ____ _     ___ _____ _   _ _____ 
 / ___|  \/  |/ ___| |   |_ _| ____| \ | |_   _|
| |   | |\/| | |   | |    | ||  _| |  \| | | |  
| |___| |  | | |___| |___ | || |___| |\  | | |  
 \____|_|  |_|\____|_____|___|_____|_| \_| |_|  
EOF

echo "------------------------------------------"
echo "1) 全新安裝 (清理舊資料 + 引導設定 .env + 重新編譯)"
echo "2) 更新程式碼 (同步最新 git + 重新編譯，保留資料)"
echo "3) 快速重啟 (僅套用 .env 變更，不更新程式，跳過編譯)"
echo "4) 清空應用資料 (僅刪除 Volumes，保留 .env 設定)"
echo "5) 取消"
read -rp "請選擇操作 [1-5]: " main_choice

NEED_GIT_SYNC=false
NEED_BUILD=false

case $main_choice in
    1)
        log "進入全新安裝流程 (清理資料 + 設定環境)..."
        NEED_GIT_SYNC=true
        NEED_BUILD=true
        
        if [[ -f docker-compose.yml ]]; then
            warn "警告：這將會永久刪除所有存儲在 Volume 中的應用資料 (包含歷史記錄，但 .env 中的設定可重新輸入)。"
            read -rp "確定要繼續全新安裝並覆蓋資料嗎？ [y/N]: " confirm_wipe
            if [[ "$confirm_wipe" == "y" || "$confirm_wipe" == "Y" ]]; then
                log "正在移除舊有的容器與磁碟卷 (Volumes)..."
                docker compose down -v --remove-orphans || true
            else
                log "已取消全新安裝流程。"
                exit 0
            fi
        fi
        
        API_KEY=$(prompt_api_key)
        echo "請選擇 Meshtastic 連線模式："
        echo "  1) TCP 模式"
        echo "  2) Serial 模式"
        read -rp "選擇 [1-2]: " mode_choice
        
        if [[ "$mode_choice" == "2" ]]; then
            DEVICE_PATH=$(select_serial_device)
            HOST_VAL="serial:${DEVICE_PATH}"
            SERIAL_VAL="${DEVICE_PATH}"
            
            # 動態生成 docker-compose.override.yml 進行掛載
            cat > docker-compose.override.yml <<EOF
services:
  callmesh-client:
    devices:
      - "\${SERIAL_DEVICE}:\${SERIAL_DEVICE}"
EOF
        else
            read -rp "請輸入 Meshtastic IP/Host [預設 meshtastic.local]: " tcp_host
            HOST_VAL="${tcp_host:-meshtastic.local}"
            SERIAL_VAL="/dev/ttyACM0"
            
            # TCP 模式不需要掛載裝置，移除 override 如果存在
            rm -f docker-compose.override.yml
        fi

        echo "是否要啟用 TCP Proxy 功能 (讓其他 App 連線到此主機的 4403 port)？"
        read -rp "啟用 Proxy? [y/N]: " proxy_choice
        PROXY_ENABLE="false"
        PROXY_HOST="127.0.0.1"
        if [[ "$proxy_choice" == "y" || "$proxy_choice" == "Y" ]]; then
            PROXY_ENABLE="true"
            PROXY_HOST="0.0.0.0"
        fi

        read -rp "CallMesh Server 位址 [留空使用預設 https://callmesh.tmmarc.org]: " base_url_input
        BASE_URL_VAL="${base_url_input:-}"

        cat > .env <<EOL
CALLMESH_API_KEY=${API_KEY}
CALLMESH_BASE_URL=${BASE_URL_VAL}
SERIAL_DEVICE=${SERIAL_VAL}
MESHTASTIC_HOST=${HOST_VAL}
MESHTASTIC_PORT=4403
MESHTASTIC_PROXY=${PROXY_ENABLE}
MESHTASTIC_PROXY_PORT=4403
MESHTASTIC_PROXY_HOST=${PROXY_HOST}
TMAG_WEB_PORT=7080
TMAG_WEB_DASHBOARD=1
TMAG_TIMEZONE=Asia/Taipei
TENMAN_DISABLE=1
AUTO_UPDATE=0
EOL
        log ".env 檔案已設定。"
        ;;
    2)
        log "進入更新程式碼流程 (保留現有資料)..."
        NEED_GIT_SYNC=true
        NEED_BUILD=true
        ;;
    3)
        log "進入快速重啟流程 (僅套用設定變更)..."
        NEED_GIT_SYNC=false
        NEED_BUILD=false
        ;;
    4)
        warn "警告：這將會永久刪除所有存儲在 Volume 中的應用資料 (monitor.json, logs 等)。"
        read -rp "確定要繼續嗎？ (y/N): " confirm
        if [[ "$confirm" == "y" || "$confirm" == "Y" ]]; then
            log "正在清理資料..."
            docker compose down -v --remove-orphans || true
            log "資料已清空。"
            NEED_GIT_SYNC=false
            NEED_BUILD=false
        else
            log "已取消清理動作。"
            exit 0
        fi
        ;;
    *)
        log "已取消操作。"
        exit 0
        ;;
esac

# --- 執行任務 ---
if [ "$NEED_GIT_SYNC" = true ]; then
    log "同步程式碼 (將本地修改備份並拉取最新版)..."
    git fetch --all
    # 如果有本地修改，先 stash 起來
    if ! git diff-index --quiet HEAD --; then
        warn "偵測到本地修改，正在備份 (git stash)..."
        git stash save "deploy_auto_stash_$(date +%s)"
        STASHED=true
    else
        STASHED=false
    fi
    
    git reset --hard origin/main
    
    # 嘗試把 stash 彈回來，如果有衝突就保留原本的
    if [ "$STASHED" = true ]; then
        log "還原本地的修改設定..."
        # 由於腳本設定了 set -e, 若 git stash pop 發生衝突會回傳非 0，這裡用 >/dev/null 2>&1 吞下並捕捉錯誤
        git stash pop >/dev/null 2>&1 || warn "還原本地設定時發生衝突。已將衝突的程式碼保留在暫存區 (stash) 中，系統將使用伺服器版本繼續啟動。請稍後手動檢查 git 狀態。"
    fi
fi

if [ "$NEED_BUILD" = true ]; then
    log "正在重新建置 Docker Image..."
    docker compose up -d --build callmesh-client
else
    log "正在啟動/重啟服務 (不重新建置)..."
    docker compose up -d callmesh-client
fi

log "清理舊的 Docker 映像檔..."
docker image prune -f

echo "------------------------------------------"
log "✅ 操作完成！"
echo "您可以執行 'sudo docker compose logs -f callmesh-client' 查看運行狀況。"
echo "------------------------------------------"
