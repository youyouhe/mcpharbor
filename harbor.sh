#!/bin/bash
# MCP Harbor 服务启停脚本
# 用法: ./harbor.sh {start|stop|restart|status}
set -u

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PID_FILE=/tmp/mcpharbor_server.pid
LOG_FILE=/tmp/mcpharbor_server.log
PORT=8931

export PYTHONPATH="$DIR/src"
export MCPHARBOR_TRANSPORT="${MCPHARBOR_TRANSPORT:-streamable-http}"
export FASTMCP_HOST="${FASTMCP_HOST:-0.0.0.0}"
export FASTMCP_PORT="${FASTMCP_PORT:-8931}"
# admin token 优先从 ~/.harbor/admin.token 读，避免密钥进仓库
if [ -z "${MCPHARBOR_ADMIN_TOKEN:-}" ] && [ -f "$HOME/.harbor/admin.token" ]; then
    export MCPHARBOR_ADMIN_TOKEN="$(cat "$HOME/.harbor/admin.token")"
fi

is_running() {
    [ -f "$PID_FILE" ] && kill -0 "$(cat "$PID_FILE")" 2>/dev/null
}

wait_ready() {
    for _ in $(seq 1 20); do
        # 查端口监听即可，/mcp 对裸 GET 返回 4xx 不适合探活
        if ss -tln 2>/dev/null | grep -q ":$PORT "; then
            return 0
        fi
        sleep 0.5
    done
    return 1
}

start() {
    if is_running; then
        echo "已在运行 (PID $(cat "$PID_FILE"))"
        return 0
    fi
    # 清理残留：上次异常退出留下的旧进程（无 pid 文件但还在跑）；^ 锚点避免误匹配包装 shell
    local stale
    stale=$(pgrep -f "^python3? -m mcpharbor\.server$" || true)
    if [ -n "$stale" ]; then
        echo "发现无 pid 文件的残留进程 (PID $stale)，先停止"
        kill $stale 2>/dev/null
        sleep 1
    fi
    cd "$DIR"
    nohup setsid python3 -m mcpharbor.server >>"$LOG_FILE" 2>&1 &
    echo $! >"$PID_FILE"
    if wait_ready; then
        echo "启动成功 (PID $(cat "$PID_FILE"))，日志: $LOG_FILE"
    else
        echo "警告：进程已拉起但 $PORT 端口 $((10))秒内未就绪，请查看日志: $LOG_FILE"
        return 1
    fi
}

stop() {
    if ! is_running; then
        echo "未在运行（或 pid 文件丢失）"
        # 仍尝试兜底清理残留进程
        pkill -f "^python3? -m mcpharbor\.server$" 2>/dev/null && echo "已清理残留进程"
        rm -f "$PID_FILE"
        return 0
    fi
    local pid
    pid=$(cat "$PID_FILE")
    kill "$pid" 2>/dev/null
    for _ in $(seq 1 10); do
        kill -0 "$pid" 2>/dev/null || break
        sleep 0.5
    done
    if kill -0 "$pid" 2>/dev/null; then
        echo "5 秒未退出，强制 kill"
        kill -9 "$pid" 2>/dev/null
    fi
    rm -f "$PID_FILE"
    echo "已停止"
}

status() {
    if is_running; then
        local pid
        pid=$(cat "$PID_FILE")
        echo "运行中 (PID $pid, 端口 $PORT)"
        ps -o etime=,cmd= -p "$pid"
    else
        echo "未运行"
        return 1
    fi
}

case "${1:-}" in
    start)   start ;;
    stop)    stop ;;
    restart) stop; start ;;
    status)  status ;;
    *) echo "用法: $0 {start|stop|restart|status}"; exit 1 ;;
esac
