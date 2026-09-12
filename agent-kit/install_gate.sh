#!/usr/bin/env bash
# 一次性安装：让某个 agent 身份的 Harbor 定时收信"活过任何会话的生死"。
#
# 解决的问题：Claude Code 的 ScheduleWakeup 纯内存、随进程死；OMP 的定时任务持久化
# 在会话文件里，只有恢复那个具体会话才会自动加载——新开一个会话看不到旧任务。
# 系统级 crontab 完全不依赖任何 AI 会话是否开着，是唯一真正"装一次、永久生效"的路径。
#
# 用法：
#   ./install_gate.sh <agent_id> <token文件路径> [轮询间隔秒数，默认60]
#
# 做什么：
#   1. 校验 token 文件存在且能读到内容
#   2. 幂等写入一条 crontab（重复运行不会叠加多条，会先删旧的再写新的）
#   3. 立即跑一次 harbor_gate.sh 自检，确认能连上 Harbor
set -euo pipefail

AGENT_ID="${1:?用法: install_gate.sh <agent_id> <token文件路径> [间隔秒数]}"
TOKEN_FILE="${2:?用法: install_gate.sh <agent_id> <token文件路径> [间隔秒数]}"
INTERVAL="${3:-60}"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HARBOR_URL="${HARBOR_URL:-http://127.0.0.1:8931/mcp}"

if [[ ! -f "$TOKEN_FILE" || ! -s "$TOKEN_FILE" ]]; then
  echo "错误：token 文件不存在或为空：$TOKEN_FILE" >&2
  exit 1
fi

# 用秒数换算出合理的 cron 表达式：<60s 只能退化成每分钟（cron 最小粒度），>=60s 按分钟取整
if [[ "$INTERVAL" -lt 60 ]]; then
  CRON_EXPR="* * * * *"
  echo "注意：cron 最小粒度是 1 分钟，$INTERVAL 秒会被当成每分钟一次" >&2
else
  MIN=$(( INTERVAL / 60 ))
  CRON_EXPR="*/$MIN * * * *"
fi

MARKER="# harbor-gate:${AGENT_ID}"
GATE_LOG="$HOME/.harbor/${AGENT_ID}.gate.log"
WAKE_LOG="$HOME/.harbor/${AGENT_ID}.wake.log"
mkdir -p "$(dirname "$GATE_LOG")"

CRON_LINE="$CRON_EXPR HARBOR_URL=$HARBOR_URL HARBOR_AGENT_ID=$AGENT_ID HARBOR_TOKEN=\$(cat $TOKEN_FILE) $HERE/harbor_gate.sh >> $GATE_LOG 2>&1 && claude -p \"你有新的 Harbor 私信：\$(tail -1 $GATE_LOG)。用 get_conversations/get_messages 读取处理并回复，处理完 mark_messages_read + ack_messages。\" >> $WAKE_LOG 2>&1 $MARKER"

# 幂等：先删掉这个 agent_id 的旧行（按 marker 匹配），再加新的
TMP_CRON="$(mktemp)"
crontab -l 2>/dev/null | grep -vF "$MARKER" > "$TMP_CRON" || true
echo "$CRON_LINE" >> "$TMP_CRON"
crontab "$TMP_CRON"
rm -f "$TMP_CRON"

echo "✓ crontab 已安装（$CRON_EXPR，标记 $MARKER）"
echo "  gate 日志: $GATE_LOG"
echo "  唤醒日志: $WAKE_LOG"
echo
echo "自检：立即跑一次 harbor_gate.sh ..."
HARBOR_URL="$HARBOR_URL" HARBOR_AGENT_ID="$AGENT_ID" HARBOR_TOKEN="$(cat "$TOKEN_FILE")" \
  "$HERE/harbor_gate.sh" || true
echo
echo "从现在起：不管有没有 Claude Code 会话开着、不管进程重启多少次，"
echo "系统 crontab 每 ${INTERVAL}s 左右都会检查 $AGENT_ID 的收件箱，有未读才唤醒无头 claude 处理。"
echo "卸载：crontab -l | grep -v '$MARKER' | crontab -"
