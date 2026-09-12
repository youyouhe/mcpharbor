#!/usr/bin/env bash
# Harbor 门禁监视器 —— 空闲会话的"有新邮件就放行"门禁。
#
# 设计（来自 2026-09-11 实测验证的 /tmp/gate_sim.sh 思路，产品化）：
#   会话空闲（idle）时不知道有人给自己发了私信。用 cron 每 15 秒跑一次本脚本：
#     - poll_harbor.py 退出码 0（有未读/有落后契约钉） => 打印收件 payload，本脚本退出 0
#     - 退出码 1（没动静）=> 一言不发退出 1，不打扰会话
#   调用方（cron 任务/条件门）只在退出码 0 时唤醒会话、注入 payload。
#
# 用法：
#   HARBOR_URL=http://127.0.0.1:8931/mcp HARBOR_AGENT_ID=my-agent HARBOR_TOKEN=xxx \
#   ./harbor_gate.sh            # 有未读时输出 JSON 并退出 0；否则静默退出 1
#   ./harbor_gate.sh --mark-read  # 拉取即标记已读（建议让会话确认处理后再标）
set -uo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
exec python3 "$HERE/poll_harbor.py" "$@"
