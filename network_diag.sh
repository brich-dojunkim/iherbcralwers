#!/usr/bin/env bash
set -euo pipefail

# 사용법:
#   ./network_diag.sh              # 기본 도메인 claude.ai
#   ./network_diag.sh claude.ai    # 특정 도메인 지정

TARGET_DOMAIN="${1:-claude.ai}"
TIMESTAMP="$(date +"%Y%m%d_%H%M%S")"
LOG_FILE="netdiag_${TARGET_DOMAIN}_${TIMESTAMP}.log"

echo "============================================================"
echo "🌐 네트워크 진단 스크립트 (macOS)"
echo "대상 도메인: ${TARGET_DOMAIN}"
echo "로그 파일:  ${LOG_FILE}"
echo "실행 시각:  ${TIMESTAMP}"
echo "============================================================"
echo

# 표준출력 + 에러를 모두 로그에 저장하면서 화면에도 출력
exec > >(tee -a "$LOG_FILE") 2>&1

echo "[1/4] ping 테스트 (10회)"
echo "------------------------------------------------------------"
ping -c 10 "${TARGET_DOMAIN}" || echo "⚠️ ping 중 에러 발생 (무시하고 다음 단계 진행)"

echo
echo "[2/4] traceroute 테스트"
echo "------------------------------------------------------------"
if command -v traceroute >/dev/null 2>&1; then
    traceroute "${TARGET_DOMAIN}" || echo "⚠️ traceroute 중 에러 발생 (일부 홉 비공개일 수 있음)"
else
    echo "⚠️ traceroute 명령어를 찾을 수 없습니다."
fi

echo
echo "[3/4] nslookup (DNS 조회)"
echo "------------------------------------------------------------"
if command -v nslookup >/dev/null 2>&1; then
    nslookup "${TARGET_DOMAIN}" || echo "⚠️ nslookup 중 에러 발생"
else
    echo "⚠️ nslookup 명령어를 찾을 수 없습니다."
fi

echo
echo "[4/4] HTTP(S) 접속 지연 상세 분석 (curl)"
echo "------------------------------------------------------------"

if command -v curl >/dev/null 2>&1; then
    URL="https://${TARGET_DOMAIN}"

    echo "요청 URL: ${URL}"
    echo

    # curl 타이밍 정보 출력
    curl -sS -o /dev/null -w \
"time_namelookup:  %{time_namelookup}s
time_connect:     %{time_connect}s
time_appconnect:  %{time_appconnect}s  (TLS 핸드셰이크까지)
time_starttransfer:%{time_starttransfer}s (첫 바이트 수신 시점)
time_total:       %{time_total}s  (전체 요청 시간)
" "${URL}" || echo "⚠️ curl 요청 중 에러 발생 (HTTPS 차단/인증서 문제 가능성)"
else
    echo "⚠️ curl 명령어를 찾을 수 없습니다."
fi

echo
echo "============================================================"
echo "✅ 네트워크 진단 완료"
echo "↪ 결과 로그 파일: ${LOG_FILE}"
echo "이 로그 파일을 IT팀에 그대로 전달하면 진단에 도움이 됩니다."
echo "============================================================"
