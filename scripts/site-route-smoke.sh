#!/usr/bin/env bash

set -euo pipefail

SITE_URL="${1:-https://pxicommand.com}"
SITE_URL="${SITE_URL%/}"

check_html_status() {
  local path="$1"
  local expected="$2"
  local code
  code="$(curl -sS -o /tmp/site-route-smoke.out -w "%{http_code}" --max-time 20 "${SITE_URL}${path}")"
  if [[ "${code}" != "${expected}" ]]; then
    echo "Expected ${expected} for ${path} but got ${code}"
    exit 1
  fi
}

check_site_content_type() {
  local path="$1"
  local expected="$2"
  local expected_prefix="$3"
  local code
  code="$(curl -sS -D /tmp/site-route-smoke.headers -o /tmp/site-route-smoke.out -w "%{http_code}" --max-time 20 "${SITE_URL}${path}")"
  if [[ "${code}" != "${expected}" ]]; then
    echo "Expected ${expected} for ${path} but got ${code}"
    exit 1
  fi
  if ! grep -qi "^content-type: ${expected_prefix}" /tmp/site-route-smoke.headers; then
    echo "Expected content-type starting with '${expected_prefix}' for ${path}"
    cat /tmp/site-route-smoke.headers
    exit 1
  fi
}

check_html_status "/" 200
check_html_status "/spec" 200
check_html_status "/alerts" 200
check_html_status "/guide" 200

signals_code="$(curl -sS -o /tmp/site-route-smoke.out -w "%{http_code}" --max-time 20 "${SITE_URL}/signals")"
signals_followed_code="$(curl -Ls -o /tmp/site-route-smoke.out -w "%{http_code}" --max-time 20 "${SITE_URL}/signals")"
if [[ "${signals_code}" != "200" && "${signals_code}" != "301" && "${signals_code}" != "302" ]]; then
  echo "Expected 200/301/302 for /signals but got ${signals_code}"
  exit 1
fi
if [[ "${signals_followed_code}" != "200" ]]; then
  echo "Expected /signals to eventually resolve to 200, got ${signals_followed_code}"
  exit 1
fi

check_html_status "/signals/latest" 200
check_site_content_type "/llms.txt" 200 "text/plain"
check_site_content_type "/agent.md" 200 "text/markdown"
check_site_content_type "/signals/agent.md" 200 "text/markdown"
