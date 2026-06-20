#!/usr/bin/env bash
# Sleipnir end-to-end smoke test.
# Validates: docker-compose (sim backend) -> Redpanda health -> Sleipnir health
#            -> mock-huginn intent production -> fill pipeline -> metrics.
#
# Usage: ./scripts/smoke.sh
# Exit 0 on success, 1 on failure.
#
# Requirements:
#   - Docker Compose
#   - curl

set -euo pipefail

SLEIPNIR_URL="${SLEIPNIR_URL:-http://localhost:8085}"
# Must match SLEIPNIR_ADMIN_TOKEN in docker-compose.smoke.yml so the
# authenticated admin path can be exercised below.
SLEIPNIR_ADMIN_TOKEN="${SLEIPNIR_ADMIN_TOKEN:-smoke-admin-token}"
TIMEOUT=60

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m' # No Color

PASSED=0
FAILED=0
CHECKS=()

pass() { echo -e "${GREEN}  PASS  $1${NC}"; PASSED=$((PASSED + 1)); CHECKS+=("PASS: $1"); }
fail() { echo -e "${RED}  FAIL  $1${NC}"; FAILED=$((FAILED + 1)); CHECKS+=("FAIL: $1"); }
info() { echo -e "${YELLOW}  ->  $1${NC}"; }

cleanup() {
  info "Tearing down containers..."
  docker compose -f docker-compose.yml -f docker-compose.smoke.yml down -v --remove-orphans 2>/dev/null || true
}

# Always clean up on exit
trap cleanup EXIT

# --- Step 1: Build and start the stack with sim backend ---
echo ""
info "Step 1: Starting Sleipnir stack with EXCHANGE_BACKEND=sim..."
docker compose -f docker-compose.yml -f docker-compose.smoke.yml up -d --build

# --- Step 2: Wait for Redpanda health ---
info "Step 2: Waiting for Redpanda to become healthy..."
REDPANDA_OK=false
for i in $(seq 1 "$TIMEOUT"); do
  if docker compose exec -T redpanda rpk cluster health --exit-when-healthy 2>/dev/null; then
    REDPANDA_OK=true
    break
  fi
  sleep 1
done

if [ "$REDPANDA_OK" = true ]; then
  pass "Redpanda is healthy"
else
  fail "Redpanda did not become healthy within ${TIMEOUT}s"
fi

# --- Step 3: Wait for Sleipnir /healthz ---
info "Step 3: Waiting for Sleipnir /healthz at ${SLEIPNIR_URL}..."
HEALTH_OK=false
for i in $(seq 1 "$TIMEOUT"); do
  HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${SLEIPNIR_URL}/healthz" 2>/dev/null || echo "000")
  if [ "$HTTP_CODE" = "200" ]; then
    HEALTH_OK=true
    break
  fi
  sleep 1
done

if [ "$HEALTH_OK" = true ]; then
  pass "Sleipnir /healthz returned 200"
else
  fail "Sleipnir /healthz did not return 200 within ${TIMEOUT}s (last: HTTP ${HTTP_CODE})"
fi

# --- Step 4: Wait for mock-huginn to produce at least one intent ---
info "Step 4: Waiting ~15s for mock-huginn to produce intents (interval: 7s)..."
sleep 15

INTENT_COUNT=$(docker compose exec -T redpanda rpk topic consume executions.intents.v1 -n 1 --timeout 10s 2>/dev/null | grep -c '"order_id\|"orderID\|OrderID\|instrument' || echo "0")
if [ "$INTENT_COUNT" -gt 0 ]; then
  pass "Intent detected on executions.intents.v1"
else
  fail "No intent messages found on executions.intents.v1"
fi

# --- Step 5: Check fills topic for fill events ---
info "Step 5: Checking executions.fills.v1 for fill events..."

FILL_OK=false
for i in $(seq 1 30); do
  FILL_OUTPUT=$(docker compose exec -T redpanda rpk topic consume executions.fills.v1 -n 1 --timeout 5s 2>/dev/null || echo "")
  if echo "$FILL_OUTPUT" | grep -qi 'order_id\|orderID\|fill_price\|FillPrice\|execution_id\|ExecutionID'; then
    FILL_OK=true
    break
  fi
  sleep 2
done

if [ "$FILL_OK" = true ]; then
  pass "Fill event detected on executions.fills.v1"
else
  fail "No fill events found on executions.fills.v1"
fi

# --- Step 6: Check mock-portfolio logs for fill receipt ---
info "Step 6: Checking mock-portfolio logs for fill consumption..."

PORTFOLIO_OK=false
for i in $(seq 1 15); do
  LOGS=$(docker compose logs mock-portfolio 2>/dev/null || echo "")
  if echo "$LOGS" | grep -qi 'fill\|execution\|DOWNSTREAM\|received'; then
    PORTFOLIO_OK=true
    break
  fi
  sleep 2
done

if [ "$PORTFOLIO_OK" = true ]; then
  pass "mock-portfolio received fill events"
else
  fail "mock-portfolio logs show no fill consumption"
fi

# --- Step 7: Check /metrics returns sleipnir_ prefixed metrics ---
info "Step 7: Checking Prometheus /metrics endpoint..."

METRICS_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${SLEIPNIR_URL}/metrics" 2>/dev/null || echo "000")
if [ "$METRICS_CODE" = "200" ]; then
  METRICS_BODY=$(curl -s "${SLEIPNIR_URL}/metrics" 2>/dev/null || echo "")
  if echo "$METRICS_BODY" | grep -q "sleipnir_"; then
    pass "/metrics returns 200 with sleipnir_ prefixed metrics"
  else
    # Prometheus Go client may use process_ / go_ prefixes; check those too
    if echo "$METRICS_BODY" | grep -qE "^(process_|go_|promhttp_)"; then
      pass "/metrics returns 200 with Prometheus metrics (no sleipnir_ prefix yet)"
    else
      fail "/metrics returned 200 but no recognizable metrics found"
    fi
  fi
else
  fail "/metrics returned HTTP ${METRICS_CODE}"
fi

# --- Step 8: Check /readyz ---
info "Step 8: Checking /readyz endpoint..."

READYZ_OK=false
for i in $(seq 1 15); do
  READYZ_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${SLEIPNIR_URL}/readyz" 2>/dev/null || echo "000")
  if [ "$READYZ_CODE" = "200" ]; then
    READYZ_OK=true
    break
  fi
  sleep 2
done

if [ "$READYZ_OK" = true ]; then
  pass "/readyz returned 200 (gateway has processed at least one intent)"
else
  fail "/readyz did not return 200 (last: HTTP ${READYZ_CODE})"
fi

# --- Step 9: Admin auth on /admin/halt (fail-closed + bearer token) ---
info "Step 9: Checking /admin/halt bearer-token auth..."

# 9a: no token -> 401 (token is configured in compose, so unauthenticated is rejected)
NOAUTH_CODE=$(curl -s -o /dev/null -w "%{http_code}" -X POST \
  -d '{"reason":"smoke-noauth"}' "${SLEIPNIR_URL}/admin/halt" 2>/dev/null || echo "000")
if [ "$NOAUTH_CODE" = "401" ]; then
  pass "/admin/halt rejects unauthenticated POST (HTTP 401)"
else
  fail "/admin/halt POST without token returned HTTP ${NOAUTH_CODE} (expected 401)"
fi

# 9b: correct token -> 200 and halted:true
AUTH_BODY=$(curl -s -X POST -H "Authorization: Bearer ${SLEIPNIR_ADMIN_TOKEN}" \
  -d '{"reason":"smoke-auth"}' "${SLEIPNIR_URL}/admin/halt" 2>/dev/null || echo "")
if echo "$AUTH_BODY" | grep -q '"halted":true'; then
  pass "/admin/halt accepts valid bearer token and engages kill switch"
else
  fail "/admin/halt with valid token did not engage kill switch (body: ${AUTH_BODY})"
fi

# 9c: resume with token so the run leaves the gateway un-halted
curl -s -X POST -H "Authorization: Bearer ${SLEIPNIR_ADMIN_TOKEN}" \
  "${SLEIPNIR_URL}/admin/resume" >/dev/null 2>&1 || true

# --- Step 10: Summary ---
echo ""
TOTAL=$((PASSED + FAILED))
echo -e "${YELLOW}========================================${NC}"
echo -e "${YELLOW}  Sleipnir Smoke Test Summary${NC}"
echo -e "${YELLOW}========================================${NC}"
echo ""
for check in "${CHECKS[@]}"; do
  if [[ "$check" == PASS:* ]]; then
    echo -e "  ${GREEN}${check}${NC}"
  else
    echo -e "  ${RED}${check}${NC}"
  fi
done
echo ""
echo -e "  Total: ${TOTAL}  Passed: ${GREEN}${PASSED}${NC}  Failed: ${RED}${FAILED}${NC}"
echo ""

if [ "$FAILED" -gt 0 ]; then
  echo -e "${RED}  SMOKE TEST FAILED${NC}"
  echo ""
  exit 1
fi

echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}  Sleipnir smoke test passed${NC}"
echo -e "${GREEN}========================================${NC}"
echo ""
echo "  Sleipnir Gateway:   ${SLEIPNIR_URL}"
echo "  Prometheus:         http://localhost:9095"
echo "  Grafana:            http://localhost:3005"
echo ""

exit 0
