#!/usr/bin/env bash
# ═══════════════════════════════════════════════════════════════════
#  test-infra.sh — Testes de infraestrutura do projeto Tabacaria
#  Uso: bash test-infra.sh
# ═══════════════════════════════════════════════════════════════════

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
ok()   { echo -e "${GREEN}  ✔ $1${NC}"; }
fail() { echo -e "${RED}  ✘ $1${NC}"; }
info() { echo -e "${YELLOW}  → $1${NC}"; }

PASS=0; FAIL=0

check() {
  local label="$1"; shift
  if "$@" &>/dev/null; then
    ok "$label"; ((PASS++))
  else
    fail "$label"; ((FAIL++))
  fi
}

echo ""
echo "══════════════════════════════════════════"
echo "  Tabacaria — Teste de Infraestrutura"
echo "══════════════════════════════════════════"

# ── 1. Docker ────────────────────────────────
echo ""
echo "[ 1/5 ] Docker"
check "docker instalado"         docker --version
check "docker compose instalado" docker compose version
check "Docker daemon rodando"    docker info

# ── 2. Pasta data/ ───────────────────────────
echo ""
echo "[ 2/5 ] Estrutura de arquivos"
check "pasta data/ existe"                 test -d ./data
check "pipeline.py existe"                 test -f ./spark/pipeline.py
check "producer.py existe"                 test -f ./producer/producer.py
check "docker-compose.yml existe"          test -f ./docker-compose.yml
check "init.sql existe"                    test -f ./postgres/init.sql
check "grafana datasource existe"          test -f ./grafana/provisioning/datasources/postgres.yaml
check "grafana dashboard JSON existe"      test -f ./grafana/provisioning/dashboards/tabacaria.json

# ── 3. Containers ────────────────────────────
echo ""
echo "[ 3/5 ] Containers (verificando status)"

check "kafka rodando"    docker compose ps kafka    | grep -q "running\|Up"
check "postgres rodando" docker compose ps postgres | grep -q "running\|Up"
check "producer rodando" docker compose ps producer | grep -q "running\|Up"
check "spark rodando"    docker compose ps spark    | grep -q "running\|Up"
check "grafana rodando"  docker compose ps grafana  | grep -q "running\|Up"

# ── 4. Kafka ─────────────────────────────────
echo ""
echo "[ 4/5 ] Kafka"

info "Verificando tópico 'transactions'..."
TOPICS=$(docker compose exec -T kafka kafka-topics.sh --bootstrap-server localhost:9092 --list 2>/dev/null)
if echo "$TOPICS" | grep -q "transactions"; then
  ok "tópico 'transactions' existe"
  ((PASS++))
else
  fail "tópico 'transactions' não encontrado"
  ((FAIL++))
fi

info "Verificando mensagens no tópico..."
MSG_COUNT=$(docker compose exec -T kafka kafka-run-class.sh kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 --topic transactions --time -1 2>/dev/null \
  | awk -F: '{sum += $3} END {print sum+0}')

if [ "$MSG_COUNT" -gt 0 ] 2>/dev/null; then
  ok "Kafka tem ${MSG_COUNT} mensagens no tópico transactions"
  ((PASS++))
else
  info "Nenhuma mensagem ainda (producer pode estar iniciando)"
fi

# ── 5. PostgreSQL ────────────────────────────
echo ""
echo "[ 5/5 ] PostgreSQL"

check "conexão PostgreSQL" \
  docker compose exec -T postgres psql -U tabacaria -d tabacaria_dw -c "SELECT 1" -q

info "Verificando tabela live_feed..."
TABLE_EXISTS=$(docker compose exec -T postgres psql -U tabacaria -d tabacaria_dw -tAc \
  "SELECT COUNT(*) FROM information_schema.tables WHERE table_name='live_feed'" 2>/dev/null)
if [ "$TABLE_EXISTS" = "1" ]; then
  ok "tabela live_feed existe"
  ((PASS++))
else
  fail "tabela live_feed não existe (init.sql não rodou?)"
  ((FAIL++))
fi

info "Contando registros na live_feed..."
ROW_COUNT=$(docker compose exec -T postgres psql -U tabacaria -d tabacaria_dw -tAc \
  "SELECT COUNT(*) FROM live_feed" 2>/dev/null | tr -d ' ')
if [ -n "$ROW_COUNT" ]; then
  ok "live_feed tem ${ROW_COUNT} registros"
  ((PASS++))
fi

# ── Resultado ────────────────────────────────
echo ""
echo "══════════════════════════════════════════"
echo -e "  Resultado: ${GREEN}${PASS} ok${NC} | ${RED}${FAIL} falhas${NC}"
echo "══════════════════════════════════════════"

if [ "$FAIL" -gt 0 ]; then
  echo ""
  echo "Dicas de diagnóstico:"
  echo "  docker compose logs kafka    # logs do Kafka"
  echo "  docker compose logs spark    # logs do Spark (JAR download na 1ª vez)"
  echo "  docker compose logs producer # logs do producer"
  echo "  docker compose ps            # status de todos os containers"
  exit 1
fi

echo ""
echo -e "${GREEN}Tudo OK! Acesse o Grafana em http://localhost:3000 (admin/admin)${NC}"
echo -e "${GREEN}Spark UI em http://localhost:4040${NC}"
echo ""
