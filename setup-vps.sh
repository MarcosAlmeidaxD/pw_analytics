#!/bin/bash
# pw_analytics — VPS Setup Script
# Ubuntu 24.04 LTS | Hostinger KVM 2
# Uso: bash setup-vps.sh

set -e
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'

log() { echo -e "${GREEN}[setup]${NC} $1"; }
warn() { echo -e "${YELLOW}[warn]${NC} $1"; }

# ── 1. Sistema ────────────────────────────────────────────────────────────────
log "Atualizando sistema..."
apt-get update -qq && apt-get upgrade -y -qq

# ── 2. Docker ─────────────────────────────────────────────────────────────────
log "Instalando Docker..."
curl -fsSL https://get.docker.com | sh
systemctl enable docker
systemctl start docker

log "Docker $(docker --version)"
log "Compose $(docker compose version)"

# ── 3. Repositório ────────────────────────────────────────────────────────────
log "Clonando pw_analytics..."
cd /opt
git clone https://github.com/MarcosAlmeidaxD/pw_analytics.git
cd pw_analytics

# ── 4. Variáveis de ambiente ──────────────────────────────────────────────────
log "Configurando .env..."
cat > .env <<EOF
DISCORD_TOKEN=${DISCORD_TOKEN:?'Defina DISCORD_TOKEN antes de rodar'}
CHANNEL_ID=${CHANNEL_ID:-1035423433040875590}
START_ID=${START_ID:-1479225035926540351}
STOP_ID=${STOP_ID:-0}
POLL_INTERVAL=${POLL_INTERVAL:-30}
EOF

log ".env criado"

# ── 5. Firewall ───────────────────────────────────────────────────────────────
log "Configurando firewall (ufw)..."
ufw allow ssh
ufw allow 3000/tcp   # Grafana
ufw allow 8080/tcp   # pvp-control
ufw --force enable

warn "Portas abertas: 22 (SSH), 3000 (Grafana), 8080 (pvp-control)"
warn "Kafka/Postgres/Prometheus ficam internos (não expostos)"

# ── 6. Subindo os serviços ────────────────────────────────────────────────────
log "Iniciando containers..."
docker compose up -d --build

log "Aguardando serviços ficarem healthy..."
sleep 30
docker compose ps

log ""
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
log "✅  pw_analytics rodando!"
log ""
log "  Grafana:     http://$(curl -s ifconfig.me):3000  (admin/admin)"
log "  PvP Control: http://$(curl -s ifconfig.me):8080"
log "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━"
