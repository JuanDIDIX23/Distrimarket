#!/usr/bin/env bash
# Ejecución local SIN Docker (útil para desarrollo y para las pruebas de tolerancia a fallos).
# Requiere PostgreSQL y Redis en ejecución. Uso:
#   scripts/stack.sh start [servicio]   |  scripts/stack.sh stop [servicio]  |  scripts/stack.sh status
# Servicios: auth inventory-1 inventory-2 orders notifications gateway
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RUN="$ROOT/.run"; mkdir -p "$RUN"
PG="postgresql://dm:dm@localhost:5432"
export PYTHONPATH="$ROOT" REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"
export JWT_SECRET="${JWT_SECRET:-dev-secret-distrimarket-2026-cambiar}" INTERNAL_TOKEN="${INTERNAL_TOKEN:-dev-internal-token}"
export BCRYPT_ROUNDS="${BCRYPT_ROUNDS:-10}" RATE_LIMIT_PER_MIN="${RATE_LIMIT_PER_MIN:-100000}"

declare -A PORT=( [auth]=8001 [inventory-1]=8002 [inventory-2]=8012 [orders]=8003 [notifications]=8004 [gateway]=8000 )
ALL="auth inventory-1 inventory-2 orders notifications gateway"

start_one() {
  local name=$1 port=${PORT[$1]} module dir db
  case $name in
    inventory-*) dir=inventory; db=inventory_db ;;
    gateway) dir=gateway; db="" ;;
    *) dir=$name; db=${name}_db ;;
  esac
  if [[ -f "$RUN/$name.pid" ]] && kill -0 "$(cat "$RUN/$name.pid")" 2>/dev/null; then echo "$name ya está en ejecución"; return; fi
  SERVICE_NAME=$dir INSTANCE_ID=$name DATABASE_URL="$PG/$db" \
    nohup python3 -m uvicorn "services.$dir.main:app" --host 0.0.0.0 --port "$port" \
      --workers "${WORKERS:-1}" --log-level warning --no-access-log >> "$RUN/$name.log" 2>&1 &
  echo $! > "$RUN/$name.pid"
  for _ in $(seq 1 50); do curl -sf "localhost:$port/health" >/dev/null && { echo "✔ $name en :$port"; return; }; sleep 0.2; done
  echo "✘ $name no arrancó (ver $RUN/$name.log)"; return 1
}
stop_one() {
  local name=$1
  # SIGKILL simula una caída abrupta (fallo de proceso/nodo), no un apagado ordenado.
  if [[ -f "$RUN/$name.pid" ]]; then kill -9 "$(cat "$RUN/$name.pid")" 2>/dev/null || true; rm -f "$RUN/$name.pid"; fi
  pkill -9 -f "uvicorn.*--port ${PORT[$name]}" 2>/dev/null || true
  for _ in $(seq 1 25); do curl -sf -m 0.2 "localhost:${PORT[$name]}/health" >/dev/null || break; sleep 0.1; done
  echo "■ $name detenido"
}

cmd=${1:-status}; shift || true
targets=${*:-$ALL}
case $cmd in
  start) for s in $targets; do start_one "$s"; done ;;
  stop) for s in $targets; do stop_one "$s"; done ;;
  status) for s in $ALL; do printf "%-14s :%s  " "$s" "${PORT[$s]}"; curl -sf "localhost:${PORT[$s]}/health" >/dev/null && echo activo || echo caído; done ;;
  *) echo "uso: $0 start|stop|status [servicio...]"; exit 1 ;;
esac
