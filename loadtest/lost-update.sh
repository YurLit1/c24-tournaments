set -eu
cd "$(dirname "$0")/.."

PY=${PY:-$(command -v py || command -v python3 || command -v python)}
N=${N:-500}
EXP="docker compose -p c24-exp"
export API_PORT=8091 PG_PORT=5434 WORKERS=${WORKERS:-8} QUEUE_CAPACITY=2000

for mode in none lock; do
  echo
  echo "=== RATING_SYNC=$mode (окремий проєкт c24-exp, окрема тестова база) ==="
  RATING_SYNC=$mode $EXP up -d --build postgres migrate api >/dev/null 2>&1
  for i in $(seq 1 60); do curl -sf "http://localhost:$API_PORT/health" >/dev/null && break; sleep 1; done
  "$PY" loadtest/lost_update.py --base "http://localhost:$API_PORT" --n "$N" --hot 3
  $EXP down -v >/dev/null 2>&1
done
