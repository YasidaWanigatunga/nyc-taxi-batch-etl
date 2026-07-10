set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RAW_DIR="${PROJECT_DIR}/data/raw"
VENV_DIR="${PROJECT_DIR}/.venv"
BASE_URL="https://d37ci6vzurychx.cloudfront.net"

MONTHS=("$@")
[ ${#MONTHS[@]} -eq 0 ] && MONTHS=("2023-01" "2023-02")

log() { printf '\033[0;36m[%s] %s\033[0m\n' "$(date '+%H:%M:%S')" "$*"; }
ok()  { printf '\033[0;32m[%s] ✔ %s\033[0m\n' "$(date '+%H:%M:%S')" "$*"; }
die() { printf '\033[0;31m[%s] ✘ %s\033[0m\n' "$(date '+%H:%M:%S')" "$*" >&2; exit 1; }
trap 'die "failed at line $LINENO"' ERR

# --- 0. Prerequisites ------------------------------------------------------
require() { command -v "$1" >/dev/null 2>&1 || die "'$1' is required but not installed."; }
log "Checking prerequisites…"
require curl; require docker; require python3
docker compose version >/dev/null 2>&1 || die "Docker Compose v2 plugin not found."
docker info >/dev/null 2>&1 || die "Docker daemon not running. Start Docker Desktop."
ok "Prerequisites present."

# --- 1. .env ---------------------------------------------------------------
if [ ! -f "${PROJECT_DIR}/.env" ]; then
  {
    echo "AIRFLOW_UID=$(id -u)"
    echo "TAXI_MONTHS=$(IFS=,; echo "${MONTHS[*]}")"
  } > "${PROJECT_DIR}/.env"
  ok ".env created (AIRFLOW_UID=$(id -u))."
fi

# --- 2. Virtual environment ------------------------------------------------
log "Creating Python virtual environment…"
python3 -m venv "${VENV_DIR}"
# shellcheck disable=SC1091
source "${VENV_DIR}/bin/activate"
pip install --quiet --upgrade pip
pip install --quiet -r "${PROJECT_DIR}/requirements-airflow.txt" \
                      -r "${PROJECT_DIR}/requirements-dashboard.txt"
ok "Virtual environment ready at .venv"

# --- 3. Data ---------------------------------------------------------------
mkdir -p "${RAW_DIR}" "${PROJECT_DIR}/logs"

download() {
  local url="$1" dest="$2"
  if [ -s "${dest}" ]; then ok "$(basename "${dest}") cached — skipping."; return; fi
  log "Downloading $(basename "${dest}") …"
  # -f: fail on 404 (never save an HTML error page as .parquet)
  # -L: follow redirects   -C -: resume   --retry: survive transient errors
  curl -fL --retry 3 --retry-delay 2 -C - -o "${dest}.part" "${url}"
  mv "${dest}.part" "${dest}"
  ok "$(basename "${dest}") → $(du -h "${dest}" | cut -f1)"
}

for m in "${MONTHS[@]}"; do
  download "${BASE_URL}/trip-data/yellow_tripdata_${m}.parquet" \
           "${RAW_DIR}/yellow_tripdata_${m}.parquet"
done
download "${BASE_URL}/misc/taxi_zone_lookup.csv" "${RAW_DIR}/taxi_zone_lookup.csv"

# --- 4. Services -----------------------------------------------------------
log "Building images and starting the stack (first run takes a few minutes)…"
docker compose up -d --build

log "Waiting for PostgreSQL…"
until docker compose exec -T postgres pg_isready -U airflow >/dev/null 2>&1; do sleep 2; done
ok "PostgreSQL healthy."

log "Waiting for the Airflow webserver…"
for _ in $(seq 1 60); do
  curl -fs http://localhost:8080/health >/dev/null 2>&1 && break
  sleep 5
done
ok "Airflow up."

cat <<'BANNER'

────────────────────────────────────────────────────────────
  Environment ready.

  Airflow UI    http://localhost:8080     (admin / admin)
  Streamlit     http://localhost:8501
  PostgreSQL    localhost:5432  db=taxi_dw  user=taxi  pw=taxi

  Next: open Airflow, un-pause `nyc_taxi_batch_etl`, hit ▶ Trigger.
────────────────────────────────────────────────────────────
BANNER