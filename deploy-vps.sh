# Deploy Hivee ke VPS
# =====================
# Jalankan semua command ini di VPS via SSH

# 1. Push dulu dari Mac lokal (jalankan di terminal Mac, BUKAN VPS):
#    cd /Users/rezaerfit/Ariel/HIVEE
#    git add -A && git commit -m "fix: connector routing and cursor" && git push origin main

# ---------------------------------------------------------------
# SEMUA COMMAND DI BAWAH INI JALANKAN DI VPS SSH
# ---------------------------------------------------------------

# 2. Stop connector lama (kalau masih jalan)
cd /docker/hivee-connector && docker compose down 2>/dev/null
cd /docker

# 3. Buat directory baru untuk Hivee stack
mkdir -p /docker/hivee
cd /docker/hivee

# 4. Download docker-compose.vps.yml (atau copy-paste manual)
cat > docker-compose.yml << 'COMPOSE_EOF'
services:
  hivee:
    image: python:3.11-slim
    container_name: hivee-app
    working_dir: /app
    command: >
      sh -c "apt-get update -qq && apt-get install -y -qq git >/dev/null 2>&1 &&
      if [ ! -f /app/main.py ]; then
        rm -rf /tmp/hivee-repo &&
        git clone https://github.com/rezaerfit-commits/HIVEE-Reza-Version.git /tmp/hivee-repo &&
        cp -r /tmp/hivee-repo/* /tmp/hivee-repo/.* /app/ 2>/dev/null || true &&
        rm -rf /tmp/hivee-repo;
      else
        cd /app && git pull origin main 2>/dev/null || true;
      fi &&
      pip install -q -r requirements.txt &&
      uvicorn main:app --host 0.0.0.0 --port 8000"
    restart: unless-stopped
    labels:
      - traefik.enable=true
      - traefik.http.routers.hivee.rule=Host(`hivee.srv1570855.hstgr.cloud`)
      - traefik.http.routers.hivee.entrypoints=websecure
      - traefik.http.routers.hivee.tls.certresolver=letsencrypt
      - traefik.http.services.hivee.loadbalancer.server.port=8000
    environment:
      - TZ=Asia/Jakarta
    ports:
      - "8000:8000"
    volumes:
      - hivee-code:/app
    networks:
      - openclaw-izjk_default

  hivee-connector:
    image: node:20-alpine
    container_name: hivee-connector-vps
    working_dir: /app
    command: >
      sh -c "apk add --no-cache git curl >/dev/null 2>&1 &&
      if [ ! -f /app/package.json ]; then
        rm -rf /tmp/conn-repo &&
        git clone https://github.com/rezaerfit-commits/HIVEE-connector.git /tmp/conn-repo &&
        cp -r /tmp/conn-repo/* /tmp/conn-repo/.* /app/ 2>/dev/null || true &&
        rm -rf /tmp/conn-repo;
      fi &&
      npm install &&
      npm run dev"
    restart: unless-stopped
    environment:
      NODE_ENV: "production"
      PORT: "43137"
      HOST: "0.0.0.0"
      LOG_LEVEL: "info"
      DATA_DIR: "/data"
      CONNECTOR_NAME: "Hivee VPS Connector"
      CONNECTOR_BIND_PUBLIC: "false"
      CLOUD_BASE_URL: "http://hivee-app:8000"
      CLOUD_WS_URL: ""
      PAIRING_TOKEN: ""
      OPENCLAW_BASE_URL: "http://openclaw:18790"
      OPENCLAW_DISCOVERY_CANDIDATES: "http://openclaw:18790"
      OPENCLAW_TOKEN: "sCirXcZyRCckVnHz9IFtHch77NKatkvp"
      OPENCLAW_TRANSPORT: "http"
      OPENCLAW_WS_PATH: ""
      OPENCLAW_REQUEST_TIMEOUT_MS: "60000"
      ENABLE_DOCKER_DISCOVERY: "false"
    ports:
      - "43137:43137"
    volumes:
      - connector-data:/data
    networks:
      - openclaw-izjk_default
    depends_on:
      - hivee

volumes:
  hivee-code:
  connector-data:

networks:
  openclaw-izjk_default:
    external: true
COMPOSE_EOF

# 5. Start Hivee stack
docker compose up -d

# 6. Check logs (wait ~30s for first startup)
sleep 30
echo "=== HIVEE LOGS ==="
docker logs hivee-app --tail 20
echo ""
echo "=== CONNECTOR LOGS ==="
docker logs hivee-connector-vps --tail 20

# 7. Test Hivee is accessible
echo ""
echo "=== CONNECTIVITY TEST ==="
curl -s -m 5 http://localhost:8000/api/connectors 2>&1 | head -1
echo ""
curl -s -m 5 https://hivee.srv1570855.hstgr.cloud/ -o /dev/null -w "HTTPS status: %{http_code}" 2>&1
echo ""

# NEXT STEPS:
# - Open https://hivee.srv1570855.hstgr.cloud in your browser
# - Generate a pairing token in Hivee UI (Settings > Connectors)
# - On VPS: update PAIRING_TOKEN and restart connector:
#   cd /docker/hivee
#   PAIRING_TOKEN=pair_xxx docker compose up -d hivee-connector
