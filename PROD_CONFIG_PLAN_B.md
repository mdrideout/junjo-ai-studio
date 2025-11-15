# Production Configuration Planning

## 1. Service & Environment Variable Flow

- `.env` feeds every container via `env_file`, supplying shared values such as `JUNJO_BUILD_TARGET`, `JUNJO_ENV`, session secrets, CORS origins, and database storage paths (`.env.example:1-104`, `docker-compose.yml:39-105`).  
- `JUNJO_BUILD_TARGET` toggles Docker build stages for dev (hot reload) vs prod (optimized binaries), so the same compose file supports both modes (`docker-compose.yml:1-20`, `.env.example:1-16`).  
- `JUNJO_HOST_DB_DATA_PATH` controls where SQLite/DuckDB/BadgerDB live on the host; backend and ingestion mount this path to persist WAL + index data across restarts (`docker-compose.yml:30-74`, `.env.example:62-88`).  
- Backend overrides (`INGESTION_HOST/PORT`, `RUN_MIGRATIONS`, DB paths) wire its ingestion client to the internal gRPC reader and define DB file locations (`docker-compose.yml:41-49`).  
- Ingestion overrides (`BACKEND_GRPC_HOST/PORT`, `JUNJO_BADGERDB_PATH`) connect the API-key interceptor to the backend internal auth service, completing the API key validation loop (`docker-compose.yml:70-74`).  
- Session security secrets (`JUNJO_SESSION_SECRET`, `JUNJO_SECURE_COOKIE_KEY`) power the FastAPI middleware stack that encrypts/signs cookies, while `JUNJO_ENV` controls HTTPS-only behavior in production (`.env.example:31-60`, `AGENTS.md:252-329`).

## 2. Relationship Map

- **Frontend ↔ Backend**: React frontend (Vite in dev, nginx in prod) calls FastAPI on port 1323. Both must share the same registrable domain so `SameSite=Strict` cookies remain valid (`docker-compose.yml:34-105`, `AGENTS.md:252-318`).  
- **Backend ↔ Ingestion**: Backend polls ingestion’s WAL reader via Docker-internal hostnames/ports, while ingestion contacts the backend’s private gRPC auth endpoint for API key validation, reflecting the architecture diagrams (`docker-compose.yml:41-79`, `AGENTS.md:17-94`, `AGENTS.md:352-368`).  
- **Clients ↔ Ingestion**: External OTel clients send authenticated gRPC spans to `grpc://<host>:50051`; only this public port is exposed, keeping internal ports (50052, 50053) private (`docker-compose.yml:63-66`, `AGENTS.md:70-94`).  
- **Shared Storage**: Both backend and ingestion read/write under the mounted `.dbdata` path—backend uses SQLite/DuckDB/QDrant while ingestion maintains the BadgerDB WAL (`docker-compose.yml:30-74`, `AGENTS.md:352-368`).

## 3. Configuration Strategies

### Local (localhost)

- Keep `.env` defaults: `JUNJO_BUILD_TARGET=development`, `JUNJO_ENV=development`, `JUNJO_HOST_DB_DATA_PATH=./.dbdata`, and `JUNJO_ALLOW_ORIGINS` including `http://localhost:5151`/`5153` so both dev and prod frontends can reach the API (`.env.example:1-88`).  
- Run `docker compose up -d`; services bind to ports 1323 (backend), 50051 (ingestion gRPC), and 5151/5152/5153 (frontend dev + prod builds) as described in the README quick start (`docker-compose.yml:34-105`, `README.md:35-120`).  
- Generate real secrets for `JUNJO_SESSION_SECRET` and `JUNJO_SECURE_COOKIE_KEY` even in dev to mirror production cookie behavior (`.env.example:31-60`, `AGENTS.md:252-329`).

### Production (domain-based)

- Set `.env` to `JUNJO_BUILD_TARGET=production` and `JUNJO_ENV=production`; point `JUNJO_ALLOW_ORIGINS` to actual HTTPS URLs (e.g., `https://app.example.com,https://api.example.com`) and `JUNJO_HOST_DB_DATA_PATH` to persistent storage such as `/mnt/junjo-data` (`.env.example:1-88`).  
- Keep backend + frontend on the same registrable domain (e.g., `app.example.com` and `api.example.com`) to preserve `SameSite=Strict` cookies without needing explicit domain configuration (`AGENTS.md:304-318`).  
- Terminate TLS via a reverse proxy (Caddy/Traefik/Nginx) that routes frontend and backend traffic to ports 5153/1323 while leaving the backend’s gRPC port 50053 and ingestion’s internal port 50052 unexposed; only expose ingestion’s public gRPC port 50051 (`docker-compose.yml:34-80`, `AGENTS.md:70-94`).  
- Configure DNS for ingestion (e.g., `otel.example.com`) and ensure firewall rules allow gRPC ingress on 50051 while restricting internal ports to the Docker network.  
- Regenerate secrets for production, set provider API keys, and schedule backups for `/mnt/junjo-data` since it contains SQLite, DuckDB, and BadgerDB state (`.env.example:31-118`, `docker-compose.yml:30-74`, `AGENTS.md:352-368`).

---

Prepared for follow-up tasks such as translating this into a full deployment runbook or infrastructure diagram.
