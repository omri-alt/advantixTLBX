# Google Sheets credentials (required for Docker deploy)

Place your service account JSON here as **`credentials.json`** (not committed to git).

```bash
cp /path/to/your-service-account.json secrets/credentials.json
```

Copy **`.env.production.example`** to **`.env`** in the repo root and fill API keys (KEYSK, KEYZP, Keitaro, Ecomnia, etc.).

Docker mounts host directories so scheduler state survives redeploys:

- **`runtime/`** — scheduler lock, heartbeat, workflow progress
- **`data/`** — AutoServer run log (`autoserver_run_log.json`), SK optimizer state

Then deploy from the repo root:

```bash
bash scripts/deploy.sh
```

Do **not** use `docker compose watch` on the server — use `scripts/deploy.sh` or `docker compose up -d --build`.
