# Google Sheets credentials (required for Docker deploy)

Place your service account JSON here as **`credentials.json`** (not committed to git).

```bash
cp /path/to/your-service-account.json secrets/credentials.json
```

Then deploy from the repo root:

```bash
bash scripts/deploy.sh
```

Do **not** use `docker compose watch` on the server — use `scripts/deploy.sh` or `docker compose up -d --build`.
