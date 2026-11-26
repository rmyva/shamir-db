# Deployment artefacts

This directory packages reference deployment artefacts for `shamir-server`
(alpha — not yet recommended for production data).

## Files

| File | What it is |
|------|------------|
| `shamir-db.service` | systemd unit. Drop into `/etc/systemd/system/` and `systemctl enable --now shamir-db` |
| `Dockerfile` | Multi-stage Docker image (Rust 1.95 bookworm builder → debian:bookworm-slim runtime, ~80 MB) |
| `server.example.ktav` | Base reference config (all fields shown), sized for a 2–4 GiB container (≈3 GiB budget): Argon2 128 MiB × 6, 1000 conns, 64 MiB result cap |
| `server.small.example.ktav` | Resource profile for a 1–2 GiB container (≈1.5 GiB budget): Argon2 64 MiB × 6, 500 conns, 32 MiB result cap |
| `server.medium.example.ktav` | Resource profile for a 4–8 GiB container (≈6 GiB budget): Argon2 128 MiB × 12, 2000 conns, 64 MiB result cap |
| `server.large.example.ktav` | Resource profile for a ≥32 GiB host: Argon2 128 MiB × 64, 10 000 conns, 1 GiB result cap (opt-in) |

## Resource profiles

Pick a profile by your container/host RAM budget (`server.example.ktav`
is the every-field-shown base, conservatively sized for a typical 2–4 GiB
host — use it directly or trim it when you want to see every knob):

| Profile | Target RAM | When to pick |
|---------|-----------|--------------|
| `server.small.example.ktav` | 1–2 GiB (sized for ~1.5 GiB) | Single-tenant / dev / small VPS. Approximate base Argon2 matrix memory ≈ 384 MiB before overhead. |
| `server.medium.example.ktav` | 4–8 GiB (sized for ~6 GiB) | Small-to-medium production. Approximate base Argon2 matrix memory ≈ 1.5 GiB before overhead. |
| `server.example.ktav` | 2–4 GiB (sized for ~3 GiB) | General-purpose safe default AND every-field-shown reference — copy & trim. Approximate base Argon2 matrix memory ≈ 768 MiB before overhead. |
| `server.large.example.ktav` | ≥32 GiB | High-capacity opt-in: 10 000 conns, 1 GiB results. Approximate base Argon2 matrix memory ≈ 8 GiB before overhead. |

Base Argon2 matrix memory estimate before implementation overhead: `argon2_concurrent_max × kdf_defaults.memory_kb` KiB. Reserve additional memory for implementation overhead, other process memory, admitted tasks waiting for permits (including passwords and task state), waiter thread stacks, bootstrap and bootstrap-token rotation, follower outbound SCRAM, and SQL `argon2id`. The estimates vary with configured KDF parameters and are not an exact process-memory ceiling.

## Quick start (systemd)

```bash
# 1. User + dirs
sudo useradd --system --no-create-home shamir
sudo mkdir -p /etc/shamir /var/lib/shamir-db /var/log/shamir-db
sudo chown -R shamir:shamir /var/lib/shamir-db /var/log/shamir-db

# 2. Binary + config
sudo cp target/release/shamir-server /usr/local/bin/
sudo cp deploy/server.example.ktav /etc/shamir/server.ktav
# edit /etc/shamir/server.ktav: addrs, listeners, browser_origin_allowlist

# 3. Service
sudo cp deploy/shamir-db.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now shamir-db

# 4. Verify
journalctl -u shamir-db -f
curl -fsS http://127.0.0.1:9090/healthz   # → "ok"
curl -fsS http://127.0.0.1:9090/readyz    # → "ready"
```

First boot: a random bootstrap token is generated and its **path** (never
the token itself) is logged at WARN level. By default it is written to
`data_dir/bootstrap_token.txt` — for backward compatibility only.
**Operators SHOULD instead pass `--bootstrap-token-path
/run/shamir/bootstrap_token.txt`** (tmpfs, per the `RuntimeDirectory=shamir`
directive in `shamir-db.service`) so the token is never captured by a
`backup --to` snapshot of `data_dir`. If left at the default `data_dir`
path, the token **will** be captured verbatim by any `backup --to` run
that happens before it is consumed or expired.

The token auto-deletes itself: the server removes the file and consumes
the record on the **first successful SCRAM login** for the bootstrap
username, or via a **24h TTL** boot-time sweep for any token nobody ever
used — whichever comes first. Manual deletion right after reading the
token and logging in remains a safe, optional immediate step; it is no
longer the primary cleanup mechanism. After logging in, have an admin
pre-create operator users via `CreateScramUser`.

## Quick start (Docker)

```bash
docker build -f deploy/Dockerfile -t shamir-db:dev .

docker run -d --name shamir-db \
  -v shamir-data:/var/lib/shamir-db \
  -v $(pwd)/deploy/server.example.ktav:/etc/shamir/server.ktav:ro \
  -p 7331:7331 \
  -p 7332:7332 \
  -p 7333:7333 \
  -p 9090:9090 \
  --memory 4g --cpus 2 \
  shamir-db:dev

docker exec shamir-db cat /var/lib/shamir-db/bootstrap_token.txt
```

## Backup

```bash
# Stop-and-copy is the supported path. Fjall is journal-based, so a copy
# that races an in-flight append loses only the torn tail batch on next
# open (RecoveryMode::TolerateCorruptTail truncates back to the last
# fully-checksummed batch) — earlier committed batches stay intact — but
# stop-and-copy is the strongest guarantee.
sudo systemctl stop shamir-db
shamir-server --config /etc/shamir/server.ktav backup --to /backups/
sudo systemctl start shamir-db

# Cron daily — BEST-EFFORT LIVE SNAPSHOT (no service stop): recovers, on
# next open, to the last complete journal batch; for a guaranteed-
# consistent snapshot, stop the service first (see the block above).
0 3 * * * /usr/local/bin/shamir-server --config /etc/shamir/server.ktav backup --to /backups/
```

Restore = stop service → `cp -r /backups/<timestamp>/* /var/lib/shamir-db/` → start.

## Monitoring assets

| File | Purpose |
|------|---------|
| `grafana/shamir-dashboard.json` | Grafana dashboard (schema v39) with process, load, auth, query, WAL, replication, visibility and admission views. |
| `alerts/shamir.rules.yml` | Prometheus alert rules for WAL retention, replica lag, query errors, visibility blockers, authentication and admission saturation. |
| `alerts/shamir-backup.rules.yml` | Backup-schedule alerts; kept separate to avoid duplicate backup alerts. |

Common alert thresholds and installation instructions live in `alerts/shamir.rules.yml`; backup freshness/failure/retention expressions live in `alerts/shamir-backup.rules.yml`. Configure Prometheus `rule_files` with both files and validate/reload; import or provision `grafana/shamir-dashboard.json`.

## Scrape with Prometheus

```yaml
# prometheus.yml
scrape_configs:
  - job_name: 'shamir-db'
    static_configs:
      - targets: ['shamir-db.internal:9090']
rule_files:
  - /etc/prometheus/rules/shamir.rules.yml
  - /etc/prometheus/rules/shamir-backup.rules.yml
```

Standard `process_*` series populate Grafana's
[Process Exporter dashboard](https://grafana.com/grafana/dashboards/?search=process)
out of the box. The ShamirDB dashboard is `grafana/shamir-dashboard.json`.

## Alerts

Load `alerts/shamir.rules.yml` for ShamirDB operational rules and
`alerts/shamir-backup.rules.yml` for backup freshness, failure-streak and
retention rules. The backup file remains the sole source of backup alerts; do
not duplicate them in application rules. See [§10 of the operations guide](../docs/guide-docs/guide/07-operations.md) for Prometheus `rule_files` wiring and Grafana provisioning/import steps.

## See also

* [Token API operator guide](../docs/guide-docs/guide/token-api.md) — listener setup and token administration.
* `docs/dev-artifacts/ops/CAPACITY_PLANNING.md` — sizing reference: per-unit RAM,
  CPU costs, disk growth rates, recommended VM sizes.
* `docs/dev-artifacts/roadmap/PRODUCTION_HARDENING_ROADMAP.md` — what's still
  outstanding for full production maturity.
