# tapis-postgres-backup

Backs up Upstream Postgres pods into Tapis Files storage and restores those backups into a Tapis-hosted Postgres pod.

The project supports three main workflows:

- `backup-once`: discover eligible Postgres pods and create a backup immediately
- `backup-loop`: run the same backup job on a fixed interval
- `restore`: download a backup set and restore it into a target Postgres pod

## What gets backed up

The backup job discovers pods that look like Upstream Postgres pods:

- pod id ends with `postgres`
- description is `postgres for upstream-docker` when present
- a Tapis volume is mounted at `/var/lib/postgresql/data`
- `POSTGRES_USER`, `POSTGRES_PASSWORD`, and `POSTGRES_DB` are present
- the mounted volume id matches the expected `postgres -> volume` name pattern

For each matching pod, the job creates:

- a custom-format `pg_dump` archive
- a `pg_dumpall --globals-only` SQL file
- `checksums.txt`
- `manifest.json`

Backups are uploaded to the configured Tapis Files system under:

```text
<TAPIS_BACKUP_ROOT_PATH>/<pod_id>/YYYY/MM/DD/
```

An inventory file for each run is also uploaded under:

```text
<TAPIS_BACKUP_ROOT_PATH>/_inventory/YYYY/MM/DD/
```

Retention is enforced per pod by date directory. By default, the newest 7 daily backups are kept.

## Requirements

- Python 3.11+
- PostgreSQL client tools available on `PATH`: `pg_dump`, `pg_dumpall`, `pg_restore`, `psql`
- network access to the Tapis API and the target Postgres pods
- a Tapis token or service credentials with permission to list and inspect pods
- permission to create directories and upload/download files in the backup system
- permission to create pods and volumes if you use restore without `--reuse-existing-pod`

The provided `Dockerfile` installs PostgreSQL 17 client tools and runs `runner.py` by default.

## Configuration

Copy `.env.example` to `.env` and fill in the values you actually use.

Core settings:

```env
TAPIS_BASE_URL=https://portals.tapis.io
TAPIS_TENANT_ID=portals

TAPIS_SERVICE_USERNAME=YOUR_SERVICE_ACCOUNT_USERNAME
TAPIS_SERVICE_PASSWORD=YOUR_SERVICE_ACCOUNT_PASSWORD

TAPIS_BACKUP_SYSTEM_ID=ptdatax.project.PTDATAX-284
TAPIS_BACKUP_ROOT_PATH=/upstream-postgres
TAPIS_BACKUP_RETENTION_DAYS=7
TAPIS_BACKUP_STAGING_DIR=/tmp/upstream-postgres-backups
TAPIS_BACKUP_TIMEOUT_SECONDS=300

TAPIS_POSTGRES_BACKUP_MODE=backup-once
TAPIS_POSTGRES_BACKUP_INTERVAL_SECONDS=86400
TAPIS_POSTGRES_BACKUP_RUN_IMMEDIATELY=true
TAPIS_POSTGRES_BACKUP_LOG_LEVEL=INFO
```

Authentication can be supplied in either of these ways:

- set `TAPIS_SERVICE_USERNAME` and `TAPIS_SERVICE_PASSWORD`
- pass `--token` to the backup or restore command

`TAPIS_PODS_BASE_URL` is optional. If unset, the code falls back to `TAPIS_BASE_URL`.

## Local Development

Create a virtual environment and install dependencies:

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run the unit tests:

```bash
python -m pytest test_backup.py
```

## Backup Commands

Run a single backup pass:

```bash
python tapis_postgres_backup.py --log-level INFO
```

Run with an explicit token:

```bash
python tapis_postgres_backup.py --token "$TAPIS_TOKEN"
```

Run through the container entrypoint:

```bash
python runner.py --mode backup-once
```

Continuous backup loop:

```bash
python runner.py --mode backup-loop
```

The backup command prints a JSON summary and returns a non-zero exit code if any target fails.

## Restore Commands

Restore the latest available backup for a pod into a fresh target pod and volume:

```bash
python tapis_postgres_restore.py \
  --pod-id weatherpostgres \
  --target-pod-id weatherpostgres-restore
```

Restore a specific backup date:

```bash
python tapis_postgres_restore.py \
  --pod-id weatherpostgres \
  --backup-date 2026-04-10 \
  --target-pod-id weatherpostgres-restore
```

Restore into an already-running pod:

```bash
python tapis_postgres_restore.py \
  --pod-id weatherpostgres \
  --target-pod-id weatherpostgres-restore \
  --reuse-existing-pod
```

Skip applying `globals.sql` before `pg_restore`:

```bash
python tapis_postgres_restore.py \
  --pod-id weatherpostgres \
  --target-pod-id weatherpostgres-restore \
  --skip-globals
```

Restore behavior:

- if `--backup-date` is omitted, the newest available backup date is used
- if `--reuse-existing-pod` is not set, the tool creates the target volume and pod
- the restored database connection details are printed as JSON at the end
- the tool does not update any API pod configuration for you

## Docker

Build the image:

```bash
docker build -t tapis-postgres-backup .
```

Run a one-shot backup in Docker:

```bash
docker run --rm --env-file .env tapis-postgres-backup python runner.py --mode backup-once
```

Run restore in Docker:

```bash
docker run --rm --env-file .env tapis-postgres-backup \
  python runner.py --mode restore -- --pod-id weatherpostgres --target-pod-id weatherpostgres-restore
```

## Tapis Actor Helpers

Two helper scripts are included for actor-based execution:

- `test_actor_once.py`: creates a temporary one-shot actor, runs one execution, prints logs, and optionally deletes the actor
- `schedule_actor.py`: creates or updates a cron-based actor for recurring backups

Smoke test a one-shot actor:

```bash
python test_actor_once.py
```

Create or update a scheduled actor:

```bash
python schedule_actor.py \
  --image ghcr.io/YOUR_ORG/YOUR_IMAGE:latest \
  --cron-schedule "2026-04-11 00 + 1 day"
```

The actor helpers expect the `ACTOR_*` variables shown in `.env.example`.

## Notes

- backup manifests intentionally omit database credentials
- restore resolves live database credentials from the source or target pod definition
- the code assumes pod hosts are reachable as `<pod_id>.pods.tacc.tapis.io` when not explicitly provided by the API
