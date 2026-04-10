# tapis-postgres-backup

Backup and restore tooling for Upstream Postgres pods managed in Tapis Pods.

Backups are:

- created as nightly logical PostgreSQL dumps
- uploaded to the Tapis Files system `ptdatax.project.PTDATAX-284`
- stored under the Corral-backed root `/corral-repl/tacc/aci/PT2050/projects/PTDATAX-284`
- retained for 7 daily restore points per pod

Restores are:

- manual but scripted
- restored back into Tapis Postgres pods
- resolved against live Tapis pod metadata for credentials

## Files

- Backup script: `tapis_postgres_backup.py`
- Restore script: `tapis_postgres_restore.py`
- Shared library: `backup.py`
- Tests: `test_backup.py`

## Prerequisites

The machine running the scripts needs:

- a Python environment with the dependencies from [`requirements.txt`](/Users/wmobley/Documents/GitHub/upstream/tapis-postgres-backup/requirements.txt)
- `pg_dump`
- `pg_dumpall`
- `pg_restore`
- `psql`
- network access to the Tapis API and the Tapis Postgres pod endpoints

The scripts use the existing Upstream settings loader from [`upstream-docker-pods/app/core/config.py`](/Users/wmobley/Documents/GitHub/upstream/upstream-docker-pods/app/core/config.py), so the `.env` file lives in:

- [`upstream-docker-pods/.env`](/Users/wmobley/Documents/GitHub/upstream/upstream-docker-pods/.env)

## Python Environment Setup

Create a dedicated virtualenv inside `tapis-postgres-backup`:

```bash
cd /path/to/upstream/tapis-postgres-backup
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

This is the recommended setup for both local testing and VM installation on `upstream-dso`.

After that, run the backup tools with:

```bash
.venv/bin/python tapis_postgres_backup.py --log-level INFO
```

## Required Configuration

Minimum useful values in [`upstream-docker-pods/.env`](/Users/wmobley/Documents/GitHub/upstream/upstream-docker-pods/.env):

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

DEFAULT_ADMIN_USERS=["wmobley","YOUR_SERVICE_ACCOUNT_USERNAME"]
```

You can also pass `--token` directly to the scripts instead of using service credentials, but the service-account flow is the intended production path.

## Initial Setup

### 1. Create the backup directory on the Tapis Files system

Use an owner token for the system:

```bash
OWNER_JWT='YOUR_OWNER_TOKEN'

curl -X POST \
  -H "X-Tapis-Token: ${OWNER_JWT}" \
  -H "Content-Type: application/json" \
  https://portals.tapis.io/v3/files/ops/ptdatax.project.PTDATAX-284 \
  -d '{"path":"/upstream-postgres"}'
```

### 2. Grant file-path write permission to the system account

Set the target account name once:

```bash
SERVICE_ACCOUNT='YOUR_SERVICE_ACCOUNT_USERNAME'
```

```bash
curl -X POST \
  -H "X-Tapis-Token: ${OWNER_JWT}" \
  -H "Content-Type: application/json" \
  https://portals.tapis.io/v3/files/permissions/ptdatax.project.PTDATAX-284/upstream-postgres \
  -d "{\"username\":\"${SERVICE_ACCOUNT}\",\"permission\":\"MODIFY\"}"
```

Verify:

```bash
curl \
  -H "X-Tapis-Token: ${OWNER_JWT}" \
  "https://portals.tapis.io/v3/files/permissions/ptdatax.project.PTDATAX-284/upstream-postgres?username=${SERVICE_ACCOUNT}"
```

### 3. Grant system read/modify permission to the system account

```bash
curl -X POST \
  -H "X-Tapis-Token: ${OWNER_JWT}" \
  -H "Content-Type: application/json" \
  "https://portals.tapis.io/v3/systems/perms/ptdatax.project.PTDATAX-284/user/${SERVICE_ACCOUNT}" \
  -d '{"permissions":["READ","MODIFY"]}'
```

Verify:

```bash
curl \
  -H "X-Tapis-Token: ${OWNER_JWT}" \
  "https://portals.tapis.io/v3/systems/perms/ptdatax.project.PTDATAX-284/user/${SERVICE_ACCOUNT}"
```

### 4. Grant Pod and Volume admin to the system account on existing bundles

For the current Upstream bundles:

```bash
OWNER_JWT='YOUR_OWNER_TOKEN'
SERVICE_ACCOUNT='YOUR_SERVICE_ACCOUNT_USERNAME'

for base in flux upstream vital
do
  for pod_id in "${base}postgres" "${base}api" "${base}"
  do
    curl -sS -X POST \
      -H "X-Tapis-Token: ${OWNER_JWT}" \
      -H "Content-Type: application/json" \
      "https://portals.tapis.io/v3/pods/${pod_id}/permissions" \
      -d "{\"user\":\"${SERVICE_ACCOUNT}\",\"level\":\"ADMIN\"}"
    echo
  done

  curl -sS -X POST \
    -H "X-Tapis-Token: ${OWNER_JWT}" \
    -H "Content-Type: application/json" \
    "https://portals.tapis.io/v3/pods/volumes/${base}volume/permissions" \
    -d "{\"user\":\"${SERVICE_ACCOUNT}\",\"level\":\"ADMIN\"}"
  echo
done
```

New bundles created through Upstream now grant `ADMIN` to the configured system account automatically when it is included in `DEFAULT_ADMIN_USERS`.

## Running a Backup

From the `tapis-postgres-backup` directory:

```bash
cd /path/to/upstream/tapis-postgres-backup
.venv/bin/python tapis_postgres_backup.py --log-level INFO
```

For a more verbose first run:

```bash
.venv/bin/python tapis_postgres_backup.py --log-level DEBUG
```

The backup job:

- lists Tapis Pods visible to the caller
- filters for Upstream Postgres pods
- runs `pg_dump` and `pg_dumpall --globals-only`
- validates the dump with `pg_restore --list`
- uploads to `tapis://ptdatax.project.PTDATAX-284/upstream-postgres/...`
- prunes to the newest 7 daily restore points per pod
- uploads an inventory summary under `/_inventory/YYYY/MM/DD/`

## Remote Backup Layout

Each pod backup is stored as:

```text
/upstream-postgres/<pod_id>/YYYY/MM/DD/<pod_id>.dump
/upstream-postgres/<pod_id>/YYYY/MM/DD/<pod_id>-globals.sql
/upstream-postgres/<pod_id>/YYYY/MM/DD/checksums.txt
/upstream-postgres/<pod_id>/YYYY/MM/DD/manifest.json
```

Nightly inventory summaries are stored as:

```text
/upstream-postgres/_inventory/YYYY/MM/DD/inventory-HHMMSS.json
```

## Running a Restore

Restore the latest-good backup into the original pod id:

```bash
cd /path/to/upstream/tapis-postgres-backup
.venv/bin/python tapis_postgres_restore.py --pod-id fluxpostgres
```

Restore a specific date into a new target pod:

```bash
.venv/bin/python tapis_postgres_restore.py \
  --pod-id fluxpostgres \
  --backup-date 2026-04-10 \
  --target-pod-id fluxrestorepostgres \
  --log-level DEBUG
```

Restore into an already-running pod:

```bash
.venv/bin/python tapis_postgres_restore.py \
  --pod-id fluxpostgres \
  --backup-date 2026-04-10 \
  --target-pod-id fluxpostgres \
  --reuse-existing-pod
```

The restore script:

- downloads the selected dump, globals file, and manifest
- reads non-secret backup metadata from the manifest
- resolves live credentials from Tapis for the relevant pod
- creates the replacement volume and pod unless `--reuse-existing-pod` is set
- waits for the target database to become reachable
- applies globals unless `--skip-globals` is used
- restores the custom-format dump with `pg_restore`
- prints the `DATABASE_URL` you should use if the API pod must be repointed

## Scheduling

Example cron entry:

```cron
15 2 * * * cd /path/to/upstream/tapis-postgres-backup && /path/to/upstream/tapis-postgres-backup/.venv/bin/python tapis_postgres_backup.py >> /var/log/upstream-postgres-backup.log 2>&1
```

## Testing

Run the local tests:

```bash
cd /path/to/upstream
tapis-postgres-backup/.venv/bin/python -m pytest tapis-postgres-backup/test_backup.py upstream-docker-pods/tests/core/test_config.py -q
```

Safe operational validation:

1. Run one real backup against non-production pods.
2. Confirm files landed under `/upstream-postgres/...`.
3. Restore one backup into a fresh test pod id.
4. Verify representative table counts and API reads.

## Security Notes

- New `manifest.json` files do not store `db_user` or `db_password`.
- Restore resolves credentials from live Tapis pod metadata instead of trusting backup metadata.
- Older backups created before this change may still contain plaintext credentials in `manifest.json`. Remove or replace those older manifests if they are still present in Tapis Files.
