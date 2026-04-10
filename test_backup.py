from datetime import UTC, date, datetime
from pathlib import Path
import sys

TOOLS_ROOT = Path(__file__).resolve().parent
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

from backup import (
    POSTGRES_DATA_MOUNT,
    UPSTREAM_POSTGRES_DESCRIPTION,
    build_backup_remote_dir,
    build_inventory_remote_path,
    discover_upstream_postgres_pods,
    parse_backup_date_from_path,
    PostgresPodTarget,
    scrub_target_for_manifest,
    select_retention_prune_candidates,
)


def test_discover_upstream_postgres_pods_filters_and_extracts_target():
    pods = [
        {
            "pod_id": "weatherpostgres",
            "description": UPSTREAM_POSTGRES_DESCRIPTION,
            "volume_mounts": {
                POSTGRES_DATA_MOUNT: {
                    "type": "tapisvolume",
                    "source_id": "weathervolume",
                }
            },
            "environment_variables": {
                "POSTGRES_USER": "weather",
                "POSTGRES_PASSWORD": "secret",
                "POSTGRES_DB": "weather",
            },
            "networking": {
                "default": {
                    "url": "weatherpostgres.pods.tacc.tapis.io",
                    "port": 5432,
                }
            },
        },
        {
            "pod_id": "weatherapi",
            "volume_mounts": {},
            "environment_variables": {},
        },
        {
            "pod_id": "otherpostgres",
            "description": "not upstream",
            "volume_mounts": {
                POSTGRES_DATA_MOUNT: {
                    "type": "tapisvolume",
                    "source_id": "othervolume",
                }
            },
            "environment_variables": {
                "POSTGRES_USER": "other",
                "POSTGRES_PASSWORD": "secret",
                "POSTGRES_DB": "other",
            },
        },
    ]

    targets = discover_upstream_postgres_pods(pods)

    assert len(targets) == 1
    target = targets[0]
    assert target.pod_id == "weatherpostgres"
    assert target.volume_id == "weathervolume"
    assert target.host == "weatherpostgres.pods.tacc.tapis.io"
    assert target.port == 443
    assert target.db_name == "weather"


def test_build_backup_remote_dir_uses_stable_date_layout():
    path = build_backup_remote_dir(
        root_path="/upstream-postgres",
        pod_id="weatherpostgres",
        backup_day=date(2026, 4, 10),
    )
    assert path == "/upstream-postgres/weatherpostgres/2026/04/10"


def test_build_inventory_remote_path_uses_inventory_namespace():
    path = build_inventory_remote_path(
        root_path="/upstream-postgres",
        backup_time=datetime(2026, 4, 10, 17, 30, 0, tzinfo=UTC),
    )
    assert path == "/upstream-postgres/_inventory/2026/04/10/inventory-173000.json"


def test_parse_backup_date_from_path():
    assert parse_backup_date_from_path("/upstream-postgres/weatherpostgres/2026/04/10") == date(2026, 4, 10)
    assert parse_backup_date_from_path("/upstream-postgres/weatherpostgres/latest") is None


def test_select_retention_prune_candidates_keeps_newest_days():
    paths = [
        f"/upstream-postgres/weatherpostgres/2026/04/{day:02d}"
        for day in range(1, 9)
    ]

    prune = select_retention_prune_candidates(paths, keep=7)

    assert prune == ["/upstream-postgres/weatherpostgres/2026/04/01"]


def test_scrub_target_for_manifest_removes_credentials():
    target = PostgresPodTarget(
        pod_id="weatherpostgres",
        host="weatherpostgres.pods.tacc.tapis.io",
        port=443,
        db_name="weather",
        db_user="weather_user",
        db_password="super-secret",
        volume_id="weathervolume",
        description=UPSTREAM_POSTGRES_DESCRIPTION,
    )

    manifest_pod = scrub_target_for_manifest(target)

    assert manifest_pod["pod_id"] == "weatherpostgres"
    assert manifest_pod["db_name"] == "weather"
    assert "db_user" not in manifest_pod
    assert "db_password" not in manifest_pod
