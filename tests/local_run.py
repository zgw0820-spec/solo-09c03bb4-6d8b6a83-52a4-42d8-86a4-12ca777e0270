#!/usr/bin/env python3
"""Local-only launcher: run the full stack without Docker.

Boots two ephemeral PostgreSQL 16 instances (primary + standby) via the
``pgserver`` PyPI package, applies the same schema/role DDL as
db/01_init.sql plus the per-database marker rows, then starts uvicorn
with the two-connection pool.  Docker Compose remains the canonical
deployment; this script exists so the service can be exercised on
machines without a container runtime.

    python tests/local_run.py
"""
from __future__ import annotations

import os
import pathlib
import sys
import tempfile
import time
from urllib.parse import urlencode, urlparse, urlunparse, parse_qsl

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import pgserver  # noqa: E402
import psycopg  # noqa: E402
import uvicorn  # noqa: E402

DDL = (ROOT / "db" / "01_init.sql").read_text(encoding="utf-8")
PRIMARY_MARKER = (ROOT / "db" / "02_primary_marker.sql").read_text(
    encoding="utf-8"
)
STANDBY_MARKER = (ROOT / "db" / "02_standby_marker.sql").read_text(
    encoding="utf-8"
)


def _with_dbname(dsn: str, dbname: str) -> str:
    """Replace only the database path component of a libpq URL."""
    parts = urlparse(dsn)
    return urlunparse(
        parts._replace(
            path=f"/{dbname}",
            query=urlencode(parse_qsl(parts.query)),
        )
    )


def _boot_database(name: str, marker_sql: str):
    """Start one ephemeral PG, load faultdb + DDL, return (pg, admin, ro)."""
    data_dir = tempfile.mkdtemp(prefix=f"faultdb-{name}-")
    print(f"[local_run] starting ephemeral PostgreSQL ({name}) in {data_dir}")
    pg = pgserver.get_server(data_dir, cleanup_mode="stop")
    admin_dsn = pg.get_uri()
    print(f"[local_run] {name} postgres up: {admin_dsn}")

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute("CREATE DATABASE faultdb")

    fault_admin_dsn = _with_dbname(admin_dsn, "faultdb")
    with psycopg.connect(fault_admin_dsn, autocommit=True) as conn:
        with conn.cursor() as cur:
            cur.execute(DDL)
            cur.execute(marker_sql)
            # pgserver authenticates over a unix socket; allow the
            # read-only role to log in without a password locally.
            cur.execute("ALTER ROLE fault_ro PASSWORD NULL")
    print(f"[local_run] {name}: schema, marker row and fault_ro role ready")

    ro_parts = urlparse(fault_admin_dsn)
    ro_dsn = urlunparse(
        ro_parts._replace(
            netloc=ro_parts.netloc.replace(
                ro_parts.username or "postgres", "fault_ro", 1
            )
        )
    )
    print(f"[local_run] {name} read-only DSN={ro_dsn}")
    return pg, fault_admin_dsn, ro_dsn


def main() -> None:
    pg_primary, primary_admin, primary_ro = _boot_database(
        "primary", PRIMARY_MARKER
    )
    pg_standby, standby_admin, standby_ro = _boot_database(
        "standby", STANDBY_MARKER
    )

    os.environ["DATABASE_DSN"] = primary_ro
    os.environ["STANDBY_DSN"] = standby_ro
    os.environ["POOL_SIZE"] = "2"
    # Exposed for tests/selftest.py's chaos and switchover scenarios.
    tmp = pathlib.Path(tempfile.gettempdir())
    (tmp / "faultdb_admin_dsn").write_text(primary_admin, encoding="utf-8")
    (tmp / "faultdb_standby_admin_dsn").write_text(
        standby_admin, encoding="utf-8"
    )
    print(f"[local_run] admin DSNs for self-test written to {tmp}")

    print("[local_run] uvicorn on http://127.0.0.1:8000 (Swagger: /docs)")
    try:
        uvicorn.run(
            "app.main:app",
            host="127.0.0.1",
            port=8000,
            log_level="info",
        )
    finally:
        time.sleep(0.2)
        pg_primary.cleanup()
        pg_standby.cleanup()


if __name__ == "__main__":
    main()
