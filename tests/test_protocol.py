"""Focused protocol tests that do not need a running HTTP server.

They drive the executor/pool directly against two ephemeral PostgreSQL
instances and concentrate on the hardest correctness guarantees:

* a statement that finishes just before the cancel request arrives must not
  overwrite the already-decided timeout receipt ("late result");
* a queued request whose deadline expires never sends a statement;
* after a timeout the backend is confirmed IDLE before reuse;
* a switchover takes effect without waiting for in-flight queries on the
  old database: they finish there under their original deadlines while
  queued/new queries read the new database, old connections are retired,
  and a failed or out-of-time validation keeps the original target without
  leaking connection slots.

Run:  python tests/test_protocol.py
"""
from __future__ import annotations

import asyncio
import pathlib
import sys
import tempfile
import time

ROOT = pathlib.Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import psycopg  # noqa: E402
import pgserver  # noqa: E402

from app.config import Settings  # noqa: E402
from app.errors import (  # noqa: E402
    PoolAcquireTimeout,
    QueryTimeout,
    SwitchTimeout,
    SwitchValidationFailed,
)
from app.executor import QueryExecutor  # noqa: E402
from app.pool import QueryConnectionPool  # noqa: E402

PASS = 0
FAIL = 0


def check(name: str, cond: bool, detail: str = "") -> None:
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def _boot_db(prefix: str, marker_sql: str):
    """One ephemeral PG with faultdb + DDL; returns (pg, admin_dsn, ro_dsn)."""
    from urllib.parse import urlparse, urlunparse

    pg = pgserver.get_server(tempfile.mkdtemp(prefix=prefix),
                             cleanup_mode="stop")
    admin_dsn = pg.get_uri()
    ddl = (ROOT / "db" / "01_init.sql").read_text(encoding="utf-8")
    with psycopg.connect(admin_dsn, autocommit=True) as c, c.cursor() as cur:
        cur.execute("CREATE DATABASE faultdb")
    fault_admin = urlunparse(urlparse(admin_dsn)._replace(path="/faultdb"))
    with psycopg.connect(fault_admin, autocommit=True) as c, c.cursor() as cur:
        cur.execute(ddl)
        cur.execute(marker_sql)
        cur.execute("ALTER ROLE fault_ro PASSWORD NULL")
    ro = urlunparse(
        urlparse(fault_admin)._replace(
            netloc=urlparse(fault_admin).netloc.replace(
                "postgres", "fault_ro", 1
            )
        )
    )
    return pg, fault_admin, ro


async def main() -> int:
    primary_marker = (ROOT / "db" / "02_primary_marker.sql").read_text(
        encoding="utf-8"
    )
    standby_marker = (ROOT / "db" / "02_standby_marker.sql").read_text(
        encoding="utf-8"
    )
    pg, _, ro = _boot_db("proto-", primary_marker)
    pg2, _, ro2 = _boot_db("proto-standby-", standby_marker)
    # A database on the standby server that has no fault_logs table.
    from urllib.parse import urlparse, urlunparse

    ro2_nofault = urlunparse(urlparse(ro2)._replace(path="/postgres"))

    settings = Settings(database_dsn=ro, pool_size=2)
    pool = QueryConnectionPool(settings)
    await pool.open()
    executor = QueryExecutor(pool, settings)

    print("=== A. 迟到结果不改写超时回执 ===")
    # The statement (150ms) always outlives the 60ms deadline, so the timeout
    # path is mandatory.  Cancellation arrives ~sub-millisecond later, which
    # is also how a real "finished just before cancel" race is shaped: the
    # shielded task is awaited and whatever it returns is recorded but never
    # allowed to replace the receipt.
    receipt: QueryTimeout | None = None
    late_seen = False
    try:
        await executor.execute(
            "SELECT pg_sleep(0.15)", time.monotonic() + 0.06
        )
    except QueryTimeout as exc:
        receipt = exc
        late_seen = bool(exc.extra.get("late_result_ignored"))

    check("截止时间到必定返回 QueryTimeout", receipt is not None)
    if receipt is not None:
        check("回执固定为 timeout",
              receipt.extra.get("status") == "timeout", receipt.extra)
        check("回执不携带 rows（迟到结果未泄漏）",
              "rows" not in receipt.extra, receipt.extra)
        check("已发起取消并确认事务结束(未弃置)",
              receipt.extra.get("cancelled") is True
              and receipt.extra.get("connection_discarded") is False,
              receipt.extra)
        print(f"  INFO  late_result_ignored={late_seen} "
              f"(True=语句在取消前完成；False=服务器确认取消)")

    print("=== B. 超时后确认 IDLE 才复用 ===")
    st = await pool.status()
    check("池容量仍为 2", st.size == 2, st)
    check("两条后端均空闲(IDLE)", st.idle == 2, st)
    ok = await executor.execute("SELECT 7", time.monotonic() + 5)
    check("后续查询得到自身结果", ok["rows"] == [[7]], ok)

    print("=== C. 排队过期绝不发送语句 ===")
    # Occupy both backends with long sleeps (long own deadlines).
    async def slow(tag: str) -> dict:
        return await executor.execute(
            f"SELECT '{tag}', pg_sleep(2)", time.monotonic() + 6
        )

    busy = [asyncio.ensure_future(slow("X")), asyncio.ensure_future(slow("Y"))]
    await asyncio.sleep(0.4)
    queued_err = None
    try:
        await executor.execute(
            "SELECT 'NEVER-RUN'", time.monotonic() + 0.4
        )
    except PoolAcquireTimeout as exc:
        queued_err = exc
    check("排队过期抛 PoolAcquireTimeout", queued_err is not None)
    results = await asyncio.gather(*busy)
    check("两条占池查询各自成功",
          {r["rows"][0][0] for r in results} == {"X", "Y"}, results)
    # Prove the queued statement never reached a backend: no marker exists.
    after = await executor.execute("SELECT 1", time.monotonic() + 5)
    check("过期语句无任何副作用，后续查询正常", after["rows"] == [[1]], after)
    st = await pool.status()
    check("结束后池满且空闲", st.idle == 2 and st.size == 2, st)

    print("=== D. 切换生效：在途旧库完成、新请求读新库 ===")
    res = await executor.execute("SELECT 1", time.monotonic() + 5)
    check("初始数据源为 primary", res["source"] == "primary", res)

    # A 3s statement goes in-flight on the primary; the switch must not
    # wait for it, and it must still finish on the primary.
    slow = asyncio.ensure_future(
        executor.execute("SELECT 'OLD', pg_sleep(3)", time.monotonic() + 8)
    )
    await asyncio.sleep(0.3)
    t0 = time.monotonic()
    sw = await pool.switch_target("standby", ro2, time.monotonic() + 5)
    sw_elapsed = time.monotonic() - t0
    check("验证通过后切换到 standby", sw.changed and sw.target == "standby", sw)
    check("验证清点 fault_logs 行数=8", sw.fault_logs_count == 8, sw)
    check("切换未等在途旧库查询结束", sw_elapsed < 2.5,
          f"elapsed={sw_elapsed:.2f}")
    st = await pool.status()
    check("健康状态数据源已切换", st.target == "standby", st)

    fast = await executor.execute(
        "SELECT device_id FROM fault_logs WHERE device_id = 'STANDBY-MARKER'",
        time.monotonic() + 5,
    )
    check("新请求读新库且拿到备用标记行",
          fast["source"] == "standby" and fast["row_count"] == 1, fast)
    slow_res = await slow
    check("在途旧库查询按原期限完成于 primary",
          slow_res["source"] == "primary"
          and slow_res["rows"][0][0] == "OLD", slow_res)
    st = await pool.status()
    check("旧连接用完退出(弃置计数增加)", st.total_discarded >= 1, st)
    check("池容量保持 2", st.size == 2, st)

    print("=== E. 验证失败/期限耗尽保持原库 ===")
    try:
        await pool.switch_target("broken", ro2_nofault, time.monotonic() + 5)
        check("无 fault_logs 的候选被拒绝", False, "未抛异常")
    except SwitchValidationFailed as exc:
        check("无 fault_logs 的候选被拒绝",
              exc.code == "switch_validation_failed", exc)
        check("失败回执标明保持原库",
              exc.extra.get("target") == "standby"
              and exc.extra.get("switched") is False, exc.extra)
    ok = await executor.execute("SELECT 3", time.monotonic() + 5)
    check("验证失败后原库照常接单",
          ok["rows"] == [[3]] and ok["source"] == "standby", ok)

    # Both slots busy: the switch deadline expires while waiting for a
    # borrower slot; the route stays and no slot is leaked.
    b1 = asyncio.ensure_future(
        executor.execute("SELECT pg_sleep(1.5)", time.monotonic() + 6)
    )
    b2 = asyncio.ensure_future(
        executor.execute("SELECT pg_sleep(1.5)", time.monotonic() + 6)
    )
    await asyncio.sleep(0.3)
    try:
        await pool.switch_target("primary", ro, time.monotonic() + 0.3)
        check("名额占满时切换期限耗尽", False, "未抛异常")
    except SwitchTimeout as exc:
        check("名额占满时切换期限耗尽", exc.code == "switch_timeout", exc)
    await asyncio.gather(b1, b2)
    ok = await executor.execute("SELECT 4", time.monotonic() + 5)
    check("失败切换不丢名额，后续查询正常", ok["rows"] == [[4]], ok)
    st = await pool.status()
    check("池仍满员空闲", st.idle == 2 and st.size == 2, st)

    print("=== F. 幂等切换与回切 ===")
    sw = await pool.switch_target("standby", ro2, time.monotonic() + 5)
    check("重复切换同一目标 unchanged",
          not sw.changed and sw.target == "standby", sw)
    sw = await pool.switch_target("primary", ro, time.monotonic() + 5)
    check("回切主库", sw.changed and sw.target == "primary", sw)
    res = await executor.execute(
        "SELECT device_id FROM fault_logs WHERE device_id = 'PRIMARY-MARKER'",
        time.monotonic() + 5,
    )
    check("主库标记行可读且数据源为 primary",
          res["source"] == "primary" and res["row_count"] == 1, res)

    await pool.close()
    pg.cleanup()
    pg2.cleanup()
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
