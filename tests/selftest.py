"""End-to-end self-test for the read-only query API.

Runs entirely on the Python standard library so it works inside the slim
application container as well as on a developer machine:

    python -m tests.selftest

Scenarios:
  1. health + normal SELECT returns the analyst's own rows
  2. read-only guard rails (multi-statement, INSERT, non-SELECT rejected)
  3. single slow query times out, is cancelled, receipt is final
  4. a queued request whose budget expires while waiting never executes
  5. after two concurrent slow queries, later requests still get their own
     results (connection reuse after confirmed rollback)
  6. pool capacity really is two (observed via pg_stat_activity)
  7. switchover to the standby and back, responses name the data source
  8. failed validation / unknown candidate / exhausted switch deadline all
     keep the original database and cost no connection slots
  9. switchover interleaved with in-flight and queued queries: old queries
     finish on the old database, queued/new queries read the new one, and
     physical connections across both databases never exceed two
"""
from __future__ import annotations

import json
import os
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request

BASE = "http://127.0.0.1:8000"

PASS = 0
FAIL = 0


def check(name: str, condition: bool, detail: str = "") -> None:
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name}  {detail}")


def post(path: str, payload: dict, timeout: float = 30.0):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        BASE + path,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())
    except urllib.error.URLError as exc:
        return -1, {"error": {"code": "connection_failed", "message": str(exc)}}


def get(path: str, timeout: float = 10.0):
    try:
        with urllib.request.urlopen(BASE + path, timeout=timeout) as resp:
            return resp.status, json.loads(resp.read().decode())
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode())
    except urllib.error.URLError as exc:
        return -1, {"error": {"code": "connection_failed", "message": str(exc)}}


def admin_dsn(env_name: str, file_name: str) -> str | None:
    """Admin DSN for chaos/verification, from env or the local_run file."""
    dsn = os.environ.get(env_name)
    if not dsn:
        path = os.path.join(tempfile.gettempdir(), file_name)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                dsn = fh.read().strip()
    return dsn or None


def run_case(title: str, fn) -> None:
    print(f"\n=== {title} ===")
    try:
        fn()
    except Exception as exc:  # noqa: BLE001 - test harness must keep going
        check(f"{title} 未抛异常", False, repr(exc))


STANDBY_OK = True  # set False by case 7 when STANDBY_DSN is not configured


def ensure_primary() -> None:
    """Idempotent: leave the shared entry point on the primary database."""
    status, health = get("/health")
    if status == 200 and health["pool"].get("target") != "primary":
        post("/switchover", {"target": "primary", "timeout_ms": 8000})


def case_health_and_basic() -> None:
    ensure_primary()
    status, body = get("/health")
    check("健康检查 200", status == 200, body)
    check("连接池容量为 2", body["pool"]["size"] == 2, body)
    check("启动即建满两条连接", body["pool"]["idle"] == 2, body)
    check("健康回执标明当前数据源", body["pool"]["target"] == "primary", body)

    status, body = post(
        "/query",
        {"sql": "SELECT count(*) AS n FROM fault_logs", "timeout_ms": 5000},
    )
    check("普通 SELECT 200", status == 200, body)
    check("返回 ok 且已执行", body["status"] == "ok" and body["executed"], body)
    check("预置 7 条故障日志 + 1 行主库标记", body["rows"][0][0] == 8, body)
    check("成功响应标明实际数据源",
          body.get("source") == "primary", body)

    status, body = post(
        "/query",
        {
            "sql": "SELECT device_id, severity FROM fault_logs "
                   "WHERE severity = 'CRITICAL' ORDER BY id",
            "timeout_ms": 5000,
        },
    )
    check("过滤查询返回自身结果", status == 200 and body["row_count"] == 2, body)
    check(
        "结果内容正确",
        [r[0] for r in body["rows"]] == ["SW-CORE-01", "SRV-DB-07"],
        body,
    )


def case_read_only_guards() -> None:
    for name, sql in [
        ("多语句", "SELECT 1; SELECT 2"),
        ("INSERT", "INSERT INTO fault_logs (device_id) VALUES ('x')"),
        ("UPDATE", "UPDATE fault_logs SET site = 'z'"),
        ("非 SELECT", "SHOW transaction_read_only"),
        ("SELECT INTO", "SELECT 1 INTO TEMP t"),
    ]:
        status, body = post("/query", {"sql": sql, "timeout_ms": 5000})
        check(f"{name} 被拒绝(400)", status == 400, body)
        check(f"{name} 错误码 invalid_sql",
              body.get("error", {}).get("code") == "invalid_sql", body)

    # pg_sleep 是允许的只读函数，用于慢查询演示。
    status, body = post(
        "/query", {"sql": "SELECT pg_sleep(0.1)", "timeout_ms": 5000}
    )
    check("pg_sleep 只读函数允许执行", status == 200 and body["status"] == "ok", body)


def case_single_timeout() -> None:
    _, before = get("/health")
    discarded_before = before["pool"]["total_discarded"]
    t0 = time.monotonic()
    status, body = post(
        "/query",
        {"sql": "SELECT pg_sleep(3)", "timeout_ms": 800},
        timeout=15,
    )
    elapsed = time.monotonic() - t0
    check("超时返回 504", status == 504, body)
    check("错误码 query_timeout",
          body.get("error", {}).get("code") == "query_timeout", body)
    check("回执 status=timeout", body.get("status") == "timeout", body)
    check("语句确实已发送执行", body.get("executed") is True, body)
    check("已发起取消", body.get("cancelled") is True, body)
    check("及时返回 (<2.5s)", elapsed < 2.5, f"elapsed={elapsed:.2f}")

    # Give the server a moment, then prove the slow statement is gone and
    # the connection was reused for a fresh, independent query.
    time.sleep(0.5)
    status, body = post(
        "/query", {"sql": "SELECT 42 AS answer", "timeout_ms": 5000}
    )
    check("超时后连接可复用且返回自身结果",
          status == 200 and body["rows"] == [[42]], body)

    status, health = get("/health")
    check("池容量仍为 2", health["pool"]["size"] == 2, health)
    check("确认事务结束后复用，本用例无新增弃置",
          health["pool"]["total_discarded"] == discarded_before, health)


def case_queued_expiry() -> None:
    results: list[tuple] = []

    def slow(marker: str, sleep_s: float, budget_ms: int) -> None:
        t0 = time.monotonic()
        status, body = post(
            "/query",
            {"sql": f"SELECT '{marker}', pg_sleep({sleep_s})",
             "timeout_ms": budget_ms},
            timeout=20,
        )
        results.append((marker, status, body, round(time.monotonic() - t0, 2)))

    # Two slow queries occupy both connections for ~2.5s.
    t1 = threading.Thread(target=slow, args=("A", 2.5, 5000))
    t2 = threading.Thread(target=slow, args=("B", 2.5, 5000))
    t1.start()
    time.sleep(0.3)
    t2.start()
    time.sleep(0.3)

    # Third request: only 500ms total budget, both backends are busy.
    t0 = time.monotonic()
    status, body = post(
        "/query",
        {"sql": "SELECT 'SHOULD-NOT-RUN', pg_sleep(0)", "timeout_ms": 500},
        timeout=10,
    )
    waited = time.monotonic() - t0
    check("排队过期返回 503", status == 503, body)
    check("错误码 acquire_timeout",
          body.get("error", {}).get("code") == "acquire_timeout", body)
    check("排队等待本身计入预算 (~0.5s)", 0.4 < waited < 1.5,
          f"waited={waited:.2f}")

    t1.join()
    t2.join()
    ok = [r for r in results if r[1] == 200 and r[2]["status"] == "ok"]
    check("两条占池慢查询最终各自成功", len(ok) == 2, results)
    check(
        "两条慢查询拿到各自的标记结果",
        {r[2]["rows"][0][0] for r in ok} == {"A", "B"},
        results,
    )

    # After the backlog drains, pool must be fully reusable.
    status, body = post(
        "/query", {"sql": "SELECT 'AFTER-BACKLOG'", "timeout_ms": 5000}
    )
    check("积压清空后新查询正常",
          status == 200 and body["rows"][0][0] == "AFTER-BACKLOG", body)


def case_capacity_and_reuse() -> None:
    barrier = threading.Barrier(3)
    finished: list = []

    def worker(i: int) -> None:
        barrier.wait()
        status, body = post(
            "/query",
            {"sql": f"SELECT {i}, pg_sleep(1.2)", "timeout_ms": 8000},
            timeout=15,
        )
        finished.append((i, status, body))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(3)]
    _, before = get("/health")
    discarded_before = before["pool"]["total_discarded"]
    for t in threads:
        t.start()
    t0 = time.monotonic()
    for t in threads:
        t.join()
    wall = time.monotonic() - t0

    check("三个并发请求全部成功",
          len(finished) == 3 and all(s == 200 for _, s, _ in finished),
          finished)
    check(
        "每个请求拿到自身结果(无串结果)",
        {b["rows"][0][0] for _, _, b in finished} == {0, 1, 2},
        finished,
    )
    # Capacity 2 -> three 1.2s sleeps must take > 1.8s (two waves).
    check("容量 2 导致第三个请求排队 (>1.8s)", wall > 1.8, f"wall={wall:.2f}")

    status, health = get("/health")
    check("结束后两条连接全部空闲", health["pool"]["idle"] == 2, health)
    check("容量保持 2、本用例无新增弃置",
          health["pool"]["size"] == 2
          and health["pool"]["total_discarded"] == discarded_before, health)


def case_discard_and_rebuild() -> None:
    """Kill a pooled backend out-of-band; the API must discard and rebuild."""
    admin = admin_dsn("ADMIN_DSN", "faultdb_admin_dsn")
    if not admin:
        print("  SKIP  弃置补建混沌测试（未设置 ADMIN_DSN）")
        return
    try:
        import psycopg
    except ImportError:
        print("  SKIP  弃置补建混沌测试（无 psycopg）")
        return

    with psycopg.connect(admin) as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT pid FROM pg_stat_activity "
            "WHERE usename = 'fault_ro' AND pid <> pg_backend_pid()"
        )
        pids = [r[0] for r in cur.fetchall()]
        check("池内存在两条 fault_ro 后端", len(pids) == 2, pids)
        victim = pids[0]
        cur.execute("SELECT pg_terminate_backend(%s)", (victim,))
        check("管理连接成功终止一个后端", cur.fetchone()[0] is True)

    # Run several sequential queries so both pooled backends are cycled;
    # whichever request draws the dead backend must transparently discard,
    # rebuild and retry — every client call still succeeds with its own rows.
    statuses, markers = [], []
    for i in range(4):
        status, body = post(
            "/query",
            {"sql": f"SELECT 'CYCLE-{i}'", "timeout_ms": 8000},
        )
        statuses.append(status)
        if status == 200:
            markers.append(body["rows"][0][0])
    check("循环借出期间所有请求均成功(客户端无感)",
          statuses == [200] * 4, statuses)
    check("每次都返回自身结果",
          markers == [f"CYCLE-{i}" for i in range(4)], markers)

    status, health = get("/health")
    check("弃置计数增加",
          health["pool"]["total_discarded"] >= 1, health)
    check("补建后容量恢复为 2 且全空闲",
          health["pool"]["size"] == 2 and health["pool"]["idle"] == 2, health)

    # Physical capacity check: while two slow queries run, the database must
    # see at most two fault_ro backends — no third connection is ever opened.
    observed: list[int] = []
    barrier = threading.Barrier(2)

    def slow(i: int) -> None:
        barrier.wait()
        post("/query",
             {"sql": f"SELECT {i}, pg_sleep(2)", "timeout_ms": 8000},
             timeout=15)

    threads = [threading.Thread(target=slow, args=(i,)) for i in range(2)]
    for t in threads:
        t.start()
    deadline = time.monotonic() + 3
    with psycopg.connect(admin) as conn, conn.cursor() as cur:
        while time.monotonic() < deadline:
            cur.execute(
                "SELECT count(*) FROM pg_stat_activity WHERE usename='fault_ro'"
            )
            observed.append(cur.fetchone()[0])
            time.sleep(0.1)
    for t in threads:
        t.join()
    check("数据库侧 fault_ro 后端数从未超过 2",
          observed and max(observed) <= 2, f"max={max(observed)}")


def case_switchover_happy_path() -> None:
    """Switch to the standby and back; responses name the data source."""
    global STANDBY_OK
    ensure_primary()
    status, body = post(
        "/switchover", {"target": "standby", "timeout_ms": 5000}
    )
    if status == 400 and body.get("error", {}).get("code") == "unknown_target":
        print("  SKIP  未配置备用库（STANDBY_DSN），跳过切换用例")
        STANDBY_OK = False
        return
    check("切换到备用库 200", status == 200, body)
    check("回执 status=switched", body.get("status") == "switched", body)
    check(
        "回执 target=standby previous=primary",
        body.get("target") == "standby" and body.get("previous") == "primary",
        body,
    )
    check("验证清点 fault_logs 行数=8", body.get("fault_logs_count") == 8, body)

    status, health = get("/health")
    check("健康回执数据源变为 standby",
          health["pool"]["target"] == "standby", health)

    status, body = post(
        "/query",
        {"sql": "SELECT message FROM fault_logs "
                "WHERE device_id = 'STANDBY-MARKER'", "timeout_ms": 5000},
    )
    check("备用库标记行可读(两库日志可区分)",
          status == 200 and body["row_count"] == 1, body)
    check("响应标明数据源 standby", body.get("source") == "standby", body)

    status, body = post(
        "/query",
        {"sql": "SELECT count(*) FROM fault_logs "
                "WHERE device_id = 'PRIMARY-MARKER'", "timeout_ms": 5000},
    )
    check("备用库上看不到主库标记行",
          status == 200 and body["rows"] == [[0]], body)

    status, body = post(
        "/switchover", {"target": "standby", "timeout_ms": 5000}
    )
    check("重复切换同一目标返回 unchanged",
          status == 200 and body.get("status") == "unchanged", body)

    status, body = post(
        "/switchover", {"target": "primary", "timeout_ms": 5000}
    )
    check("切回主库",
          status == 200 and body.get("status") == "switched"
          and body.get("target") == "primary", body)
    status, body = post(
        "/query",
        {"sql": "SELECT message FROM fault_logs "
                "WHERE device_id = 'PRIMARY-MARKER'", "timeout_ms": 5000},
    )
    check("主库标记行可读且数据源为 primary",
          status == 200 and body["row_count"] == 1
          and body.get("source") == "primary", body)


def case_switch_failures_keep_primary() -> None:
    """Failed switches keep the original database and cost no slots."""
    if not STANDBY_OK:
        print("  SKIP  未配置备用库")
        return
    ensure_primary()

    status, body = post(
        "/switchover", {"target": "no-such-db", "timeout_ms": 1000}
    )
    check("未知候选库 400", status == 400, body)
    check("错误码 unknown_target",
          body.get("error", {}).get("code") == "unknown_target", body)

    standby_admin = admin_dsn("STANDBY_ADMIN_DSN", "faultdb_standby_admin_dsn")
    if not standby_admin:
        print("  SKIP  候选库验证失败用例（无备用库管理连接）")
    else:
        import psycopg

        # Make the candidate fail validation: revoke its fault_logs SELECT.
        with psycopg.connect(standby_admin, autocommit=True) as conn, \
                conn.cursor() as cur:
            cur.execute("REVOKE SELECT ON fault_logs FROM fault_ro")
        try:
            status, body = post(
                "/switchover", {"target": "standby", "timeout_ms": 5000}
            )
            check("候选库验证失败 502", status == 502, body)
            check("错误码 switch_validation_failed",
                  body.get("error", {}).get("code")
                  == "switch_validation_failed", body)
            check("回执标明保持原库",
                  body.get("switched") is False
                  and body.get("target") == "primary", body)
            status, q = post(
                "/query", {"sql": "SELECT 'STILL-PRIMARY'", "timeout_ms": 5000}
            )
            check("验证失败后原库照常接单",
                  status == 200 and q.get("source") == "primary", q)
        finally:
            with psycopg.connect(standby_admin, autocommit=True) as conn, \
                    conn.cursor() as cur:
                cur.execute("GRANT SELECT ON fault_logs TO fault_ro")

        status, body = post(
            "/switchover", {"target": "standby", "timeout_ms": 5000}
        )
        check("恢复权限后切换成功",
              status == 200 and body.get("status") == "switched", body)
        status, body = post(
            "/switchover", {"target": "primary", "timeout_ms": 5000}
        )
        check("切回主库", status == 200 and body.get("target") == "primary",
              body)

    # Exhaust the switch deadline while both slots are busy: the switch
    # waits for a borrower slot inside its own total budget, then fails
    # without touching the route or leaking a slot.
    results: list[tuple] = []

    def slow(tag: str) -> None:
        results.append((tag, *post(
            "/query",
            {"sql": f"SELECT '{tag}', pg_sleep(2.5)", "timeout_ms": 8000},
            timeout=20,
        )))

    t1 = threading.Thread(target=slow, args=("S1",))
    t2 = threading.Thread(target=slow, args=("S2",))
    t1.start()
    t2.start()
    time.sleep(0.4)  # both slots now busy for ~2.5s
    t0 = time.monotonic()
    status, body = post("/switchover", {"target": "standby", "timeout_ms": 400})
    waited = time.monotonic() - t0
    check("名额占满时切换期限耗尽 504", status == 504, body)
    check("错误码 switch_timeout",
          body.get("error", {}).get("code") == "switch_timeout", body)
    check("等待名额计入切换期限 (~0.4s)", 0.3 < waited < 2.0,
          f"waited={waited:.2f}")
    check("期限耗尽仍保持原库", body.get("target") == "primary", body)
    t1.join()
    t2.join()
    check("两条占池慢查询仍各自成功",
          len(results) == 2 and all(s == 200 for _, s, _ in results), results)
    status, q = post(
        "/query", {"sql": "SELECT 'AFTER-SWITCH-TIMEOUT'", "timeout_ms": 5000}
    )
    check("失败切换不丢连接名额，后续查询正常",
          status == 200 and q["rows"][0][0] == "AFTER-SWITCH-TIMEOUT", q)
    status, health = get("/health")
    check("池容量与空闲数恢复",
          health["pool"]["size"] == 2 and health["pool"]["idle"] == 2, health)


def case_switch_interleaved_queries() -> None:
    """Switch while old queries run and new ones queue behind the switch."""
    if not STANDBY_OK:
        print("  SKIP  未配置备用库")
        return
    ensure_primary()

    primary_admin = admin_dsn("ADMIN_DSN", "faultdb_admin_dsn")
    standby_admin = admin_dsn("STANDBY_ADMIN_DSN", "faultdb_standby_admin_dsn")
    can_observe = bool(primary_admin and standby_admin)
    if can_observe:
        try:
            import psycopg  # noqa: F401
        except ImportError:
            can_observe = False
    if not can_observe:
        print("  INFO  无管理连接，跳过两库物理连接数观测，仅验证路由语义")

    observed: list[int] = []
    stop = threading.Event()

    def poll() -> None:
        import psycopg

        try:
            with psycopg.connect(primary_admin) as p, \
                    psycopg.connect(standby_admin) as s:
                while not stop.is_set():
                    with p.cursor() as cur:
                        cur.execute(
                            "SELECT count(*) FROM pg_stat_activity "
                            "WHERE usename = 'fault_ro'"
                        )
                        pc = cur.fetchone()[0]
                    with s.cursor() as cur:
                        cur.execute(
                            "SELECT count(*) FROM pg_stat_activity "
                            "WHERE usename = 'fault_ro'"
                        )
                        sc = cur.fetchone()[0]
                    observed.append(pc + sc)
                    time.sleep(0.05)
        except Exception as exc:  # noqa: BLE001 - observer must not crash
            print(f"  INFO  物理连接观测中止: {exc!r}")

    poll_thread = None
    if can_observe:
        poll_thread = threading.Thread(target=poll)
        poll_thread.start()

    _, before = get("/health")
    discarded_before = before["pool"]["total_discarded"]

    outcomes: dict = {}

    def run_query(tag: str, sleep_s: float, budget_ms: int) -> None:
        outcomes[tag] = post(
            "/query",
            {"sql": f"SELECT '{tag}', pg_sleep({sleep_s})",
             "timeout_ms": budget_ms},
            timeout=25,
        )
        outcomes[tag + "_done"] = time.monotonic()

    def run_switch() -> None:
        outcomes["switch"] = post(
            "/switchover", {"target": "standby", "timeout_ms": 6000}
        )
        outcomes["switch_done"] = time.monotonic()

    # A1 (2.5s) and A2 (4s) occupy both slots on the primary; the switch
    # queues for the first freed slot; Q queues behind the switch and must
    # land on the new target once the flip is effective.
    t_a1 = threading.Thread(target=run_query, args=("A1", 2.5, 9000))
    t_a2 = threading.Thread(target=run_query, args=("A2", 4.0, 9000))
    t_a1.start()
    t_a2.start()
    time.sleep(0.4)  # both in flight on the primary
    t_sw = threading.Thread(target=run_switch)
    t_sw.start()
    time.sleep(0.3)  # switch registered as slot waiter before Q
    t_q = threading.Thread(target=run_query, args=("Q", 0.0, 9000))
    t_q.start()
    for t in (t_a1, t_a2, t_sw, t_q):
        t.join()
    stop.set()
    if poll_thread is not None:
        poll_thread.join()

    st, sw = outcomes["switch"]
    check("交错中切换在总期限内完成",
          st == 200 and sw.get("status") == "switched", outcomes["switch"])
    check("切换生效未等在途旧库查询(A2)结束",
          outcomes["switch_done"] < outcomes["A2_done"],
          f"switch_done={outcomes['switch_done']:.2f} "
          f"A2_done={outcomes['A2_done']:.2f}")

    st, a1 = outcomes["A1"]
    check("在途旧库查询 A1 按原期限完成于主库",
          st == 200 and a1.get("source") == "primary"
          and a1["rows"][0][0] == "A1", outcomes["A1"])
    st, a2 = outcomes["A2"]
    check("在途旧库查询 A2 按原期限完成于主库",
          st == 200 and a2.get("source") == "primary"
          and a2["rows"][0][0] == "A2", outcomes["A2"])
    st, q = outcomes["Q"]
    check("切换生效后排队请求读新库",
          st == 200 and q.get("source") == "standby", outcomes["Q"])
    check("排队请求结果未被串改", q["rows"][0][0] == "Q", q)

    st, n = post(
        "/query",
        {"sql": "SELECT device_id FROM fault_logs "
                "WHERE device_id = 'STANDBY-MARKER'", "timeout_ms": 5000},
    )
    check("新请求读新库(备用标记行)",
          st == 200 and n.get("source") == "standby"
          and n["row_count"] == 1, n)

    if can_observe and observed:
        check("两库物理连接合计从未超过 2",
              max(observed) <= 2, f"max={max(observed)}")
    _, after = get("/health")
    check("旧连接用完退出(弃置计数增加)",
          after["pool"]["total_discarded"] >= discarded_before + 1, after)
    check("切换后池恢复满员空闲且数据源为 standby",
          after["pool"]["idle"] == 2
          and after["pool"]["target"] == "standby", after)

    status, back = post("/switchover", {"target": "primary", "timeout_ms": 5000})
    check("用例结束切回主库",
          status == 200 and back.get("target") == "primary", back)


def main() -> int:
    print(f"self-test against {BASE}")
    run_case("1. 健康检查与普通查询", case_health_and_basic)
    run_case("2. 只读约束", case_read_only_guards)
    run_case("3. 单条慢查询超时与取消", case_single_timeout)
    run_case("4. 排队期间过期不执行", case_queued_expiry)
    run_case("5. 容量为 2 与接续查询", case_capacity_and_reuse)
    run_case("6. 弃置并补建 / 物理容量", case_discard_and_rebuild)
    run_case("7. 切换备用库与回切", case_switchover_happy_path)
    run_case("8. 切换失败保持原库", case_switch_failures_keep_primary)
    run_case("9. 切换与新旧查询交错", case_switch_interleaved_queries)
    print(f"\n结果: {PASS} 通过, {FAIL} 失败")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
