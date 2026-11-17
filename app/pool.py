"""A fixed-size pool of **two** read-only PostgreSQL connections.

Design goals, mapped to the operational requirements:

* ``POOL_SIZE`` (default 2) physical connections at most.  A bounded
  semaphore serialises requests so a third request *waits* instead of
  opening another backend.
* Waiting for a free connection consumes the same deadline budget as
  execution (:meth:`QueryConnectionPool.acquire` takes an absolute
  monotonic deadline).  If the deadline passes while queued, no statement
  is ever sent on any connection.
* After a timeout the handler must prove the backend is idle again
  (``transaction_status == IDLE`` after a successful ``ROLLBACK``) before
  the connection is returned to the pool.  If that cannot be confirmed the
  connection is closed and discarded, and a replacement is built so the
  pool keeps its capacity of two.
* Every connection is opened in a read-only, read-committed session.

Switchover (maintenance of the primary database):

* The pool routes new connections through an immutable :class:`Route`
  (generation + candidate name + DSN).  :meth:`switch_target` validates
  the candidate and then swaps that one reference, so the flip takes
  effect immediately without waiting for in-flight queries on the old
  database: they finish on their borrowed connections under their original
  deadlines, while queued and newly arriving requests build connections to
  the new target.
* Connections carry the generation they were opened for.  A connection
  whose generation is stale is retired (closed, not reused): idle ones are
  drained at the flip, in-flight ones when they are released — old
  connections leave as they are used up.  Every retirement is replenished
  in the background inside the slot accounting, so the pool returns to
  full capacity on the new target without delaying the releasing request.
* The validation probe borrows one of the two borrower *slots* and opens
  its short-lived probe connection inside that accounting, so the physical
  connections across **both** databases never exceed the pool size.  The
  slot wait consumes the switchover's own total deadline, and the slot is
  released again whether validation succeeds, fails, or runs out of time.
"""
from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from dataclasses import dataclass

from psycopg import AsyncConnection
from psycopg.pq import TransactionStatus

from .config import Settings
from .errors import PoolAcquireTimeout, SwitchTimeout, SwitchValidationFailed

logger = logging.getLogger("querypool")


@dataclass(frozen=True)
class Route:
    """Where new connections go: a named candidate DSN plus a generation.

    The generation increments on every effective switchover so connections
    opened before the flip are recognised as stale and retired instead of
    being reused.  Instances are immutable and the pool swaps the whole
    object in one assignment, so concurrent readers never see a torn
    (name, dsn, generation) combination.
    """

    generation: int
    name: str
    dsn: str


@dataclass
class PoolConn:
    """A borrowed physical connection and the route it was opened for."""

    conn: AsyncConnection
    generation: int
    target: str


@dataclass
class PoolStatus:
    size: int
    idle: int
    in_use: int
    total_created: int
    total_discarded: int
    closed: bool
    target: str
    generation: int


@dataclass(frozen=True)
class SwitchResult:
    changed: bool
    target: str
    previous: str
    generation: int
    fault_logs_count: int


class QueryConnectionPool:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # One slot per *borrower*.  Acquiring a slot guarantees the right to
        # use one physical backend, even if that backend must be (re)built.
        self._slots = asyncio.BoundedSemaphore(settings.pool_size)
        self._free: asyncio.Queue[PoolConn] = asyncio.Queue()
        # Number of physical backends currently tracked (idle + in use).
        # It can temporarily fall below pool_size when a rebuild fails.
        self._backends = 0
        self._closed = False
        self._lock = asyncio.Lock()
        # Serialises switchovers so two concurrent switches cannot flip the
        # route in an interleaved order.
        self._switch_lock = asyncio.Lock()
        self._route = Route(0, "primary", settings.database_dsn)
        self._total_created = 0
        self._total_discarded = 0

    # ------------------------------------------------------------------ #
    # Lifecycle
    # ------------------------------------------------------------------ #
    async def open(self) -> None:
        """Eagerly establish the full pool so failures surface at startup."""
        async with self._lock:
            for _ in range(self._settings.pool_size):
                await self._free.put(await self._make_connection(self._route))
        logger.info(
            "pool opened: size=%d created=%d target=%s",
            self._settings.pool_size,
            self._total_created,
            self._route.name,
        )

    async def close(self) -> None:
        async with self._lock:
            self._closed = True
            while True:
                try:
                    pconn = self._free.get_nowait()
                except asyncio.QueueEmpty:
                    break
                self._backends -= 1
                await self._safe_close(pconn.conn)
            logger.info("pool closed")

    async def status(self) -> PoolStatus:
        idle = self._free.qsize()
        route = self._route
        return PoolStatus(
            size=self._settings.pool_size,
            idle=idle,
            in_use=max(0, self._backends - idle),
            total_created=self._total_created,
            total_discarded=self._total_discarded,
            closed=self._closed,
            target=route.name,
            generation=route.generation,
        )

    # ------------------------------------------------------------------ #
    # Connection factory
    # ------------------------------------------------------------------ #
    async def _make_connection(self, route: Route) -> PoolConn:
        """Open a fresh connection pinned to a read-only session."""
        # Start in autocommit so the session SETs below do not leave an
        # implicit transaction open (psycopg opens one on the first command
        # of a non-autocommit connection).  Switch back afterwards so every
        # request manages explicit BEGIN/COMMIT itself.
        conn = await AsyncConnection.connect(
            route.dsn,
            connect_timeout=self._settings.connect_timeout,
            autocommit=True,
        )
        # Session-level safety net: even an accidental COMMIT cannot write.
        await conn.execute("SET default_transaction_read_only = on")
        await conn.execute(
            "SET default_transaction_isolation = 'read committed'"
        )
        # Async connections expose a setter coroutine, not a writable property.
        await conn.set_autocommit(False)
        assert conn.info.transaction_status == TransactionStatus.IDLE
        self._backends += 1
        self._total_created += 1
        logger.info(
            "connection created (target=%s backends=%d total_created=%d)",
            route.name,
            self._backends,
            self._total_created,
        )
        return PoolConn(conn, route.generation, route.name)

    async def _safe_close(self, conn: AsyncConnection) -> None:
        with contextlib.suppress(Exception):
            await conn.close()

    @staticmethod
    def _is_idle(conn: AsyncConnection) -> bool:
        """True only when the backend has no open/in-progress transaction."""
        return (not conn.closed) and (
            conn.info.transaction_status == TransactionStatus.IDLE
        )

    # ------------------------------------------------------------------ #
    # Acquire / release
    # ------------------------------------------------------------------ #
    async def _acquire_slot(self, deadline: float) -> None:
        """Reserve one borrower slot, waiting at most until ``deadline``."""
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise PoolAcquireTimeout("总截止时间已在排队前耗尽，语句未执行")
        try:
            await asyncio.wait_for(self._slots.acquire(), timeout=remaining)
        except TimeoutError as exc:
            raise PoolAcquireTimeout(
                "等待连接超过总截止时间，排队期间已过期，语句未执行"
            ) from exc

    async def acquire(self, deadline: float) -> PoolConn:
        """Reserve one connection, waiting at most until ``deadline``.

        Time spent here counts against the request's total deadline.  On
        timeout the semaphore slot is released and *nothing has been sent
        to the database* for this request.  A request that was still queued
        when a switchover took effect builds its connection to the *new*
        target: the route is read only after the slot is granted.
        """
        await self._acquire_slot(deadline)
        try:
            return await self._take_idle_or_build(deadline)
        except Exception:
            # Any failure to obtain a physical backend frees the borrower
            # slot so the pool cannot deadlock after a failed rebuild.
            self._slots.release()
            raise

    async def _take_idle_or_build(self, deadline: float) -> PoolConn:
        # No lock across awaits: all mutations happen on one event loop and
        # each step is atomic between awaits, so no interleaving can corrupt
        # the counters or the queue.  The route is re-read every iteration
        # so a switchover that lands while we work is picked up at once.
        while True:
            route = self._route
            if self._free.qsize() > 0:
                pconn = self._free.get_nowait()
                if pconn.generation != route.generation:
                    # Idle connection opened before the switchover: retire
                    # it instead of lending out a backend on the old target.
                    logger.info(
                        "retiring pre-switch idle connection to %s",
                        pconn.target,
                    )
                    await self._retire(pconn)
                    continue
            else:
                # Pool shrank after an earlier discarded connection: rebuild
                # lazily within the caller's remaining deadline budget.
                if deadline - time.monotonic() <= 0:
                    raise PoolAcquireTimeout(
                        "总截止时间在补建连接前耗尽，语句未执行"
                    )
                logger.info("no idle backend; lazily rebuilding within pool")
                pconn = await self._make_connection(route)

            if pconn.generation != self._route.generation:
                # A switchover flipped the route while this connection was
                # being opened: it is stale at birth, rebuild on the new
                # target rather than handing out the old database.
                logger.info(
                    "connection to %s opened across a switchover; rebuilding "
                    "on %s",
                    pconn.target,
                    self._route.name,
                )
                await self._retire(pconn)
                continue

            if not self._is_idle(pconn.conn):
                # An idle backend turned stale; replace it before handing out.
                logger.warning(
                    "idle connection not healthy (closed=%s tx=%s), replacing",
                    pconn.conn.closed,
                    pconn.conn.info.transaction_status,
                )
                await self._retire(pconn)
                continue
            return pconn

    async def release_healthy(self, pconn: PoolConn) -> None:
        """Return a connection that has been confirmed IDLE to the pool."""
        if not self._is_idle(pconn.conn):
            # Never put an unverified connection back; rebuild instead.
            await self.discard_and_replace(pconn)
            return
        if pconn.generation != self._route.generation:
            # In-flight query on the old target finished after the flip:
            # the old connection leaves service instead of being reused.
            logger.info(
                "retiring pre-switch connection to %s on release",
                pconn.target,
            )
            await self._retire(pconn)
            self._slots.release()
            # Refill in the background so the release path stays fast and
            # the old query's response is not delayed by a rebuild.
            asyncio.ensure_future(self._replenish_idle())
            return
        await self._free.put(pconn)
        self._slots.release()

    async def _replenish_idle(self) -> None:
        """Best-effort background refill after retirements (e.g. a flip).

        Slot-accounted like any other build, so physical connections stay
        within the pool size.  If no slot frees up soon, borrowers will
        rebuild lazily on their next acquire instead.
        """
        try:
            await self._acquire_slot(time.monotonic() + 30)
        except PoolAcquireTimeout:
            return
        try:
            async with self._lock:
                if self._closed or self._backends >= self._settings.pool_size:
                    return
                await self._free.put(await self._make_connection(self._route))
        except Exception:
            logger.exception("background replenish failed; "
                             "pool will lazily rebuild on next acquire")
        finally:
            self._slots.release()

    async def discard_and_replace(self, pconn: PoolConn) -> None:
        """Drop an unverifiable connection and build its replacement.

        The replacement is built *before* the slot is released so the pool
        usually offers the full capacity of two healthy backends afterwards.
        If the replacement cannot be built the slot is released anyway and
        the pool temporarily shrinks; the next acquire lazily restores it.
        The replacement follows the *current* route, so a discard after a
        switchover rebuilds onto the new target.
        """
        logger.warning(
            "discarding connection (closed=%s tx=%s) and rebuilding",
            pconn.conn.closed,
            pconn.conn.info.transaction_status,
        )
        await self._retire(pconn)
        try:
            await self._free.put(await self._make_connection(self._route))
        except Exception:
            logger.exception("failed to rebuild discarded connection; "
                             "pool will lazily rebuild on next acquire")
        finally:
            self._slots.release()

    async def _retire(self, pconn: PoolConn) -> None:
        """Close a connection and forget it (stale, unhealthy, discarded)."""
        await self._safe_close(pconn.conn)
        self._backends -= 1
        self._total_discarded += 1

    # ------------------------------------------------------------------ #
    # Switchover
    # ------------------------------------------------------------------ #
    async def switch_target(
        self, name: str, dsn: str, deadline: float
    ) -> SwitchResult:
        """Validate candidate ``name`` and move new queries to it.

        The probe borrows one borrower slot (so physical connections across
        both databases stay within the pool size) and must complete a
        read-only ``fault_logs`` query before ``deadline``.  On any failure
        or when the deadline runs out the route is left untouched and the
        original database keeps serving.  On success the route flips at
        once: in-flight queries on the old target keep their connections
        and finish under their original deadlines, while queued and new
        requests connect to the new target.
        """
        async with self._switch_lock:
            if self._closed:
                raise SwitchValidationFailed(
                    "连接池已关闭，无法切换", extra=self._kept_extra()
                )
            try:
                await self._acquire_slot(deadline)
            except PoolAcquireTimeout as exc:
                raise SwitchTimeout(
                    f"等待连接名额超过切换总期限，保持当前库 "
                    f"{self._route.name} 接单",
                    extra=self._kept_extra(),
                ) from exc
            try:
                count = await self._probe_candidate(dsn, deadline)
            finally:
                # The slot is always returned, even when validation fails:
                # a failed switchover must not cost the pool a slot.
                self._slots.release()

            async with self._lock:
                prev = self._route
                if prev.name == name and prev.dsn == dsn:
                    logger.info(
                        "switchover to %s requested; already active", name
                    )
                    return SwitchResult(
                        False, prev.name, prev.name, prev.generation, count
                    )
                self._route = Route(prev.generation + 1, name, dsn)
                logger.warning(
                    "switchover effective: %s -> %s (generation %d); "
                    "in-flight queries on %s keep their connections",
                    prev.name,
                    name,
                    prev.generation + 1,
                    prev.name,
                )
                # Drain idle connections to the previous target at once
                # (the old database is heading into maintenance); in-flight
                # ones are retired on release.  Each retirement is refilled
                # in the background under slot accounting.
                retired = 0
                while True:
                    try:
                        stale = self._free.get_nowait()
                    except asyncio.QueueEmpty:
                        break
                    await self._retire(stale)
                    retired += 1
                if retired:
                    logger.info(
                        "retired %d idle pre-switch connection(s) to %s",
                        retired,
                        prev.name,
                    )
                for _ in range(retired):
                    asyncio.ensure_future(self._replenish_idle())
                return SwitchResult(
                    True, name, prev.name, prev.generation + 1, count
                )

    def _kept_extra(self) -> dict:
        """Receipt fields proving the original target is still active."""
        return {"switched": False, "target": self._route.name}

    async def _probe_candidate(self, dsn: str, deadline: float) -> int:
        """Prove the candidate serves read-only ``fault_logs`` queries.

        Runs on a short-lived direct connection (accounted for by the
        caller's borrower slot), forced into a read-only session, with the
        whole probe bounded by the switchover's remaining total deadline.
        """
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            raise SwitchTimeout(
                "切换总期限在验证候选库前耗尽，保持原库接单",
                extra=self._kept_extra(),
            )
        conn: AsyncConnection | None = None
        try:
            async with asyncio.timeout(remaining):
                conn = await AsyncConnection.connect(
                    dsn,
                    connect_timeout=min(
                        self._settings.connect_timeout, max(1, int(remaining))
                    ),
                    autocommit=True,
                )
                # Read-only session, same posture as pooled connections.
                await conn.execute("SET default_transaction_read_only = on")
                await conn.execute(
                    "SELECT set_config('statement_timeout', %s, false)",
                    (str(max(1, int(remaining * 1000))),),
                )
                cur = await conn.execute("SELECT count(*) FROM fault_logs")
                row = await cur.fetchone()
        except TimeoutError as exc:
            raise SwitchTimeout(
                "验证候选库超过切换总期限，保持原库接单",
                extra=self._kept_extra(),
            ) from exc
        except SwitchTimeout:
            raise
        except Exception as exc:
            raise SwitchValidationFailed(
                f"候选库只读验证失败（{type(exc).__name__}: {exc}），"
                f"保持原库接单",
                extra=self._kept_extra(),
            ) from exc
        finally:
            if conn is not None:
                await self._safe_close(conn)
        assert row is not None
        return int(row[0])

    # ------------------------------------------------------------------ #
    # Timeout recovery primitives used by the request handler
    # ------------------------------------------------------------------ #
    async def confirm_idle_or_discard(self, conn: AsyncConnection) -> bool:
        """Best-effort proof that no statement is running anymore.

        Returns ``True`` only when the backend reports the IDLE transaction
        state.  Any failure means the connection must be discarded.
        """
        try:
            if conn.closed:
                return False
            status = conn.info.transaction_status
            if status != TransactionStatus.IDLE:
                # A cancelled statement leaves an aborted transaction;
                # ROLLBACK both ends it and proves the backend responds.
                await conn.execute("ROLLBACK")
                status = conn.info.transaction_status
            return (not conn.closed) and status == TransactionStatus.IDLE
        except Exception:
            logger.exception("could not confirm connection idle")
            return False
