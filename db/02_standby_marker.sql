-- 备用只读库标记行：让值班人员能用一条 SELECT 区分当前读到的是备用库。
-- 由 docker-entrypoint-initdb.d 在 01_init.sql 之后执行一次（幂等）。

INSERT INTO fault_logs (device_id, site, severity, fault_code, message, occurred_at)
SELECT 'STANDBY-MARKER', '上海-B区', 'INFO', 'SOURCE_MARKER',
       '备用库标记：此行仅存在于备用只读库 fault_logs', now()
WHERE NOT EXISTS (SELECT 1 FROM fault_logs WHERE device_id = 'STANDBY-MARKER');
