-- 主库标记行：让值班人员能用一条 SELECT 区分当前读到的是主库。
-- 由 docker-entrypoint-initdb.d 在 01_init.sql 之后执行一次（幂等）。

INSERT INTO fault_logs (device_id, site, severity, fault_code, message, occurred_at)
SELECT 'PRIMARY-MARKER', '北京-A区', 'INFO', 'SOURCE_MARKER',
       '主库标记：此行仅存在于主库 fault_logs', now()
WHERE NOT EXISTS (SELECT 1 FROM fault_logs WHERE device_id = 'PRIMARY-MARKER');
