SELECT pipeline_name, task_name, status, rows_read, rows_written
FROM emit_dev.ops.pipeline_runs
ORDER BY started_at DESC;
