CREATE INDEX IF NOT EXISTS idx_plugin_tasks_list_order
  ON plugin_tasks(next_run_at ASC, created_at ASC, id ASC);

CREATE INDEX IF NOT EXISTS idx_plugin_tasks_plugin_list_order
  ON plugin_tasks(plugin_id, next_run_at ASC, created_at ASC, id ASC);
