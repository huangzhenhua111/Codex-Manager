CREATE INDEX IF NOT EXISTS idx_app_users_list_order
  ON app_users(created_at ASC, username ASC, id ASC);
