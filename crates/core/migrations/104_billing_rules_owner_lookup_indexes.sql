CREATE INDEX IF NOT EXISTS idx_billing_rules_user_lookup
  ON billing_rules(user_id);

CREATE INDEX IF NOT EXISTS idx_billing_rules_project_lookup
  ON billing_rules(project_id);

CREATE INDEX IF NOT EXISTS idx_billing_rules_api_key_lookup
  ON billing_rules(api_key_id);
