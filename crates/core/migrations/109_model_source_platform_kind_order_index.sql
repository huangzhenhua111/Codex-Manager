CREATE INDEX IF NOT EXISTS idx_model_source_mappings_platform_kind_enabled_priority
    ON model_source_mappings(platform_model_slug, source_kind, enabled, priority DESC, weight DESC, source_id, upstream_model);
