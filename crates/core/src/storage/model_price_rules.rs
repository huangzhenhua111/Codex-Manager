use rusqlite::{params, params_from_iter, types::Value, Result, Row};

use super::key_id_filters::{normalize_text_ids, text_id_in_clause, SQLITE_IN_CLAUSE_BATCH_SIZE};
use super::{ModelPriceRule, Storage};

fn model_price_rule_select_columns() -> &'static str {
    "id, provider, model_pattern, match_type, billing_mode,
        currency, unit, input_price_per_1m, cached_input_price_per_1m,
        output_price_per_1m, reasoning_output_price_per_1m,
        cache_write_5m_price_per_1m, cache_write_1h_price_per_1m,
        cache_hit_price_per_1m, long_context_threshold_tokens,
        long_context_input_price_per_1m,
        long_context_cached_input_price_per_1m,
        long_context_output_price_per_1m, source, source_url,
        seed_version, enabled, priority, created_at, updated_at"
}

fn model_price_rule_count_for_seed_sql() -> &'static str {
    "SELECT COUNT(1)
     FROM model_price_rules
     WHERE source = 'official_seed' AND seed_version = ?1"
}

fn enabled_model_price_rules_sql() -> String {
    format!(
        "SELECT {columns}
         FROM model_price_rules
         WHERE enabled = 1
         ORDER BY priority DESC, length(model_pattern) DESC, model_pattern ASC",
        columns = model_price_rule_select_columns(),
    )
}

impl Storage {
    pub fn upsert_model_price_rule(&self, rule: &ModelPriceRule) -> Result<()> {
        self.conn.execute(
            "INSERT INTO model_price_rules (
                id, provider, model_pattern, match_type, billing_mode,
                currency, unit, input_price_per_1m, cached_input_price_per_1m,
                output_price_per_1m, reasoning_output_price_per_1m,
                cache_write_5m_price_per_1m, cache_write_1h_price_per_1m,
                cache_hit_price_per_1m, long_context_threshold_tokens,
                long_context_input_price_per_1m,
                long_context_cached_input_price_per_1m,
                long_context_output_price_per_1m, source, source_url,
                seed_version, enabled, priority, created_at, updated_at
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5,
                ?6, ?7, ?8, ?9,
                ?10, ?11,
                ?12, ?13,
                ?14, ?15,
                ?16,
                ?17,
                ?18, ?19, ?20,
                ?21, ?22, ?23, ?24, ?25
             )
             ON CONFLICT(id) DO UPDATE SET
                provider = excluded.provider,
                model_pattern = excluded.model_pattern,
                match_type = excluded.match_type,
                billing_mode = excluded.billing_mode,
                currency = excluded.currency,
                unit = excluded.unit,
                input_price_per_1m = excluded.input_price_per_1m,
                cached_input_price_per_1m = excluded.cached_input_price_per_1m,
                output_price_per_1m = excluded.output_price_per_1m,
                reasoning_output_price_per_1m = excluded.reasoning_output_price_per_1m,
                cache_write_5m_price_per_1m = excluded.cache_write_5m_price_per_1m,
                cache_write_1h_price_per_1m = excluded.cache_write_1h_price_per_1m,
                cache_hit_price_per_1m = excluded.cache_hit_price_per_1m,
                long_context_threshold_tokens = excluded.long_context_threshold_tokens,
                long_context_input_price_per_1m = excluded.long_context_input_price_per_1m,
                long_context_cached_input_price_per_1m = excluded.long_context_cached_input_price_per_1m,
                long_context_output_price_per_1m = excluded.long_context_output_price_per_1m,
                source = excluded.source,
                source_url = excluded.source_url,
                seed_version = excluded.seed_version,
                enabled = excluded.enabled,
                priority = excluded.priority,
                updated_at = excluded.updated_at",
            params![
                &rule.id,
                &rule.provider,
                &rule.model_pattern,
                &rule.match_type,
                &rule.billing_mode,
                &rule.currency,
                &rule.unit,
                rule.input_price_per_1m,
                rule.cached_input_price_per_1m,
                rule.output_price_per_1m,
                rule.reasoning_output_price_per_1m,
                rule.cache_write_5m_price_per_1m,
                rule.cache_write_1h_price_per_1m,
                rule.cache_hit_price_per_1m,
                rule.long_context_threshold_tokens,
                rule.long_context_input_price_per_1m,
                rule.long_context_cached_input_price_per_1m,
                rule.long_context_output_price_per_1m,
                &rule.source,
                &rule.source_url,
                &rule.seed_version,
                if rule.enabled { 1_i64 } else { 0_i64 },
                rule.priority,
                rule.created_at,
                rule.updated_at,
            ],
        )?;
        Ok(())
    }

    pub fn count_model_price_rules_for_seed(&self, seed_version: &str) -> Result<i64> {
        self.conn.query_row(
            model_price_rule_count_for_seed_sql(),
            [seed_version],
            |row| row.get(0),
        )
    }

    pub fn list_enabled_model_price_rules(&self) -> Result<Vec<ModelPriceRule>> {
        let sql = enabled_model_price_rules_sql();
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        let mut items = Vec::new();
        while let Some(row) = rows.next()? {
            items.push(map_model_price_rule_row(row)?);
        }
        Ok(items)
    }

    pub fn find_enabled_custom_exact_model_price_rule(
        &self,
        model_pattern: &str,
    ) -> Result<Option<ModelPriceRule>> {
        let mut stmt = self
            .conn
            .prepare(enabled_custom_exact_model_price_rule_sql())?;
        let mut rows = stmt.query([model_pattern.trim()])?;
        rows.next()?.map(map_model_price_rule_row).transpose()
    }

    pub fn list_enabled_model_price_rule_patterns_for_patterns(
        &self,
        model_patterns: &[String],
    ) -> Result<Vec<String>> {
        let mut patterns = normalize_text_ids(model_patterns)
            .into_iter()
            .map(|pattern| pattern.to_ascii_lowercase())
            .collect::<Vec<_>>();
        patterns.sort();
        patterns.dedup();
        if patterns.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for chunk in patterns.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(list_enabled_model_price_rule_patterns_for_patterns_chunk(
                self, chunk,
            )?);
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    pub(super) fn ensure_model_price_rules_table(&self) -> Result<()> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS model_price_rules (
                id TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                model_pattern TEXT NOT NULL,
                match_type TEXT NOT NULL,
                billing_mode TEXT NOT NULL,
                currency TEXT NOT NULL,
                unit TEXT NOT NULL,
                input_price_per_1m REAL,
                cached_input_price_per_1m REAL,
                output_price_per_1m REAL,
                reasoning_output_price_per_1m REAL,
                cache_write_5m_price_per_1m REAL,
                cache_write_1h_price_per_1m REAL,
                cache_hit_price_per_1m REAL,
                long_context_threshold_tokens INTEGER,
                long_context_input_price_per_1m REAL,
                long_context_cached_input_price_per_1m REAL,
                long_context_output_price_per_1m REAL,
                source TEXT NOT NULL,
                source_url TEXT,
                seed_version TEXT,
                enabled INTEGER NOT NULL DEFAULT 1,
                priority INTEGER NOT NULL DEFAULT 0,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_model_price_rules_provider_model_mode
             ON model_price_rules(provider, model_pattern, billing_mode)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_model_price_rules_source_seed
             ON model_price_rules(source, seed_version)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_model_price_rules_enabled_priority
             ON model_price_rules(enabled, priority DESC)",
            [],
        )?;
        self.ensure_model_price_rules_custom_exact_lookup_index()?;
        self.ensure_model_price_rules_enabled_pattern_lookup_index()?;
        Ok(())
    }

    pub(super) fn ensure_model_price_rules_custom_exact_lookup_index(&self) -> Result<()> {
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_model_price_rules_custom_exact_lookup
             ON model_price_rules(
                source,
                enabled,
                match_type COLLATE NOCASE,
                model_pattern COLLATE NOCASE,
                priority DESC,
                id ASC
             )",
            [],
        )?;
        Ok(())
    }

    pub(super) fn ensure_model_price_rules_enabled_pattern_lookup_index(&self) -> Result<()> {
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_model_price_rules_enabled_pattern_lookup
             ON model_price_rules(enabled, LOWER(TRIM(model_pattern)))",
            [],
        )?;
        Ok(())
    }
}

fn map_model_price_rule_row(row: &Row<'_>) -> Result<ModelPriceRule> {
    Ok(ModelPriceRule {
        id: row.get(0)?,
        provider: row.get(1)?,
        model_pattern: row.get(2)?,
        match_type: row.get(3)?,
        billing_mode: row.get(4)?,
        currency: row.get(5)?,
        unit: row.get(6)?,
        input_price_per_1m: row.get(7)?,
        cached_input_price_per_1m: row.get(8)?,
        output_price_per_1m: row.get(9)?,
        reasoning_output_price_per_1m: row.get(10)?,
        cache_write_5m_price_per_1m: row.get(11)?,
        cache_write_1h_price_per_1m: row.get(12)?,
        cache_hit_price_per_1m: row.get(13)?,
        long_context_threshold_tokens: row.get(14)?,
        long_context_input_price_per_1m: row.get(15)?,
        long_context_cached_input_price_per_1m: row.get(16)?,
        long_context_output_price_per_1m: row.get(17)?,
        source: row.get(18)?,
        source_url: row.get(19)?,
        seed_version: row.get(20)?,
        enabled: row.get(21)?,
        priority: row.get(22)?,
        created_at: row.get(23)?,
        updated_at: row.get(24)?,
    })
}

fn list_enabled_model_price_rule_patterns_for_patterns_chunk(
    storage: &Storage,
    model_patterns: &[String],
) -> Result<Vec<String>> {
    let Some((condition, params)) = text_id_in_clause("LOWER(TRIM(model_pattern))", model_patterns)
    else {
        return Ok(Vec::new());
    };
    let mut values = Vec::<Value>::with_capacity(params.len() + 1);
    values.push(Value::Integer(1));
    values.extend(params);
    let sql = enabled_model_price_rule_patterns_for_patterns_chunk_sql(&condition);
    let mut stmt = storage.conn.prepare(&sql)?;
    let rows = stmt.query_map(params_from_iter(values), |row| row.get::<_, String>(0))?;
    rows.collect()
}

fn enabled_model_price_rule_patterns_for_patterns_chunk_sql(pattern_condition: &str) -> String {
    format!(
        "SELECT DISTINCT LOWER(TRIM(model_pattern))
         FROM model_price_rules
         WHERE enabled = ?1 AND {pattern_condition}"
    )
}

fn enabled_custom_exact_model_price_rule_sql() -> &'static str {
    "SELECT
        id, provider, model_pattern, match_type, billing_mode,
        currency, unit, input_price_per_1m, cached_input_price_per_1m,
        output_price_per_1m, reasoning_output_price_per_1m,
        cache_write_5m_price_per_1m, cache_write_1h_price_per_1m,
        cache_hit_price_per_1m, long_context_threshold_tokens,
        long_context_input_price_per_1m,
        long_context_cached_input_price_per_1m,
        long_context_output_price_per_1m, source, source_url,
        seed_version, enabled, priority, created_at, updated_at
     FROM model_price_rules
     WHERE source = 'custom'
       AND enabled = 1
       AND match_type = 'exact' COLLATE NOCASE
       AND model_pattern = ?1 COLLATE NOCASE
     ORDER BY priority DESC, id ASC
     LIMIT 1"
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect_query_plan_details_with_params(
        storage: &Storage,
        sql: &str,
        params: Vec<Value>,
    ) -> Vec<String> {
        let mut stmt = storage.conn.prepare(sql).expect("prepare explain");
        let mut rows = stmt.query(params_from_iter(params)).expect("query explain");
        collect_query_plan_rows(&mut rows)
    }

    fn collect_query_plan_rows(rows: &mut rusqlite::Rows<'_>) -> Vec<String> {
        let mut details = Vec::new();
        while let Some(row) = rows.next().expect("next explain row") {
            let detail: String = row.get(3).expect("detail");
            details.push(detail.to_ascii_lowercase());
        }
        details
    }

    fn price_rule(id: &str, model_pattern: &str, source: &str, priority: i64) -> ModelPriceRule {
        ModelPriceRule {
            id: id.to_string(),
            provider: "openai".to_string(),
            model_pattern: model_pattern.to_string(),
            match_type: "exact".to_string(),
            billing_mode: "standard".to_string(),
            currency: "USD".to_string(),
            unit: "per_1m_tokens".to_string(),
            input_price_per_1m: Some(1.0),
            cached_input_price_per_1m: None,
            output_price_per_1m: Some(2.0),
            reasoning_output_price_per_1m: None,
            cache_write_5m_price_per_1m: None,
            cache_write_1h_price_per_1m: None,
            cache_hit_price_per_1m: None,
            long_context_threshold_tokens: None,
            long_context_input_price_per_1m: None,
            long_context_cached_input_price_per_1m: None,
            long_context_output_price_per_1m: None,
            source: source.to_string(),
            source_url: None,
            seed_version: None,
            enabled: true,
            priority,
            created_at: 1,
            updated_at: 1,
        }
    }

    #[test]
    fn count_model_price_rules_for_seed_uses_source_seed_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                model_price_rule_count_for_seed_sql()
            ),
            vec![Value::Text("2026-06".to_string())],
        );

        assert!(
            details
                .iter()
                .any(|detail| detail.contains("idx_model_price_rules_source_seed")),
            "expected seed count to use source/seed index, got {details:?}"
        );
    }
    #[test]
    fn find_enabled_custom_exact_model_price_rule_uses_lookup_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                enabled_custom_exact_model_price_rule_sql()
            ),
            vec![Value::Text("gpt-5".to_string())],
        );

        assert!(
            details
                .iter()
                .any(|detail| detail.contains("idx_model_price_rules_custom_exact_lookup")),
            "expected custom exact lookup to use index, got {details:?}"
        );
    }

    #[test]
    fn find_enabled_custom_exact_model_price_rule_filters_in_sql() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let official = price_rule("official", "gpt-5", "official_seed", 30_000);
        let low = price_rule("custom-low", "gpt-5", "custom", 100);
        let high = price_rule("custom-high", "GPT-5", "custom", 200);
        storage
            .upsert_model_price_rule(&official)
            .expect("insert official");
        storage
            .upsert_model_price_rule(&low)
            .expect("insert low custom");
        storage
            .upsert_model_price_rule(&high)
            .expect("insert high custom");

        let rule = storage
            .find_enabled_custom_exact_model_price_rule(" gpt-5 ")
            .expect("find rule")
            .expect("rule exists");

        assert_eq!(rule.id, "custom-high");
    }

    #[test]
    fn enabled_model_price_rule_pattern_lookup_uses_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let sql = enabled_model_price_rule_patterns_for_patterns_chunk_sql(
            "LOWER(TRIM(model_pattern)) IN ('gpt-5', 'claude-test')",
        );
        let details = collect_query_plan_details_with_params(
            &storage,
            &format!("EXPLAIN QUERY PLAN {sql}"),
            vec![Value::Integer(1)],
        );

        assert!(
            details
                .iter()
                .any(|detail| detail.contains("idx_model_price_rules_enabled_pattern_lookup")),
            "expected enabled pattern lookup to use index, got {details:?}"
        );
        assert!(
            !details
                .iter()
                .any(|detail| detail.contains("use temp b-tree for order by")),
            "enabled pattern lookup chunk should avoid per-chunk ORDER BY temp sorting, got {details:?}"
        );
    }

    #[test]
    fn list_enabled_model_price_rule_patterns_filters_in_sql() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let mut enabled = price_rule("enabled", " GPT-5 ", "official_seed", 1);
        let mut disabled = price_rule("disabled", "claude-disabled", "custom", 1);
        disabled.enabled = false;
        storage
            .upsert_model_price_rule(&enabled)
            .expect("insert enabled");
        storage
            .upsert_model_price_rule(&disabled)
            .expect("insert disabled");

        let patterns = storage
            .list_enabled_model_price_rule_patterns_for_patterns(&[
                "gpt-5".to_string(),
                "CLAUDE-DISABLED".to_string(),
                "missing".to_string(),
            ])
            .expect("list patterns");

        assert_eq!(patterns, vec!["gpt-5".to_string()]);

        enabled.enabled = false;
        storage
            .upsert_model_price_rule(&enabled)
            .expect("disable enabled");
        let patterns = storage
            .list_enabled_model_price_rule_patterns_for_patterns(&["gpt-5".to_string()])
            .expect("list disabled patterns");
        assert!(patterns.is_empty());
    }
}
