use std::collections::BTreeSet;

use rusqlite::{params, params_from_iter, types::Value, Result, Row};

use super::key_id_filters::{normalize_text_ids, text_id_in_clause, SQLITE_IN_CLAUSE_BATCH_SIZE};
use super::{
    now_ts, AccountQuotaCapacityOverride, AccountQuotaCapacityTemplate, QuotaSourceModelAssignment,
    Storage,
};

const QUOTA_SOURCE_MODEL_ASSIGNMENT_COLUMNS: &str =
    "source_kind, source_id, model_slug, updated_at";
const ACCOUNT_QUOTA_CAPACITY_OVERRIDE_COLUMNS: &str =
    "account_id, primary_window_tokens, secondary_window_tokens, updated_at";

impl Storage {
    pub fn list_quota_source_model_assignments(&self) -> Result<Vec<QuotaSourceModelAssignment>> {
        let mut stmt = self
            .conn
            .prepare(quota_source_model_assignments_list_sql())?;
        let rows = stmt.query_map([], map_quota_source_model_assignment_row)?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    pub fn list_quota_source_model_assignments_for_kind(
        &self,
        source_kind: &str,
    ) -> Result<Vec<QuotaSourceModelAssignment>> {
        let source_kind = normalize_required_text(source_kind);
        if source_kind.is_empty() {
            return Ok(Vec::new());
        }

        let mut stmt = self
            .conn
            .prepare(quota_source_model_assignments_for_kind_sql())?;
        let rows = stmt.query_map([source_kind], map_quota_source_model_assignment_row)?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    pub fn list_quota_source_model_assignments_for_model(
        &self,
        source_kind: &str,
        model_slug: &str,
    ) -> Result<Vec<QuotaSourceModelAssignment>> {
        let source_kind = normalize_required_text(source_kind);
        let model_slug = normalize_required_text(model_slug);
        if source_kind.is_empty() || model_slug.is_empty() {
            return Ok(Vec::new());
        }

        let mut stmt = self
            .conn
            .prepare(quota_source_model_assignments_for_model_sql())?;
        let rows = stmt.query_map(
            params![source_kind, model_slug],
            map_quota_source_model_assignment_row,
        )?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    pub fn list_quota_source_model_assignment_targets_for_model(
        &self,
        source_kind: &str,
        model_slug: &str,
    ) -> Result<Vec<QuotaSourceModelAssignment>> {
        let source_kind = normalize_required_text(source_kind);
        let model_slug = normalize_required_text(model_slug);
        if source_kind.is_empty() || model_slug.is_empty() {
            return Ok(Vec::new());
        }

        let mut stmt = self
            .conn
            .prepare(quota_source_model_assignment_targets_for_model_sql())?;
        let rows = stmt.query_map(
            params![source_kind, model_slug],
            map_quota_source_model_assignment_row,
        )?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    pub fn list_quota_assignment_source_ids_for_kind(
        &self,
        source_kind: &str,
    ) -> Result<Vec<String>> {
        let source_kind = normalize_required_text(source_kind);
        if source_kind.is_empty() {
            return Ok(Vec::new());
        }

        let mut stmt = self
            .conn
            .prepare(quota_assignment_source_ids_for_kind_sql())?;
        let rows = stmt.query_map([source_kind], |row| row.get(0))?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    pub fn list_quota_source_model_assignments_for_sources(
        &self,
        source_kind: &str,
        source_ids: &[String],
    ) -> Result<Vec<QuotaSourceModelAssignment>> {
        let source_kind = normalize_required_text(source_kind);
        let source_ids = normalize_text_ids(source_ids);
        if source_kind.is_empty() || source_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut items = Vec::new();
        for chunk in source_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            items.extend(list_quota_source_model_assignments_for_sources_chunk(
                self,
                &source_kind,
                chunk,
            )?);
        }
        items.sort_by(|a, b| {
            a.source_kind
                .cmp(&b.source_kind)
                .then_with(|| a.source_id.cmp(&b.source_id))
                .then_with(|| a.model_slug.cmp(&b.model_slug))
        });
        Ok(items)
    }

    pub fn list_quota_source_model_assignments_for(
        &self,
        source_kind: &str,
        source_id: &str,
    ) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(quota_assignment_models_for_source_sql())?;
        let rows = stmt.query_map(params![source_kind, source_id], |row| row.get(0))?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    pub fn set_quota_source_model_assignments(
        &mut self,
        source_kind: &str,
        source_id: &str,
        model_slugs: &[String],
    ) -> Result<()> {
        let source_kind = normalize_required_text(source_kind);
        let source_id = normalize_required_text(source_id);
        if source_kind.is_empty() || source_id.is_empty() {
            return Ok(());
        }

        let now = now_ts();
        let tx = self.conn.unchecked_transaction()?;
        tx.execute(
            delete_quota_source_model_assignments_for_source_sql(),
            params![source_kind, source_id],
        )?;
        for model_slug in normalize_model_slugs(model_slugs) {
            tx.execute(
                "INSERT INTO quota_source_model_assignments (
                    source_kind, source_id, model_slug, updated_at
                 ) VALUES (?1, ?2, ?3, ?4)",
                params![source_kind, source_id, model_slug, now],
            )?;
        }
        tx.commit()
    }

    pub fn list_account_quota_capacity_templates(
        &self,
    ) -> Result<Vec<AccountQuotaCapacityTemplate>> {
        let mut stmt = self
            .conn
            .prepare(account_quota_capacity_templates_list_sql())?;
        let rows = stmt.query_map([], map_account_quota_capacity_template_row)?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    pub fn upsert_account_quota_capacity_template(
        &self,
        plan_type: &str,
        primary_window_tokens: Option<i64>,
        secondary_window_tokens: Option<i64>,
    ) -> Result<()> {
        let plan_type = normalize_required_text(plan_type).to_ascii_lowercase();
        if plan_type.is_empty() {
            return Ok(());
        }
        self.conn.execute(
            "INSERT INTO account_quota_capacity_templates (
                plan_type, primary_window_tokens, secondary_window_tokens, updated_at
             ) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(plan_type) DO UPDATE SET
                primary_window_tokens = excluded.primary_window_tokens,
                secondary_window_tokens = excluded.secondary_window_tokens,
                updated_at = excluded.updated_at",
            params![
                plan_type,
                positive_tokens(primary_window_tokens),
                positive_tokens(secondary_window_tokens),
                now_ts()
            ],
        )?;
        Ok(())
    }

    pub fn list_account_quota_capacity_overrides(
        &self,
    ) -> Result<Vec<AccountQuotaCapacityOverride>> {
        let mut stmt = self
            .conn
            .prepare(account_quota_capacity_overrides_list_sql())?;
        let rows = stmt.query_map([], map_account_quota_capacity_override_row)?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    pub fn list_account_quota_capacity_overrides_for_accounts(
        &self,
        account_ids: &[String],
    ) -> Result<Vec<AccountQuotaCapacityOverride>> {
        let account_ids = normalize_text_ids(account_ids);
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut items = Vec::new();
        for chunk in account_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            items.extend(list_account_quota_capacity_overrides_for_accounts_chunk(
                self, chunk,
            )?);
        }
        items.sort_by(|a, b| a.account_id.cmp(&b.account_id));
        Ok(items)
    }

    pub fn upsert_account_quota_capacity_override(
        &self,
        account_id: &str,
        primary_window_tokens: Option<i64>,
        secondary_window_tokens: Option<i64>,
    ) -> Result<()> {
        let account_id = normalize_required_text(account_id);
        if account_id.is_empty() {
            return Ok(());
        }
        let primary = positive_tokens(primary_window_tokens);
        let secondary = positive_tokens(secondary_window_tokens);
        if primary.is_none() && secondary.is_none() {
            self.conn
                .execute(delete_account_quota_capacity_override_sql(), [account_id])?;
            return Ok(());
        }
        self.conn.execute(
            "INSERT INTO account_quota_capacity_overrides (
                account_id, primary_window_tokens, secondary_window_tokens, updated_at
             ) VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(account_id) DO UPDATE SET
                primary_window_tokens = excluded.primary_window_tokens,
                secondary_window_tokens = excluded.secondary_window_tokens,
                updated_at = excluded.updated_at",
            params![account_id, primary, secondary, now_ts()],
        )?;
        Ok(())
    }

    pub(super) fn ensure_quota_pool_tables(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS quota_source_model_assignments (
                source_kind TEXT NOT NULL,
                source_id TEXT NOT NULL,
                model_slug TEXT NOT NULL,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (source_kind, source_id, model_slug)
            );
            CREATE INDEX IF NOT EXISTS idx_quota_source_model_assignments_source
                ON quota_source_model_assignments(source_kind, source_id);
            CREATE INDEX IF NOT EXISTS idx_quota_source_model_assignments_model
                ON quota_source_model_assignments(model_slug, source_kind, source_id);
            CREATE TABLE IF NOT EXISTS account_quota_capacity_templates (
                plan_type TEXT PRIMARY KEY,
                primary_window_tokens INTEGER,
                secondary_window_tokens INTEGER,
                updated_at INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS account_quota_capacity_overrides (
                account_id TEXT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
                primary_window_tokens INTEGER,
                secondary_window_tokens INTEGER,
                updated_at INTEGER NOT NULL
            );",
        )?;
        Ok(())
    }
}

fn map_quota_source_model_assignment_row(row: &Row<'_>) -> Result<QuotaSourceModelAssignment> {
    Ok(QuotaSourceModelAssignment {
        source_kind: row.get(0)?,
        source_id: row.get(1)?,
        model_slug: row.get(2)?,
        updated_at: row.get(3)?,
    })
}

fn quota_source_model_assignments_list_sql() -> &'static str {
    "SELECT source_kind, source_id, model_slug, updated_at
     FROM quota_source_model_assignments
     ORDER BY source_kind ASC, source_id ASC, model_slug ASC"
}

fn quota_source_model_assignments_for_kind_sql() -> &'static str {
    "SELECT source_kind, source_id, model_slug, updated_at
     FROM quota_source_model_assignments
     WHERE source_kind = ?1
     ORDER BY source_kind ASC, source_id ASC, model_slug ASC"
}

fn quota_source_model_assignments_for_model_sql() -> &'static str {
    "SELECT source_kind, source_id, model_slug, updated_at
     FROM quota_source_model_assignments
     WHERE source_kind = ?1 AND model_slug = ?2
     ORDER BY source_kind ASC, source_id ASC, model_slug ASC"
}

fn quota_source_model_assignment_targets_for_model_sql() -> &'static str {
    "SELECT
        sources.source_kind,
        sources.source_id,
        COALESCE(matches.model_slug, '') AS model_slug,
        COALESCE(matches.updated_at, 0) AS updated_at
     FROM (
        SELECT DISTINCT source_kind, source_id
        FROM quota_source_model_assignments
        WHERE source_kind = ?1
     ) sources
     LEFT JOIN quota_source_model_assignments matches
        ON matches.source_kind = sources.source_kind
       AND matches.source_id = sources.source_id
       AND matches.model_slug = ?2
     ORDER BY sources.source_kind ASC, sources.source_id ASC, model_slug ASC"
}

fn quota_assignment_source_ids_for_kind_sql() -> &'static str {
    "SELECT DISTINCT source_id
     FROM quota_source_model_assignments
     WHERE source_kind = ?1
     ORDER BY source_id ASC"
}

fn quota_assignment_models_for_source_sql() -> &'static str {
    "SELECT model_slug
     FROM quota_source_model_assignments
     WHERE source_kind = ?1 AND source_id = ?2
     ORDER BY model_slug ASC"
}

fn delete_quota_source_model_assignments_for_source_sql() -> &'static str {
    "DELETE FROM quota_source_model_assignments
     WHERE source_kind = ?1 AND source_id = ?2"
}

fn account_quota_capacity_templates_list_sql() -> &'static str {
    "SELECT plan_type, primary_window_tokens, secondary_window_tokens, updated_at
     FROM account_quota_capacity_templates
     ORDER BY plan_type ASC"
}

fn account_quota_capacity_overrides_list_sql() -> &'static str {
    "SELECT account_id, primary_window_tokens, secondary_window_tokens, updated_at
     FROM account_quota_capacity_overrides
     ORDER BY account_id ASC"
}

fn delete_account_quota_capacity_override_sql() -> &'static str {
    "DELETE FROM account_quota_capacity_overrides WHERE account_id = ?1"
}

fn list_quota_source_model_assignments_for_sources_chunk(
    storage: &Storage,
    source_kind: &str,
    source_ids: &[String],
) -> Result<Vec<QuotaSourceModelAssignment>> {
    let Some((source_condition, source_params)) = text_id_in_clause("source_id", source_ids) else {
        return Ok(Vec::new());
    };
    let sql = quota_source_model_assignments_for_sources_chunk_sql(&source_condition);
    let mut values = Vec::with_capacity(source_params.len() + 1);
    values.push(Value::Text(source_kind.to_string()));
    values.extend(source_params);
    let mut stmt = storage.conn.prepare(&sql)?;
    let rows = stmt.query_map(
        params_from_iter(values),
        map_quota_source_model_assignment_row,
    )?;
    let mut items = Vec::new();
    for row in rows {
        items.push(row?);
    }
    Ok(items)
}

fn quota_source_model_assignments_for_sources_chunk_sql(source_condition: &str) -> String {
    format!(
        "SELECT {QUOTA_SOURCE_MODEL_ASSIGNMENT_COLUMNS}
         FROM quota_source_model_assignments
         WHERE source_kind = ? AND {source_condition}"
    )
}

fn map_account_quota_capacity_template_row(row: &Row<'_>) -> Result<AccountQuotaCapacityTemplate> {
    Ok(AccountQuotaCapacityTemplate {
        plan_type: row.get(0)?,
        primary_window_tokens: row.get(1)?,
        secondary_window_tokens: row.get(2)?,
        updated_at: row.get(3)?,
    })
}

fn map_account_quota_capacity_override_row(row: &Row<'_>) -> Result<AccountQuotaCapacityOverride> {
    Ok(AccountQuotaCapacityOverride {
        account_id: row.get(0)?,
        primary_window_tokens: row.get(1)?,
        secondary_window_tokens: row.get(2)?,
        updated_at: row.get(3)?,
    })
}

fn list_account_quota_capacity_overrides_for_accounts_chunk(
    storage: &Storage,
    account_ids: &[String],
) -> Result<Vec<AccountQuotaCapacityOverride>> {
    let Some((condition, params)) = text_id_in_clause("account_id", account_ids) else {
        return Ok(Vec::new());
    };
    let sql = account_quota_capacity_overrides_for_accounts_chunk_sql(&condition);
    let mut stmt = storage.conn.prepare(&sql)?;
    let rows = stmt.query_map(
        params_from_iter(params),
        map_account_quota_capacity_override_row,
    )?;
    let mut items = Vec::new();
    for row in rows {
        items.push(row?);
    }
    Ok(items)
}

fn account_quota_capacity_overrides_for_accounts_chunk_sql(condition: &str) -> String {
    format!(
        "SELECT {ACCOUNT_QUOTA_CAPACITY_OVERRIDE_COLUMNS}
         FROM account_quota_capacity_overrides
         WHERE {condition}"
    )
}

fn normalize_required_text(value: &str) -> String {
    value.trim().to_string()
}

fn normalize_model_slugs(values: &[String]) -> Vec<String> {
    values
        .iter()
        .filter_map(|value| {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

fn positive_tokens(value: Option<i64>) -> Option<i64> {
    value.filter(|tokens| *tokens > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{now_ts, Account};

    fn collect_query_plan_details(storage: &Storage, sql: &str) -> Vec<String> {
        collect_query_plan_details_with_params(storage, sql, Vec::new())
    }

    fn collect_query_plan_details_with_params(
        storage: &Storage,
        sql: &str,
        params: Vec<Value>,
    ) -> Vec<String> {
        let mut stmt = storage.conn.prepare(sql).expect("prepare explain");
        let mut rows = stmt.query(params_from_iter(params)).expect("query explain");
        let mut details = Vec::new();
        while let Some(row) = rows.next().expect("next explain row") {
            let detail: String = row.get(3).expect("detail");
            details.push(detail.to_ascii_lowercase());
        }
        details
    }

    fn assert_no_temp_ordering(details: &[String], label: &str) {
        assert!(
            !details
                .iter()
                .any(|detail| detail.contains("use temp b-tree for order by")),
            "{label} should avoid temp ORDER BY sorting, got {details:?}"
        );
    }

    fn sample_account(id: &str, now: i64) -> Account {
        Account {
            id: id.to_string(),
            label: id.to_string(),
            issuer: "issuer".to_string(),
            chatgpt_account_id: None,
            workspace_id: None,
            group_name: None,
            sort: 0,
            status: "active".to_string(),
            created_at: now,
            updated_at: now,
        }
    }

    #[test]
    fn account_quota_helpers_filter_to_requested_accounts() {
        let mut storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        for account_id in ["acc-a", "acc-b"] {
            storage
                .insert_account(&sample_account(account_id, now))
                .expect("insert account");
            storage
                .upsert_account_quota_capacity_override(account_id, Some(100), None)
                .expect("upsert override");
            storage
                .set_quota_source_model_assignments(
                    "openai_account",
                    account_id,
                    &[format!("model-{account_id}")],
                )
                .expect("set assignments");
        }
        storage
            .set_quota_source_model_assignments(
                "aggregate_api",
                "acc-b",
                &["aggregate-model".to_string()],
            )
            .expect("set aggregate assignments");

        let requested = vec!["acc-b".to_string(), "missing".to_string()];
        let overrides = storage
            .list_account_quota_capacity_overrides_for_accounts(&requested)
            .expect("list overrides");
        let assignments = storage
            .list_quota_source_model_assignments_for_sources("openai_account", &requested)
            .expect("list assignments");

        assert_eq!(overrides.len(), 1);
        assert_eq!(overrides[0].account_id, "acc-b");
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].source_kind, "openai_account");
        assert_eq!(assignments[0].source_id, "acc-b");
        assert_eq!(assignments[0].model_slug, "model-acc-b");
    }

    #[test]
    fn quota_source_model_assignments_for_kind_filters_source_kind() {
        let mut storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        storage
            .set_quota_source_model_assignments("aggregate_api", "agg-b", &["model-b".to_string()])
            .expect("set aggregate b assignments");
        storage
            .set_quota_source_model_assignments(
                "aggregate_api",
                "agg-a",
                &["model-c".to_string(), "model-a".to_string()],
            )
            .expect("set aggregate a assignments");
        storage
            .set_quota_source_model_assignments(
                "openai_account",
                "agg-a",
                &["account-model".to_string()],
            )
            .expect("set account assignments");

        let assignments = storage
            .list_quota_source_model_assignments_for_kind("aggregate_api")
            .expect("list aggregate assignments");

        assert_eq!(
            assignments
                .into_iter()
                .map(|item| (item.source_kind, item.source_id, item.model_slug))
                .collect::<Vec<_>>(),
            vec![
                (
                    "aggregate_api".to_string(),
                    "agg-a".to_string(),
                    "model-a".to_string()
                ),
                (
                    "aggregate_api".to_string(),
                    "agg-a".to_string(),
                    "model-c".to_string()
                ),
                (
                    "aggregate_api".to_string(),
                    "agg-b".to_string(),
                    "model-b".to_string()
                )
            ]
        );
    }

    #[test]
    fn quota_source_model_assignments_for_model_filters_with_model_index() {
        let mut storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        storage
            .set_quota_source_model_assignments(
                "openai_account",
                "acc-a",
                &["gpt-target".to_string(), "gpt-other".to_string()],
            )
            .expect("set account a assignments");
        storage
            .set_quota_source_model_assignments(
                "openai_account",
                "acc-b",
                &["gpt-other".to_string()],
            )
            .expect("set account b assignments");
        storage
            .set_quota_source_model_assignments(
                "aggregate_api",
                "agg-a",
                &["gpt-target".to_string()],
            )
            .expect("set aggregate assignments");

        let assignments = storage
            .list_quota_source_model_assignments_for_model("openai_account", "gpt-target")
            .expect("list model assignments");

        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].source_kind, "openai_account");
        assert_eq!(assignments[0].source_id, "acc-a");
        assert_eq!(assignments[0].model_slug, "gpt-target");

        let details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                quota_source_model_assignments_for_model_sql()
            ),
            vec![
                Value::Text("openai_account".to_string()),
                Value::Text("gpt-target".to_string()),
            ],
        );
        assert!(
            details
                .iter()
                .any(|detail| detail.contains("idx_quota_source_model_assignments_model")),
            "quota assignment model lookup should use model index, got {details:?}"
        );
    }

    #[test]
    fn quota_assignment_source_ids_for_kind_lists_distinct_sources() {
        let mut storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        storage
            .set_quota_source_model_assignments(
                "openai_account",
                "acc-b",
                &["gpt-b".to_string(), "gpt-a".to_string()],
            )
            .expect("set account b assignments");
        storage
            .set_quota_source_model_assignments("openai_account", "acc-a", &["gpt-a".to_string()])
            .expect("set account a assignments");
        storage
            .set_quota_source_model_assignments("aggregate_api", "agg-a", &["gpt-a".to_string()])
            .expect("set aggregate assignments");

        let source_ids = storage
            .list_quota_assignment_source_ids_for_kind("openai_account")
            .expect("list source ids");

        assert_eq!(source_ids, vec!["acc-a".to_string(), "acc-b".to_string()]);
    }

    #[test]
    fn quota_source_model_assignment_targets_for_model_preserve_empty_implicit_sources() {
        let mut storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        storage
            .set_quota_source_model_assignments(
                "openai_account",
                "acc-target",
                &["gpt-target".to_string()],
            )
            .expect("set target assignment");
        storage
            .set_quota_source_model_assignments(
                "openai_account",
                "acc-other",
                &["gpt-other".to_string()],
            )
            .expect("set other assignment");
        storage
            .set_quota_source_model_assignments(
                "aggregate_api",
                "agg-target",
                &["gpt-target".to_string()],
            )
            .expect("set aggregate assignment");

        let targets = storage
            .list_quota_source_model_assignment_targets_for_model("openai_account", "gpt-target")
            .expect("list target assignments");

        assert_eq!(
            targets
                .iter()
                .map(|item| {
                    (
                        item.source_kind.as_str(),
                        item.source_id.as_str(),
                        item.model_slug.as_str(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                ("openai_account", "acc-other", ""),
                ("openai_account", "acc-target", "gpt-target")
            ]
        );

        let details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                quota_source_model_assignment_targets_for_model_sql()
            ),
            vec![
                Value::Text("openai_account".to_string()),
                Value::Text("gpt-target".to_string()),
            ],
        );
        assert!(
            details.iter().any(|detail| {
                detail.contains("idx_quota_source_model_assignments_source")
                    || detail.contains("sqlite_autoindex_quota_source_model_assignments")
            }),
            "target assignment source lookup should use source index, got {details:?}"
        );
    }

    #[test]
    fn account_quota_helpers_chunk_large_account_sets() {
        let mut storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        let target = "acc-0949";
        storage
            .insert_account(&sample_account(target, now))
            .expect("insert target account");
        storage
            .upsert_account_quota_capacity_override(target, Some(250), Some(500))
            .expect("upsert override");
        storage
            .set_quota_source_model_assignments(
                "openai_account",
                target,
                &["gpt-large".to_string()],
            )
            .expect("set assignments");

        let requested = (0..950)
            .map(|index| format!("acc-{index:04}"))
            .collect::<Vec<_>>();
        let overrides = storage
            .list_account_quota_capacity_overrides_for_accounts(&requested)
            .expect("list overrides");
        let assignments = storage
            .list_quota_source_model_assignments_for_sources("openai_account", &requested)
            .expect("list assignments");

        assert_eq!(overrides.len(), 1);
        assert_eq!(overrides[0].account_id, target);
        assert_eq!(overrides[0].primary_window_tokens, Some(250));
        assert_eq!(assignments.len(), 1);
        assert_eq!(assignments[0].source_id, target);
        assert_eq!(assignments[0].model_slug, "gpt-large");
    }

    #[test]
    fn quota_pool_list_queries_use_existing_index_ordering() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let kind_details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                quota_source_model_assignments_for_kind_sql()
            ),
            vec![Value::Text("openai_account".to_string())],
        );
        assert!(
            kind_details.iter().any(|detail| {
                detail.contains("idx_quota_source_model_assignments_source")
                    || detail.contains("sqlite_autoindex_quota_source_model_assignments")
            }),
            "quota assignment kind list should use source/primary key index, got {kind_details:?}"
        );
        assert_no_temp_ordering(&kind_details, "quota assignment kind list");

        let source_id_details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                quota_assignment_source_ids_for_kind_sql()
            ),
            vec![Value::Text("openai_account".to_string())],
        );
        assert!(
            source_id_details.iter().any(|detail| {
                detail.contains("idx_quota_source_model_assignments_source")
                    || detail.contains("sqlite_autoindex_quota_source_model_assignments")
            }),
            "quota assignment source id list should use source/primary key index, got {source_id_details:?}"
        );
        assert_no_temp_ordering(&source_id_details, "quota assignment source id list");

        let source_model_details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                quota_assignment_models_for_source_sql()
            ),
            vec![
                Value::Text("openai_account".to_string()),
                Value::Text("acc-a".to_string()),
            ],
        );
        assert!(
            source_model_details
                .iter()
                .any(|detail| detail.contains("sqlite_autoindex_quota_source_model_assignments")),
            "quota assignment source model list should use primary key index, got {source_model_details:?}"
        );
        assert_no_temp_ordering(&source_model_details, "quota assignment source model list");

        let template_details = collect_query_plan_details(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                account_quota_capacity_templates_list_sql()
            ),
        );
        assert_no_temp_ordering(&template_details, "account quota capacity template list");

        let override_details = collect_query_plan_details(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                account_quota_capacity_overrides_list_sql()
            ),
        );
        assert_no_temp_ordering(&override_details, "account quota capacity override list");
    }

    #[test]
    fn quota_pool_delete_helpers_use_existing_lookup_indexes() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let assignment_details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                delete_quota_source_model_assignments_for_source_sql()
            ),
            vec![
                Value::Text("openai_account".to_string()),
                Value::Text("acc-a".to_string()),
            ],
        );
        assert!(
            assignment_details.iter().any(|detail| {
                detail.contains("idx_quota_source_model_assignments_source")
                    || detail.contains("sqlite_autoindex_quota_source_model_assignments")
            }),
            "quota assignment source cleanup should use source/primary key index, got {assignment_details:?}"
        );

        let override_details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                delete_account_quota_capacity_override_sql()
            ),
            vec![Value::Text("acc-a".to_string())],
        );
        assert!(
            override_details
                .iter()
                .any(|detail| detail.contains("sqlite_autoindex_account_quota_capacity_overrides")),
            "quota capacity override delete should use account primary key index, got {override_details:?}"
        );
    }

    #[test]
    fn quota_helper_chunk_queries_defer_final_ordering_to_rust() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let Some((assignment_condition, assignment_params)) =
            text_id_in_clause("source_id", &["acc-a".to_string(), "acc-b".to_string()])
        else {
            panic!("expected assignment condition");
        };
        let mut assignment_values = vec![Value::Text("openai_account".to_string())];
        assignment_values.extend(assignment_params);
        let assignment_details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                quota_source_model_assignments_for_sources_chunk_sql(&assignment_condition)
            ),
            assignment_values,
        );
        let Some((override_condition, override_params)) =
            text_id_in_clause("account_id", &["acc-a".to_string(), "acc-b".to_string()])
        else {
            panic!("expected override condition");
        };
        let override_details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                account_quota_capacity_overrides_for_accounts_chunk_sql(&override_condition)
            ),
            override_params,
        );

        assert!(
            assignment_details.iter().any(|detail| {
                detail.contains("idx_quota_source_model_assignments_source")
                    || detail.contains("sqlite_autoindex_quota_source_model_assignments")
            }),
            "quota assignment chunk query should use source lookup index, got {assignment_details:?}"
        );
        assert!(
            override_details.iter().any(|detail| {
                detail.contains("sqlite_autoindex_account_quota_capacity_overrides")
            }),
            "quota override chunk query should use account lookup index, got {override_details:?}"
        );
        assert_no_temp_ordering(&assignment_details, "quota assignment chunk query");
        assert_no_temp_ordering(&override_details, "quota override chunk query");
    }
}
