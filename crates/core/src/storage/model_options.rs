use std::collections::BTreeMap;

use rusqlite::{params, params_from_iter, types::Value as SqlValue, OptionalExtension, Row};
use serde_json::Value;

use crate::rpc::types::{ModelInfo, ModelReasoningLevel};

use super::key_id_filters::{text_id_in_clause, SQLITE_IN_CLAUSE_BATCH_SIZE};
use super::{
    now_ts, ModelCatalogModelRecord, ModelCatalogReasoningLevelRecord, ModelCatalogScopeRecord,
    ModelCatalogStorageSnapshot, ModelCatalogStringItemRecord, Storage,
};

const STRING_ITEM_KIND_ADDITIONAL_SPEED_TIERS: &str = "additional_speed_tiers";
const STRING_ITEM_KIND_EXPERIMENTAL_SUPPORTED_TOOLS: &str = "experimental_supported_tools";
const STRING_ITEM_KIND_INPUT_MODALITIES: &str = "input_modalities";
const STRING_ITEM_KIND_AVAILABLE_IN_PLANS: &str = "available_in_plans";
const MODEL_CATALOG_MODEL_ORDER_CLAUSE: &str = "ORDER BY sort_index ASC, updated_at DESC, slug ASC";
const MODEL_CATALOG_API_AVAILABLE_CONDITION: &str = "TRIM(slug) <> ''
               AND COALESCE(supported_in_api, 1) = 1
               AND LOWER(TRIM(COALESCE(visibility, ''))) NOT IN ('hide', 'hidden', 'disabled', 'unavailable')";

fn normalize_model_catalog_slugs(slugs: &[String]) -> Vec<String> {
    let mut normalized = slugs
        .iter()
        .map(|slug| slug.trim().to_string())
        .filter(|slug| !slug.is_empty())
        .collect::<Vec<_>>();
    normalized.sort();
    normalized.dedup();
    normalized
}

fn model_catalog_model_select_sql() -> &'static str {
    "SELECT
        scope, slug, display_name, source_kind, user_edited,
        description, default_reasoning_level, shell_type, visibility, supported_in_api, priority,
        availability_nux_json, upgrade_json, base_instructions,
        model_messages_json, supports_reasoning_summaries,
        default_reasoning_summary, support_verbosity,
        default_verbosity_json, apply_patch_tool_type,
        web_search_tool_type, truncation_mode, truncation_limit,
        truncation_extra_json, supports_parallel_tool_calls,
        supports_image_detail_original, context_window,
        auto_compact_token_limit, effective_context_window_percent,
        minimal_client_version_json, supports_search_tool,
        extra_json, sort_index, updated_at
     FROM model_catalog_models"
}
fn model_catalog_scope_by_scope_sql() -> &'static str {
    "SELECT scope, extra_json, updated_at
     FROM model_catalog_scopes
     WHERE scope = ?1
     LIMIT 1"
}

fn model_catalog_models_for_scope_sql() -> String {
    format!(
        "{select_sql}
         WHERE scope = ?1
         {order_clause}",
        select_sql = model_catalog_model_select_sql(),
        order_clause = MODEL_CATALOG_MODEL_ORDER_CLAUSE,
    )
}

fn model_catalog_ordered_slug_sql(extra_conditions: &[&str], limit: Option<i64>) -> String {
    let mut sql = "SELECT slug
         FROM model_catalog_models
         WHERE scope = ?1"
        .to_string();
    for condition in extra_conditions {
        sql.push_str("\n           AND ");
        sql.push_str(condition);
    }
    sql.push('\n');
    sql.push_str("         ");
    sql.push_str(MODEL_CATALOG_MODEL_ORDER_CLAUSE);
    if let Some(limit) = limit {
        sql.push_str(&format!("\n         LIMIT {limit}"));
    }
    sql
}

fn existing_model_catalog_slugs_chunk_sql(slug_condition: &str) -> String {
    format!(
        "SELECT slug
         FROM model_catalog_models
         WHERE scope = ?1
           AND {slug_condition}"
    )
}

fn remote_unedited_model_catalog_models_for_slugs_chunk_sql(slug_condition: &str) -> String {
    format!(
        "{select_sql}
         WHERE scope = ?1
           AND {slug_condition}
           AND source_kind = 'remote'
           AND COALESCE(user_edited, 0) = 0",
        select_sql = model_catalog_model_select_sql(),
    )
}

fn model_catalog_reasoning_levels_list_sql() -> &'static str {
    "SELECT scope, slug, effort, description, extra_json, sort_index, updated_at
     FROM model_catalog_reasoning_levels
     WHERE scope = ?1
     ORDER BY slug ASC, sort_index ASC, effort ASC"
}

fn model_catalog_string_items_list_sql() -> &'static str {
    "SELECT scope, slug, value, sort_index, updated_at
     FROM model_catalog_string_items
     WHERE scope = ?1 AND item_kind = ?2
     ORDER BY slug ASC, sort_index ASC, value ASC"
}

fn model_catalog_string_items_for_kinds_sql(placeholders: &str) -> String {
    format!(
        "SELECT item_kind, scope, slug, value, sort_index, updated_at
         FROM model_catalog_string_items
         WHERE scope = ?1 AND item_kind IN ({placeholders})
         ORDER BY item_kind ASC, slug ASC, sort_index ASC, value ASC"
    )
}

fn delete_model_catalog_model_sql() -> &'static str {
    "DELETE FROM model_catalog_models WHERE scope = ?1 AND slug = ?2"
}

fn delete_model_catalog_reasoning_levels_sql() -> &'static str {
    "DELETE FROM model_catalog_reasoning_levels WHERE scope = ?1 AND slug = ?2"
}

fn delete_model_catalog_string_items_sql() -> &'static str {
    "DELETE FROM model_catalog_string_items
     WHERE scope = ?1 AND slug = ?2 AND item_kind = ?3"
}

fn delete_model_catalog_string_item_kinds_sql(condition: &str) -> String {
    format!(
        "DELETE FROM model_catalog_string_items
         WHERE scope = ?1 AND slug = ?2 AND {condition}"
    )
}

fn map_model_catalog_model_record(row: &Row<'_>) -> rusqlite::Result<ModelCatalogModelRecord> {
    Ok(ModelCatalogModelRecord {
        scope: row.get(0)?,
        slug: row.get(1)?,
        display_name: row.get(2)?,
        source_kind: row.get(3)?,
        user_edited: row.get(4)?,
        description: row.get(5)?,
        default_reasoning_level: row.get(6)?,
        shell_type: row.get(7)?,
        visibility: row.get(8)?,
        supported_in_api: row.get(9)?,
        priority: row.get(10)?,
        availability_nux_json: row.get(11)?,
        upgrade_json: row.get(12)?,
        base_instructions: row.get(13)?,
        model_messages_json: row.get(14)?,
        supports_reasoning_summaries: row.get(15)?,
        default_reasoning_summary: row.get(16)?,
        support_verbosity: row.get(17)?,
        default_verbosity_json: row.get(18)?,
        apply_patch_tool_type: row.get(19)?,
        web_search_tool_type: row.get(20)?,
        truncation_mode: row.get(21)?,
        truncation_limit: row.get(22)?,
        truncation_extra_json: row.get(23)?,
        supports_parallel_tool_calls: row.get(24)?,
        supports_image_detail_original: row.get(25)?,
        context_window: row.get(26)?,
        auto_compact_token_limit: row.get(27)?,
        effective_context_window_percent: row.get(28)?,
        minimal_client_version_json: row.get(29)?,
        supports_search_tool: row.get(30)?,
        extra_json: row.get(31)?,
        sort_index: row.get(32)?,
        updated_at: row.get(33)?,
    })
}

impl Storage {
    pub fn upsert_model_catalog_scope(
        &self,
        record: &ModelCatalogScopeRecord,
    ) -> rusqlite::Result<()> {
        self.conn.execute(
            "INSERT INTO model_catalog_scopes (scope, extra_json, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(scope) DO UPDATE SET
               extra_json = excluded.extra_json,
               updated_at = excluded.updated_at",
            params![record.scope, record.extra_json, record.updated_at],
        )?;
        Ok(())
    }

    pub fn get_model_catalog_scope(
        &self,
        scope: &str,
    ) -> rusqlite::Result<Option<ModelCatalogScopeRecord>> {
        let mut stmt = self.conn.prepare(model_catalog_scope_by_scope_sql())?;
        let mut rows = stmt.query([scope])?;
        if let Some(row) = rows.next()? {
            return Ok(Some(ModelCatalogScopeRecord {
                scope: row.get(0)?,
                extra_json: row.get(1)?,
                updated_at: row.get(2)?,
            }));
        }
        Ok(None)
    }

    /// 函数 `upsert_model_catalog_models`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-12
    ///
    /// # 参数
    /// - self: 参数 self
    /// - models: 参数 models
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn upsert_model_catalog_models(
        &self,
        models: &[ModelCatalogModelRecord],
    ) -> rusqlite::Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        for model in models {
            tx.execute(
                "INSERT INTO model_catalog_models (
                    scope, slug, display_name, source_kind, user_edited,
                    description, default_reasoning_level, shell_type, visibility, supported_in_api, priority,
                    availability_nux_json, upgrade_json, base_instructions,
                    model_messages_json, supports_reasoning_summaries,
                    default_reasoning_summary, support_verbosity,
                    default_verbosity_json, apply_patch_tool_type,
                    web_search_tool_type, truncation_mode, truncation_limit,
                    truncation_extra_json, supports_parallel_tool_calls,
                    supports_image_detail_original, context_window,
                    auto_compact_token_limit, effective_context_window_percent,
                    minimal_client_version_json, supports_search_tool,
                    extra_json, sort_index, updated_at
                 ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                    ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24,
                    ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34
                 )
                 ON CONFLICT(scope, slug) DO UPDATE SET
                    display_name = excluded.display_name,
                    source_kind = excluded.source_kind,
                    user_edited = excluded.user_edited,
                    description = excluded.description,
                    default_reasoning_level = excluded.default_reasoning_level,
                    shell_type = excluded.shell_type,
                    visibility = excluded.visibility,
                    supported_in_api = excluded.supported_in_api,
                    priority = excluded.priority,
                    availability_nux_json = excluded.availability_nux_json,
                    upgrade_json = excluded.upgrade_json,
                    base_instructions = excluded.base_instructions,
                    model_messages_json = excluded.model_messages_json,
                    supports_reasoning_summaries = excluded.supports_reasoning_summaries,
                    default_reasoning_summary = excluded.default_reasoning_summary,
                    support_verbosity = excluded.support_verbosity,
                    default_verbosity_json = excluded.default_verbosity_json,
                    apply_patch_tool_type = excluded.apply_patch_tool_type,
                    web_search_tool_type = excluded.web_search_tool_type,
                    truncation_mode = excluded.truncation_mode,
                    truncation_limit = excluded.truncation_limit,
                    truncation_extra_json = excluded.truncation_extra_json,
                    supports_parallel_tool_calls = excluded.supports_parallel_tool_calls,
                    supports_image_detail_original = excluded.supports_image_detail_original,
                    context_window = excluded.context_window,
                    auto_compact_token_limit = excluded.auto_compact_token_limit,
                    effective_context_window_percent = excluded.effective_context_window_percent,
                    minimal_client_version_json = excluded.minimal_client_version_json,
                    supports_search_tool = excluded.supports_search_tool,
                    extra_json = excluded.extra_json,
                    sort_index = excluded.sort_index,
                    updated_at = excluded.updated_at",
                params![
                    model.scope,
                    model.slug,
                    model.display_name,
                    model.source_kind,
                    model.user_edited,
                    model.description,
                    model.default_reasoning_level,
                    model.shell_type,
                    model.visibility,
                    model.supported_in_api,
                    model.priority,
                    model.availability_nux_json,
                    model.upgrade_json,
                    model.base_instructions,
                    model.model_messages_json,
                    model.supports_reasoning_summaries,
                    model.default_reasoning_summary,
                    model.support_verbosity,
                    model.default_verbosity_json,
                    model.apply_patch_tool_type,
                    model.web_search_tool_type,
                    model.truncation_mode,
                    model.truncation_limit,
                    model.truncation_extra_json,
                    model.supports_parallel_tool_calls,
                    model.supports_image_detail_original,
                    model.context_window,
                    model.auto_compact_token_limit,
                    model.effective_context_window_percent,
                    model.minimal_client_version_json,
                    model.supports_search_tool,
                    model.extra_json,
                    model.sort_index,
                    model.updated_at,
                ],
            )?;
        }
        tx.commit()?;
        self.sync_default_model_group_models_from_catalog()?;
        Ok(())
    }

    fn sync_default_model_group_models_from_catalog(&self) -> rusqlite::Result<()> {
        if !self.has_table("model_groups")? || !self.has_table("model_group_models")? {
            return Ok(());
        }

        self.prune_default_model_group_models_not_in_catalog()?;
        let now = now_ts();
        self.conn.execute(
            "INSERT OR IGNORE INTO model_group_models (
                group_id, platform_model_slug, enabled, rate_multiplier_millis,
                billing_model_slug, note, created_at, updated_at
             )
             SELECT g.id, m.slug, 1, NULL, NULL, 'catalog_sync', ?1, ?1
             FROM model_groups g
             JOIN model_catalog_models m
               ON m.scope = 'default'
              AND COALESCE(m.supported_in_api, 1) = 1
              AND TRIM(m.slug) <> ''
             WHERE g.is_default = 1",
            params![now],
        )?;
        Ok(())
    }

    /// 函数 `list_model_catalog_models`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-12
    ///
    /// # 参数
    /// - self: 参数 self
    /// - scope: 参数 scope
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn list_model_catalog_models(
        &self,
        scope: &str,
    ) -> rusqlite::Result<Vec<ModelCatalogModelRecord>> {
        let sql = model_catalog_models_for_scope_sql();
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map([scope], map_model_catalog_model_record)?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    pub fn load_model_catalog_storage_snapshot(
        &self,
        scope: &str,
    ) -> rusqlite::Result<ModelCatalogStorageSnapshot> {
        let models = self.list_model_catalog_models(scope)?;
        if models.is_empty() {
            return Ok(ModelCatalogStorageSnapshot::default());
        }
        let string_items = self.list_model_catalog_string_items_for_kinds(
            scope,
            &[
                STRING_ITEM_KIND_ADDITIONAL_SPEED_TIERS,
                STRING_ITEM_KIND_EXPERIMENTAL_SUPPORTED_TOOLS,
                STRING_ITEM_KIND_INPUT_MODALITIES,
                STRING_ITEM_KIND_AVAILABLE_IN_PLANS,
            ],
        )?;
        let mut additional_speed_tiers = Vec::new();
        let mut experimental_supported_tools = Vec::new();
        let mut input_modalities = Vec::new();
        let mut available_in_plans = Vec::new();
        for (item_kind, item) in string_items {
            match item_kind.as_str() {
                STRING_ITEM_KIND_ADDITIONAL_SPEED_TIERS => additional_speed_tiers.push(item),
                STRING_ITEM_KIND_EXPERIMENTAL_SUPPORTED_TOOLS => {
                    experimental_supported_tools.push(item)
                }
                STRING_ITEM_KIND_INPUT_MODALITIES => input_modalities.push(item),
                STRING_ITEM_KIND_AVAILABLE_IN_PLANS => available_in_plans.push(item),
                _ => {}
            }
        }
        Ok(ModelCatalogStorageSnapshot {
            scope: self.get_model_catalog_scope(scope)?,
            models,
            reasoning_levels: self.list_model_catalog_reasoning_levels(scope)?,
            additional_speed_tiers,
            experimental_supported_tools,
            input_modalities,
            available_in_plans,
        })
    }

    pub fn list_remote_unedited_model_catalog_models_for_slugs(
        &self,
        scope: &str,
        slugs: &[String],
    ) -> rusqlite::Result<Vec<ModelCatalogModelRecord>> {
        let slugs = normalize_model_catalog_slugs(slugs);
        if slugs.is_empty() {
            return Ok(Vec::new());
        }
        let mut items = Vec::new();
        for chunk in slugs.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            let Some((slug_condition, slug_params)) = text_id_in_clause("slug", chunk) else {
                continue;
            };
            let sql = remote_unedited_model_catalog_models_for_slugs_chunk_sql(&slug_condition);
            let mut values = Vec::with_capacity(slug_params.len() + 1);
            values.push(SqlValue::Text(scope.trim().to_string()));
            values.extend(slug_params);
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(values), map_model_catalog_model_record)?;
            for row in rows {
                items.push(row?);
            }
        }
        items.sort_by(|left, right| {
            left.sort_index
                .cmp(&right.sort_index)
                .then_with(|| right.updated_at.cmp(&left.updated_at))
                .then_with(|| left.slug.cmp(&right.slug))
        });
        Ok(items)
    }

    pub fn list_remote_unedited_model_catalog_slugs(
        &self,
        scope: &str,
    ) -> rusqlite::Result<Vec<String>> {
        let sql = model_catalog_ordered_slug_sql(
            &["source_kind = 'remote'", "COALESCE(user_edited, 0) = 0"],
            None,
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map([scope], |row| row.get::<_, String>(0))?;
        rows.collect()
    }

    pub fn list_api_available_model_catalog_slugs(
        &self,
        scope: &str,
    ) -> rusqlite::Result<Vec<String>> {
        let sql = model_catalog_ordered_slug_sql(&[MODEL_CATALOG_API_AVAILABLE_CONDITION], None);
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map([scope], |row| row.get::<_, String>(0))?;
        rows.collect()
    }

    pub fn find_first_api_available_model_catalog_slug(
        &self,
        scope: &str,
    ) -> rusqlite::Result<Option<String>> {
        let sql = model_catalog_ordered_slug_sql(&[MODEL_CATALOG_API_AVAILABLE_CONDITION], Some(1));
        self.conn
            .query_row(&sql, [scope], |row| row.get::<_, String>(0))
            .optional()
    }

    pub fn list_api_available_model_catalog_slugs_with_prefix(
        &self,
        scope: &str,
        slug_prefix: &str,
    ) -> rusqlite::Result<Vec<String>> {
        let normalized_prefix = slug_prefix.trim().to_ascii_lowercase();
        if normalized_prefix.is_empty() {
            return self.list_api_available_model_catalog_slugs(scope);
        }
        let like_pattern = format!("{normalized_prefix}%");
        let sql = model_catalog_ordered_slug_sql(
            &[
                MODEL_CATALOG_API_AVAILABLE_CONDITION,
                "LOWER(TRIM(slug)) LIKE ?2",
            ],
            None,
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params![scope, like_pattern], |row| row.get::<_, String>(0))?;
        rows.collect()
    }

    pub fn model_catalog_model_exists(&self, scope: &str, slug: &str) -> rusqlite::Result<bool> {
        let found = self
            .conn
            .query_row(
                "SELECT 1
                 FROM model_catalog_models
                 WHERE scope = ?1
                   AND slug = ?2
                 LIMIT 1",
                params![scope, slug],
                |_| Ok(()),
            )
            .optional()?
            .is_some();
        Ok(found)
    }

    pub fn list_existing_model_catalog_slugs(
        &self,
        scope: &str,
        slugs: &[String],
    ) -> rusqlite::Result<Vec<String>> {
        let slugs = normalize_model_catalog_slugs(slugs);
        if slugs.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for chunk in slugs.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            let Some((slug_condition, slug_params)) = text_id_in_clause("slug", chunk) else {
                continue;
            };
            let sql = existing_model_catalog_slugs_chunk_sql(&slug_condition);
            let mut values = Vec::with_capacity(slug_params.len() + 1);
            values.push(SqlValue::Text(scope.trim().to_string()));
            values.extend(slug_params);
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(values), |row| row.get::<_, String>(0))?;
            for row in rows {
                out.push(row?);
            }
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    pub fn count_available_model_catalog_models(&self, scope: &str) -> rusqlite::Result<i64> {
        let sql = format!(
            "SELECT COUNT(1)
             FROM model_catalog_models
             WHERE scope = ?1
               AND {MODEL_CATALOG_API_AVAILABLE_CONDITION}"
        );
        self.conn.query_row(&sql, [scope], |row| row.get(0))
    }

    pub fn delete_model_catalog_model(&self, scope: &str, slug: &str) -> rusqlite::Result<()> {
        self.conn
            .execute(delete_model_catalog_model_sql(), params![scope, slug])?;
        Ok(())
    }

    pub fn upsert_model_catalog_reasoning_levels(
        &self,
        levels: &[ModelCatalogReasoningLevelRecord],
    ) -> rusqlite::Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        for level in levels {
            tx.execute(
                "INSERT INTO model_catalog_reasoning_levels (
                    scope, slug, effort, description, extra_json, sort_index, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(scope, slug, effort) DO UPDATE SET
                    description = excluded.description,
                    extra_json = excluded.extra_json,
                    sort_index = excluded.sort_index,
                    updated_at = excluded.updated_at",
                params![
                    level.scope,
                    level.slug,
                    level.effort,
                    level.description,
                    level.extra_json,
                    level.sort_index,
                    level.updated_at,
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn list_model_catalog_reasoning_levels(
        &self,
        scope: &str,
    ) -> rusqlite::Result<Vec<ModelCatalogReasoningLevelRecord>> {
        let mut stmt = self
            .conn
            .prepare(model_catalog_reasoning_levels_list_sql())?;
        let rows = stmt.query_map([scope], |row| {
            Ok(ModelCatalogReasoningLevelRecord {
                scope: row.get(0)?,
                slug: row.get(1)?,
                effort: row.get(2)?,
                description: row.get(3)?,
                extra_json: row.get(4)?,
                sort_index: row.get(5)?,
                updated_at: row.get(6)?,
            })
        })?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    pub fn delete_model_catalog_reasoning_levels(
        &self,
        scope: &str,
        slug: &str,
    ) -> rusqlite::Result<()> {
        self.conn.execute(
            delete_model_catalog_reasoning_levels_sql(),
            params![scope, slug],
        )?;
        Ok(())
    }

    pub fn upsert_model_catalog_additional_speed_tiers(
        &self,
        items: &[ModelCatalogStringItemRecord],
    ) -> rusqlite::Result<()> {
        self.upsert_model_catalog_string_items(STRING_ITEM_KIND_ADDITIONAL_SPEED_TIERS, items)
    }

    pub fn list_model_catalog_additional_speed_tiers(
        &self,
        scope: &str,
    ) -> rusqlite::Result<Vec<ModelCatalogStringItemRecord>> {
        self.list_model_catalog_string_items(STRING_ITEM_KIND_ADDITIONAL_SPEED_TIERS, scope)
    }

    pub fn upsert_model_catalog_experimental_supported_tools(
        &self,
        items: &[ModelCatalogStringItemRecord],
    ) -> rusqlite::Result<()> {
        self.upsert_model_catalog_string_items(STRING_ITEM_KIND_EXPERIMENTAL_SUPPORTED_TOOLS, items)
    }

    pub fn list_model_catalog_experimental_supported_tools(
        &self,
        scope: &str,
    ) -> rusqlite::Result<Vec<ModelCatalogStringItemRecord>> {
        self.list_model_catalog_string_items(STRING_ITEM_KIND_EXPERIMENTAL_SUPPORTED_TOOLS, scope)
    }

    pub fn upsert_model_catalog_input_modalities(
        &self,
        items: &[ModelCatalogStringItemRecord],
    ) -> rusqlite::Result<()> {
        self.upsert_model_catalog_string_items(STRING_ITEM_KIND_INPUT_MODALITIES, items)
    }

    pub fn list_model_catalog_input_modalities(
        &self,
        scope: &str,
    ) -> rusqlite::Result<Vec<ModelCatalogStringItemRecord>> {
        self.list_model_catalog_string_items(STRING_ITEM_KIND_INPUT_MODALITIES, scope)
    }

    pub fn upsert_model_catalog_available_in_plans(
        &self,
        items: &[ModelCatalogStringItemRecord],
    ) -> rusqlite::Result<()> {
        self.upsert_model_catalog_string_items(STRING_ITEM_KIND_AVAILABLE_IN_PLANS, items)
    }

    pub fn list_model_catalog_available_in_plans(
        &self,
        scope: &str,
    ) -> rusqlite::Result<Vec<ModelCatalogStringItemRecord>> {
        self.list_model_catalog_string_items(STRING_ITEM_KIND_AVAILABLE_IN_PLANS, scope)
    }

    pub fn delete_model_catalog_string_items(
        &self,
        scope: &str,
        slug: &str,
        item_kind: &str,
    ) -> rusqlite::Result<()> {
        self.conn.execute(
            delete_model_catalog_string_items_sql(),
            params![scope, slug, item_kind],
        )?;
        Ok(())
    }

    pub fn delete_model_catalog_string_item_kinds(
        &self,
        scope: &str,
        slug: &str,
        item_kinds: &[&str],
    ) -> rusqlite::Result<()> {
        let item_kinds = item_kinds
            .iter()
            .map(|item| item.to_string())
            .collect::<Vec<_>>();
        let Some((condition, mut values)) = text_id_in_clause("item_kind", &item_kinds) else {
            return Ok(());
        };
        let sql = delete_model_catalog_string_item_kinds_sql(&condition);
        values.insert(0, SqlValue::Text(slug.to_string()));
        values.insert(0, SqlValue::Text(scope.to_string()));
        self.conn.execute(&sql, params_from_iter(values))?;
        Ok(())
    }

    fn upsert_model_catalog_string_items(
        &self,
        item_kind: &str,
        items: &[ModelCatalogStringItemRecord],
    ) -> rusqlite::Result<()> {
        let sql =
            "INSERT INTO model_catalog_string_items (scope, slug, item_kind, value, sort_index, updated_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(scope, slug, item_kind, value) DO UPDATE SET
               sort_index = excluded.sort_index,
               updated_at = excluded.updated_at";
        let tx = self.conn.unchecked_transaction()?;
        for item in items {
            tx.execute(
                sql,
                params![
                    item.scope,
                    item.slug,
                    item_kind,
                    item.value,
                    item.sort_index,
                    item.updated_at
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    fn list_model_catalog_string_items(
        &self,
        item_kind: &str,
        scope: &str,
    ) -> rusqlite::Result<Vec<ModelCatalogStringItemRecord>> {
        let mut stmt = self.conn.prepare(model_catalog_string_items_list_sql())?;
        let rows = stmt.query_map(params![scope, item_kind], |row| {
            Ok(ModelCatalogStringItemRecord {
                scope: row.get(0)?,
                slug: row.get(1)?,
                value: row.get(2)?,
                sort_index: row.get(3)?,
                updated_at: row.get(4)?,
            })
        })?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    fn list_model_catalog_string_items_for_kinds(
        &self,
        scope: &str,
        item_kinds: &[&str],
    ) -> rusqlite::Result<Vec<(String, ModelCatalogStringItemRecord)>> {
        if item_kinds.is_empty() {
            return Ok(Vec::new());
        }
        let placeholders = (0..item_kinds.len())
            .map(|index| format!("?{}", index + 2))
            .collect::<Vec<_>>()
            .join(", ");
        let sql = model_catalog_string_items_for_kinds_sql(&placeholders);
        let mut values = Vec::with_capacity(item_kinds.len() + 1);
        values.push(SqlValue::Text(scope.trim().to_string()));
        values.extend(
            item_kinds
                .iter()
                .map(|item_kind| SqlValue::Text(item_kind.to_string())),
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params_from_iter(values), |row| {
            Ok((
                row.get::<_, String>(0)?,
                ModelCatalogStringItemRecord {
                    scope: row.get(1)?,
                    slug: row.get(2)?,
                    value: row.get(3)?,
                    sort_index: row.get(4)?,
                    updated_at: row.get(5)?,
                },
            ))
        })?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    fn ensure_model_catalog_child_tables(&self) -> rusqlite::Result<()> {
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_model_catalog_models_scope_order
               ON model_catalog_models(scope, sort_index, updated_at DESC, slug);
             CREATE INDEX IF NOT EXISTS idx_model_catalog_models_scope_supported_in_api
               ON model_catalog_models(scope, supported_in_api, sort_index, slug);
             CREATE INDEX IF NOT EXISTS idx_model_catalog_models_scope_visibility
               ON model_catalog_models(scope, visibility, sort_index, slug);
             CREATE TABLE IF NOT EXISTS model_catalog_reasoning_levels (
                scope TEXT NOT NULL,
                slug TEXT NOT NULL,
                effort TEXT NOT NULL,
                description TEXT NOT NULL,
                extra_json TEXT NOT NULL DEFAULT '{}',
                sort_index INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (scope, slug, effort)
             );
             CREATE INDEX IF NOT EXISTS idx_model_catalog_reasoning_levels_scope_sort
               ON model_catalog_reasoning_levels(scope, slug, sort_index, effort);
             CREATE TABLE IF NOT EXISTS model_catalog_string_items (
                scope TEXT NOT NULL,
                slug TEXT NOT NULL,
                item_kind TEXT NOT NULL,
                value TEXT NOT NULL,
                sort_index INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (scope, slug, item_kind, value)
             );
             CREATE INDEX IF NOT EXISTS idx_model_catalog_string_items_scope_kind_sort
               ON model_catalog_string_items(scope, item_kind, slug, sort_index, value);",
        )?;
        self.conn.execute_batch(
            "DROP TABLE IF EXISTS model_catalog_additional_speed_tiers;
             DROP TABLE IF EXISTS model_catalog_experimental_supported_tools;
             DROP TABLE IF EXISTS model_catalog_input_modalities;
             DROP TABLE IF EXISTS model_catalog_available_in_plans;",
        )?;
        Ok(())
    }

    fn rebuild_model_catalog_models_without_legacy_json(&self) -> rusqlite::Result<()> {
        if !self.has_table("model_catalog_models")?
            || !self.has_column("model_catalog_models", "model_json")?
        {
            return Ok(());
        }

        self.ensure_column("model_catalog_models", "description", "TEXT")?;
        self.ensure_column(
            "model_catalog_models",
            "source_kind",
            "TEXT NOT NULL DEFAULT 'remote'",
        )?;
        self.ensure_column(
            "model_catalog_models",
            "user_edited",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        self.ensure_column("model_catalog_models", "default_reasoning_level", "TEXT")?;
        self.ensure_column("model_catalog_models", "shell_type", "TEXT")?;
        self.ensure_column("model_catalog_models", "visibility", "TEXT")?;
        self.ensure_column("model_catalog_models", "supported_in_api", "INTEGER")?;
        self.ensure_column("model_catalog_models", "priority", "INTEGER")?;
        self.ensure_column("model_catalog_models", "availability_nux_json", "TEXT")?;
        self.ensure_column("model_catalog_models", "upgrade_json", "TEXT")?;
        self.ensure_column("model_catalog_models", "base_instructions", "TEXT")?;
        self.ensure_column("model_catalog_models", "model_messages_json", "TEXT")?;
        self.ensure_column(
            "model_catalog_models",
            "supports_reasoning_summaries",
            "INTEGER",
        )?;
        self.ensure_column("model_catalog_models", "default_reasoning_summary", "TEXT")?;
        self.ensure_column("model_catalog_models", "support_verbosity", "INTEGER")?;
        self.ensure_column("model_catalog_models", "default_verbosity_json", "TEXT")?;
        self.ensure_column("model_catalog_models", "apply_patch_tool_type", "TEXT")?;
        self.ensure_column("model_catalog_models", "web_search_tool_type", "TEXT")?;
        self.ensure_column("model_catalog_models", "truncation_mode", "TEXT")?;
        self.ensure_column("model_catalog_models", "truncation_limit", "INTEGER")?;
        self.ensure_column("model_catalog_models", "truncation_extra_json", "TEXT")?;
        self.ensure_column(
            "model_catalog_models",
            "supports_parallel_tool_calls",
            "INTEGER",
        )?;
        self.ensure_column(
            "model_catalog_models",
            "supports_image_detail_original",
            "INTEGER",
        )?;
        self.ensure_column("model_catalog_models", "context_window", "INTEGER")?;
        self.ensure_column(
            "model_catalog_models",
            "auto_compact_token_limit",
            "INTEGER",
        )?;
        self.ensure_column(
            "model_catalog_models",
            "effective_context_window_percent",
            "INTEGER",
        )?;
        self.ensure_column(
            "model_catalog_models",
            "minimal_client_version_json",
            "TEXT",
        )?;
        self.ensure_column("model_catalog_models", "supports_search_tool", "INTEGER")?;
        self.ensure_column(
            "model_catalog_models",
            "extra_json",
            "TEXT NOT NULL DEFAULT '{}'",
        )?;
        self.ensure_model_catalog_child_tables()?;

        let legacy_rows = self.read_legacy_model_catalog_model_rows()?;
        let mut model_rows = Vec::new();
        let mut reasoning_rows = Vec::new();
        let mut additional_speed_tiers = Vec::new();
        let mut experimental_supported_tools = Vec::new();
        let mut input_modalities = Vec::new();
        let mut available_in_plans = Vec::new();

        for row in legacy_rows {
            let parsed = row
                .model_json
                .as_deref()
                .and_then(|raw| serde_json::from_str::<ModelInfo>(raw).ok());
            model_rows.push(model_record_from_legacy_row(&row, parsed.as_ref())?);
            if let Some(model) = parsed.as_ref() {
                reasoning_rows.extend(reasoning_records_from_model(
                    &row.scope,
                    &model.slug,
                    &model.supported_reasoning_levels,
                    row.updated_at,
                )?);
                additional_speed_tiers.extend(string_records_from_values(
                    &row.scope,
                    &model.slug,
                    &model.additional_speed_tiers,
                    row.updated_at,
                ));
                experimental_supported_tools.extend(string_records_from_values(
                    &row.scope,
                    &model.slug,
                    &model.experimental_supported_tools,
                    row.updated_at,
                ));
                input_modalities.extend(string_records_from_values(
                    &row.scope,
                    &model.slug,
                    &model.input_modalities,
                    row.updated_at,
                ));
                available_in_plans.extend(string_records_from_values(
                    &row.scope,
                    &model.slug,
                    &model.available_in_plans,
                    row.updated_at,
                ));
            }
        }

        let tx = self.conn.unchecked_transaction()?;
        tx.execute("DROP TABLE IF EXISTS model_catalog_models_rebuilt", [])?;
        tx.execute(
            "CREATE TABLE model_catalog_models_rebuilt (
                scope TEXT NOT NULL,
                slug TEXT NOT NULL,
                display_name TEXT NOT NULL,
                source_kind TEXT NOT NULL DEFAULT 'remote',
                user_edited INTEGER NOT NULL DEFAULT 0,
                description TEXT,
                default_reasoning_level TEXT,
                shell_type TEXT,
                visibility TEXT,
                supported_in_api INTEGER,
                priority INTEGER,
                availability_nux_json TEXT,
                upgrade_json TEXT,
                base_instructions TEXT,
                model_messages_json TEXT,
                supports_reasoning_summaries INTEGER,
                default_reasoning_summary TEXT,
                support_verbosity INTEGER,
                default_verbosity_json TEXT,
                apply_patch_tool_type TEXT,
                web_search_tool_type TEXT,
                truncation_mode TEXT,
                truncation_limit INTEGER,
                truncation_extra_json TEXT,
                supports_parallel_tool_calls INTEGER,
                supports_image_detail_original INTEGER,
                context_window INTEGER,
                auto_compact_token_limit INTEGER,
                effective_context_window_percent INTEGER,
                minimal_client_version_json TEXT,
                supports_search_tool INTEGER,
                extra_json TEXT NOT NULL DEFAULT '{}',
                sort_index INTEGER NOT NULL DEFAULT 0,
                updated_at INTEGER NOT NULL,
                PRIMARY KEY (scope, slug)
            )",
            [],
        )?;
        for model in &model_rows {
            tx.execute(
                "INSERT INTO model_catalog_models_rebuilt (
                    scope, slug, display_name, source_kind, user_edited,
                    description, default_reasoning_level, shell_type, visibility, supported_in_api, priority,
                    availability_nux_json, upgrade_json, base_instructions,
                    model_messages_json, supports_reasoning_summaries,
                    default_reasoning_summary, support_verbosity,
                    default_verbosity_json, apply_patch_tool_type,
                    web_search_tool_type, truncation_mode, truncation_limit,
                    truncation_extra_json, supports_parallel_tool_calls,
                    supports_image_detail_original, context_window,
                    auto_compact_token_limit, effective_context_window_percent,
                    minimal_client_version_json, supports_search_tool,
                    extra_json, sort_index, updated_at
                ) VALUES (
                    ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13,
                    ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24,
                    ?25, ?26, ?27, ?28, ?29, ?30, ?31, ?32, ?33, ?34
                )",
                params![
                    model.scope,
                    model.slug,
                    model.display_name,
                    model.source_kind,
                    model.user_edited,
                    model.description,
                    model.default_reasoning_level,
                    model.shell_type,
                    model.visibility,
                    model.supported_in_api,
                    model.priority,
                    model.availability_nux_json,
                    model.upgrade_json,
                    model.base_instructions,
                    model.model_messages_json,
                    model.supports_reasoning_summaries,
                    model.default_reasoning_summary,
                    model.support_verbosity,
                    model.default_verbosity_json,
                    model.apply_patch_tool_type,
                    model.web_search_tool_type,
                    model.truncation_mode,
                    model.truncation_limit,
                    model.truncation_extra_json,
                    model.supports_parallel_tool_calls,
                    model.supports_image_detail_original,
                    model.context_window,
                    model.auto_compact_token_limit,
                    model.effective_context_window_percent,
                    model.minimal_client_version_json,
                    model.supports_search_tool,
                    model.extra_json,
                    model.sort_index,
                    model.updated_at,
                ],
            )?;
        }
        tx.execute("DROP TABLE model_catalog_models", [])?;
        tx.execute(
            "ALTER TABLE model_catalog_models_rebuilt RENAME TO model_catalog_models",
            [],
        )?;
        tx.commit()?;

        self.ensure_model_catalog_child_tables()?;
        self.upsert_model_catalog_reasoning_levels(&reasoning_rows)?;
        self.upsert_model_catalog_additional_speed_tiers(&additional_speed_tiers)?;
        self.upsert_model_catalog_experimental_supported_tools(&experimental_supported_tools)?;
        self.upsert_model_catalog_input_modalities(&input_modalities)?;
        self.upsert_model_catalog_available_in_plans(&available_in_plans)?;
        Ok(())
    }

    /// 函数 `ensure_model_catalog_models_table`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-12
    ///
    /// # 参数
    /// - self: 参数 self
    ///
    /// # 返回
    /// 返回函数执行结果
    pub(super) fn ensure_model_catalog_models_table(&self) -> rusqlite::Result<()> {
        self.conn.execute_batch(include_str!(
            "../../migrations/047_model_catalog_models.sql"
        ))?;
        self.rebuild_model_catalog_models_without_legacy_json()?;
        self.ensure_column("model_catalog_models", "description", "TEXT")?;
        self.ensure_column(
            "model_catalog_models",
            "source_kind",
            "TEXT NOT NULL DEFAULT 'remote'",
        )?;
        self.ensure_column(
            "model_catalog_models",
            "user_edited",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        self.ensure_column("model_catalog_models", "default_reasoning_level", "TEXT")?;
        self.ensure_column("model_catalog_models", "shell_type", "TEXT")?;
        self.ensure_column("model_catalog_models", "visibility", "TEXT")?;
        self.ensure_column("model_catalog_models", "supported_in_api", "INTEGER")?;
        self.ensure_column("model_catalog_models", "priority", "INTEGER")?;
        self.ensure_column("model_catalog_models", "availability_nux_json", "TEXT")?;
        self.ensure_column("model_catalog_models", "upgrade_json", "TEXT")?;
        self.ensure_column("model_catalog_models", "base_instructions", "TEXT")?;
        self.ensure_column("model_catalog_models", "model_messages_json", "TEXT")?;
        self.ensure_column(
            "model_catalog_models",
            "supports_reasoning_summaries",
            "INTEGER",
        )?;
        self.ensure_column("model_catalog_models", "default_reasoning_summary", "TEXT")?;
        self.ensure_column("model_catalog_models", "support_verbosity", "INTEGER")?;
        self.ensure_column("model_catalog_models", "default_verbosity_json", "TEXT")?;
        self.ensure_column("model_catalog_models", "apply_patch_tool_type", "TEXT")?;
        self.ensure_column("model_catalog_models", "web_search_tool_type", "TEXT")?;
        self.ensure_column("model_catalog_models", "truncation_mode", "TEXT")?;
        self.ensure_column("model_catalog_models", "truncation_limit", "INTEGER")?;
        self.ensure_column("model_catalog_models", "truncation_extra_json", "TEXT")?;
        self.ensure_column(
            "model_catalog_models",
            "supports_parallel_tool_calls",
            "INTEGER",
        )?;
        self.ensure_column(
            "model_catalog_models",
            "supports_image_detail_original",
            "INTEGER",
        )?;
        self.ensure_column("model_catalog_models", "context_window", "INTEGER")?;
        self.ensure_column(
            "model_catalog_models",
            "auto_compact_token_limit",
            "INTEGER",
        )?;
        self.ensure_column(
            "model_catalog_models",
            "effective_context_window_percent",
            "INTEGER",
        )?;
        self.ensure_column(
            "model_catalog_models",
            "minimal_client_version_json",
            "TEXT",
        )?;
        self.ensure_column("model_catalog_models", "supports_search_tool", "INTEGER")?;
        self.ensure_column(
            "model_catalog_models",
            "extra_json",
            "TEXT NOT NULL DEFAULT '{}'",
        )?;
        self.ensure_model_catalog_child_tables()?;
        Ok(())
    }
}

#[derive(Debug, Clone, Default)]
struct LegacyModelCatalogModelRow {
    scope: String,
    slug: String,
    display_name: String,
    source_kind: Option<String>,
    user_edited: Option<bool>,
    description: Option<String>,
    default_reasoning_level: Option<String>,
    shell_type: Option<String>,
    visibility: Option<String>,
    supported_in_api: Option<bool>,
    priority: Option<i64>,
    availability_nux_json: Option<String>,
    upgrade_json: Option<String>,
    base_instructions: Option<String>,
    model_messages_json: Option<String>,
    supports_reasoning_summaries: Option<bool>,
    default_reasoning_summary: Option<String>,
    support_verbosity: Option<bool>,
    default_verbosity_json: Option<String>,
    apply_patch_tool_type: Option<String>,
    web_search_tool_type: Option<String>,
    truncation_mode: Option<String>,
    truncation_limit: Option<i64>,
    truncation_extra_json: Option<String>,
    supports_parallel_tool_calls: Option<bool>,
    supports_image_detail_original: Option<bool>,
    context_window: Option<i64>,
    auto_compact_token_limit: Option<i64>,
    effective_context_window_percent: Option<i64>,
    minimal_client_version_json: Option<String>,
    supports_search_tool: Option<bool>,
    extra_json: Option<String>,
    model_json: Option<String>,
    sort_index: i64,
    updated_at: i64,
}

impl Storage {
    fn read_legacy_model_catalog_model_rows(
        &self,
    ) -> rusqlite::Result<Vec<LegacyModelCatalogModelRow>> {
        let mut stmt = self.conn.prepare(
            "SELECT
                scope, slug, display_name, source_kind, user_edited,
                description, default_reasoning_level, shell_type, visibility, supported_in_api, priority,
                availability_nux_json, upgrade_json, base_instructions,
                model_messages_json, supports_reasoning_summaries,
                default_reasoning_summary, support_verbosity,
                default_verbosity_json, apply_patch_tool_type,
                web_search_tool_type, truncation_mode, truncation_limit,
                truncation_extra_json, supports_parallel_tool_calls,
                supports_image_detail_original, context_window,
                auto_compact_token_limit, effective_context_window_percent,
                minimal_client_version_json, supports_search_tool,
                extra_json, model_json, sort_index, updated_at
             FROM model_catalog_models
             ORDER BY scope ASC, sort_index ASC, updated_at DESC, slug ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok(LegacyModelCatalogModelRow {
                scope: row.get(0)?,
                slug: row.get(1)?,
                display_name: row.get(2)?,
                source_kind: row.get(3)?,
                user_edited: row.get(4)?,
                description: row.get(5)?,
                default_reasoning_level: row.get(6)?,
                shell_type: row.get(7)?,
                visibility: row.get(8)?,
                supported_in_api: row.get(9)?,
                priority: row.get(10)?,
                availability_nux_json: row.get(11)?,
                upgrade_json: row.get(12)?,
                base_instructions: row.get(13)?,
                model_messages_json: row.get(14)?,
                supports_reasoning_summaries: row.get(15)?,
                default_reasoning_summary: row.get(16)?,
                support_verbosity: row.get(17)?,
                default_verbosity_json: row.get(18)?,
                apply_patch_tool_type: row.get(19)?,
                web_search_tool_type: row.get(20)?,
                truncation_mode: row.get(21)?,
                truncation_limit: row.get(22)?,
                truncation_extra_json: row.get(23)?,
                supports_parallel_tool_calls: row.get(24)?,
                supports_image_detail_original: row.get(25)?,
                context_window: row.get(26)?,
                auto_compact_token_limit: row.get(27)?,
                effective_context_window_percent: row.get(28)?,
                minimal_client_version_json: row.get(29)?,
                supports_search_tool: row.get(30)?,
                extra_json: row.get(31)?,
                model_json: row.get(32)?,
                sort_index: row.get(33)?,
                updated_at: row.get(34)?,
            })
        })?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }
}

fn model_record_from_legacy_row(
    row: &LegacyModelCatalogModelRow,
    parsed: Option<&ModelInfo>,
) -> rusqlite::Result<ModelCatalogModelRecord> {
    let truncation = parsed.and_then(|model| model.truncation_policy.clone());
    let parsed_extra = parsed
        .map(|model| serialize_extra_map(&model.extra))
        .transpose()?
        .unwrap_or_else(|| "{}".to_string());
    Ok(ModelCatalogModelRecord {
        scope: row.scope.clone(),
        slug: row.slug.clone(),
        display_name: if row.display_name.trim().is_empty() {
            parsed
                .map(|model| model.display_name.clone())
                .unwrap_or_else(|| row.slug.clone())
        } else {
            row.display_name.clone()
        },
        source_kind: row
            .source_kind
            .clone()
            .filter(|value| !value.trim().is_empty())
            .unwrap_or_else(|| "remote".to_string()),
        user_edited: row.user_edited.unwrap_or(false),
        description: row
            .description
            .clone()
            .or_else(|| parsed.and_then(|model| model.description.clone())),
        default_reasoning_level: row
            .default_reasoning_level
            .clone()
            .or_else(|| parsed.and_then(|model| model.default_reasoning_level.clone())),
        shell_type: row
            .shell_type
            .clone()
            .or_else(|| parsed.and_then(|model| model.shell_type.clone())),
        visibility: row
            .visibility
            .clone()
            .or_else(|| parsed.and_then(|model| model.visibility.clone())),
        supported_in_api: row
            .supported_in_api
            .or_else(|| parsed.map(|model| model.supported_in_api)),
        priority: row.priority.or_else(|| parsed.map(|model| model.priority)),
        availability_nux_json: choose_json_text(
            row.availability_nux_json.as_deref(),
            parsed.and_then(|model| model.availability_nux.as_ref()),
        )?,
        upgrade_json: choose_json_text(
            row.upgrade_json.as_deref(),
            parsed.and_then(|model| model.upgrade.as_ref()),
        )?,
        base_instructions: row
            .base_instructions
            .clone()
            .or_else(|| parsed.and_then(|model| model.base_instructions.clone())),
        model_messages_json: choose_json_text(
            row.model_messages_json.as_deref(),
            parsed.and_then(|model| model.model_messages.as_ref()),
        )?,
        supports_reasoning_summaries: row
            .supports_reasoning_summaries
            .or_else(|| parsed.and_then(|model| model.supports_reasoning_summaries)),
        default_reasoning_summary: row
            .default_reasoning_summary
            .clone()
            .or_else(|| parsed.and_then(|model| model.default_reasoning_summary.clone())),
        support_verbosity: row
            .support_verbosity
            .or_else(|| parsed.and_then(|model| model.support_verbosity)),
        default_verbosity_json: choose_json_text(
            row.default_verbosity_json.as_deref(),
            parsed.and_then(|model| model.default_verbosity.as_ref()),
        )?,
        apply_patch_tool_type: row
            .apply_patch_tool_type
            .clone()
            .or_else(|| parsed.and_then(|model| model.apply_patch_tool_type.clone())),
        web_search_tool_type: row
            .web_search_tool_type
            .clone()
            .or_else(|| parsed.and_then(|model| model.web_search_tool_type.clone())),
        truncation_mode: row
            .truncation_mode
            .clone()
            .or_else(|| truncation.as_ref().map(|policy| policy.mode.clone())),
        truncation_limit: row
            .truncation_limit
            .or_else(|| truncation.as_ref().map(|policy| policy.limit)),
        truncation_extra_json: choose_extra_text(
            row.truncation_extra_json.as_deref(),
            truncation.as_ref().map(|policy| &policy.extra),
        )?,
        supports_parallel_tool_calls: row
            .supports_parallel_tool_calls
            .or_else(|| parsed.and_then(|model| model.supports_parallel_tool_calls)),
        supports_image_detail_original: row
            .supports_image_detail_original
            .or_else(|| parsed.and_then(|model| model.supports_image_detail_original)),
        context_window: row
            .context_window
            .or_else(|| parsed.and_then(|model| model.context_window)),
        auto_compact_token_limit: row
            .auto_compact_token_limit
            .or_else(|| parsed.and_then(|model| model.auto_compact_token_limit)),
        effective_context_window_percent: row
            .effective_context_window_percent
            .or_else(|| parsed.and_then(|model| model.effective_context_window_percent)),
        minimal_client_version_json: choose_json_text(
            row.minimal_client_version_json.as_deref(),
            parsed.and_then(|model| model.minimal_client_version.as_ref()),
        )?,
        supports_search_tool: row
            .supports_search_tool
            .or_else(|| parsed.and_then(|model| model.supports_search_tool)),
        extra_json: row
            .extra_json
            .clone()
            .filter(|text| !text.trim().is_empty())
            .unwrap_or(parsed_extra),
        sort_index: row.sort_index,
        updated_at: row.updated_at,
    })
}

fn reasoning_records_from_model(
    scope: &str,
    slug: &str,
    levels: &[ModelReasoningLevel],
    updated_at: i64,
) -> rusqlite::Result<Vec<ModelCatalogReasoningLevelRecord>> {
    let mut records = Vec::new();
    for (index, level) in levels.iter().enumerate() {
        records.push(ModelCatalogReasoningLevelRecord {
            scope: scope.to_string(),
            slug: slug.to_string(),
            effort: level.effort.clone(),
            description: level.description.clone(),
            extra_json: serialize_extra_map(&level.extra)?,
            sort_index: index as i64,
            updated_at,
        });
    }
    Ok(records)
}

fn string_records_from_values(
    scope: &str,
    slug: &str,
    values: &[String],
    updated_at: i64,
) -> Vec<ModelCatalogStringItemRecord> {
    values
        .iter()
        .enumerate()
        .map(|(index, value)| ModelCatalogStringItemRecord {
            scope: scope.to_string(),
            slug: slug.to_string(),
            value: value.clone(),
            sort_index: index as i64,
            updated_at,
        })
        .collect()
}

fn choose_json_text(
    existing: Option<&str>,
    fallback: Option<&Value>,
) -> rusqlite::Result<Option<String>> {
    match existing {
        Some(text) if !text.trim().is_empty() => Ok(Some(text.to_string())),
        _ => fallback
            .map(|value| {
                serde_json::to_string(value)
                    .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
            })
            .transpose(),
    }
}

fn choose_extra_text(
    existing: Option<&str>,
    fallback: Option<&BTreeMap<String, Value>>,
) -> rusqlite::Result<Option<String>> {
    match existing {
        Some(text) if !text.trim().is_empty() => Ok(Some(text.to_string())),
        _ => fallback.map(serialize_extra_map).transpose(),
    }
}

fn serialize_extra_map(extra: &BTreeMap<String, Value>) -> rusqlite::Result<String> {
    serde_json::to_string(extra)
        .map_err(|err| rusqlite::Error::ToSqlConversionFailure(Box::new(err)))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn model_record(slug: &str) -> ModelCatalogModelRecord {
        ModelCatalogModelRecord {
            scope: "default".to_string(),
            slug: slug.to_string(),
            display_name: slug.to_string(),
            source_kind: "remote".to_string(),
            visibility: Some("list".to_string()),
            supported_in_api: Some(true),
            extra_json: "{}".to_string(),
            ..ModelCatalogModelRecord::default()
        }
    }

    fn collect_query_plan_details(storage: &Storage, sql: &str) -> Vec<String> {
        let mut stmt = storage.conn.prepare(sql).expect("prepare explain");
        let mut rows = stmt.query([]).expect("query explain");
        collect_query_plan_rows(&mut rows)
    }

    fn collect_query_plan_details_with_params(
        storage: &Storage,
        sql: &str,
        params: Vec<SqlValue>,
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

    fn assert_no_temp_ordering(details: &[String], label: &str) {
        assert!(
            !details
                .iter()
                .any(|detail| detail.contains("use temp b-tree for order by")),
            "{label} should avoid temp ORDER BY sorting, got {details:?}"
        );
    }

    #[test]
    fn count_available_model_catalog_models_uses_visibility_and_api_filters() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let mut hidden = model_record("hidden-model");
        hidden.visibility = Some("hide".to_string());
        let mut remote_hidden = model_record("remote-hidden-model");
        remote_hidden.visibility = Some("hidden".to_string());
        let mut unsupported = model_record("unsupported-model");
        unsupported.supported_in_api = Some(false);
        let mut default_supported = model_record("default-supported-model");
        default_supported.supported_in_api = None;
        storage
            .upsert_model_catalog_models(&[
                model_record("available-model"),
                hidden,
                remote_hidden,
                unsupported,
                default_supported,
            ])
            .expect("seed models");

        assert_eq!(
            storage
                .count_available_model_catalog_models("default")
                .expect("count models"),
            2
        );
    }

    #[test]
    fn model_catalog_model_exists_checks_single_slug() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        storage
            .upsert_model_catalog_models(&[model_record("available-model")])
            .expect("seed model");

        assert!(storage
            .model_catalog_model_exists("default", "available-model")
            .expect("check existing model"));
        assert!(!storage
            .model_catalog_model_exists("default", "missing-model")
            .expect("check missing model"));
    }

    #[test]
    fn list_existing_model_catalog_slugs_filters_requested_slugs() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        storage
            .upsert_model_catalog_models(&[
                model_record("available-a"),
                model_record("available-b"),
                model_record("unrequested"),
            ])
            .expect("seed models");

        let slugs = storage
            .list_existing_model_catalog_slugs(
                "default",
                &[
                    "available-b".to_string(),
                    "missing".to_string(),
                    "available-a".to_string(),
                    "available-a".to_string(),
                ],
            )
            .expect("list existing slugs");

        assert_eq!(slugs, vec!["available-a", "available-b"]);
    }

    #[test]
    fn model_catalog_slug_chunk_queries_defer_final_ordering_to_rust() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let existing_sql =
            existing_model_catalog_slugs_chunk_sql("slug IN ('available-a', 'available-b')");
        let existing_details = collect_query_plan_details_with_params(
            &storage,
            &format!("EXPLAIN QUERY PLAN {existing_sql}"),
            vec![SqlValue::Text("default".to_string())],
        );
        let remote_sql = remote_unedited_model_catalog_models_for_slugs_chunk_sql(
            "slug IN ('available-a', 'available-b')",
        );
        let remote_details = collect_query_plan_details_with_params(
            &storage,
            &format!("EXPLAIN QUERY PLAN {remote_sql}"),
            vec![SqlValue::Text("default".to_string())],
        );

        assert!(
            existing_details
                .iter()
                .any(|detail| detail.contains("model_catalog_models") && detail.contains("index")),
            "existing slug chunk query should use a model catalog lookup index, got {existing_details:?}"
        );
        assert!(
            remote_details
                .iter()
                .any(|detail| detail.contains("model_catalog_models") && detail.contains("index")),
            "remote catalog chunk query should use a model catalog lookup index, got {remote_details:?}"
        );
        assert!(
            !existing_details
                .iter()
                .any(|detail| detail.contains("use temp b-tree for order by")),
            "existing slug chunk query should avoid per-chunk ORDER BY temp sorting, got {existing_details:?}"
        );
        assert!(
            !remote_details
                .iter()
                .any(|detail| detail.contains("use temp b-tree for order by")),
            "remote catalog chunk query should avoid per-chunk ORDER BY temp sorting, got {remote_details:?}"
        );
    }

    #[test]
    fn list_remote_unedited_model_catalog_models_for_slugs_preserves_catalog_order() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let mut later = model_record("later");
        later.sort_index = 2;
        later.updated_at = 10;
        let mut first = model_record("first");
        first.sort_index = 1;
        first.updated_at = 5;
        let mut newer_same_sort = model_record("newer-same-sort");
        newer_same_sort.sort_index = 1;
        newer_same_sort.updated_at = 20;
        storage
            .upsert_model_catalog_models(&[later, first, newer_same_sort])
            .expect("seed models");

        let rows = storage
            .list_remote_unedited_model_catalog_models_for_slugs(
                "default",
                &[
                    "later".to_string(),
                    "first".to_string(),
                    "newer-same-sort".to_string(),
                ],
            )
            .expect("list remote catalog models");

        assert_eq!(
            rows.into_iter().map(|row| row.slug).collect::<Vec<_>>(),
            vec![
                "newer-same-sort".to_string(),
                "first".to_string(),
                "later".to_string()
            ]
        );
    }

    #[test]
    fn list_remote_unedited_model_catalog_slugs_filters_in_sql() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let mut user_edited = model_record("user-edited");
        user_edited.user_edited = true;
        let mut custom = model_record("custom-model");
        custom.source_kind = "custom".to_string();
        storage
            .upsert_model_catalog_models(&[
                model_record("remote-a"),
                user_edited,
                custom,
                model_record("remote-b"),
            ])
            .expect("seed models");

        let slugs = storage
            .list_remote_unedited_model_catalog_slugs("default")
            .expect("list remote unedited slugs");

        assert_eq!(slugs, vec!["remote-a", "remote-b"]);
    }

    #[test]
    fn list_api_available_model_catalog_slugs_filters_in_sql() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let mut hidden = model_record("hidden-model");
        hidden.visibility = Some("hidden".to_string());
        let mut hide = model_record("hide-model");
        hide.visibility = Some("hide".to_string());
        let mut disabled = model_record("disabled-model");
        disabled.visibility = Some("disabled".to_string());
        let mut unavailable = model_record("unavailable-model");
        unavailable.visibility = Some("unavailable".to_string());
        let mut unsupported = model_record("unsupported-model");
        unsupported.supported_in_api = Some(false);
        let mut default_supported = model_record("default-supported-model");
        default_supported.supported_in_api = None;
        default_supported.sort_index = 1;
        let mut available = model_record("available-model");
        available.sort_index = 0;
        storage
            .upsert_model_catalog_models(&[
                default_supported,
                hidden,
                hide,
                unsupported,
                disabled,
                unavailable,
                available,
            ])
            .expect("seed models");

        let slugs = storage
            .list_api_available_model_catalog_slugs("default")
            .expect("list api available slugs");

        assert_eq!(slugs, vec!["available-model", "default-supported-model"]);
    }

    #[test]
    fn find_first_api_available_model_catalog_slug_uses_catalog_order() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let mut hidden = model_record("hidden-model");
        hidden.visibility = Some("hidden".to_string());
        hidden.sort_index = 0;
        let mut unsupported = model_record("unsupported-model");
        unsupported.supported_in_api = Some(false);
        unsupported.sort_index = 1;
        let mut selected = model_record("selected-model");
        selected.sort_index = 2;
        let mut later = model_record("later-model");
        later.sort_index = 3;
        storage
            .upsert_model_catalog_models(&[hidden, unsupported, later, selected])
            .expect("seed models");

        let slug = storage
            .find_first_api_available_model_catalog_slug("default")
            .expect("find first api available slug");

        assert_eq!(slug.as_deref(), Some("selected-model"));
    }

    #[test]
    fn list_api_available_model_catalog_slugs_with_prefix_filters_in_sql() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let mut gpt = model_record("gpt-available");
        gpt.sort_index = 1;
        let mut codex = model_record("gpt-codex");
        codex.sort_index = 0;
        let other = model_record("claude-available");
        let mut hidden = model_record("gpt-hidden");
        hidden.visibility = Some("hide".to_string());
        storage
            .upsert_model_catalog_models(&[gpt, codex, other, hidden])
            .expect("seed models");

        let slugs = storage
            .list_api_available_model_catalog_slugs_with_prefix("default", " GPT-")
            .expect("list prefixed api available slugs");

        assert_eq!(slugs, vec!["gpt-codex", "gpt-available"]);
    }

    #[test]
    fn delete_model_catalog_string_item_kinds_deletes_selected_kinds_for_slug() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();
        storage
            .upsert_model_catalog_additional_speed_tiers(&[
                ModelCatalogStringItemRecord {
                    scope: "default".to_string(),
                    slug: "target-model".to_string(),
                    value: "fast".to_string(),
                    sort_index: 0,
                    updated_at: now,
                },
                ModelCatalogStringItemRecord {
                    scope: "default".to_string(),
                    slug: "other-model".to_string(),
                    value: "fast".to_string(),
                    sort_index: 0,
                    updated_at: now,
                },
            ])
            .expect("seed speed tiers");
        storage
            .upsert_model_catalog_experimental_supported_tools(&[ModelCatalogStringItemRecord {
                scope: "default".to_string(),
                slug: "target-model".to_string(),
                value: "web_search".to_string(),
                sort_index: 0,
                updated_at: now,
            }])
            .expect("seed tools");
        storage
            .upsert_model_catalog_input_modalities(&[ModelCatalogStringItemRecord {
                scope: "default".to_string(),
                slug: "target-model".to_string(),
                value: "text".to_string(),
                sort_index: 0,
                updated_at: now,
            }])
            .expect("seed input modalities");

        storage
            .delete_model_catalog_string_item_kinds(
                "default",
                "target-model",
                &["additional_speed_tiers", "experimental_supported_tools"],
            )
            .expect("delete selected string item kinds");

        assert!(storage
            .list_model_catalog_additional_speed_tiers("default")
            .expect("list speed tiers")
            .iter()
            .all(|item| item.slug != "target-model"));
        assert!(storage
            .list_model_catalog_experimental_supported_tools("default")
            .expect("list tools")
            .iter()
            .all(|item| item.slug != "target-model"));
        assert_eq!(
            storage
                .list_model_catalog_input_modalities("default")
                .expect("list input modalities")
                .into_iter()
                .map(|item| item.slug)
                .collect::<Vec<_>>(),
            vec!["target-model".to_string()]
        );
        assert_eq!(
            storage
                .list_model_catalog_additional_speed_tiers("default")
                .expect("list remaining speed tiers")
                .into_iter()
                .map(|item| item.slug)
                .collect::<Vec<_>>(),
            vec!["other-model".to_string()]
        );
    }

    #[test]
    fn list_model_catalog_models_uses_scope_order_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let sql = model_catalog_models_for_scope_sql();
        let details = collect_query_plan_details_with_params(
            &storage,
            &format!("EXPLAIN QUERY PLAN {sql}"),
            vec![SqlValue::Text("default".to_string())],
        );

        assert!(
            details
                .iter()
                .any(|detail| detail.contains("idx_model_catalog_models_scope_order")),
            "expected model catalog scope order index, got {details:?}"
        );
    }

    #[test]
    fn model_catalog_ordered_slug_queries_use_scope_order_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let remote_sql = model_catalog_ordered_slug_sql(
            &["source_kind = 'remote'", "COALESCE(user_edited, 0) = 0"],
            None,
        );
        let api_sql =
            model_catalog_ordered_slug_sql(&[MODEL_CATALOG_API_AVAILABLE_CONDITION], None);
        let first_api_sql =
            model_catalog_ordered_slug_sql(&[MODEL_CATALOG_API_AVAILABLE_CONDITION], Some(1));
        let prefixed_api_sql = model_catalog_ordered_slug_sql(
            &[
                MODEL_CATALOG_API_AVAILABLE_CONDITION,
                "LOWER(TRIM(slug)) LIKE ?2",
            ],
            None,
        );

        for (label, sql, params) in [
            (
                "remote unedited catalog slug list",
                format!("EXPLAIN QUERY PLAN {remote_sql}"),
                vec![SqlValue::Text("default".to_string())],
            ),
            (
                "API available catalog slug list",
                format!("EXPLAIN QUERY PLAN {api_sql}"),
                vec![SqlValue::Text("default".to_string())],
            ),
            (
                "first API available catalog slug",
                format!("EXPLAIN QUERY PLAN {first_api_sql}"),
                vec![SqlValue::Text("default".to_string())],
            ),
            (
                "prefixed API available catalog slug list",
                format!("EXPLAIN QUERY PLAN {prefixed_api_sql}"),
                vec![
                    SqlValue::Text("default".to_string()),
                    SqlValue::Text("gpt-%".to_string()),
                ],
            ),
        ] {
            let details = collect_query_plan_details_with_params(&storage, &sql, params);
            assert!(
                details
                    .iter()
                    .any(|detail| detail.contains("idx_model_catalog_models_scope_order")),
                "{label} should use model catalog scope order index, got {details:?}"
            );
            assert_no_temp_ordering(&details, label);
        }
    }

    #[test]
    fn model_catalog_child_list_queries_use_existing_order_indexes() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let reasoning_details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                model_catalog_reasoning_levels_list_sql()
            ),
            vec![SqlValue::Text("default".to_string())],
        );
        assert!(
            reasoning_details
                .iter()
                .any(|detail| detail.contains("idx_model_catalog_reasoning_levels_scope_sort")),
            "reasoning level list should use scope sort index, got {reasoning_details:?}"
        );
        assert_no_temp_ordering(&reasoning_details, "reasoning level list");

        let string_item_details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                model_catalog_string_items_list_sql()
            ),
            vec![
                SqlValue::Text("default".to_string()),
                SqlValue::Text(STRING_ITEM_KIND_INPUT_MODALITIES.to_string()),
            ],
        );
        assert!(
            string_item_details
                .iter()
                .any(|detail| detail.contains("idx_model_catalog_string_items_scope_kind_sort")),
            "string item list should use scope kind sort index, got {string_item_details:?}"
        );
        assert_no_temp_ordering(&string_item_details, "string item list");

        let multi_kind_sql = model_catalog_string_items_for_kinds_sql("?2, ?3");
        let multi_kind_details = collect_query_plan_details_with_params(
            &storage,
            &format!("EXPLAIN QUERY PLAN {multi_kind_sql}"),
            vec![
                SqlValue::Text("default".to_string()),
                SqlValue::Text(STRING_ITEM_KIND_INPUT_MODALITIES.to_string()),
                SqlValue::Text(STRING_ITEM_KIND_AVAILABLE_IN_PLANS.to_string()),
            ],
        );
        assert!(
            multi_kind_details
                .iter()
                .any(|detail| detail.contains("idx_model_catalog_string_items_scope_kind_sort")),
            "multi-kind string item list should use scope kind sort index, got {multi_kind_details:?}"
        );
        assert_no_temp_ordering(&multi_kind_details, "multi-kind string item list");
    }

    #[test]
    fn model_catalog_model_exists_uses_primary_key_lookup() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let details = collect_query_plan_details(
            &storage,
            "EXPLAIN QUERY PLAN
             SELECT 1
             FROM model_catalog_models
             WHERE scope = 'default'
               AND slug = 'available-model'
             LIMIT 1",
        );

        assert!(details.iter().any(
            |detail| detail.contains("search model_catalog_models") && detail.contains("index")
        ));
    }

    #[test]
    fn model_catalog_delete_helpers_use_existing_lookup_indexes() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let model_delete_details = collect_query_plan_details_with_params(
            &storage,
            &format!("EXPLAIN QUERY PLAN {}", delete_model_catalog_model_sql()),
            vec![
                SqlValue::Text("default".to_string()),
                SqlValue::Text("gpt-test".to_string()),
            ],
        );
        let reasoning_delete_details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                delete_model_catalog_reasoning_levels_sql()
            ),
            vec![
                SqlValue::Text("default".to_string()),
                SqlValue::Text("gpt-test".to_string()),
            ],
        );
        let string_item_delete_details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                delete_model_catalog_string_items_sql()
            ),
            vec![
                SqlValue::Text("default".to_string()),
                SqlValue::Text("gpt-test".to_string()),
                SqlValue::Text(STRING_ITEM_KIND_INPUT_MODALITIES.to_string()),
            ],
        );
        let string_item_kind_sql = delete_model_catalog_string_item_kinds_sql(
            "item_kind IN ('input_modalities', 'available_in_plans')",
        );
        let string_item_kind_delete_details = collect_query_plan_details_with_params(
            &storage,
            &format!("EXPLAIN QUERY PLAN {string_item_kind_sql}"),
            vec![
                SqlValue::Text("default".to_string()),
                SqlValue::Text("gpt-test".to_string()),
            ],
        );

        assert!(
            model_delete_details.iter().any(|detail| {
                detail.contains("model_catalog_models") && detail.contains("index")
            }),
            "model catalog model delete should use a model catalog lookup index, got {model_delete_details:?}"
        );
        assert!(
            reasoning_delete_details.iter().any(|detail| {
                detail.contains("idx_model_catalog_reasoning_levels_scope_sort")
                    || detail.contains("sqlite_autoindex_model_catalog_reasoning_levels_1")
            }),
            "reasoning level delete should use an existing reasoning-level lookup index, got {reasoning_delete_details:?}"
        );
        for (label, details) in [
            (
                "single string item kind delete",
                &string_item_delete_details,
            ),
            (
                "multi string item kind delete",
                &string_item_kind_delete_details,
            ),
        ] {
            assert!(
                details.iter().any(|detail| {
                    detail.contains("sqlite_autoindex_model_catalog_string_items_1")
                        || detail.contains("idx_model_catalog_string_items_scope_kind_sort")
                }),
                "{label} should use an existing string item lookup index, got {details:?}"
            );
        }
    }

    #[test]
    fn model_catalog_scope_lookup_uses_primary_key_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let details = collect_query_plan_details_with_params(
            &storage,
            &format!("EXPLAIN QUERY PLAN {}", model_catalog_scope_by_scope_sql()),
            vec![SqlValue::Text("default".to_string())],
        );

        assert!(
            details.iter().any(|detail| {
                detail.contains("search model_catalog_scopes") && detail.contains("index")
            }),
            "scope lookup should use the model catalog scope primary-key index, got {details:?}"
        );
    }
}
