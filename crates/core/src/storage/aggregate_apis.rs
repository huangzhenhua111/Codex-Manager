use rusqlite::{params, params_from_iter, types::Value, Result, Row};
use std::collections::HashMap;

use super::key_id_filters::{normalize_text_ids, text_id_in_clause, SQLITE_IN_CLAUSE_BATCH_SIZE};
use super::model_sources::{
    delete_model_source_mapping_preferences_for_source_sql,
    delete_model_source_mappings_for_source_sql, delete_model_source_models_for_source_sql,
};
use super::{
    now_ts, AggregateApi, AggregateApiDashboardSourceMetadata, AggregateApiListSnapshot,
    AggregateApiListSummary, AggregateApiOverviewStats, AggregateApiQuotaSourceSummary,
    AggregateApiSecretConfig, AggregateApiSupplierIdentity, AggregateApiSupplierModel,
    AggregateApiUpdateConfig, AggregateApiWithSecrets, Storage,
};

const AGGREGATE_API_SELECT_SQL: &str = "SELECT
    id,
    provider_type,
    supplier_name,
    sort,
    url,
    auth_type,
    auth_params_json,
    action,
    model_override,
    status,
    created_at,
    updated_at,
    last_test_at,
    last_test_status,
    last_test_error,
    balance_query_enabled,
    balance_query_template,
    balance_query_base_url,
    balance_query_user_id,
    balance_query_config_json,
    last_balance_at,
    last_balance_status,
    last_balance_error,
    last_balance_json
 FROM aggregate_apis";
const AGGREGATE_API_MODEL_SOURCE_KIND: &str = "aggregate_api";
const AGGREGATE_API_ACTIVE_STATUS_CONDITION: &str = "LOWER(TRIM(COALESCE(status, ''))) = 'active'";
const AGGREGATE_API_NORMALIZED_PROVIDER_SQL: &str =
    "REPLACE(LOWER(TRIM(COALESCE(provider_type, ''))), '-', '_')";

fn aggregate_api_by_id_sql() -> String {
    format!(
        "{AGGREGATE_API_SELECT_SQL}
         WHERE id = ?1
         LIMIT 1"
    )
}

fn aggregate_api_with_secrets_by_id_sql() -> &'static str {
    "SELECT
        a.id,
        a.provider_type,
        a.supplier_name,
        a.sort,
        a.url,
        a.auth_type,
        a.auth_params_json,
        a.action,
        a.model_override,
        a.status,
        a.created_at,
        a.updated_at,
        a.last_test_at,
        a.last_test_status,
        a.last_test_error,
        a.balance_query_enabled,
        a.balance_query_template,
        a.balance_query_base_url,
        a.balance_query_user_id,
        a.balance_query_config_json,
        a.last_balance_at,
        a.last_balance_status,
        a.last_balance_error,
        a.last_balance_json,
        s.secret_value,
        bs.access_token
     FROM aggregate_apis a
     LEFT JOIN aggregate_api_secrets s ON s.aggregate_api_id = a.id
     LEFT JOIN aggregate_api_balance_secrets bs ON bs.aggregate_api_id = a.id
     WHERE a.id = ?1
     LIMIT 1"
}

fn aggregate_api_status_by_id_sql() -> &'static str {
    "SELECT status
     FROM aggregate_apis
     WHERE id = ?1
     LIMIT 1"
}

fn aggregate_api_auth_type_by_id_sql() -> &'static str {
    "SELECT auth_type
     FROM aggregate_apis
     WHERE id = ?1
     LIMIT 1"
}

fn aggregate_api_secret_config_by_id_sql() -> &'static str {
    "SELECT a.auth_type, s.secret_value
     FROM aggregate_apis a
     LEFT JOIN aggregate_api_secrets s ON s.aggregate_api_id = a.id
     WHERE a.id = ?1
     LIMIT 1"
}

fn aggregate_api_update_config_by_id_sql() -> &'static str {
    "SELECT
        auth_type,
        balance_query_enabled,
        balance_query_template,
        balance_query_base_url,
        balance_query_user_id,
        balance_query_config_json
     FROM aggregate_apis
     WHERE id = ?1
     LIMIT 1"
}

fn update_aggregate_api_url_sql() -> &'static str {
    "UPDATE aggregate_apis SET url = ?1, updated_at = ?2 WHERE id = ?3"
}

fn update_aggregate_api_supplier_name_sql() -> &'static str {
    "UPDATE aggregate_apis SET supplier_name = ?1, updated_at = ?2 WHERE id = ?3"
}

fn update_aggregate_api_sort_sql() -> &'static str {
    "UPDATE aggregate_apis SET sort = ?1, updated_at = ?2 WHERE id = ?3"
}

fn update_aggregate_api_status_sql() -> &'static str {
    "UPDATE aggregate_apis SET status = ?1, updated_at = ?2 WHERE id = ?3"
}

fn update_aggregate_api_provider_type_sql() -> &'static str {
    "UPDATE aggregate_apis SET provider_type = ?1, updated_at = ?2 WHERE id = ?3"
}

fn update_aggregate_api_auth_type_sql() -> &'static str {
    "UPDATE aggregate_apis SET auth_type = ?1, updated_at = ?2 WHERE id = ?3"
}

fn update_aggregate_api_auth_params_json_sql() -> &'static str {
    "UPDATE aggregate_apis SET auth_params_json = ?1, updated_at = ?2 WHERE id = ?3"
}

fn update_aggregate_api_action_sql() -> &'static str {
    "UPDATE aggregate_apis SET action = ?1, updated_at = ?2 WHERE id = ?3"
}

fn update_aggregate_api_model_override_sql() -> &'static str {
    "UPDATE aggregate_apis SET model_override = ?1, updated_at = ?2 WHERE id = ?3"
}

fn update_aggregate_api_balance_query_sql() -> &'static str {
    "UPDATE aggregate_apis
     SET balance_query_enabled = ?1,
         balance_query_template = ?2,
         balance_query_base_url = ?3,
         balance_query_user_id = ?4,
         balance_query_config_json = ?5,
         updated_at = ?6
     WHERE id = ?7"
}

fn update_aggregate_api_balance_result_sql() -> &'static str {
    "UPDATE aggregate_apis
     SET last_balance_at = ?1,
         last_balance_status = ?2,
         last_balance_error = ?3,
         last_balance_json = ?4,
         updated_at = ?1
     WHERE id = ?5"
}

fn update_aggregate_api_test_result_sql() -> &'static str {
    "UPDATE aggregate_apis
     SET last_test_at = ?1,
         last_test_status = ?2,
         last_test_error = ?3,
         updated_at = ?1
     WHERE id = ?4"
}

fn update_aggregate_api_last_test_error_sql() -> &'static str {
    "UPDATE aggregate_apis SET last_test_error = ?1 WHERE id = ?2"
}

fn delete_aggregate_api_by_id_sql() -> &'static str {
    "DELETE FROM aggregate_apis WHERE id = ?1"
}

fn delete_aggregate_api_secret_by_id_sql() -> &'static str {
    "DELETE FROM aggregate_api_secrets WHERE aggregate_api_id = ?1"
}

fn delete_aggregate_api_balance_secret_by_id_sql() -> &'static str {
    "DELETE FROM aggregate_api_balance_secrets WHERE aggregate_api_id = ?1"
}

fn delete_aggregate_api_supplier_model_sql() -> &'static str {
    "DELETE FROM aggregate_api_supplier_models
     WHERE supplier_key = ?1 AND provider_type = ?2 AND upstream_model = ?3"
}

fn aggregate_api_exists_sql() -> &'static str {
    "SELECT EXISTS(SELECT 1 FROM aggregate_apis WHERE id = ?1)"
}

fn aggregate_api_supplier_identity_by_id_sql() -> &'static str {
    "SELECT id, provider_type, supplier_name, url
     FROM aggregate_apis
     WHERE id = ?1
     LIMIT 1"
}

fn aggregate_api_secret_by_id_sql() -> &'static str {
    "SELECT secret_value FROM aggregate_api_secrets WHERE aggregate_api_id = ?1 LIMIT 1"
}

fn aggregate_api_balance_secret_by_id_sql() -> &'static str {
    "SELECT access_token FROM aggregate_api_balance_secrets WHERE aggregate_api_id = ?1 LIMIT 1"
}

impl Storage {
    /// 函数 `insert_aggregate_api`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - api: 参数 api
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn insert_aggregate_api(&self, api: &AggregateApi) -> Result<()> {
        self.conn.execute(
            "INSERT OR REPLACE INTO aggregate_apis (
                id,
                provider_type,
                supplier_name,
                sort,
                url,
                auth_type,
                auth_params_json,
                action,
                model_override,
                status,
                created_at,
                updated_at,
                last_test_at,
                last_test_status,
                last_test_error,
                balance_query_enabled,
                balance_query_template,
                balance_query_base_url,
                balance_query_user_id,
                balance_query_config_json,
                last_balance_at,
                last_balance_status,
                last_balance_error,
                last_balance_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?18, ?19, ?20, ?21, ?22, ?23, ?24)",
            params![
                &api.id,
                &api.provider_type,
                &api.supplier_name,
                api.sort,
                &api.url,
                &api.auth_type,
                &api.auth_params_json,
                &api.action,
                &api.model_override,
                &api.status,
                api.created_at,
                api.updated_at,
                &api.last_test_at,
                &api.last_test_status,
                &api.last_test_error,
                api.balance_query_enabled,
                &api.balance_query_template,
                &api.balance_query_base_url,
                &api.balance_query_user_id,
                &api.balance_query_config_json,
                &api.last_balance_at,
                &api.last_balance_status,
                &api.last_balance_error,
                &api.last_balance_json,
            ],
        )?;
        Ok(())
    }

    /// 函数 `list_aggregate_apis`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn list_aggregate_apis(&self) -> Result<Vec<AggregateApi>> {
        let sql = aggregate_api_list_sql();
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(map_aggregate_api_row(row)?);
        }
        Ok(out)
    }

    pub fn list_aggregate_api_summaries(&self) -> Result<Vec<AggregateApiListSummary>> {
        let sql = aggregate_api_list_sql();
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map([], map_aggregate_api_list_summary_row)?;
        rows.collect()
    }

    pub fn load_aggregate_api_list_snapshot(&self) -> Result<AggregateApiListSnapshot> {
        let items = self.list_aggregate_api_summaries()?;
        let api_ids = items.iter().map(|item| item.id.clone()).collect::<Vec<_>>();
        let model_assignments = self.list_quota_source_model_assignments_for_sources(
            AGGREGATE_API_MODEL_SOURCE_KIND,
            &api_ids,
        )?;
        Ok(AggregateApiListSnapshot {
            items,
            model_assignments,
        })
    }

    /// 函数 `find_aggregate_api_by_id`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - api_id: 参数 api_id
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn find_aggregate_api_by_id(&self, api_id: &str) -> Result<Option<AggregateApi>> {
        let sql = aggregate_api_by_id_sql();
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query([api_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(map_aggregate_api_row(row)?))
        } else {
            Ok(None)
        }
    }

    pub fn find_aggregate_api_with_secrets_by_id(
        &self,
        api_id: &str,
    ) -> Result<Option<AggregateApiWithSecrets>> {
        let mut stmt = self.conn.prepare(aggregate_api_with_secrets_by_id_sql())?;
        let mut rows = stmt.query([api_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(map_aggregate_api_with_secrets_row(row)?))
        } else {
            Ok(None)
        }
    }

    pub fn find_aggregate_api_status_by_id(&self, api_id: &str) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare(aggregate_api_status_by_id_sql())?;
        let mut rows = stmt.query([api_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    pub fn find_aggregate_api_auth_type_by_id(&self, api_id: &str) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare(aggregate_api_auth_type_by_id_sql())?;
        let mut rows = stmt.query([api_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    pub fn find_aggregate_api_secret_config_by_id(
        &self,
        api_id: &str,
    ) -> Result<Option<AggregateApiSecretConfig>> {
        let mut stmt = self.conn.prepare(aggregate_api_secret_config_by_id_sql())?;
        let mut rows = stmt.query([api_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(AggregateApiSecretConfig {
                auth_type: row.get(0)?,
                secret_value: row.get(1)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn find_aggregate_api_update_config_by_id(
        &self,
        api_id: &str,
    ) -> Result<Option<AggregateApiUpdateConfig>> {
        let mut stmt = self.conn.prepare(aggregate_api_update_config_by_id_sql())?;
        let mut rows = stmt.query([api_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(AggregateApiUpdateConfig {
                auth_type: row.get(0)?,
                balance_query_enabled: row.get(1)?,
                balance_query_template: row.get(2)?,
                balance_query_base_url: row.get(3)?,
                balance_query_user_id: row.get(4)?,
                balance_query_config_json: row.get(5)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn aggregate_api_exists(&self, api_id: &str) -> Result<bool> {
        self.conn
            .query_row(aggregate_api_exists_sql(), [api_id], |row| row.get(0))
    }

    pub fn find_aggregate_api_supplier_identity_by_id(
        &self,
        api_id: &str,
    ) -> Result<Option<AggregateApiSupplierIdentity>> {
        let mut stmt = self
            .conn
            .prepare(aggregate_api_supplier_identity_by_id_sql())?;
        let mut rows = stmt.query([api_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(AggregateApiSupplierIdentity {
                id: row.get(0)?,
                provider_type: row.get(1)?,
                supplier_name: row.get(2)?,
                url: row.get(3)?,
            }))
        } else {
            Ok(None)
        }
    }

    pub fn list_aggregate_apis_for_ids(&self, api_ids: &[String]) -> Result<Vec<AggregateApi>> {
        let api_ids = normalize_text_ids(api_ids);
        if api_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for chunk in api_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(list_aggregate_apis_for_ids_chunk(self, chunk)?);
        }
        out.sort_by(|left, right| {
            left.sort
                .cmp(&right.sort)
                .then_with(|| right.updated_at.cmp(&left.updated_at))
                .then_with(|| left.id.cmp(&right.id))
        });
        Ok(out)
    }

    pub fn list_aggregate_api_dashboard_source_metadata_for_ids(
        &self,
        api_ids: &[String],
    ) -> Result<Vec<AggregateApiDashboardSourceMetadata>> {
        let api_ids = normalize_text_ids(api_ids);
        if api_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for chunk in api_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(list_aggregate_api_dashboard_source_metadata_for_ids_chunk(
                self, chunk,
            )?);
        }
        out.sort_by(|left, right| {
            left.1
                .cmp(&right.1)
                .then_with(|| right.2.cmp(&left.2))
                .then_with(|| left.0.id.cmp(&right.0.id))
        });
        Ok(out.into_iter().map(|item| item.0).collect())
    }

    pub fn list_aggregate_api_ids(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(aggregate_api_list_ids_sql())?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect()
    }

    pub fn list_aggregate_api_quota_source_summaries(
        &self,
    ) -> Result<Vec<AggregateApiQuotaSourceSummary>> {
        let mut stmt = self
            .conn
            .prepare(aggregate_api_quota_source_summaries_list_sql())?;
        let rows = stmt.query_map([], map_aggregate_api_quota_source_summary_row)?;
        rows.collect()
    }

    pub fn list_active_balance_query_aggregate_api_ids(&self) -> Result<Vec<String>> {
        let mut stmt = self
            .conn
            .prepare(active_balance_query_aggregate_api_ids_sql())?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect()
    }

    pub fn list_balance_query_aggregate_api_ids(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(balance_query_aggregate_api_ids_sql())?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect()
    }

    pub fn list_balance_query_aggregate_api_ids_for_ids(
        &self,
        api_ids: &[String],
    ) -> Result<Vec<String>> {
        let api_ids = normalize_text_ids(api_ids);
        if api_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for chunk in api_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(list_balance_query_aggregate_api_ids_for_ids_chunk(
                self, chunk,
            )?);
        }
        out.sort_by(|left, right| {
            left.1
                .cmp(&right.1)
                .then_with(|| right.2.cmp(&left.2))
                .then_with(|| left.0.cmp(&right.0))
        });
        Ok(out.into_iter().map(|item| item.0).collect())
    }

    pub fn list_active_aggregate_api_ids(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(active_aggregate_api_ids_sql())?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect()
    }

    pub fn list_active_aggregate_apis(&self) -> Result<Vec<AggregateApi>> {
        let mut stmt = self.conn.prepare(&active_aggregate_api_select_sql(None))?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(map_aggregate_api_row(row)?);
        }
        Ok(out)
    }

    pub fn list_active_aggregate_apis_by_provider_type(
        &self,
        provider_type: &str,
    ) -> Result<Vec<AggregateApi>> {
        let Some((provider_condition, params)) =
            aggregate_api_provider_type_condition(provider_type)
        else {
            return Ok(Vec::new());
        };

        let sql = active_aggregate_api_select_sql(Some(&provider_condition));
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query(params_from_iter(params))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(map_aggregate_api_row(row)?);
        }
        Ok(out)
    }

    pub fn aggregate_api_overview_stats(&self) -> Result<AggregateApiOverviewStats> {
        self.conn.query_row(
            "SELECT
                COUNT(1) AS source_count,
                IFNULL(SUM(CASE WHEN balance_query_enabled = 1 THEN 1 ELSE 0 END), 0)
                    AS enabled_balance_query_count,
                IFNULL(SUM(CASE WHEN last_balance_status = 'success' THEN 1 ELSE 0 END), 0)
                    AS ok_count,
                IFNULL(SUM(CASE WHEN last_balance_status IN ('error', 'failed') THEN 1 ELSE 0 END), 0)
                    AS error_count,
                MAX(last_balance_at) AS last_refreshed_at
             FROM aggregate_apis",
            [],
            |row| {
                Ok(AggregateApiOverviewStats {
                    source_count: row.get(0)?,
                    enabled_balance_query_count: row.get(1)?,
                    ok_count: row.get(2)?,
                    error_count: row.get(3)?,
                    last_refreshed_at: row.get(4)?,
                })
            },
        )
    }

    pub fn list_aggregate_api_balance_jsons(&self) -> Result<Vec<String>> {
        let mut stmt = self.conn.prepare(aggregate_api_balance_jsons_list_sql())?;
        let rows = stmt.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect()
    }

    /// 函数 `update_aggregate_api`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - api_id: 参数 api_id
    /// - url: 参数 url
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn update_aggregate_api(&self, api_id: &str, url: &str) -> Result<()> {
        self.conn
            .execute(update_aggregate_api_url_sql(), (url, now_ts(), api_id))?;
        Ok(())
    }

    /// 函数 `update_aggregate_api_supplier_name`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - api_id: 参数 api_id
    /// - supplier_name: 参数 supplier_name
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn update_aggregate_api_supplier_name(
        &self,
        api_id: &str,
        supplier_name: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            update_aggregate_api_supplier_name_sql(),
            (supplier_name, now_ts(), api_id),
        )?;
        Ok(())
    }

    /// 函数 `update_aggregate_api_sort`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - api_id: 参数 api_id
    /// - sort: 参数 sort
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn update_aggregate_api_sort(&self, api_id: &str, sort: i64) -> Result<()> {
        self.conn
            .execute(update_aggregate_api_sort_sql(), (sort, now_ts(), api_id))?;
        Ok(())
    }

    pub fn update_aggregate_api_status(&self, api_id: &str, status: &str) -> Result<()> {
        self.conn.execute(
            update_aggregate_api_status_sql(),
            (status, now_ts(), api_id),
        )?;
        Ok(())
    }

    /// 函数 `update_aggregate_api_type`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - api_id: 参数 api_id
    /// - provider_type: 参数 provider_type
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn update_aggregate_api_type(&self, api_id: &str, provider_type: &str) -> Result<()> {
        self.conn.execute(
            update_aggregate_api_provider_type_sql(),
            (provider_type, now_ts(), api_id),
        )?;
        Ok(())
    }

    pub fn update_aggregate_api_auth_type(&self, api_id: &str, auth_type: &str) -> Result<()> {
        self.conn.execute(
            update_aggregate_api_auth_type_sql(),
            (auth_type, now_ts(), api_id),
        )?;
        Ok(())
    }

    pub fn update_aggregate_api_auth_params_json(
        &self,
        api_id: &str,
        auth_params_json: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            update_aggregate_api_auth_params_json_sql(),
            (auth_params_json, now_ts(), api_id),
        )?;
        Ok(())
    }

    pub fn update_aggregate_api_action(&self, api_id: &str, action: Option<&str>) -> Result<()> {
        self.conn.execute(
            update_aggregate_api_action_sql(),
            (action, now_ts(), api_id),
        )?;
        Ok(())
    }

    pub fn update_aggregate_api_model_override(
        &self,
        api_id: &str,
        model_override: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            update_aggregate_api_model_override_sql(),
            (model_override, now_ts(), api_id),
        )?;
        Ok(())
    }

    pub fn update_aggregate_api_balance_query(
        &self,
        api_id: &str,
        enabled: bool,
        template: Option<&str>,
        base_url: Option<&str>,
        user_id: Option<&str>,
        config_json: Option<&str>,
    ) -> Result<()> {
        self.conn.execute(
            update_aggregate_api_balance_query_sql(),
            (
                enabled,
                template,
                base_url,
                user_id,
                config_json,
                now_ts(),
                api_id,
            ),
        )?;
        Ok(())
    }

    pub fn update_aggregate_api_balance_result(
        &self,
        api_id: &str,
        ok: bool,
        balance_json: Option<&str>,
        error: Option<&str>,
    ) -> Result<()> {
        let now = now_ts();
        let status = if ok { Some("success") } else { Some("failed") };
        self.conn.execute(
            update_aggregate_api_balance_result_sql(),
            (now, status, error, balance_json, api_id),
        )?;
        Ok(())
    }

    /// 函数 `delete_aggregate_api`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - api_id: 参数 api_id
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn delete_aggregate_api(&self, api_id: &str) -> Result<()> {
        let tx = self.conn.unchecked_transaction()?;
        tx.execute(delete_aggregate_api_balance_secret_by_id_sql(), [api_id])?;
        tx.execute(delete_aggregate_api_secret_by_id_sql(), [api_id])?;
        tx.execute(
            delete_model_source_mapping_preferences_for_source_sql(),
            (AGGREGATE_API_MODEL_SOURCE_KIND, api_id),
        )?;
        tx.execute(
            delete_model_source_mappings_for_source_sql(),
            (AGGREGATE_API_MODEL_SOURCE_KIND, api_id),
        )?;
        tx.execute(
            delete_model_source_models_for_source_sql(),
            (AGGREGATE_API_MODEL_SOURCE_KIND, api_id),
        )?;
        tx.execute(delete_aggregate_api_by_id_sql(), [api_id])?;
        tx.commit()?;
        Ok(())
    }

    /// 函数 `upsert_aggregate_api_secret`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - api_id: 参数 api_id
    /// - secret_value: 参数 secret_value
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn upsert_aggregate_api_secret(&self, api_id: &str, secret_value: &str) -> Result<()> {
        let now = now_ts();
        self.conn.execute(
            "INSERT INTO aggregate_api_secrets (aggregate_api_id, secret_value, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?3)
             ON CONFLICT(aggregate_api_id) DO UPDATE SET
               secret_value = excluded.secret_value,
               updated_at = excluded.updated_at",
            (api_id, secret_value, now),
        )?;
        Ok(())
    }

    /// 函数 `find_aggregate_api_secret_by_id`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - api_id: 参数 api_id
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn find_aggregate_api_secret_by_id(&self, api_id: &str) -> Result<Option<String>> {
        let mut stmt = self.conn.prepare(aggregate_api_secret_by_id_sql())?;
        let mut rows = stmt.query([api_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    pub fn list_aggregate_api_secrets_for_ids(
        &self,
        api_ids: &[String],
    ) -> Result<HashMap<String, String>> {
        let api_ids = normalize_text_ids(api_ids);
        if api_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut out = HashMap::new();
        for chunk in api_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            for (api_id, secret) in list_aggregate_api_secrets_for_ids_chunk(self, chunk)? {
                out.insert(api_id, secret);
            }
        }
        Ok(out)
    }

    pub fn upsert_aggregate_api_balance_secret(
        &self,
        api_id: &str,
        access_token: &str,
    ) -> Result<()> {
        let now = now_ts();
        self.conn.execute(
            "INSERT INTO aggregate_api_balance_secrets (aggregate_api_id, access_token, created_at, updated_at)
             VALUES (?1, ?2, ?3, ?3)
             ON CONFLICT(aggregate_api_id) DO UPDATE SET
               access_token = excluded.access_token,
               updated_at = excluded.updated_at",
            (api_id, access_token, now),
        )?;
        Ok(())
    }

    pub fn delete_aggregate_api_balance_secret(&self, api_id: &str) -> Result<()> {
        self.conn
            .execute(delete_aggregate_api_balance_secret_by_id_sql(), [api_id])?;
        Ok(())
    }

    pub fn find_aggregate_api_balance_secret_by_id(&self, api_id: &str) -> Result<Option<String>> {
        let mut stmt = self
            .conn
            .prepare(aggregate_api_balance_secret_by_id_sql())?;
        let mut rows = stmt.query([api_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(row.get(0)?))
        } else {
            Ok(None)
        }
    }

    /// 函数 `update_aggregate_api_test_result`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - api_id: 参数 api_id
    /// - ok: 参数 ok
    /// - status_code: 参数 status_code
    /// - error: 参数 error
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn update_aggregate_api_test_result(
        &self,
        api_id: &str,
        ok: bool,
        status_code: Option<i64>,
        error: Option<&str>,
    ) -> Result<()> {
        let now = now_ts();
        let last_test_status = if ok { Some("success") } else { Some("failed") };
        self.conn.execute(
            update_aggregate_api_test_result_sql(),
            (now, last_test_status, error, api_id),
        )?;
        if let Some(code) = status_code {
            if !ok {
                let message = format!("http_status={code}");
                self.conn.execute(
                    update_aggregate_api_last_test_error_sql(),
                    (message, api_id),
                )?;
            }
        }
        Ok(())
    }

    /// 函数 `ensure_aggregate_apis_table`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - super: 参数 super
    ///
    /// # 返回
    /// 返回函数执行结果
    pub(super) fn ensure_aggregate_apis_table(&self) -> Result<()> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS aggregate_apis (
                id TEXT PRIMARY KEY,
                provider_type TEXT NOT NULL DEFAULT 'codex',
                supplier_name TEXT,
                sort INTEGER NOT NULL DEFAULT 0,
                url TEXT NOT NULL,
                auth_type TEXT NOT NULL DEFAULT 'apikey',
                auth_params_json TEXT,
                action TEXT,
                model_override TEXT,
                status TEXT NOT NULL DEFAULT 'active',
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL,
                last_test_at INTEGER,
                last_test_status TEXT,
                last_test_error TEXT,
                balance_query_enabled INTEGER NOT NULL DEFAULT 0,
                balance_query_template TEXT,
                balance_query_base_url TEXT,
                balance_query_user_id TEXT,
                balance_query_config_json TEXT,
                last_balance_at INTEGER,
                last_balance_status TEXT,
                last_balance_error TEXT,
                last_balance_json TEXT
            )",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_aggregate_apis_list_order
             ON aggregate_apis(sort ASC, updated_at DESC, id ASC)",
            [],
        )?;
        self.ensure_aggregate_api_balance_query_lookup_index()?;
        self.ensure_aggregate_api_status_order_index()?;
        self.ensure_aggregate_api_provider_status_order_index()?;
        self.ensure_column("aggregate_apis", "provider_type", "TEXT")?;
        self.ensure_column("aggregate_apis", "supplier_name", "TEXT")?;
        self.ensure_column("aggregate_apis", "sort", "INTEGER DEFAULT 0")?;
        self.ensure_column(
            "aggregate_apis",
            "auth_type",
            "TEXT NOT NULL DEFAULT 'apikey'",
        )?;
        self.ensure_column("aggregate_apis", "auth_params_json", "TEXT")?;
        self.ensure_column("aggregate_apis", "action", "TEXT")?;
        self.ensure_column("aggregate_apis", "model_override", "TEXT")?;
        self.ensure_column(
            "aggregate_apis",
            "balance_query_enabled",
            "INTEGER NOT NULL DEFAULT 0",
        )?;
        self.ensure_column("aggregate_apis", "balance_query_template", "TEXT")?;
        self.ensure_column("aggregate_apis", "balance_query_base_url", "TEXT")?;
        self.ensure_column("aggregate_apis", "balance_query_user_id", "TEXT")?;
        self.ensure_column("aggregate_apis", "balance_query_config_json", "TEXT")?;
        self.ensure_column("aggregate_apis", "last_balance_at", "INTEGER")?;
        self.ensure_column("aggregate_apis", "last_balance_status", "TEXT")?;
        self.ensure_column("aggregate_apis", "last_balance_error", "TEXT")?;
        self.ensure_column("aggregate_apis", "last_balance_json", "TEXT")?;
        self.conn.execute(
            "UPDATE aggregate_apis
             SET provider_type = COALESCE(NULLIF(TRIM(provider_type), ''), 'codex')
             WHERE provider_type IS NULL OR TRIM(provider_type) = ''",
            [],
        )?;
        self.conn.execute(
            "UPDATE aggregate_apis
             SET auth_type = COALESCE(NULLIF(TRIM(auth_type), ''), 'apikey')
             WHERE auth_type IS NULL OR TRIM(auth_type) = ''",
            [],
        )?;
        self.conn.execute(
            "UPDATE aggregate_apis
             SET sort = COALESCE(sort, 0)
             WHERE sort IS NULL",
            [],
        )?;
        Ok(())
    }

    pub(super) fn ensure_aggregate_api_balance_query_lookup_index(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_aggregate_apis_balance_query_lookup
               ON aggregate_apis(
                 balance_query_enabled,
                 LOWER(TRIM(COALESCE(status, ''))),
                 sort ASC,
                 updated_at DESC,
                 id ASC
               );",
        )?;
        Ok(())
    }

    pub(super) fn ensure_aggregate_api_balance_query_order_index(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_aggregate_apis_balance_query_order
               ON aggregate_apis(
                 balance_query_enabled,
                 sort ASC,
                 updated_at DESC,
                 id ASC
               );",
        )?;
        Ok(())
    }

    pub(super) fn ensure_aggregate_api_status_order_index(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_aggregate_apis_status_order
               ON aggregate_apis(
                 LOWER(TRIM(COALESCE(status, ''))),
                 sort ASC,
                 created_at DESC,
                 id ASC
               );",
        )?;
        Ok(())
    }

    pub(super) fn ensure_aggregate_api_provider_status_order_index(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE INDEX IF NOT EXISTS idx_aggregate_apis_provider_status_order
               ON aggregate_apis(
                 LOWER(TRIM(COALESCE(status, ''))),
                 REPLACE(LOWER(TRIM(COALESCE(provider_type, ''))), '-', '_'),
                 sort ASC,
                 created_at DESC,
                 id ASC
               );",
        )?;
        Ok(())
    }

    /// 函数 `ensure_aggregate_api_secrets_table`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - super: 参数 super
    ///
    /// # 返回
    /// 返回函数执行结果
    pub(super) fn ensure_aggregate_api_secrets_table(&self) -> Result<()> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS aggregate_api_secrets (
                aggregate_api_id TEXT PRIMARY KEY REFERENCES aggregate_apis(id) ON DELETE CASCADE,
                secret_value TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_aggregate_api_secrets_updated_at ON aggregate_api_secrets(updated_at)",
            [],
        )?;
        Ok(())
    }

    pub(super) fn ensure_aggregate_api_balance_secrets_table(&self) -> Result<()> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS aggregate_api_balance_secrets (
                aggregate_api_id TEXT PRIMARY KEY REFERENCES aggregate_apis(id) ON DELETE CASCADE,
                access_token TEXT NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_aggregate_api_balance_secrets_updated_at ON aggregate_api_balance_secrets(updated_at)",
            [],
        )?;
        Ok(())
    }

    pub(super) fn ensure_aggregate_api_supplier_model_tables(&self) -> Result<()> {
        self.conn.execute_batch(include_str!(
            "../../migrations/059_aggregate_api_supplier_models.sql"
        ))
    }

    pub fn list_aggregate_api_supplier_models(
        &self,
        supplier_key: Option<&str>,
        provider_type: Option<&str>,
    ) -> Result<Vec<AggregateApiSupplierModel>> {
        let supplier_key = supplier_key
            .map(normalize_supplier_model_text)
            .filter(|value| !value.is_empty());
        let provider_type = provider_type
            .map(normalize_supplier_model_text)
            .filter(|value| !value.is_empty());
        let mut clauses = Vec::new();
        let mut params = Vec::new();
        if let Some(value) = supplier_key {
            clauses.push("supplier_key = ?".to_string());
            params.push(Value::Text(value));
        }
        if let Some(value) = provider_type {
            clauses.push("provider_type = ?".to_string());
            params.push(Value::Text(value));
        }
        let where_clause = if clauses.is_empty() {
            String::new()
        } else {
            format!(" WHERE {}", clauses.join(" AND "))
        };
        let sql = format!(
            "SELECT supplier_key, provider_type, upstream_model, display_name,
                    status, created_at, updated_at
             FROM aggregate_api_supplier_models{where_clause}
             ORDER BY supplier_key ASC, provider_type ASC, upstream_model ASC"
        );
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(
            params_from_iter(params),
            map_aggregate_api_supplier_model_row,
        )?;
        rows.collect()
    }

    pub fn upsert_aggregate_api_supplier_model(
        &self,
        model: &AggregateApiSupplierModel,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO aggregate_api_supplier_models (
                supplier_key, provider_type, upstream_model, display_name,
                status, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(supplier_key, provider_type, upstream_model) DO UPDATE SET
                display_name = excluded.display_name,
                status = excluded.status,
                updated_at = excluded.updated_at",
            params![
                &model.supplier_key,
                &model.provider_type,
                &model.upstream_model,
                &model.display_name,
                &model.status,
                model.created_at,
                model.updated_at,
            ],
        )?;
        Ok(())
    }

    pub fn delete_aggregate_api_supplier_model(
        &self,
        supplier_key: &str,
        provider_type: &str,
        upstream_model: &str,
    ) -> Result<()> {
        self.conn.execute(
            delete_aggregate_api_supplier_model_sql(),
            params![
                normalize_supplier_model_text(supplier_key),
                normalize_supplier_model_text(provider_type),
                normalize_supplier_model_text(upstream_model),
            ],
        )?;
        Ok(())
    }
}

/// 函数 `map_aggregate_api_row`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - row: 参数 row
///
/// # 返回
/// 返回函数执行结果
fn map_aggregate_api_row(row: &Row<'_>) -> Result<AggregateApi> {
    Ok(AggregateApi {
        id: row.get(0)?,
        provider_type: row.get(1)?,
        supplier_name: row.get(2)?,
        sort: row.get(3)?,
        url: row.get(4)?,
        auth_type: row.get(5)?,
        auth_params_json: row.get(6)?,
        action: row.get(7)?,
        model_override: row.get(8)?,
        status: row.get(9)?,
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
        last_test_at: row.get(12)?,
        last_test_status: row.get(13)?,
        last_test_error: row.get(14)?,
        balance_query_enabled: row.get(15)?,
        balance_query_template: row.get(16)?,
        balance_query_base_url: row.get(17)?,
        balance_query_user_id: row.get(18)?,
        balance_query_config_json: row.get(19)?,
        last_balance_at: row.get(20)?,
        last_balance_status: row.get(21)?,
        last_balance_error: row.get(22)?,
        last_balance_json: row.get(23)?,
    })
}

fn map_aggregate_api_with_secrets_row(row: &Row<'_>) -> Result<AggregateApiWithSecrets> {
    Ok(AggregateApiWithSecrets {
        api: map_aggregate_api_row(row)?,
        secret_value: row.get(24)?,
        balance_access_token: row.get(25)?,
    })
}

fn map_aggregate_api_list_summary_row(row: &Row<'_>) -> Result<AggregateApiListSummary> {
    Ok(AggregateApiListSummary {
        id: row.get(0)?,
        provider_type: row.get(1)?,
        supplier_name: row.get(2)?,
        sort: row.get(3)?,
        url: row.get(4)?,
        auth_type: row.get(5)?,
        auth_params_json: row.get(6)?,
        action: row.get(7)?,
        model_override: row.get(8)?,
        status: row.get(9)?,
        created_at: row.get(10)?,
        updated_at: row.get(11)?,
        last_test_at: row.get(12)?,
        last_test_status: row.get(13)?,
        last_test_error: row.get(14)?,
        balance_query_enabled: row.get(15)?,
        balance_query_template: row.get(16)?,
        balance_query_base_url: row.get(17)?,
        balance_query_user_id: row.get(18)?,
        balance_query_config_json: row.get(19)?,
        last_balance_at: row.get(20)?,
        last_balance_status: row.get(21)?,
        last_balance_error: row.get(22)?,
        last_balance_json: row.get(23)?,
    })
}

fn map_aggregate_api_quota_source_summary_row(
    row: &Row<'_>,
) -> Result<AggregateApiQuotaSourceSummary> {
    Ok(AggregateApiQuotaSourceSummary {
        id: row.get(0)?,
        provider_type: row.get(1)?,
        supplier_name: row.get(2)?,
        url: row.get(3)?,
        status: row.get(4)?,
        balance_query_enabled: row.get(5)?,
        last_balance_at: row.get(6)?,
        last_balance_status: row.get(7)?,
        last_balance_error: row.get(8)?,
        last_balance_json: row.get(9)?,
    })
}

fn map_aggregate_api_dashboard_source_metadata_row(
    row: &Row<'_>,
) -> Result<AggregateApiDashboardSourceMetadata> {
    Ok(AggregateApiDashboardSourceMetadata {
        id: row.get(0)?,
        provider_type: row.get(1)?,
        supplier_name: row.get(2)?,
        url: row.get(3)?,
        status: row.get(4)?,
    })
}

fn map_aggregate_api_supplier_model_row(row: &Row<'_>) -> Result<AggregateApiSupplierModel> {
    Ok(AggregateApiSupplierModel {
        supplier_key: row.get(0)?,
        provider_type: row.get(1)?,
        upstream_model: row.get(2)?,
        display_name: row.get(3)?,
        status: row.get(4)?,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
    })
}

fn list_aggregate_apis_for_ids_chunk(
    storage: &Storage,
    api_ids: &[String],
) -> Result<Vec<AggregateApi>> {
    let Some((condition, params)) = text_id_in_clause("id", api_ids) else {
        return Ok(Vec::new());
    };
    let sql = aggregate_apis_for_ids_chunk_sql(&condition);
    let mut stmt = storage.conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(params))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(map_aggregate_api_row(row)?);
    }
    Ok(out)
}

fn aggregate_apis_for_ids_chunk_sql(api_condition: &str) -> String {
    format!("{AGGREGATE_API_SELECT_SQL} WHERE {api_condition}")
}

fn list_aggregate_api_dashboard_source_metadata_for_ids_chunk(
    storage: &Storage,
    api_ids: &[String],
) -> Result<Vec<(AggregateApiDashboardSourceMetadata, i64, i64)>> {
    let Some((condition, params)) = text_id_in_clause("id", api_ids) else {
        return Ok(Vec::new());
    };
    let sql = aggregate_api_dashboard_source_metadata_for_ids_chunk_sql(&condition);
    let mut stmt = storage.conn.prepare(&sql)?;
    let rows = stmt.query_map(params_from_iter(params), |row| {
        Ok((
            map_aggregate_api_dashboard_source_metadata_row(row)?,
            row.get(5)?,
            row.get(6)?,
        ))
    })?;
    rows.collect()
}

fn aggregate_api_dashboard_source_metadata_for_ids_chunk_sql(api_condition: &str) -> String {
    format!(
        "SELECT id, provider_type, supplier_name, url, status, sort, updated_at
         FROM aggregate_apis
         WHERE {api_condition}"
    )
}

fn list_balance_query_aggregate_api_ids_for_ids_chunk(
    storage: &Storage,
    api_ids: &[String],
) -> Result<Vec<(String, i64, i64)>> {
    let Some((condition, params)) = text_id_in_clause("id", api_ids) else {
        return Ok(Vec::new());
    };
    let sql = balance_query_aggregate_api_ids_for_ids_chunk_sql(&condition);
    let mut stmt = storage.conn.prepare(&sql)?;
    let rows = stmt.query_map(params_from_iter(params), |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    })?;
    rows.collect()
}

fn balance_query_aggregate_api_ids_for_ids_chunk_sql(api_condition: &str) -> String {
    // 中文注释：一元 + 让 SQLite 不把 balance_query_enabled 当首选索引列，
    // 小批量 id 查询应优先走主键条件，再在命中行里判断余额查询开关。
    format!(
        "SELECT id, sort, updated_at
         FROM aggregate_apis
         WHERE +balance_query_enabled = 1
           AND {api_condition}"
    )
}

fn list_aggregate_api_secrets_for_ids_chunk(
    storage: &Storage,
    api_ids: &[String],
) -> Result<Vec<(String, String)>> {
    let Some((condition, params)) = text_id_in_clause("aggregate_api_id", api_ids) else {
        return Ok(Vec::new());
    };
    let sql = aggregate_api_secrets_for_ids_chunk_sql(&condition);
    let mut stmt = storage.conn.prepare(&sql)?;
    let rows = stmt.query_map(params_from_iter(params), |row| {
        Ok((row.get(0)?, row.get(1)?))
    })?;
    rows.collect()
}

fn aggregate_api_secrets_for_ids_chunk_sql(secret_condition: &str) -> String {
    format!(
        "SELECT aggregate_api_id, secret_value
         FROM aggregate_api_secrets
         WHERE {secret_condition}"
    )
}

fn aggregate_api_list_ids_sql() -> &'static str {
    "SELECT id
     FROM aggregate_apis
     ORDER BY sort ASC, updated_at DESC, id ASC"
}

fn aggregate_api_list_sql() -> String {
    format!("{AGGREGATE_API_SELECT_SQL} ORDER BY sort ASC, updated_at DESC")
}

fn aggregate_api_quota_source_summaries_list_sql() -> &'static str {
    "SELECT
        id,
        provider_type,
        supplier_name,
        url,
        status,
        balance_query_enabled,
        last_balance_at,
        last_balance_status,
        last_balance_error,
        last_balance_json
     FROM aggregate_apis
     ORDER BY sort ASC, updated_at DESC, id ASC"
}

fn aggregate_api_balance_jsons_list_sql() -> &'static str {
    "SELECT last_balance_json
     FROM aggregate_apis
     WHERE last_balance_json IS NOT NULL
       AND TRIM(last_balance_json) <> ''
     ORDER BY sort ASC, updated_at DESC, id ASC"
}

fn active_balance_query_aggregate_api_ids_sql() -> &'static str {
    "SELECT id
     FROM aggregate_apis
     WHERE balance_query_enabled = 1
       AND LOWER(TRIM(COALESCE(status, ''))) = 'active'
     ORDER BY sort ASC, updated_at DESC, id ASC"
}

fn balance_query_aggregate_api_ids_sql() -> &'static str {
    "SELECT id
     FROM aggregate_apis
     WHERE balance_query_enabled = 1
     ORDER BY sort ASC, updated_at DESC, id ASC"
}

fn active_aggregate_api_ids_sql() -> &'static str {
    "SELECT id
     FROM aggregate_apis
     WHERE LOWER(TRIM(COALESCE(status, ''))) = 'active'
     ORDER BY sort ASC, created_at DESC, id ASC"
}

#[cfg(test)]
fn active_aggregate_api_ids_sql_with_provider(provider_condition: &str) -> String {
    format!(
        "SELECT id
         FROM aggregate_apis
         WHERE {AGGREGATE_API_ACTIVE_STATUS_CONDITION}
           AND {provider_condition}
         ORDER BY sort ASC, created_at DESC, id ASC"
    )
}

fn active_aggregate_api_select_sql(provider_condition: Option<&str>) -> String {
    match provider_condition {
        Some(provider_condition) => format!(
            "{AGGREGATE_API_SELECT_SQL}
             WHERE {AGGREGATE_API_ACTIVE_STATUS_CONDITION}
               AND {provider_condition}
             ORDER BY sort ASC, created_at DESC, id ASC"
        ),
        None => format!(
            "{AGGREGATE_API_SELECT_SQL}
             WHERE {AGGREGATE_API_ACTIVE_STATUS_CONDITION}
             ORDER BY sort ASC, created_at DESC, id ASC"
        ),
    }
}

fn aggregate_api_provider_type_condition(provider_type: &str) -> Option<(String, Vec<Value>)> {
    let provider_type = normalize_aggregate_api_provider_type(provider_type);
    if provider_type.is_empty() {
        return None;
    }

    const CLAUDE_ALIASES: &[&str] = &["claude", "anthropic", "anthropic_native", "claude_code"];
    const GEMINI_ALIASES: &[&str] = &[
        "gemini",
        "gemini_native",
        "google",
        "google_ai",
        "google_gemini",
    ];

    match provider_type.as_str() {
        "claude" | "anthropic" | "anthropic_native" | "claude_code" => {
            let placeholders = vec!["?"; CLAUDE_ALIASES.len()].join(", ");
            Some((
                format!("{AGGREGATE_API_NORMALIZED_PROVIDER_SQL} IN ({placeholders})"),
                CLAUDE_ALIASES
                    .iter()
                    .map(|value| Value::Text((*value).to_string()))
                    .collect(),
            ))
        }
        "gemini" | "gemini_native" | "google" | "google_ai" | "google_gemini" => {
            let placeholders = vec!["?"; GEMINI_ALIASES.len()].join(", ");
            Some((
                format!("{AGGREGATE_API_NORMALIZED_PROVIDER_SQL} IN ({placeholders})"),
                GEMINI_ALIASES
                    .iter()
                    .map(|value| Value::Text((*value).to_string()))
                    .collect(),
            ))
        }
        _ => {
            let aliases = CLAUDE_ALIASES
                .iter()
                .chain(GEMINI_ALIASES.iter())
                .map(|value| Value::Text((*value).to_string()))
                .collect::<Vec<_>>();
            let placeholders = vec!["?"; aliases.len()].join(", ");
            Some((
                format!("{AGGREGATE_API_NORMALIZED_PROVIDER_SQL} NOT IN ({placeholders})"),
                aliases,
            ))
        }
    }
}

fn normalize_aggregate_api_provider_type(value: &str) -> String {
    value.trim().to_ascii_lowercase().replace('-', "_")
}

fn normalize_supplier_model_text(value: &str) -> String {
    value.trim().to_string()
}

#[cfg(test)]
mod supplier_model_tests {
    use super::*;

    fn sample_aggregate_api(id: &str, now: i64) -> AggregateApi {
        AggregateApi {
            id: id.to_string(),
            provider_type: "openai-compatible".to_string(),
            supplier_name: Some(id.to_string()),
            sort: 0,
            url: format!("https://{id}.example.test"),
            auth_type: "bearer".to_string(),
            auth_params_json: None,
            action: None,
            model_override: None,
            status: "active".to_string(),
            created_at: now,
            updated_at: now,
            last_test_at: None,
            last_test_status: None,
            last_test_error: None,
            balance_query_enabled: false,
            balance_query_template: None,
            balance_query_base_url: None,
            balance_query_user_id: None,
            balance_query_config_json: None,
            last_balance_at: None,
            last_balance_status: None,
            last_balance_error: None,
            last_balance_json: None,
        }
    }

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

    #[test]
    fn list_aggregate_apis_for_ids_filters_and_preserves_api_order() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut first = sample_aggregate_api("api-first", now);
        first.sort = 1;
        first.updated_at = now;
        let mut second = sample_aggregate_api("api-second", now);
        second.sort = 0;
        second.updated_at = now.saturating_sub(10);
        let mut ignored = sample_aggregate_api("api-ignored", now);
        ignored.sort = -1;

        for api in [&first, &second, &ignored] {
            storage.insert_aggregate_api(api).expect("insert api");
        }

        let requested = vec![
            "api-first".to_string(),
            "api-missing".to_string(),
            "api-second".to_string(),
            "api-first".to_string(),
        ];
        let apis = storage
            .list_aggregate_apis_for_ids(&requested)
            .expect("list aggregate apis for ids");

        assert_eq!(
            apis.into_iter().map(|api| api.id).collect::<Vec<_>>(),
            vec!["api-second".to_string(), "api-first".to_string()]
        );
    }

    #[test]
    fn aggregate_api_id_chunk_queries_defer_final_ordering_to_rust() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let chunk_queries = [
            (
                "aggregate API row chunk",
                aggregate_apis_for_ids_chunk_sql("id IN ('api-a', 'api-b')"),
                "sqlite_autoindex_aggregate_apis_1",
            ),
            (
                "aggregate API dashboard metadata chunk",
                aggregate_api_dashboard_source_metadata_for_ids_chunk_sql(
                    "id IN ('api-a', 'api-b')",
                ),
                "sqlite_autoindex_aggregate_apis_1",
            ),
            (
                "balance query aggregate API id chunk",
                balance_query_aggregate_api_ids_for_ids_chunk_sql("id IN ('api-a', 'api-b')"),
                "sqlite_autoindex_aggregate_apis_1",
            ),
            (
                "aggregate API secret chunk",
                aggregate_api_secrets_for_ids_chunk_sql("aggregate_api_id IN ('api-a', 'api-b')"),
                "sqlite_autoindex_aggregate_api_secrets_1",
            ),
        ];

        for (label, sql, expected_index) in chunk_queries {
            let details =
                collect_query_plan_details(&storage, &format!("EXPLAIN QUERY PLAN {sql}"));

            assert!(
                details.iter().any(|detail| detail.contains(expected_index)
                    || detail.contains("using index")),
                "{label} should use an id lookup index, got {details:?}"
            );
            assert!(
                !details
                    .iter()
                    .any(|detail| detail.contains("use temp b-tree for order by")),
                "{label} should avoid per-chunk ORDER BY temp sorting, got {details:?}"
            );
        }
    }

    #[test]
    fn list_aggregate_api_summaries_reads_list_fields_in_api_order() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut first = sample_aggregate_api("api-first", now);
        first.sort = 1;
        first.updated_at = now + 10;
        first.auth_params_json = Some(r#"{"location":"header"}"#.to_string());
        first.balance_query_config_json = Some(r#"{"path":"/balance"}"#.to_string());
        first.last_balance_json = Some(r#"{"remaining":1.5}"#.to_string());
        storage
            .insert_aggregate_api(&first)
            .expect("insert first api");

        let mut second = sample_aggregate_api("api-second", now);
        second.sort = 0;
        second.updated_at = now + 5;
        second.last_test_status = Some("ok".to_string());
        storage
            .insert_aggregate_api(&second)
            .expect("insert second api");

        let summaries = storage
            .list_aggregate_api_summaries()
            .expect("list aggregate api summaries");

        assert_eq!(
            summaries
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            vec!["api-second", "api-first"]
        );
        let first_summary = summaries
            .iter()
            .find(|item| item.id == "api-first")
            .expect("first summary exists");
        assert_eq!(
            first_summary.auth_params_json.as_deref(),
            Some(r#"{"location":"header"}"#)
        );
        assert_eq!(
            first_summary.balance_query_config_json.as_deref(),
            Some(r#"{"path":"/balance"}"#)
        );
        assert_eq!(
            first_summary.last_balance_json.as_deref(),
            Some(r#"{"remaining":1.5}"#)
        );
    }

    #[test]
    fn aggregate_api_list_snapshot_loads_summaries_and_existing_model_assignments() {
        let mut storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut first = sample_aggregate_api("api-snapshot-first", now);
        first.sort = 1;
        let mut second = sample_aggregate_api("api-snapshot-second", now);
        second.sort = 0;
        storage
            .insert_aggregate_api(&first)
            .expect("insert first api");
        storage
            .insert_aggregate_api(&second)
            .expect("insert second api");
        storage
            .set_quota_source_model_assignments(
                "aggregate_api",
                "api-snapshot-first",
                &["gpt-first".to_string(), "gpt-shared".to_string()],
            )
            .expect("set first assignments");
        storage
            .set_quota_source_model_assignments(
                "aggregate_api",
                "api-missing",
                &["gpt-hidden".to_string()],
            )
            .expect("set missing assignments");

        let snapshot = storage
            .load_aggregate_api_list_snapshot()
            .expect("load aggregate api list snapshot");

        assert_eq!(
            snapshot
                .items
                .iter()
                .map(|item| item.id.as_str())
                .collect::<Vec<_>>(),
            vec!["api-snapshot-second", "api-snapshot-first"]
        );
        assert_eq!(
            snapshot
                .model_assignments
                .iter()
                .map(|assignment| {
                    (
                        assignment.source_id.as_str(),
                        assignment.model_slug.as_str(),
                    )
                })
                .collect::<Vec<_>>(),
            vec![
                ("api-snapshot-first", "gpt-first"),
                ("api-snapshot-first", "gpt-shared")
            ]
        );
    }

    #[test]
    fn list_aggregate_api_dashboard_source_metadata_for_ids_reads_dashboard_fields_only() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut first = sample_aggregate_api("api-dashboard-first", now);
        first.provider_type = "openai-compatible".to_string();
        first.supplier_name = Some("First Supplier".to_string());
        first.url = "https://first.example.test".to_string();
        first.auth_params_json = Some(r#"{"ignored":"secret"}"#.to_string());
        first.balance_query_template = Some("ignored-template".to_string());
        first.last_balance_json = Some(r#"{"ignored":true}"#.to_string());
        first.sort = 1;
        first.updated_at = now;
        let mut second = sample_aggregate_api("api-dashboard-second", now);
        second.provider_type = "claude".to_string();
        second.supplier_name = None;
        second.url = "https://second.example.test".to_string();
        second.status = "disabled".to_string();
        second.sort = 0;
        second.updated_at = now.saturating_sub(10);
        let mut ignored = sample_aggregate_api("api-dashboard-ignored", now);
        ignored.sort = -1;

        for api in [&first, &second, &ignored] {
            storage.insert_aggregate_api(api).expect("insert api");
        }

        let metadata = storage
            .list_aggregate_api_dashboard_source_metadata_for_ids(&[
                "api-dashboard-first".to_string(),
                "api-dashboard-missing".to_string(),
                "api-dashboard-second".to_string(),
                "api-dashboard-first".to_string(),
            ])
            .expect("list dashboard aggregate api metadata");

        assert_eq!(metadata.len(), 2);
        assert_eq!(metadata[0].id, "api-dashboard-second");
        assert_eq!(metadata[0].provider_type, "claude");
        assert_eq!(metadata[0].supplier_name, None);
        assert_eq!(metadata[0].url, "https://second.example.test");
        assert_eq!(metadata[0].status, "disabled");
        assert_eq!(metadata[1].id, "api-dashboard-first");
        assert_eq!(metadata[1].provider_type, "openai-compatible");
        assert_eq!(metadata[1].supplier_name.as_deref(), Some("First Supplier"));
        assert_eq!(metadata[1].url, "https://first.example.test");
        assert_eq!(metadata[1].status, "active");
    }

    #[test]
    fn aggregate_api_status_auth_type_and_exists_helpers_read_minimal_api_state() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut api = sample_aggregate_api("api-status-helper", now);
        api.status = " disabled ".to_string();
        api.auth_type = " userpass ".to_string();
        api.auth_params_json = Some(r#"{"ignored":"secret"}"#.to_string());
        api.balance_query_template = Some("ignored-template".to_string());
        api.last_balance_json = Some(r#"{"ignored":true}"#.to_string());
        storage.insert_aggregate_api(&api).expect("insert api");

        assert_eq!(
            storage
                .find_aggregate_api_status_by_id("api-status-helper")
                .expect("find aggregate api status")
                .as_deref(),
            Some(" disabled ")
        );
        assert_eq!(
            storage
                .find_aggregate_api_status_by_id("api-status-missing")
                .expect("find missing aggregate api status"),
            None
        );
        assert_eq!(
            storage
                .find_aggregate_api_auth_type_by_id("api-status-helper")
                .expect("find aggregate api auth type")
                .as_deref(),
            Some(" userpass ")
        );
        assert_eq!(
            storage
                .find_aggregate_api_auth_type_by_id("api-status-missing")
                .expect("find missing aggregate api auth type"),
            None
        );
        assert!(storage
            .aggregate_api_exists("api-status-helper")
            .expect("aggregate api exists"));
        assert!(!storage
            .aggregate_api_exists("api-status-missing")
            .expect("missing aggregate api exists"));
    }

    #[test]
    fn aggregate_api_direct_lookup_helpers_use_primary_key_indexes() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let api_by_id_sql = aggregate_api_by_id_sql();
        let lookup_queries = [
            (
                "aggregate api by id",
                api_by_id_sql.as_str(),
                vec![Value::Text("api-index".to_string())],
                "sqlite_autoindex_aggregate_apis_1",
            ),
            (
                "aggregate api status by id",
                aggregate_api_status_by_id_sql(),
                vec![Value::Text("api-index".to_string())],
                "sqlite_autoindex_aggregate_apis_1",
            ),
            (
                "aggregate api auth type by id",
                aggregate_api_auth_type_by_id_sql(),
                vec![Value::Text("api-index".to_string())],
                "sqlite_autoindex_aggregate_apis_1",
            ),
            (
                "aggregate api exists by id",
                aggregate_api_exists_sql(),
                vec![Value::Text("api-index".to_string())],
                "sqlite_autoindex_aggregate_apis_1",
            ),
            (
                "aggregate api update config by id",
                aggregate_api_update_config_by_id_sql(),
                vec![Value::Text("api-index".to_string())],
                "sqlite_autoindex_aggregate_apis_1",
            ),
            (
                "aggregate api supplier identity by id",
                aggregate_api_supplier_identity_by_id_sql(),
                vec![Value::Text("api-index".to_string())],
                "sqlite_autoindex_aggregate_apis_1",
            ),
            (
                "aggregate api secret by id",
                aggregate_api_secret_by_id_sql(),
                vec![Value::Text("api-index".to_string())],
                "sqlite_autoindex_aggregate_api_secrets_1",
            ),
            (
                "aggregate api balance secret by id",
                aggregate_api_balance_secret_by_id_sql(),
                vec![Value::Text("api-index".to_string())],
                "sqlite_autoindex_aggregate_api_balance_secrets_1",
            ),
        ];

        for (label, sql, params, expected_index) in lookup_queries {
            let details = collect_query_plan_details_with_params(
                &storage,
                &format!("EXPLAIN QUERY PLAN {sql}"),
                params,
            );
            assert!(
                details.iter().any(|detail| detail.contains(expected_index)),
                "{label} should use {expected_index}, got {details:?}"
            );
        }

        let secret_config_details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                aggregate_api_secret_config_by_id_sql()
            ),
            vec![Value::Text("api-index".to_string())],
        );
        assert!(
            secret_config_details
                .iter()
                .any(|detail| detail.contains("sqlite_autoindex_aggregate_apis_1")),
            "secret config should use aggregate api primary key, got {secret_config_details:?}"
        );
        assert!(
            secret_config_details
                .iter()
                .any(|detail| detail.contains("sqlite_autoindex_aggregate_api_secrets_1")),
            "secret config should use aggregate secret primary key, got {secret_config_details:?}"
        );

        let with_secrets_details = collect_query_plan_details_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                aggregate_api_with_secrets_by_id_sql()
            ),
            vec![Value::Text("api-index".to_string())],
        );
        for expected_index in [
            "sqlite_autoindex_aggregate_apis_1",
            "sqlite_autoindex_aggregate_api_secrets_1",
            "sqlite_autoindex_aggregate_api_balance_secrets_1",
        ] {
            assert!(
                with_secrets_details
                    .iter()
                    .any(|detail| detail.contains(expected_index)),
                "with-secrets lookup should use {expected_index}, got {with_secrets_details:?}"
            );
        }
    }
    #[test]
    fn find_aggregate_api_secret_config_by_id_reads_auth_type_and_joined_secret() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut with_secret = sample_aggregate_api("api-secret-config", now);
        with_secret.auth_type = " userpass ".to_string();
        with_secret.auth_params_json = Some(r#"{"ignored":"params"}"#.to_string());
        with_secret.last_balance_json = Some(r#"{"ignored":true}"#.to_string());
        storage
            .insert_aggregate_api(&with_secret)
            .expect("insert api with secret");
        storage
            .upsert_aggregate_api_secret("api-secret-config", "joined-secret")
            .expect("insert secret");

        let without_secret = sample_aggregate_api("api-no-secret-config", now);
        storage
            .insert_aggregate_api(&without_secret)
            .expect("insert api without secret");

        let config = storage
            .find_aggregate_api_secret_config_by_id("api-secret-config")
            .expect("find secret config")
            .expect("secret config exists");
        assert_eq!(config.auth_type, " userpass ");
        assert_eq!(config.secret_value.as_deref(), Some("joined-secret"));

        let missing_secret = storage
            .find_aggregate_api_secret_config_by_id("api-no-secret-config")
            .expect("find missing secret config")
            .expect("api without secret exists");
        assert_eq!(missing_secret.auth_type, "bearer");
        assert_eq!(missing_secret.secret_value, None);

        assert!(storage
            .find_aggregate_api_secret_config_by_id("api-secret-config-missing")
            .expect("find missing api secret config")
            .is_none());
    }

    #[test]
    fn find_aggregate_api_with_secrets_by_id_reads_api_and_joined_secrets() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut api = sample_aggregate_api("api-with-joined-secrets", now);
        api.provider_type = "claude".to_string();
        api.balance_query_enabled = true;
        api.balance_query_template = Some("newapi".to_string());
        storage
            .insert_aggregate_api(&api)
            .expect("insert aggregate api");
        storage
            .upsert_aggregate_api_secret("api-with-joined-secrets", "provider-secret")
            .expect("insert provider secret");
        storage
            .upsert_aggregate_api_balance_secret("api-with-joined-secrets", "balance-token")
            .expect("insert balance secret");

        let joined = storage
            .find_aggregate_api_with_secrets_by_id("api-with-joined-secrets")
            .expect("find aggregate api with secrets")
            .expect("aggregate api with secrets exists");

        assert_eq!(joined.api.id, "api-with-joined-secrets");
        assert_eq!(joined.api.provider_type, "claude");
        assert_eq!(joined.api.balance_query_template.as_deref(), Some("newapi"));
        assert_eq!(joined.secret_value.as_deref(), Some("provider-secret"));
        assert_eq!(
            joined.balance_access_token.as_deref(),
            Some("balance-token")
        );
        assert!(storage
            .find_aggregate_api_with_secrets_by_id("api-with-joined-secrets-missing")
            .expect("find missing aggregate api with secrets")
            .is_none());
    }

    #[test]
    fn list_aggregate_api_secrets_for_ids_filters_and_chunks_ids() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();
        let mut expected = HashMap::new();
        for index in 0..950 {
            let api_id = format!("api-secret-batch-{index:04}");
            storage
                .insert_aggregate_api(&sample_aggregate_api(&api_id, now + index))
                .expect("insert aggregate api");
            if index % 125 == 0 {
                let secret = format!("secret-{index:04}");
                storage
                    .upsert_aggregate_api_secret(&api_id, &secret)
                    .expect("insert aggregate api secret");
                expected.insert(api_id, secret);
            }
        }
        let mut requested = (0..950)
            .map(|index| format!("api-secret-batch-{index:04}"))
            .collect::<Vec<_>>();
        requested.push(" ".to_string());
        requested.push("api-secret-batch-0000".to_string());

        let secrets = storage
            .list_aggregate_api_secrets_for_ids(&requested)
            .expect("list aggregate api secrets");

        assert_eq!(secrets, expected);
    }

    #[test]
    fn find_aggregate_api_supplier_identity_by_id_reads_supplier_fields_only() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut api = sample_aggregate_api("api-supplier-identity", now);
        api.provider_type = "openai-compatible".to_string();
        api.supplier_name = Some("Template Supplier".to_string());
        api.url = "https://supplier.example.test/v1".to_string();
        api.auth_params_json = Some(r#"{"ignored":"secret"}"#.to_string());
        api.balance_query_template = Some("ignored-template".to_string());
        api.last_balance_json = Some(r#"{"ignored":true}"#.to_string());
        storage.insert_aggregate_api(&api).expect("insert api");

        let identity = storage
            .find_aggregate_api_supplier_identity_by_id("api-supplier-identity")
            .expect("find supplier identity")
            .expect("supplier identity present");

        assert_eq!(identity.id, "api-supplier-identity");
        assert_eq!(identity.provider_type, "openai-compatible");
        assert_eq!(identity.supplier_name.as_deref(), Some("Template Supplier"));
        assert_eq!(identity.url, "https://supplier.example.test/v1");
        assert!(storage
            .find_aggregate_api_supplier_identity_by_id("api-supplier-missing")
            .expect("find missing supplier identity")
            .is_none());
    }

    #[test]
    fn find_aggregate_api_update_config_by_id_reads_update_fields_only() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut api = sample_aggregate_api("api-update-config", now);
        api.auth_type = "userpass".to_string();
        api.auth_params_json = Some(r#"{"ignored":"secret"}"#.to_string());
        api.balance_query_enabled = true;
        api.balance_query_template = Some("custom".to_string());
        api.balance_query_base_url = Some("https://balance.example.test".to_string());
        api.balance_query_user_id = Some("user-1".to_string());
        api.balance_query_config_json =
            Some(r#"{"path":"/usage","remainingPath":"left"}"#.to_string());
        api.last_balance_json = Some(r#"{"ignored":true}"#.to_string());
        storage.insert_aggregate_api(&api).expect("insert api");

        let config = storage
            .find_aggregate_api_update_config_by_id("api-update-config")
            .expect("find update config")
            .expect("update config present");

        assert_eq!(config.auth_type, "userpass");
        assert!(config.balance_query_enabled);
        assert_eq!(config.balance_query_template.as_deref(), Some("custom"));
        assert_eq!(
            config.balance_query_base_url.as_deref(),
            Some("https://balance.example.test")
        );
        assert_eq!(config.balance_query_user_id.as_deref(), Some("user-1"));
        assert_eq!(
            config.balance_query_config_json.as_deref(),
            Some(r#"{"path":"/usage","remainingPath":"left"}"#)
        );
        assert!(storage
            .find_aggregate_api_update_config_by_id("api-update-missing")
            .expect("find missing update config")
            .is_none());
    }

    #[test]
    fn list_aggregate_api_ids_reads_only_ids_in_api_order() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut first = sample_aggregate_api("api-first", now);
        first.sort = 1;
        first.updated_at = now;
        let mut second = sample_aggregate_api("api-second", now);
        second.sort = 0;
        second.updated_at = now.saturating_sub(10);

        for api in [&first, &second] {
            storage.insert_aggregate_api(api).expect("insert api");
        }

        assert_eq!(
            storage.list_aggregate_api_ids().expect("list api ids"),
            vec!["api-second".to_string(), "api-first".to_string()]
        );
    }

    #[test]
    fn aggregate_api_list_queries_use_list_order_index_without_temp_sort() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let list_queries = [
            (
                "aggregate API ids",
                aggregate_api_list_ids_sql().to_string(),
            ),
            ("aggregate API rows", aggregate_api_list_sql()),
            (
                "quota source summaries",
                aggregate_api_quota_source_summaries_list_sql().to_string(),
            ),
            (
                "balance jsons",
                aggregate_api_balance_jsons_list_sql().to_string(),
            ),
        ];

        for (label, sql) in list_queries {
            let details =
                collect_query_plan_details(&storage, &format!("EXPLAIN QUERY PLAN {sql}"));

            assert!(
                details
                    .iter()
                    .any(|detail| detail.contains("idx_aggregate_apis_list_order")),
                "expected {label} to use idx_aggregate_apis_list_order, got {details:?}"
            );
            assert!(
                !details
                    .iter()
                    .any(|detail| detail.contains("use temp b-tree for order by")),
                "expected {label} to avoid temp ORDER BY sorting, got {details:?}"
            );
        }
    }

    #[test]
    fn list_aggregate_api_quota_source_summaries_reads_only_quota_fields() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut first = sample_aggregate_api("api-first-quota-source", now);
        first.sort = 1;
        first.supplier_name = Some("First Supplier".to_string());
        first.auth_params_json = Some("ignored-secret".to_string());
        first.last_balance_at = Some(now);
        first.last_balance_status = Some("success".to_string());
        first.last_balance_json = Some(r#"{"remaining":1.25,"unit":"USD"}"#.to_string());
        let mut second = sample_aggregate_api("api-second-quota-source", now);
        second.sort = 0;
        second.url = "https://second.example.test/v1".to_string();
        second.supplier_name = None;
        second.status = "disabled".to_string();
        second.balance_query_enabled = false;
        second.last_balance_error = Some("balance failed".to_string());

        for api in [&first, &second] {
            storage.insert_aggregate_api(api).expect("insert api");
        }

        let summaries = storage
            .list_aggregate_api_quota_source_summaries()
            .expect("list aggregate api quota source summaries");

        assert_eq!(summaries.len(), 2);
        assert_eq!(summaries[0].id, "api-second-quota-source");
        assert_eq!(summaries[0].url, "https://second.example.test/v1");
        assert_eq!(summaries[0].supplier_name, None);
        assert_eq!(summaries[0].status, "disabled");
        assert!(!summaries[0].balance_query_enabled);
        assert_eq!(
            summaries[0].last_balance_error.as_deref(),
            Some("balance failed")
        );
        assert_eq!(summaries[1].id, "api-first-quota-source");
        assert_eq!(
            summaries[1].supplier_name.as_deref(),
            Some("First Supplier")
        );
        assert_eq!(summaries[1].last_balance_status.as_deref(), Some("success"));
        assert_eq!(
            summaries[1].last_balance_json.as_deref(),
            Some(r#"{"remaining":1.25,"unit":"USD"}"#)
        );
    }

    #[test]
    fn list_active_balance_query_aggregate_api_ids_filters_and_preserves_order() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut active_second = sample_aggregate_api("ag-active-second", now);
        active_second.balance_query_enabled = true;
        active_second.sort = 1;
        let mut disabled = sample_aggregate_api("ag-disabled", now);
        disabled.balance_query_enabled = true;
        disabled.status = "disabled".to_string();
        let mut no_balance = sample_aggregate_api("ag-no-balance", now);
        no_balance.status = "active".to_string();
        no_balance.balance_query_enabled = false;
        let mut active_first = sample_aggregate_api("ag-active-first", now);
        active_first.status = " ACTIVE ".to_string();
        active_first.balance_query_enabled = true;
        active_first.sort = 0;

        for api in [&active_second, &disabled, &no_balance, &active_first] {
            storage.insert_aggregate_api(api).expect("insert api");
        }

        let ids = storage
            .list_active_balance_query_aggregate_api_ids()
            .expect("list active balance query ids");

        assert_eq!(
            ids,
            vec![
                "ag-active-first".to_string(),
                "ag-active-second".to_string()
            ]
        );
    }

    #[test]
    fn list_balance_query_aggregate_api_ids_filters_without_status_and_preserves_order() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut active_second = sample_aggregate_api("ag-refresh-second", now);
        active_second.balance_query_enabled = true;
        active_second.sort = 1;
        let mut disabled_first = sample_aggregate_api("ag-refresh-disabled", now);
        disabled_first.balance_query_enabled = true;
        disabled_first.status = "disabled".to_string();
        disabled_first.sort = 0;
        let mut no_balance = sample_aggregate_api("ag-refresh-no-balance", now);
        no_balance.balance_query_enabled = false;
        no_balance.sort = -1;

        for api in [&active_second, &disabled_first, &no_balance] {
            storage.insert_aggregate_api(api).expect("insert api");
        }

        assert_eq!(
            storage
                .list_balance_query_aggregate_api_ids()
                .expect("list balance query aggregate api ids"),
            vec![
                "ag-refresh-disabled".to_string(),
                "ag-refresh-second".to_string()
            ]
        );
    }

    #[test]
    fn list_balance_query_aggregate_api_ids_for_ids_filters_and_preserves_order() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut first = sample_aggregate_api("ag-refresh-id-first", now);
        first.balance_query_enabled = true;
        first.sort = 1;
        let mut second = sample_aggregate_api("ag-refresh-id-second", now);
        second.balance_query_enabled = true;
        second.sort = 0;
        let mut ignored = sample_aggregate_api("ag-refresh-id-ignored", now);
        ignored.balance_query_enabled = false;
        ignored.sort = -1;

        for api in [&first, &second, &ignored] {
            storage.insert_aggregate_api(api).expect("insert api");
        }

        assert_eq!(
            storage
                .list_balance_query_aggregate_api_ids_for_ids(&[
                    "ag-refresh-id-first".to_string(),
                    "ag-refresh-id-ignored".to_string(),
                    "ag-refresh-id-second".to_string(),
                    "ag-refresh-id-first".to_string(),
                    "ag-refresh-id-missing".to_string(),
                ])
                .expect("list balance query ids for ids"),
            vec![
                "ag-refresh-id-second".to_string(),
                "ag-refresh-id-first".to_string()
            ]
        );
    }

    #[test]
    fn list_active_aggregate_api_ids_filters_and_preserves_order() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut active_second = sample_aggregate_api("ag-active-id-second", now);
        active_second.status = "active".to_string();
        active_second.sort = 1;
        let mut disabled = sample_aggregate_api("ag-disabled-id", now);
        disabled.status = "disabled".to_string();
        let mut active_first = sample_aggregate_api("ag-active-id-first", now);
        active_first.status = " ACTIVE ".to_string();
        active_first.sort = 0;
        active_first.created_at = now.saturating_add(10);

        for api in [&active_second, &disabled, &active_first] {
            storage.insert_aggregate_api(api).expect("insert api");
        }

        assert_eq!(
            storage
                .list_active_aggregate_api_ids()
                .expect("list active aggregate api ids"),
            vec![
                "ag-active-id-first".to_string(),
                "ag-active-id-second".to_string()
            ]
        );
    }

    #[test]
    fn list_active_aggregate_apis_filters_and_preserves_order() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut codex_second = sample_aggregate_api("ag-codex-second", now);
        codex_second.provider_type = "codex".to_string();
        codex_second.sort = 1;
        let mut disabled = sample_aggregate_api("ag-codex-disabled", now);
        disabled.provider_type = "codex".to_string();
        disabled.status = "disabled".to_string();
        let mut claude = sample_aggregate_api("ag-claude", now);
        claude.provider_type = "claude".to_string();
        let mut codex_first = sample_aggregate_api("ag-codex-first", now);
        codex_first.provider_type = "codex".to_string();
        codex_first.status = " ACTIVE ".to_string();
        codex_first.sort = 0;
        codex_first.created_at = now.saturating_add(10);

        for api in [&codex_second, &disabled, &claude, &codex_first] {
            storage.insert_aggregate_api(api).expect("insert api");
        }

        let ids = storage
            .list_active_aggregate_apis()
            .expect("list active aggregate apis")
            .into_iter()
            .map(|api| api.id)
            .collect::<Vec<_>>();

        assert_eq!(
            ids,
            vec![
                "ag-codex-first".to_string(),
                "ag-claude".to_string(),
                "ag-codex-second".to_string()
            ]
        );
    }

    #[test]
    fn list_active_aggregate_apis_by_provider_type_filters_in_sql_and_preserves_order() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut codex_second = sample_aggregate_api("ag-provider-codex-second", now);
        codex_second.provider_type = "codex".to_string();
        codex_second.sort = 1;
        let mut disabled = sample_aggregate_api("ag-provider-codex-disabled", now);
        disabled.provider_type = "codex".to_string();
        disabled.status = "disabled".to_string();
        let mut claude = sample_aggregate_api("ag-provider-claude", now);
        claude.provider_type = "claude".to_string();
        let mut gemini = sample_aggregate_api("ag-provider-gemini", now);
        gemini.provider_type = "google-gemini".to_string();
        let mut codex_first = sample_aggregate_api("ag-provider-codex-first", now);
        codex_first.provider_type = " openai-compatible ".to_string();
        codex_first.status = " ACTIVE ".to_string();
        codex_first.sort = 0;
        codex_first.created_at = now.saturating_add(10);

        for api in [&codex_second, &disabled, &claude, &gemini, &codex_first] {
            storage.insert_aggregate_api(api).expect("insert api");
        }

        let ids = storage
            .list_active_aggregate_apis_by_provider_type(" Codex ")
            .expect("list active aggregate apis by provider")
            .into_iter()
            .map(|api| api.id)
            .collect::<Vec<_>>();

        assert_eq!(
            ids,
            vec![
                "ag-provider-codex-first".to_string(),
                "ag-provider-codex-second".to_string()
            ]
        );
        assert!(storage
            .list_active_aggregate_apis_by_provider_type(" ")
            .expect("list empty provider")
            .is_empty());
    }

    #[test]
    fn aggregate_api_overview_stats_reads_counts_without_full_rows() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");
        let now = now_ts();

        let mut ok = sample_aggregate_api("ag-ok", now);
        ok.balance_query_enabled = true;
        ok.last_balance_status = Some("success".to_string());
        ok.last_balance_at = Some(now);
        ok.last_balance_json = Some(r#"{"remaining":12.5,"unit":"USD"}"#.to_string());
        let mut failed = sample_aggregate_api("ag-failed", now);
        failed.last_balance_status = Some("failed".to_string());
        failed.last_balance_at = Some(now.saturating_add(10));
        failed.last_balance_json = Some("   ".to_string());
        let mut errored = sample_aggregate_api("ag-error", now);
        errored.last_balance_status = Some("error".to_string());
        errored.last_balance_json = Some(r#"{"remaining":2,"unit":"USD"}"#.to_string());
        let mut unknown = sample_aggregate_api("ag-unknown", now);
        unknown.balance_query_enabled = true;
        unknown.last_balance_json = Some("not-json".to_string());

        for api in [&ok, &failed, &errored, &unknown] {
            storage.insert_aggregate_api(api).expect("insert api");
        }

        let stats = storage
            .aggregate_api_overview_stats()
            .expect("aggregate overview stats");
        assert_eq!(stats.source_count, 4);
        assert_eq!(stats.enabled_balance_query_count, 2);
        assert_eq!(stats.ok_count, 1);
        assert_eq!(stats.error_count, 2);
        assert_eq!(stats.last_refreshed_at, Some(now.saturating_add(10)));

        assert_eq!(
            storage
                .list_aggregate_api_balance_jsons()
                .expect("list balance jsons"),
            vec![
                r#"{"remaining":2,"unit":"USD"}"#.to_string(),
                r#"{"remaining":12.5,"unit":"USD"}"#.to_string(),
                "not-json".to_string()
            ]
        );
    }

    #[test]
    fn active_balance_query_lookup_uses_balance_query_index() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let details = collect_query_plan_details(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                active_balance_query_aggregate_api_ids_sql()
            ),
        );

        assert!(
            details
                .iter()
                .any(|detail| detail.contains("idx_aggregate_apis_balance_query_lookup")),
            "expected active balance query lookup to use idx_aggregate_apis_balance_query_lookup, got {details:?}"
        );
    }

    #[test]
    fn active_aggregate_api_lookup_uses_status_order_index() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let details = collect_query_plan_details(
            &storage,
            &format!("EXPLAIN QUERY PLAN {}", active_aggregate_api_ids_sql()),
        );

        assert!(
            details
                .iter()
                .any(|detail| detail.contains("idx_aggregate_apis_status_order")),
            "expected active aggregate api lookup to use idx_aggregate_apis_status_order, got {details:?}"
        );
    }

    #[test]
    fn active_aggregate_api_provider_lookup_uses_provider_status_order_index() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let details = collect_query_plan_details(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                active_aggregate_api_ids_sql_with_provider(&format!(
                    "{AGGREGATE_API_NORMALIZED_PROVIDER_SQL} = 'codex'"
                ))
            ),
        );

        assert!(
            details
                .iter()
                .any(|detail| detail.contains("idx_aggregate_apis_provider_status_order")),
            "expected active provider aggregate api lookup to use idx_aggregate_apis_provider_status_order, got {details:?}"
        );
    }

    #[test]
    fn balance_query_order_lookup_uses_balance_query_order_index() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let details = collect_query_plan_details(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                balance_query_aggregate_api_ids_sql()
            ),
        );

        assert!(
            details
                .iter()
                .any(|detail| detail.contains("idx_aggregate_apis_balance_query_order")),
            "expected balance query lookup to use idx_aggregate_apis_balance_query_order, got {details:?}"
        );
    }

    #[test]
    fn supplier_models_can_be_upserted_listed_and_deleted() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage
            .ensure_aggregate_api_supplier_model_tables()
            .expect("ensure tables");
        let now = now_ts();
        let model = AggregateApiSupplierModel {
            supplier_key: "test-supplier".to_string(),
            provider_type: "codex".to_string(),
            upstream_model: "provider-model".to_string(),
            display_name: Some("Provider Model".to_string()),
            status: "available".to_string(),
            created_at: now,
            updated_at: now,
        };

        storage
            .upsert_aggregate_api_supplier_model(&model)
            .expect("upsert model");
        let items = storage
            .list_aggregate_api_supplier_models(Some("test-supplier"), Some("codex"))
            .expect("list models");
        assert_eq!(items.len(), 1);
        assert_eq!(items[0].upstream_model, "provider-model");
        assert!(storage
            .list_aggregate_api_supplier_models(Some("missing-supplier"), Some("codex"))
            .expect("list missing supplier")
            .is_empty());
        assert!(storage
            .list_aggregate_api_supplier_models(Some("test-supplier"), Some("missing-provider"))
            .expect("list missing provider")
            .is_empty());

        let mut disabled = model.clone();
        disabled.status = "disabled".to_string();
        disabled.updated_at = now + 1;
        storage
            .upsert_aggregate_api_supplier_model(&disabled)
            .expect("update model");
        let items = storage
            .list_aggregate_api_supplier_models(Some("test-supplier"), Some("codex"))
            .expect("list updated models");
        assert_eq!(items[0].status, "disabled");

        storage
            .delete_aggregate_api_supplier_model("test-supplier", "codex", "provider-model")
            .expect("delete model");
        let items = storage
            .list_aggregate_api_supplier_models(Some("test-supplier"), Some("codex"))
            .expect("list deleted models");
        assert!(items.is_empty());
    }

    #[test]
    fn supplier_model_filter_query_uses_supplier_index() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage
            .ensure_aggregate_api_supplier_model_tables()
            .expect("ensure tables");

        let details = collect_query_plan_details(
            &storage,
            "EXPLAIN QUERY PLAN
             SELECT supplier_key, provider_type, upstream_model, display_name,
                    status, created_at, updated_at
             FROM aggregate_api_supplier_models
             WHERE supplier_key = 'test-supplier' AND provider_type = 'codex'
             ORDER BY supplier_key ASC, provider_type ASC, upstream_model ASC",
        );

        assert!(details.iter().any(|detail| {
            detail.contains("search aggregate_api_supplier_models") && detail.contains("index")
        }));
    }
    #[test]
    fn aggregate_api_write_helpers_use_primary_key_indexes() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        fn assert_aggregate_api_pk(storage: &Storage, label: &str, sql: &str, params: Vec<Value>) {
            let details = collect_query_plan_details_with_params(
                storage,
                &format!("EXPLAIN QUERY PLAN {sql}"),
                params,
            );
            assert!(
                details.iter().any(
                    |detail| detail.contains("sqlite_autoindex_aggregate_apis_1")
                        || detail.contains("using index")
                ),
                "expected {label} to use aggregate API primary-key index, got {details:?}"
            );
        }

        assert_aggregate_api_pk(
            &storage,
            "url update",
            update_aggregate_api_url_sql(),
            vec![
                Value::Text("url".to_string()),
                Value::Integer(1),
                Value::Text("api-a".to_string()),
            ],
        );
        assert_aggregate_api_pk(
            &storage,
            "supplier update",
            update_aggregate_api_supplier_name_sql(),
            vec![
                Value::Text("supplier".to_string()),
                Value::Integer(1),
                Value::Text("api-a".to_string()),
            ],
        );
        assert_aggregate_api_pk(
            &storage,
            "sort update",
            update_aggregate_api_sort_sql(),
            vec![
                Value::Integer(1),
                Value::Integer(2),
                Value::Text("api-a".to_string()),
            ],
        );
        assert_aggregate_api_pk(
            &storage,
            "status update",
            update_aggregate_api_status_sql(),
            vec![
                Value::Text("active".to_string()),
                Value::Integer(1),
                Value::Text("api-a".to_string()),
            ],
        );
        assert_aggregate_api_pk(
            &storage,
            "provider type update",
            update_aggregate_api_provider_type_sql(),
            vec![
                Value::Text("openai".to_string()),
                Value::Integer(1),
                Value::Text("api-a".to_string()),
            ],
        );
        assert_aggregate_api_pk(
            &storage,
            "auth type update",
            update_aggregate_api_auth_type_sql(),
            vec![
                Value::Text("bearer".to_string()),
                Value::Integer(1),
                Value::Text("api-a".to_string()),
            ],
        );
        assert_aggregate_api_pk(
            &storage,
            "auth params update",
            update_aggregate_api_auth_params_json_sql(),
            vec![
                Value::Text("{}".to_string()),
                Value::Integer(1),
                Value::Text("api-a".to_string()),
            ],
        );
        assert_aggregate_api_pk(
            &storage,
            "action update",
            update_aggregate_api_action_sql(),
            vec![
                Value::Text("proxy".to_string()),
                Value::Integer(1),
                Value::Text("api-a".to_string()),
            ],
        );
        assert_aggregate_api_pk(
            &storage,
            "model override update",
            update_aggregate_api_model_override_sql(),
            vec![
                Value::Text("gpt-5".to_string()),
                Value::Integer(1),
                Value::Text("api-a".to_string()),
            ],
        );
        assert_aggregate_api_pk(
            &storage,
            "balance query update",
            update_aggregate_api_balance_query_sql(),
            vec![
                Value::Integer(1),
                Value::Text("template".to_string()),
                Value::Text("base".to_string()),
                Value::Text("user".to_string()),
                Value::Text("{}".to_string()),
                Value::Integer(1),
                Value::Text("api-a".to_string()),
            ],
        );
        assert_aggregate_api_pk(
            &storage,
            "balance result update",
            update_aggregate_api_balance_result_sql(),
            vec![
                Value::Integer(1),
                Value::Text("success".to_string()),
                Value::Null,
                Value::Text("{}".to_string()),
                Value::Text("api-a".to_string()),
            ],
        );
        assert_aggregate_api_pk(
            &storage,
            "test result update",
            update_aggregate_api_test_result_sql(),
            vec![
                Value::Integer(1),
                Value::Text("success".to_string()),
                Value::Null,
                Value::Text("api-a".to_string()),
            ],
        );
        assert_aggregate_api_pk(
            &storage,
            "last test error update",
            update_aggregate_api_last_test_error_sql(),
            vec![
                Value::Text("http_status=500".to_string()),
                Value::Text("api-a".to_string()),
            ],
        );
        assert_aggregate_api_pk(
            &storage,
            "delete aggregate api",
            delete_aggregate_api_by_id_sql(),
            vec![Value::Text("api-a".to_string())],
        );

        for (label, sql, expected_index, params) in [
            (
                "delete aggregate api secret",
                delete_aggregate_api_secret_by_id_sql(),
                "sqlite_autoindex_aggregate_api_secrets_1",
                vec![Value::Text("api-a".to_string())],
            ),
            (
                "delete aggregate api balance secret",
                delete_aggregate_api_balance_secret_by_id_sql(),
                "sqlite_autoindex_aggregate_api_balance_secrets_1",
                vec![Value::Text("api-a".to_string())],
            ),
            (
                "delete aggregate api supplier model",
                delete_aggregate_api_supplier_model_sql(),
                "sqlite_autoindex_aggregate_api_supplier_models_1",
                vec![
                    Value::Text("supplier".to_string()),
                    Value::Text("codex".to_string()),
                    Value::Text("gpt-5".to_string()),
                ],
            ),
        ] {
            let details = collect_query_plan_details_with_params(
                &storage,
                &format!("EXPLAIN QUERY PLAN {sql}"),
                params,
            );
            assert!(
                details.iter().any(|detail| detail.contains(expected_index)),
                "expected {label} to use {expected_index}, got {details:?}"
            );
        }
    }
}
