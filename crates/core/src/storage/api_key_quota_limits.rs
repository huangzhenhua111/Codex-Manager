use std::collections::HashMap;

use rusqlite::{params_from_iter, OptionalExtension, Result};

use super::key_id_filters::{key_id_in_clause, normalize_key_ids, SQLITE_IN_CLAUSE_BATCH_SIZE};
use super::{now_ts, ApiKeyQuotaOverviewStats, Storage};

fn api_key_quota_limit_select_columns() -> &'static str {
    "key_id, quota_limit_tokens"
}

fn api_key_quota_limit_value_by_key_sql() -> &'static str {
    "SELECT quota_limit_tokens
     FROM api_key_quota_limits
     WHERE key_id = ?1
     LIMIT 1"
}

pub(super) fn delete_api_key_quota_limit_by_key_sql() -> &'static str {
    "DELETE FROM api_key_quota_limits WHERE key_id = ?1"
}

fn api_key_quota_limit_list_sql() -> String {
    format!(
        "SELECT {columns}
         FROM api_key_quota_limits
         WHERE quota_limit_tokens > 0",
        columns = api_key_quota_limit_select_columns(),
    )
}

impl Storage {
    pub fn upsert_api_key_quota_limit(
        &self,
        key_id: &str,
        quota_limit_tokens: Option<i64>,
    ) -> Result<()> {
        let normalized = quota_limit_tokens.filter(|value| *value > 0);
        let Some(limit) = normalized else {
            self.conn
                .execute(delete_api_key_quota_limit_by_key_sql(), [key_id])?;
            return Ok(());
        };

        let now = now_ts();
        self.conn.execute(
            "INSERT INTO api_key_quota_limits (
                key_id, quota_limit_tokens, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?3)
             ON CONFLICT(key_id) DO UPDATE SET
                quota_limit_tokens = excluded.quota_limit_tokens,
                updated_at = excluded.updated_at",
            (key_id, limit, now),
        )?;
        Ok(())
    }

    pub fn find_api_key_quota_limit(&self, key_id: &str) -> Result<Option<i64>> {
        self.conn
            .query_row(api_key_quota_limit_value_by_key_sql(), [key_id], |row| {
                row.get(0)
            })
            .optional()
    }

    pub fn list_api_key_quota_limits(&self) -> Result<HashMap<String, i64>> {
        let mut stmt = self.conn.prepare(&api_key_quota_limit_list_sql())?;
        let mut rows = stmt.query([])?;
        let mut out = HashMap::new();
        while let Some(row) = rows.next()? {
            out.insert(row.get(0)?, row.get(1)?);
        }
        Ok(out)
    }

    pub fn list_api_key_quota_limits_for_ids(
        &self,
        key_ids: &[String],
    ) -> Result<HashMap<String, i64>> {
        let key_ids = normalize_key_ids(key_ids);
        if key_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut out = HashMap::new();
        for chunk in key_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(list_api_key_quota_limits_for_ids_chunk(self, chunk)?);
        }
        Ok(out)
    }

    pub fn api_key_total_token_usage(&self, key_id: &str) -> Result<i64> {
        let key_id = key_id.trim();
        if key_id.is_empty() {
            return Ok(0);
        }
        let mut stmt = self.conn.prepare(api_key_total_token_usage_sql())?;
        let mut rows = stmt.query([key_id])?;
        if let Some(row) = rows.next()? {
            let total: i64 = row.get(0)?;
            return Ok(total.max(0));
        }
        Ok(0)
    }

    pub fn api_key_remaining_quota_tokens(&self) -> Result<i64> {
        let sql = api_key_remaining_quota_tokens_sql();
        self.conn.query_row(&sql, [], |row| row.get(0))
    }

    pub fn api_key_quota_overview_stats(&self) -> Result<ApiKeyQuotaOverviewStats> {
        let sql = format!(
            "{key_usage_cte}
             SELECT
                COUNT(k.id) AS key_count,
                IFNULL(SUM(CASE WHEN q.quota_limit_tokens > 0 THEN 1 ELSE 0 END), 0) AS limited_key_count,
                IFNULL(SUM(CASE WHEN q.quota_limit_tokens > 0 THEN q.quota_limit_tokens ELSE 0 END), 0) AS total_limit_tokens,
                IFNULL(SUM(IFNULL(u.used_tokens, 0)), 0) AS total_used_tokens,
                IFNULL(
                    SUM(
                        CASE
                            WHEN q.quota_limit_tokens > 0 THEN
                                MAX(q.quota_limit_tokens - IFNULL(u.used_tokens, 0), 0)
                            ELSE 0
                        END
                    ),
                    0
                ) AS total_remaining_tokens,
                IFNULL(SUM(IFNULL(u.estimated_cost_usd, 0.0)), 0.0) AS estimated_cost_usd
             FROM api_keys k
             LEFT JOIN api_key_quota_limits q
               ON q.key_id = k.id
              AND q.quota_limit_tokens > 0
             LEFT JOIN key_usage u ON u.key_id = k.id",
            key_usage_cte = api_key_usage_cte_sql(true, ApiKeyUsageScope::AllKeys),
        );
        self.conn.query_row(&sql, [], |row| {
            Ok(ApiKeyQuotaOverviewStats {
                key_count: row.get(0)?,
                limited_key_count: row.get(1)?,
                total_limit_tokens: row.get(2)?,
                total_used_tokens: row.get(3)?,
                total_remaining_tokens: row.get(4)?,
                estimated_cost_usd: row.get(5)?,
            })
        })
    }

    pub(super) fn ensure_api_key_quota_limits_table(&self) -> Result<()> {
        self.conn.execute(
            "CREATE TABLE IF NOT EXISTS api_key_quota_limits (
                key_id TEXT PRIMARY KEY REFERENCES api_keys(id) ON DELETE CASCADE,
                quota_limit_tokens INTEGER NOT NULL,
                created_at INTEGER NOT NULL,
                updated_at INTEGER NOT NULL
            )",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_api_key_quota_limits_updated_at
             ON api_key_quota_limits(updated_at DESC)",
            [],
        )?;
        Ok(())
    }
}

fn api_key_total_token_usage_sql() -> &'static str {
    "WITH selected_stats AS (
        SELECT
            input_tokens,
            cached_input_tokens,
            output_tokens,
            total_tokens
        FROM request_token_stats
        WHERE key_id = ?1
          AND TRIM(key_id) <> ''
        UNION ALL
        SELECT
            input_tokens,
            cached_input_tokens,
            output_tokens,
            total_tokens
        FROM request_token_stat_hourly_rollups
        WHERE key_id = ?1
          AND TRIM(key_id) <> ''
        UNION ALL
        SELECT
            input_tokens,
            cached_input_tokens,
            output_tokens,
            total_tokens
        FROM request_token_stat_rollups
        WHERE key_id = ?1
          AND TRIM(key_id) <> ''
     )
     SELECT
        IFNULL(
            SUM(
                CASE
                    WHEN total_tokens IS NOT NULL THEN
                        CASE WHEN total_tokens > 0 THEN total_tokens ELSE 0 END
                    ELSE
                        CASE
                            WHEN IFNULL(input_tokens, 0) - IFNULL(cached_input_tokens, 0) + IFNULL(output_tokens, 0) > 0
                                THEN IFNULL(input_tokens, 0) - IFNULL(cached_input_tokens, 0) + IFNULL(output_tokens, 0)
                            ELSE 0
                        END
                END
            ),
            0
        ) AS total_tokens
     FROM selected_stats"
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ApiKeyUsageScope {
    AllKeys,
    LimitedQuotaKeys,
}

fn api_key_remaining_quota_tokens_sql() -> String {
    format!(
        "{key_usage_cte}
         SELECT
            IFNULL(
                SUM(MAX(q.quota_limit_tokens - IFNULL(u.used_tokens, 0), 0)),
                0
            ) AS total_remaining_tokens
         FROM api_key_quota_limits q
         INNER JOIN api_keys k ON k.id = q.key_id
         LEFT JOIN key_usage u ON u.key_id = q.key_id
         WHERE q.quota_limit_tokens > 0",
        key_usage_cte = api_key_usage_cte_sql(false, ApiKeyUsageScope::LimitedQuotaKeys),
    )
}

fn api_key_usage_cte_sql(include_cost: bool, scope: ApiKeyUsageScope) -> String {
    let raw_cost_select = if include_cost {
        ",
                        CASE WHEN IFNULL(s.estimated_cost_usd, 0.0) > 0.0 THEN s.estimated_cost_usd ELSE 0.0 END AS estimated_cost_usd"
    } else {
        ""
    };
    let rollup_cost_select = if include_cost {
        ",
                        CASE WHEN IFNULL({rollup_alias}.estimated_cost_usd, 0.0) > 0.0 THEN {rollup_alias}.estimated_cost_usd ELSE 0.0 END AS estimated_cost_usd"
    } else {
        ""
    };
    let grouped_cost_select = if include_cost {
        ",
                    IFNULL(SUM(IFNULL(estimated_cost_usd, 0.0)), 0.0) AS estimated_cost_usd"
    } else {
        ""
    };
    let raw_from = api_key_usage_from_sql("request_token_stats", "s", scope);
    let hourly_from = api_key_usage_from_sql("request_token_stat_hourly_rollups", "h", scope);
    let legacy_from = api_key_usage_from_sql("request_token_stat_rollups", "r", scope);
    let raw_where = api_key_usage_where_sql("s", scope);
    let hourly_where = api_key_usage_where_sql("h", scope);
    let legacy_where = api_key_usage_where_sql("r", scope);
    let hourly_cost_select = rollup_cost_select.replace("{rollup_alias}", "h");
    let legacy_cost_select = rollup_cost_select.replace("{rollup_alias}", "r");

    format!(
        "WITH key_usage AS (
                SELECT
                    key_id,
                    IFNULL(SUM(IFNULL(total_tokens, 0)), 0) AS used_tokens{grouped_cost_select}
                FROM (
                    SELECT
                        s.key_id AS key_id,
                        CASE
                            WHEN s.total_tokens IS NOT NULL THEN
                                CASE WHEN s.total_tokens > 0 THEN s.total_tokens ELSE 0 END
                            ELSE
                                CASE
                                    WHEN IFNULL(s.input_tokens, 0) - IFNULL(s.cached_input_tokens, 0) + IFNULL(s.output_tokens, 0) > 0
                                        THEN IFNULL(s.input_tokens, 0) - IFNULL(s.cached_input_tokens, 0) + IFNULL(s.output_tokens, 0)
                                    ELSE 0
                                END
                        END AS total_tokens{raw_cost_select}
                    FROM {raw_from}
                    WHERE {raw_where}
                    UNION ALL
                    SELECT
                        NULLIF(TRIM(h.key_id), '') AS key_id,
                        CASE WHEN IFNULL(h.total_tokens, 0) > 0 THEN h.total_tokens ELSE 0 END AS total_tokens{hourly_cost_select}
                    FROM {hourly_from}
                    WHERE {hourly_where}
                    UNION ALL
                    SELECT
                        NULLIF(TRIM(r.key_id), '') AS key_id,
                        CASE WHEN IFNULL(r.total_tokens, 0) > 0 THEN r.total_tokens ELSE 0 END AS total_tokens{legacy_cost_select}
                    FROM {legacy_from}
                    WHERE {legacy_where}
                )
                WHERE key_id IS NOT NULL AND TRIM(key_id) <> ''
                GROUP BY key_id
             )",
    )
}

fn api_key_usage_from_sql(table: &str, alias: &str, scope: ApiKeyUsageScope) -> String {
    match scope {
        ApiKeyUsageScope::AllKeys => format!("{table} {alias}"),
        ApiKeyUsageScope::LimitedQuotaKeys => {
            format!("api_key_quota_limits q_{alias} CROSS JOIN {table} {alias}")
        }
    }
}

fn api_key_usage_where_sql(alias: &str, scope: ApiKeyUsageScope) -> String {
    let key_filter = format!("{alias}.key_id IS NOT NULL AND TRIM({alias}.key_id) <> ''");
    match scope {
        ApiKeyUsageScope::AllKeys => key_filter,
        ApiKeyUsageScope::LimitedQuotaKeys => {
            format!(
                "q_{alias}.quota_limit_tokens > 0 AND {alias}.key_id = q_{alias}.key_id AND {key_filter}"
            )
        }
    }
}

fn list_api_key_quota_limits_for_ids_chunk(
    storage: &Storage,
    key_ids: &[String],
) -> Result<HashMap<String, i64>> {
    let Some((clause, params)) = key_id_in_clause("key_id", key_ids) else {
        return Ok(HashMap::new());
    };
    let sql = api_key_quota_limits_for_ids_chunk_sql(&clause);
    let mut stmt = storage.conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(params))?;
    let mut out = HashMap::new();
    while let Some(row) = rows.next()? {
        out.insert(row.get(0)?, row.get(1)?);
    }
    Ok(out)
}

fn api_key_quota_limits_for_ids_chunk_sql(key_condition: &str) -> String {
    format!(
        "SELECT {columns}
         FROM api_key_quota_limits
         WHERE quota_limit_tokens > 0
           AND {key_condition}",
        columns = api_key_quota_limit_select_columns(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn collect_query_plan_details(storage: &Storage, sql: &str) -> Vec<String> {
        let mut stmt = storage.conn.prepare(sql).expect("prepare explain");
        let mut rows = stmt.query([]).expect("query explain");
        let mut details = Vec::new();
        while let Some(row) = rows.next().expect("read explain row") {
            let detail: String = row.get(3).expect("plan detail");
            details.push(detail.to_ascii_lowercase());
        }
        details
    }

    #[test]
    fn api_key_total_usage_filters_each_stats_table_by_key_id() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let sql = format!(
            "EXPLAIN QUERY PLAN {}",
            api_key_total_token_usage_sql().replace("?1", "'key-quota-1'")
        );

        let details = collect_query_plan_details(&storage, &sql);

        assert!(
            details.iter().any(|detail| {
                detail.contains("search request_token_stats")
                    && detail.contains("using index")
                    && detail.contains("key_id=?")
            }),
            "expected raw stats key lookup index search in plan, got {details:?}"
        );
        assert!(
            details
                .iter()
                .any(|detail| detail.contains("idx_request_token_stat_hourly_rollups_key_bucket")),
            "expected hourly rollup key lookup index in plan, got {details:?}"
        );
        assert!(
            details
                .iter()
                .any(|detail| detail.contains("idx_request_token_stat_rollups_key_id")),
            "expected legacy rollup key lookup index in plan, got {details:?}"
        );
    }

    #[test]
    fn api_key_remaining_quota_usage_scopes_stats_to_limited_keys() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let sql = format!(
            "EXPLAIN QUERY PLAN {}",
            api_key_remaining_quota_tokens_sql()
        );

        let details = collect_query_plan_details(&storage, &sql);

        assert!(
            details.iter().any(|detail| {
                detail.contains("search s")
                    && detail.contains("idx_request_token_stats_key")
                    && detail.contains("key_id=?")
            }),
            "expected limited-key raw usage lookup by key index, got {details:?}"
        );
        assert!(
            details.iter().any(|detail| {
                detail.contains("search h")
                    && detail.contains("idx_request_token_stat_hourly_rollups_key_bucket")
            }),
            "expected limited-key hourly usage lookup by key index, got {details:?}"
        );
        assert!(
            details.iter().any(|detail| {
                detail.contains("search r")
                    && detail.contains("idx_request_token_stat_rollups_key_id")
            }),
            "expected limited-key legacy rollup lookup by key index, got {details:?}"
        );
    }

    #[test]
    fn api_key_quota_limits_for_ids_uses_key_lookup_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let sql = api_key_quota_limits_for_ids_chunk_sql("key_id IN ('key-a', 'key-b')");
        let details = collect_query_plan_details(&storage, &format!("EXPLAIN QUERY PLAN {sql}"));

        assert!(
            details
                .iter()
                .any(|detail| detail.contains("sqlite_autoindex_api_key_quota_limits_1")),
            "expected quota limit chunk query to use quota limit primary-key lookup, got {details:?}"
        );
        assert!(
            !details
                .iter()
                .any(|detail| detail.contains("use temp b-tree for order by")),
            "quota limit chunk query should avoid per-chunk ORDER BY temp sorting, got {details:?}"
        );
    }

    #[test]
    fn api_key_quota_limit_lookup_uses_primary_key_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let sql = format!(
            "EXPLAIN QUERY PLAN {}",
            api_key_quota_limit_value_by_key_sql().replace("?1", "'key-quota-1'")
        );

        let details = collect_query_plan_details(&storage, &sql);

        assert!(
            details
                .iter()
                .any(|detail| detail.contains("sqlite_autoindex_api_key_quota_limits_1")),
            "expected quota limit lookup to use quota limit primary-key index, got {details:?}"
        );

        let delete_sql = format!(
            "EXPLAIN QUERY PLAN {}",
            delete_api_key_quota_limit_by_key_sql().replace("?1", "'key-quota-1'")
        );
        let delete_details = collect_query_plan_details(&storage, &delete_sql);
        assert!(
            delete_details
                .iter()
                .any(|detail| detail.contains("sqlite_autoindex_api_key_quota_limits_1")),
            "expected quota limit delete to use quota limit primary-key index, got {delete_details:?}"
        );
    }

    #[test]
    fn api_key_total_usage_ignores_blank_key_ids() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        storage
            .conn
            .execute(
                "INSERT INTO request_token_stats (
                    request_log_id, key_id, account_id, model,
                    input_tokens, cached_input_tokens, output_tokens, total_tokens,
                    reasoning_output_tokens, estimated_cost_usd, created_at
                 ) VALUES (1, '', NULL, 'gpt-test', 1, 0, 1, 2, 0, 0.0, 1)",
                [],
            )
            .expect("seed blank key usage");

        assert_eq!(
            storage
                .api_key_total_token_usage(" ")
                .expect("blank key usage"),
            0
        );
    }
}
