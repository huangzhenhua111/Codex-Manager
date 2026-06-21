use std::collections::HashMap;

use rusqlite::{params_from_iter, OptionalExtension, Result};

use super::key_id_filters::{key_id_in_clause, normalize_key_ids, SQLITE_IN_CLAUSE_BATCH_SIZE};
use super::{now_ts, ApiKeyQuotaOverviewStats, Storage};

impl Storage {
    pub fn upsert_api_key_quota_limit(
        &self,
        key_id: &str,
        quota_limit_tokens: Option<i64>,
    ) -> Result<()> {
        let normalized = quota_limit_tokens.filter(|value| *value > 0);
        let Some(limit) = normalized else {
            self.conn.execute(
                "DELETE FROM api_key_quota_limits WHERE key_id = ?1",
                [key_id],
            )?;
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
            .query_row(
                "SELECT quota_limit_tokens
                 FROM api_key_quota_limits
                 WHERE key_id = ?1
                 LIMIT 1",
                [key_id],
                |row| row.get(0),
            )
            .optional()
    }

    pub fn list_api_key_quota_limits(&self) -> Result<HashMap<String, i64>> {
        let mut stmt = self.conn.prepare(
            "SELECT key_id, quota_limit_tokens
             FROM api_key_quota_limits
             WHERE quota_limit_tokens > 0",
        )?;
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
        let mut stmt = self.conn.prepare(
            "WITH all_stats AS (
                SELECT
                    key_id,
                    input_tokens,
                    cached_input_tokens,
                    output_tokens,
                    total_tokens
                FROM request_token_stats
                UNION ALL
                SELECT
                    NULLIF(key_id, '') AS key_id,
                    input_tokens,
                    cached_input_tokens,
                    output_tokens,
                    total_tokens
                FROM request_token_stat_hourly_rollups
                UNION ALL
                SELECT
                    NULLIF(key_id, '') AS key_id,
                    input_tokens,
                    cached_input_tokens,
                    output_tokens,
                    total_tokens
                FROM request_token_stat_rollups
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
             FROM all_stats
             WHERE key_id = ?1",
        )?;
        let mut rows = stmt.query([key_id])?;
        if let Some(row) = rows.next()? {
            let total: i64 = row.get(0)?;
            return Ok(total.max(0));
        }
        Ok(0)
    }

    pub fn api_key_remaining_quota_tokens(&self) -> Result<i64> {
        let sql = format!(
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
            key_usage_cte = api_key_usage_cte_sql(false),
        );
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
            key_usage_cte = api_key_usage_cte_sql(true),
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

fn api_key_usage_cte_sql(include_cost: bool) -> String {
    let raw_cost_select = if include_cost {
        ",
                        CASE WHEN IFNULL(estimated_cost_usd, 0.0) > 0.0 THEN estimated_cost_usd ELSE 0.0 END AS estimated_cost_usd"
    } else {
        ""
    };
    let rollup_cost_select = if include_cost {
        ",
                        CASE WHEN IFNULL(estimated_cost_usd, 0.0) > 0.0 THEN estimated_cost_usd ELSE 0.0 END AS estimated_cost_usd"
    } else {
        ""
    };
    let grouped_cost_select = if include_cost {
        ",
                    IFNULL(SUM(IFNULL(estimated_cost_usd, 0.0)), 0.0) AS estimated_cost_usd"
    } else {
        ""
    };

    format!(
        "WITH key_usage AS (
                SELECT
                    key_id,
                    IFNULL(SUM(IFNULL(total_tokens, 0)), 0) AS used_tokens{grouped_cost_select}
                FROM (
                    SELECT
                        key_id,
                        CASE
                            WHEN total_tokens IS NOT NULL THEN
                                CASE WHEN total_tokens > 0 THEN total_tokens ELSE 0 END
                            ELSE
                                CASE
                                    WHEN IFNULL(input_tokens, 0) - IFNULL(cached_input_tokens, 0) + IFNULL(output_tokens, 0) > 0
                                        THEN IFNULL(input_tokens, 0) - IFNULL(cached_input_tokens, 0) + IFNULL(output_tokens, 0)
                                    ELSE 0
                                END
                        END AS total_tokens{raw_cost_select}
                    FROM request_token_stats
                    WHERE key_id IS NOT NULL AND TRIM(key_id) <> ''
                    UNION ALL
                    SELECT
                        NULLIF(TRIM(key_id), '') AS key_id,
                        CASE WHEN IFNULL(total_tokens, 0) > 0 THEN total_tokens ELSE 0 END AS total_tokens{rollup_cost_select}
                    FROM request_token_stat_hourly_rollups
                    WHERE key_id IS NOT NULL AND TRIM(key_id) <> ''
                    UNION ALL
                    SELECT
                        NULLIF(TRIM(key_id), '') AS key_id,
                        CASE WHEN IFNULL(total_tokens, 0) > 0 THEN total_tokens ELSE 0 END AS total_tokens{rollup_cost_select}
                    FROM request_token_stat_rollups
                    WHERE key_id IS NOT NULL AND TRIM(key_id) <> ''
                )
                WHERE key_id IS NOT NULL AND TRIM(key_id) <> ''
                GROUP BY key_id
             )",
    )
}

fn list_api_key_quota_limits_for_ids_chunk(
    storage: &Storage,
    key_ids: &[String],
) -> Result<HashMap<String, i64>> {
    let Some((clause, params)) = key_id_in_clause("key_id", key_ids) else {
        return Ok(HashMap::new());
    };
    let sql = format!(
        "SELECT key_id, quota_limit_tokens
         FROM api_key_quota_limits
         WHERE quota_limit_tokens > 0
           AND {clause}"
    );
    let mut stmt = storage.conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(params))?;
    let mut out = HashMap::new();
    while let Some(row) = rows.next()? {
        out.insert(row.get(0)?, row.get(1)?);
    }
    Ok(out)
}
