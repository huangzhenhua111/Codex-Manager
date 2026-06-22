use rusqlite::{params_from_iter, Result, Row};

use super::key_id_filters::{normalize_text_ids, text_id_in_clause, SQLITE_IN_CLAUSE_BATCH_SIZE};
use super::{
    Storage, UsageSnapshotCleanupRow, UsageSnapshotQuotaSourceRow, UsageSnapshotRecord,
    UsageSnapshotSummaryRow,
};

const DEFAULT_USAGE_SNAPSHOTS_RETAIN_PER_ACCOUNT: usize = 1;
const USAGE_SNAPSHOTS_RETAIN_PER_ACCOUNT_ENV: &str =
    "CODEXMANAGER_USAGE_SNAPSHOTS_RETAIN_PER_ACCOUNT";

pub(super) fn usage_snapshots_retain_per_account() -> usize {
    std::env::var(USAGE_SNAPSHOTS_RETAIN_PER_ACCOUNT_ENV)
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_USAGE_SNAPSHOTS_RETAIN_PER_ACCOUNT)
}

fn usage_snapshot_count_for_account_sql() -> &'static str {
    "SELECT COUNT(1) FROM usage_snapshots WHERE account_id = ?1"
}

fn usage_snapshot_count_sql() -> &'static str {
    "SELECT COUNT(1) FROM usage_snapshots"
}

fn latest_usage_snapshot_sql() -> &'static str {
    "SELECT account_id, used_percent, window_minutes, resets_at, secondary_used_percent, secondary_window_minutes, secondary_resets_at, credits_json, captured_at FROM usage_snapshots ORDER BY captured_at DESC, id DESC LIMIT 1"
}

fn latest_usage_snapshot_for_account_sql() -> &'static str {
    "SELECT account_id, used_percent, window_minutes, resets_at, secondary_used_percent, secondary_window_minutes, secondary_resets_at, credits_json, captured_at
     FROM usage_snapshots
     WHERE account_id = ?1
     ORDER BY captured_at DESC, id DESC
     LIMIT 1"
}

fn latest_usage_snapshot_summary_rows_sql() -> String {
    format!(
        "{cte}
        SELECT
            account_id,
            used_percent,
            window_minutes,
            secondary_used_percent,
            secondary_window_minutes,
            credits_json
        FROM ranked
        WHERE rn = 1",
        cte = latest_usage_ranked_cte_sql(
            "account_id,
                used_percent,
                window_minutes,
                secondary_used_percent,
                secondary_window_minutes,
                credits_json",
            None,
        ),
    )
}

pub(super) fn delete_usage_snapshots_for_account_sql() -> &'static str {
    "DELETE FROM usage_snapshots WHERE account_id = ?1"
}

fn prune_usage_snapshots_for_account_sql() -> &'static str {
    "DELETE FROM usage_snapshots
     WHERE account_id = ?1
       AND id NOT IN (
         SELECT id
         FROM usage_snapshots
         WHERE account_id = ?1
         ORDER BY captured_at DESC, id DESC
         LIMIT ?2
       )"
}

fn prune_usage_snapshots_all_accounts_sql() -> &'static str {
    "DELETE FROM usage_snapshots
     WHERE id IN (
         SELECT id
         FROM (
             SELECT
                 id,
                 ROW_NUMBER() OVER (
                     PARTITION BY account_id
                     ORDER BY captured_at DESC, id DESC
                 ) AS rn
             FROM usage_snapshots
         )
         WHERE rn > ?1
     )"
}

impl Storage {
    /// 函数 `insert_usage_snapshot`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - snap: 参数 snap
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn insert_usage_snapshot(&self, snap: &UsageSnapshotRecord) -> Result<()> {
        self.conn.execute(
            "INSERT INTO usage_snapshots (account_id, used_percent, window_minutes, resets_at, secondary_used_percent, secondary_window_minutes, secondary_resets_at, credits_json, captured_at) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            (
                &snap.account_id,
                snap.used_percent,
                snap.window_minutes,
                snap.resets_at,
                snap.secondary_used_percent,
                snap.secondary_window_minutes,
                snap.secondary_resets_at,
                &snap.credits_json,
                snap.captured_at,
            ),
        )?;
        Ok(())
    }

    /// 函数 `prune_usage_snapshots_for_account`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - account_id: 参数 account_id
    /// - retain: 参数 retain
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn prune_usage_snapshots_for_account(
        &self,
        account_id: &str,
        retain: usize,
    ) -> Result<usize> {
        if retain == 0 {
            return Ok(0);
        }
        self.conn.execute(
            prune_usage_snapshots_for_account_sql(),
            (account_id, retain as i64),
        )
    }

    pub fn prune_usage_snapshots_all_accounts(&self, retain: usize) -> Result<usize> {
        if retain == 0 {
            return Ok(0);
        }
        self.conn
            .execute(prune_usage_snapshots_all_accounts_sql(), [retain as i64])
    }

    /// 函数 `usage_snapshot_count_for_account`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - account_id: 参数 account_id
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn usage_snapshot_count_for_account(&self, account_id: &str) -> Result<i64> {
        self.conn.query_row(
            usage_snapshot_count_for_account_sql(),
            [account_id],
            |row| row.get(0),
        )
    }

    pub fn usage_snapshot_count(&self) -> Result<i64> {
        self.conn
            .query_row(usage_snapshot_count_sql(), [], |row| row.get(0))
    }

    /// 函数 `latest_usage_snapshot`
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
    pub fn latest_usage_snapshot(&self) -> Result<Option<UsageSnapshotRecord>> {
        let mut stmt = self.conn.prepare(latest_usage_snapshot_sql())?;
        let mut rows = stmt.query([])?;
        if let Some(row) = rows.next()? {
            Ok(Some(map_usage_snapshot_row(row)?))
        } else {
            Ok(None)
        }
    }

    /// 函数 `latest_usage_snapshot_for_account`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - account_id: 参数 account_id
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn latest_usage_snapshot_for_account(
        &self,
        account_id: &str,
    ) -> Result<Option<UsageSnapshotRecord>> {
        let mut stmt = self.conn.prepare(latest_usage_snapshot_for_account_sql())?;
        let mut rows = stmt.query([account_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(map_usage_snapshot_row(row)?))
        } else {
            Ok(None)
        }
    }

    /// 函数 `latest_usage_snapshots_by_account`
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
    pub fn latest_usage_snapshots_by_account(&self) -> Result<Vec<UsageSnapshotRecord>> {
        self.latest_usage_snapshots_by_account_limited(None)
    }

    pub fn latest_usage_snapshots_by_account_limited(
        &self,
        limit: Option<usize>,
    ) -> Result<Vec<UsageSnapshotRecord>> {
        if limit == Some(0) {
            return Ok(Vec::new());
        }
        // 中文注释：窗口函数 + 复合索引可稳定处理“同 captured_at 并发写入”场景；
        // 不这样做会依赖复杂子查询拼接，后续维护和优化都更难。
        let mut sql = format!(
            "{cte}
            SELECT
                account_id,
                used_percent,
                window_minutes,
                resets_at,
                secondary_used_percent,
                secondary_window_minutes,
                secondary_resets_at,
                credits_json,
                captured_at
            FROM ranked
            WHERE rn = 1
            ORDER BY captured_at DESC, id DESC",
            cte = latest_usage_ranked_cte_sql(
                "account_id,
                    used_percent,
                    window_minutes,
                    resets_at,
                    secondary_used_percent,
                    secondary_window_minutes,
                    secondary_resets_at,
                    credits_json,
                    captured_at",
                None,
            ),
        );
        let mut params = Vec::new();
        if let Some(limit) = limit {
            sql.push_str(" LIMIT ?");
            params.push(limit as i64);
        }
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query(params_from_iter(params))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(map_usage_snapshot_row(row)?);
        }
        Ok(out)
    }

    pub fn latest_usage_snapshot_summary_rows(&self) -> Result<Vec<UsageSnapshotSummaryRow>> {
        let sql = latest_usage_snapshot_summary_rows_sql();
        let mut stmt = self.conn.prepare(&sql)?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(map_usage_snapshot_summary_row(row)?);
        }
        Ok(out)
    }

    pub fn latest_usage_snapshots_for_accounts(
        &self,
        account_ids: &[String],
    ) -> Result<Vec<UsageSnapshotRecord>> {
        let account_ids = normalize_text_ids(account_ids);
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for chunk in account_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(latest_usage_snapshots_for_accounts_chunk(self, chunk)?);
        }
        out.sort_by(|a, b| {
            b.captured_at
                .cmp(&a.captured_at)
                .then_with(|| a.account_id.cmp(&b.account_id))
        });
        Ok(out)
    }

    pub fn latest_usage_quota_source_rows_for_accounts(
        &self,
        account_ids: &[String],
    ) -> Result<Vec<UsageSnapshotQuotaSourceRow>> {
        let account_ids = normalize_text_ids(account_ids);
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for chunk in account_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(latest_usage_quota_source_rows_for_accounts_chunk(
                self, chunk,
            )?);
        }
        out.sort_by(|a, b| {
            b.captured_at
                .cmp(&a.captured_at)
                .then_with(|| a.account_id.cmp(&b.account_id))
        });
        Ok(out)
    }

    pub fn latest_usage_cleanup_rows_for_accounts(
        &self,
        account_ids: &[String],
    ) -> Result<Vec<UsageSnapshotCleanupRow>> {
        let account_ids = normalize_text_ids(account_ids);
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for chunk in account_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(latest_usage_cleanup_rows_for_accounts_chunk(self, chunk)?);
        }
        out.sort_by(|a, b| a.account_id.cmp(&b.account_id));
        Ok(out)
    }

    pub fn low_quota_account_ids_for_accounts(
        &self,
        account_ids: &[String],
        primary_min_remaining_percent: f64,
        secondary_min_remaining_percent: f64,
    ) -> Result<Vec<String>> {
        let account_ids = normalize_text_ids(account_ids);
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }
        let primary_threshold = normalize_remaining_threshold(primary_min_remaining_percent);
        let secondary_threshold = normalize_remaining_threshold(secondary_min_remaining_percent);
        if primary_threshold <= 0.0 && secondary_threshold <= 0.0 {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for chunk in account_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(low_quota_account_ids_for_accounts_chunk(
                self,
                chunk,
                primary_threshold,
                secondary_threshold,
            )?);
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    /// 函数 `ensure_usage_secondary_columns`
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
    pub(super) fn ensure_usage_secondary_columns(&self) -> Result<()> {
        self.ensure_column("usage_snapshots", "secondary_used_percent", "REAL")?;
        self.ensure_column("usage_snapshots", "secondary_window_minutes", "INTEGER")?;
        self.ensure_column("usage_snapshots", "secondary_resets_at", "INTEGER")?;
        Ok(())
    }
}

fn normalize_remaining_threshold(value: f64) -> f64 {
    if value.is_finite() {
        value.clamp(0.0, 100.0)
    } else {
        0.0
    }
}

/// 函数 `map_usage_snapshot_row`
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
fn map_usage_snapshot_row(row: &Row<'_>) -> Result<UsageSnapshotRecord> {
    Ok(UsageSnapshotRecord {
        account_id: row.get(0)?,
        used_percent: row.get(1)?,
        window_minutes: row.get(2)?,
        resets_at: row.get(3)?,
        secondary_used_percent: row.get(4)?,
        secondary_window_minutes: row.get(5)?,
        secondary_resets_at: row.get(6)?,
        credits_json: row.get(7)?,
        captured_at: row.get(8)?,
    })
}

fn map_usage_snapshot_summary_row(row: &Row<'_>) -> Result<UsageSnapshotSummaryRow> {
    Ok(UsageSnapshotSummaryRow {
        account_id: row.get(0)?,
        used_percent: row.get(1)?,
        window_minutes: row.get(2)?,
        secondary_used_percent: row.get(3)?,
        secondary_window_minutes: row.get(4)?,
        credits_json: row.get(5)?,
    })
}

fn map_usage_quota_source_row(row: &Row<'_>) -> Result<UsageSnapshotQuotaSourceRow> {
    Ok(UsageSnapshotQuotaSourceRow {
        account_id: row.get(0)?,
        used_percent: row.get(1)?,
        secondary_used_percent: row.get(2)?,
        captured_at: row.get(3)?,
    })
}

fn map_usage_cleanup_row(row: &Row<'_>) -> Result<UsageSnapshotCleanupRow> {
    Ok(UsageSnapshotCleanupRow {
        account_id: row.get(0)?,
        used_percent: row.get(1)?,
        window_minutes: row.get(2)?,
        secondary_used_percent: row.get(3)?,
        secondary_window_minutes: row.get(4)?,
        credits_json: row.get(5)?,
    })
}

fn latest_usage_ranked_cte_sql(select_columns: &str, where_condition: Option<&str>) -> String {
    let where_clause = where_condition
        .map(|condition| format!("WHERE {condition}"))
        .unwrap_or_default();
    format!(
        "WITH ranked AS (
            SELECT
                id,
                {select_columns},
                ROW_NUMBER() OVER (
                    PARTITION BY account_id
                    ORDER BY captured_at DESC, id DESC
                ) AS rn
            FROM usage_snapshots
            {where_clause}
        )"
    )
}

fn latest_usage_snapshots_for_accounts_chunk(
    storage: &Storage,
    account_ids: &[String],
) -> Result<Vec<UsageSnapshotRecord>> {
    let Some((condition, params)) = text_id_in_clause("account_id", account_ids) else {
        return Ok(Vec::new());
    };
    let sql = latest_usage_snapshots_for_accounts_chunk_sql(&condition);
    let mut stmt = storage.conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(params))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(map_usage_snapshot_row(row)?);
    }
    Ok(out)
}

fn latest_usage_snapshots_for_accounts_chunk_sql(account_condition: &str) -> String {
    format!(
        "{cte}
        SELECT
            account_id,
            used_percent,
            window_minutes,
            resets_at,
            secondary_used_percent,
            secondary_window_minutes,
            secondary_resets_at,
            credits_json,
            captured_at
        FROM ranked
        WHERE rn = 1",
        cte = latest_usage_ranked_cte_sql(
            "account_id,
                used_percent,
                window_minutes,
                resets_at,
                secondary_used_percent,
                secondary_window_minutes,
                secondary_resets_at,
                credits_json,
                captured_at",
            Some(account_condition),
        ),
    )
}

fn latest_usage_quota_source_rows_for_accounts_chunk(
    storage: &Storage,
    account_ids: &[String],
) -> Result<Vec<UsageSnapshotQuotaSourceRow>> {
    let Some((condition, params)) = text_id_in_clause("account_id", account_ids) else {
        return Ok(Vec::new());
    };
    let sql = latest_usage_quota_source_rows_for_accounts_chunk_sql(&condition);
    let mut stmt = storage.conn.prepare(&sql)?;
    let rows = stmt.query_map(params_from_iter(params), map_usage_quota_source_row)?;
    rows.collect()
}

fn latest_usage_quota_source_rows_for_accounts_chunk_sql(account_condition: &str) -> String {
    format!(
        "{cte}
        SELECT account_id, used_percent, secondary_used_percent, captured_at
        FROM ranked
        WHERE rn = 1",
        cte = latest_usage_ranked_cte_sql(
            "account_id,
                used_percent,
                secondary_used_percent,
                captured_at",
            Some(account_condition),
        ),
    )
}

fn latest_usage_cleanup_rows_for_accounts_chunk(
    storage: &Storage,
    account_ids: &[String],
) -> Result<Vec<UsageSnapshotCleanupRow>> {
    let Some((condition, params)) = text_id_in_clause("account_id", account_ids) else {
        return Ok(Vec::new());
    };
    let sql = latest_usage_cleanup_rows_for_accounts_chunk_sql(&condition);
    let mut stmt = storage.conn.prepare(&sql)?;
    let rows = stmt.query_map(params_from_iter(params), map_usage_cleanup_row)?;
    rows.collect()
}

fn latest_usage_cleanup_rows_for_accounts_chunk_sql(account_condition: &str) -> String {
    format!(
        "{cte}
        SELECT
            account_id,
            used_percent,
            window_minutes,
            secondary_used_percent,
            secondary_window_minutes,
            credits_json
        FROM ranked
        WHERE rn = 1",
        cte = latest_usage_ranked_cte_sql(
            "account_id,
                used_percent,
                window_minutes,
                secondary_used_percent,
                secondary_window_minutes,
                credits_json",
            Some(account_condition),
        ),
    )
}

fn low_quota_account_ids_for_accounts_chunk(
    storage: &Storage,
    account_ids: &[String],
    primary_min_remaining_percent: f64,
    secondary_min_remaining_percent: f64,
) -> Result<Vec<String>> {
    let Some((condition, params)) = text_id_in_clause("account_id", account_ids) else {
        return Ok(Vec::new());
    };
    let sql = low_quota_account_ids_for_accounts_chunk_sql(&condition);
    let mut values = params;
    values.extend([
        rusqlite::types::Value::Real(primary_min_remaining_percent),
        rusqlite::types::Value::Real(primary_min_remaining_percent),
        rusqlite::types::Value::Real(secondary_min_remaining_percent),
        rusqlite::types::Value::Real(secondary_min_remaining_percent),
    ]);
    let mut stmt = storage.conn.prepare(&sql)?;
    let rows = stmt.query_map(params_from_iter(values), |row| row.get(0))?;
    rows.collect()
}

fn low_quota_account_ids_for_accounts_chunk_sql(account_condition: &str) -> String {
    format!(
        "{cte}
        SELECT account_id
        FROM ranked
        WHERE rn = 1
          AND (
                (? > 0.0 AND used_percent IS NOT NULL AND (100.0 - used_percent) <= ?)
                OR (? > 0.0 AND secondary_used_percent IS NOT NULL AND (100.0 - secondary_used_percent) <= ?)
          )",
        cte = latest_usage_ranked_cte_sql(
            "account_id,
                used_percent,
                secondary_used_percent",
            Some(account_condition),
        ),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::{now_ts, Account};

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

    fn sample_snapshot(
        account_id: &str,
        captured_at: i64,
        used_percent: f64,
    ) -> UsageSnapshotRecord {
        UsageSnapshotRecord {
            account_id: account_id.to_string(),
            used_percent: Some(used_percent),
            window_minutes: Some(180),
            resets_at: None,
            secondary_used_percent: None,
            secondary_window_minutes: None,
            secondary_resets_at: None,
            credits_json: None,
            captured_at,
        }
    }

    fn collect_query_plan(storage: &Storage, sql: &str) -> String {
        collect_query_plan_with_params(storage, sql, Vec::new())
    }

    fn collect_query_plan_with_params(
        storage: &Storage,
        sql: &str,
        params: Vec<rusqlite::types::Value>,
    ) -> String {
        let mut stmt = storage.conn.prepare(sql).expect("prepare explain");
        let mut rows = stmt.query(params_from_iter(params)).expect("query explain");
        let mut plan = String::new();
        while let Some(row) = rows.next().expect("read explain row") {
            let detail: String = row.get(3).expect("plan detail");
            plan.push_str(&detail);
            plan.push('\n');
        }
        plan
    }

    #[test]
    fn latest_usage_snapshots_for_accounts_filters_and_returns_latest_per_account() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        for account_id in ["acc-a", "acc-b", "acc-c"] {
            storage
                .insert_account(&sample_account(account_id, now))
                .expect("insert account");
        }
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-a", now, 10.0))
            .expect("insert old a");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-a", now + 1, 20.0))
            .expect("insert new a");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-b", now + 2, 30.0))
            .expect("insert b");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-c", now + 3, 40.0))
            .expect("insert c");

        let requested = vec![
            "acc-a".to_string(),
            "acc-c".to_string(),
            "missing".to_string(),
        ];
        let items = storage
            .latest_usage_snapshots_for_accounts(&requested)
            .expect("list snapshots");
        let by_account = items
            .into_iter()
            .map(|item| (item.account_id.clone(), item))
            .collect::<std::collections::HashMap<_, _>>();

        assert_eq!(by_account.len(), 2);
        assert_eq!(
            by_account.get("acc-a").and_then(|item| item.used_percent),
            Some(20.0)
        );
        assert_eq!(
            by_account.get("acc-c").and_then(|item| item.used_percent),
            Some(40.0)
        );
        assert!(!by_account.contains_key("acc-b"));
    }

    #[test]
    fn latest_usage_snapshots_by_account_limited_returns_recent_latest_snapshots() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        for account_id in ["acc-a", "acc-b", "acc-c"] {
            storage
                .insert_account(&sample_account(account_id, now))
                .expect("insert account");
        }
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-a", now, 10.0))
            .expect("insert old a");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-a", now + 5, 15.0))
            .expect("insert latest a");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-b", now + 3, 30.0))
            .expect("insert b");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-c", now + 4, 40.0))
            .expect("insert c");

        let items = storage
            .latest_usage_snapshots_by_account_limited(Some(2))
            .expect("list limited snapshots");

        assert_eq!(
            items
                .iter()
                .map(|item| (item.account_id.as_str(), item.used_percent))
                .collect::<Vec<_>>(),
            vec![("acc-a", Some(15.0)), ("acc-c", Some(40.0))]
        );
    }

    #[test]
    fn latest_usage_snapshots_by_account_limited_zero_returns_empty() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();
        storage
            .insert_account(&sample_account("acc-zero", now))
            .expect("insert account");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-zero", now, 10.0))
            .expect("insert usage snapshot");

        let items = storage
            .latest_usage_snapshots_by_account_limited(Some(0))
            .expect("list zero limited snapshots");

        assert!(items.is_empty());
    }

    #[test]
    fn latest_usage_quota_source_rows_for_accounts_reads_only_quota_source_fields() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        for account_id in ["acc-a", "acc-b", "acc-c"] {
            storage
                .insert_account(&sample_account(account_id, now))
                .expect("insert account");
        }
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-a", now, 10.0))
            .expect("insert old a");
        storage
            .insert_usage_snapshot(&UsageSnapshotRecord {
                window_minutes: Some(999),
                resets_at: Some(now + 999),
                secondary_used_percent: Some(88.0),
                secondary_window_minutes: Some(10080),
                secondary_resets_at: Some(now + 10080),
                credits_json: Some(r#"{"planType":"plus"}"#.to_string()),
                ..sample_snapshot("acc-a", now + 5, 15.0)
            })
            .expect("insert latest a");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-b", now + 3, 30.0))
            .expect("insert b");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-c", now + 4, 40.0))
            .expect("insert c");

        let requested = vec![
            "acc-b".to_string(),
            "missing".to_string(),
            "acc-a".to_string(),
        ];
        let rows = storage
            .latest_usage_quota_source_rows_for_accounts(&requested)
            .expect("list quota source rows");

        assert_eq!(
            rows.iter()
                .map(|row| (
                    row.account_id.as_str(),
                    row.used_percent,
                    row.secondary_used_percent,
                    row.captured_at
                ))
                .collect::<Vec<_>>(),
            vec![
                ("acc-a", Some(15.0), Some(88.0), now + 5),
                ("acc-b", Some(30.0), None, now + 3)
            ]
        );
    }

    #[test]
    fn latest_usage_cleanup_rows_for_accounts_reads_cleanup_fields_only() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        for account_id in ["acc-a", "acc-b", "acc-c"] {
            storage
                .insert_account(&sample_account(account_id, now))
                .expect("insert account");
        }
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-a", now, 10.0))
            .expect("insert old a");
        storage
            .insert_usage_snapshot(&UsageSnapshotRecord {
                window_minutes: Some(999),
                resets_at: Some(now + 999),
                secondary_used_percent: Some(88.0),
                secondary_window_minutes: Some(10080),
                secondary_resets_at: Some(now + 10080),
                credits_json: Some(r#"{"planType":"free"}"#.to_string()),
                ..sample_snapshot("acc-a", now + 5, 15.0)
            })
            .expect("insert latest a");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-b", now + 3, 30.0))
            .expect("insert b");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-c", now + 4, 40.0))
            .expect("insert c");

        let rows = storage
            .latest_usage_cleanup_rows_for_accounts(&[
                "acc-b".to_string(),
                "missing".to_string(),
                "acc-a".to_string(),
            ])
            .expect("list cleanup rows");

        assert_eq!(
            rows.iter()
                .map(|row| (
                    row.account_id.as_str(),
                    row.used_percent,
                    row.window_minutes,
                    row.secondary_used_percent,
                    row.secondary_window_minutes,
                    row.credits_json.as_deref()
                ))
                .collect::<Vec<_>>(),
            vec![
                (
                    "acc-a",
                    Some(15.0),
                    Some(999),
                    Some(88.0),
                    Some(10080),
                    Some(r#"{"planType":"free"}"#)
                ),
                ("acc-b", Some(30.0), Some(180), None, None, None)
            ]
        );
    }

    #[test]
    fn low_quota_account_ids_for_accounts_filters_latest_snapshot_in_sql() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        for account_id in [
            "acc-primary-low",
            "acc-secondary-low",
            "acc-recovered",
            "acc-ok",
        ] {
            storage
                .insert_account(&sample_account(account_id, now))
                .expect("insert account");
        }
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-primary-low", now, 96.0))
            .expect("insert primary low");
        storage
            .insert_usage_snapshot(&UsageSnapshotRecord {
                secondary_used_percent: Some(92.0),
                secondary_window_minutes: Some(10_080),
                ..sample_snapshot("acc-secondary-low", now + 1, 10.0)
            })
            .expect("insert secondary low");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-recovered", now + 2, 99.0))
            .expect("insert old recovered low");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-recovered", now + 3, 20.0))
            .expect("insert latest recovered ok");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-ok", now + 4, 50.0))
            .expect("insert ok");

        let account_ids = vec![
            "acc-ok".to_string(),
            "acc-secondary-low".to_string(),
            "acc-primary-low".to_string(),
            "acc-recovered".to_string(),
            "missing".to_string(),
        ];
        let low_quota = storage
            .low_quota_account_ids_for_accounts(&account_ids, 5.0, 10.0)
            .expect("list low quota accounts");

        assert_eq!(
            low_quota,
            vec![
                "acc-primary-low".to_string(),
                "acc-secondary-low".to_string()
            ]
        );
    }

    #[test]
    fn low_quota_account_ids_for_accounts_returns_empty_when_thresholds_disabled() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();
        storage
            .insert_account(&sample_account("acc-low", now))
            .expect("insert account");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-low", now, 100.0))
            .expect("insert low");

        let low_quota = storage
            .low_quota_account_ids_for_accounts(&["acc-low".to_string()], 0.0, -5.0)
            .expect("list low quota accounts");

        assert!(low_quota.is_empty());
    }

    #[test]
    fn low_quota_account_ids_for_accounts_chunks_large_account_sets() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();
        let target = "acc-0949";
        storage
            .insert_account(&sample_account(target, now))
            .expect("insert target account");
        storage
            .insert_usage_snapshot(&sample_snapshot(target, now, 98.0))
            .expect("insert target low");

        let account_ids = (0..950)
            .map(|index| format!("acc-{index:04}"))
            .collect::<Vec<_>>();
        let low_quota = storage
            .low_quota_account_ids_for_accounts(&account_ids, 5.0, 0.0)
            .expect("list low quota accounts");

        assert_eq!(low_quota, vec![target.to_string()]);
    }

    #[test]
    fn usage_account_chunk_queries_defer_final_ordering_to_rust() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let account_condition = "account_id IN ('acc-a', 'acc-b')";
        let latest_sql = latest_usage_snapshots_for_accounts_chunk_sql(account_condition);
        let latest_plan = collect_query_plan(&storage, &format!("EXPLAIN QUERY PLAN {latest_sql}"));
        let quota_source_sql =
            latest_usage_quota_source_rows_for_accounts_chunk_sql(account_condition);
        let quota_source_plan =
            collect_query_plan(&storage, &format!("EXPLAIN QUERY PLAN {quota_source_sql}"));
        let cleanup_sql = latest_usage_cleanup_rows_for_accounts_chunk_sql(account_condition);
        let cleanup_plan =
            collect_query_plan(&storage, &format!("EXPLAIN QUERY PLAN {cleanup_sql}"));
        let low_quota_sql = low_quota_account_ids_for_accounts_chunk_sql(account_condition);
        let low_quota_plan = collect_query_plan_with_params(
            &storage,
            &format!("EXPLAIN QUERY PLAN {low_quota_sql}"),
            vec![
                rusqlite::types::Value::Real(5.0),
                rusqlite::types::Value::Real(5.0),
                rusqlite::types::Value::Real(10.0),
                rusqlite::types::Value::Real(10.0),
            ],
        );

        for (label, plan) in [
            ("latest usage chunk query", &latest_plan),
            ("quota source chunk query", &quota_source_plan),
            ("usage cleanup chunk query", &cleanup_plan),
            ("low quota chunk query", &low_quota_plan),
        ] {
            assert!(
                plan.contains("idx_usage_snapshots_account_captured_id"),
                "{label} should use account captured lookup index, got {plan}"
            );
        }

        assert!(
            !latest_plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "latest usage chunk output should not require an outer per-chunk sort, got {latest_plan}"
        );
        assert!(
            !quota_source_plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "quota source chunk output should not require an outer per-chunk sort, got {quota_source_plan}"
        );
        assert!(
            !cleanup_plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "usage cleanup chunk output should not require an outer per-chunk sort, got {cleanup_plan}"
        );
        assert!(
            !low_quota_plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "low quota chunk output should not require an outer per-chunk sort, got {low_quota_plan}"
        );
    }

    #[test]
    fn latest_usage_snapshot_lookup_helpers_use_existing_indexes() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let latest_plan = collect_query_plan(
            &storage,
            &format!("EXPLAIN QUERY PLAN {}", latest_usage_snapshot_sql()),
        );
        let latest_for_account_plan = collect_query_plan_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                latest_usage_snapshot_for_account_sql()
            ),
            vec![rusqlite::types::Value::Text("acc-a".to_string())],
        );
        let count_for_account_plan = collect_query_plan_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                usage_snapshot_count_for_account_sql()
            ),
            vec![rusqlite::types::Value::Text("acc-a".to_string())],
        );
        let summary_sql = latest_usage_snapshot_summary_rows_sql();
        let summary_plan =
            collect_query_plan(&storage, &format!("EXPLAIN QUERY PLAN {summary_sql}"));

        assert!(
            latest_plan.contains("idx_usage_snapshots_captured_id"),
            "latest usage snapshot should use captured/id ordering index, got {latest_plan}"
        );
        assert!(
            latest_for_account_plan.contains("idx_usage_snapshots_account_captured_id"),
            "account latest usage snapshot should use account captured lookup index, got {latest_for_account_plan}"
        );
        assert!(
            count_for_account_plan.contains("idx_usage_snapshots_account_captured_id"),
            "account usage count should use account captured lookup index, got {count_for_account_plan}"
        );
        assert!(
            summary_plan.contains("idx_usage_snapshots_account_captured_id"),
            "summary usage query should use account captured lookup index, got {summary_plan}"
        );

        for (label, plan) in [
            ("latest usage snapshot", &latest_plan),
            ("account latest usage snapshot", &latest_for_account_plan),
            ("summary usage query", &summary_plan),
        ] {
            assert!(
                !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
                "{label} should read in index order without temp ordering, got {plan}"
            );
        }
    }
    #[test]
    fn latest_usage_snapshot_summary_rows_return_latest_usage_fields() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        storage
            .insert_usage_snapshot(&UsageSnapshotRecord {
                account_id: "acc-summary".to_string(),
                used_percent: Some(10.0),
                window_minutes: Some(180),
                resets_at: Some(now + 180),
                secondary_used_percent: None,
                secondary_window_minutes: None,
                secondary_resets_at: None,
                credits_json: None,
                captured_at: now,
            })
            .expect("insert old summary snapshot");
        storage
            .insert_usage_snapshot(&UsageSnapshotRecord {
                account_id: "acc-summary".to_string(),
                used_percent: Some(25.0),
                window_minutes: Some(240),
                resets_at: Some(now + 240),
                secondary_used_percent: Some(40.0),
                secondary_window_minutes: Some(10080),
                secondary_resets_at: Some(now + 10080),
                credits_json: Some(r#"{"planType":"free"}"#.to_string()),
                captured_at: now + 1,
            })
            .expect("insert new summary snapshot");

        let rows = storage
            .latest_usage_snapshot_summary_rows()
            .expect("list summary rows");

        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].account_id, "acc-summary");
        assert_eq!(rows[0].used_percent, Some(25.0));
        assert_eq!(rows[0].window_minutes, Some(240));
        assert_eq!(rows[0].secondary_used_percent, Some(40.0));
        assert_eq!(rows[0].secondary_window_minutes, Some(10080));
        assert_eq!(
            rows[0].credits_json.as_deref(),
            Some(r#"{"planType":"free"}"#)
        );
    }

    #[test]
    fn usage_snapshot_count_counts_all_snapshot_rows() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        storage
            .insert_usage_snapshot(&sample_snapshot("acc-a", now, 10.0))
            .expect("insert a snapshot");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-a", now + 1, 20.0))
            .expect("insert second a snapshot");
        storage
            .insert_usage_snapshot(&sample_snapshot("acc-b", now + 2, 30.0))
            .expect("insert b snapshot");

        assert_eq!(storage.usage_snapshot_count().expect("count snapshots"), 3);
    }

    #[test]
    fn latest_usage_snapshots_for_accounts_chunks_large_account_sets() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        let target = "acc-0949";
        storage
            .insert_account(&sample_account(target, now))
            .expect("insert target account");
        storage
            .insert_usage_snapshot(&sample_snapshot(target, now, 45.0))
            .expect("insert old target");
        storage
            .insert_usage_snapshot(&sample_snapshot(target, now + 1, 55.0))
            .expect("insert new target");

        let requested = (0..950)
            .map(|index| format!("acc-{index:04}"))
            .collect::<Vec<_>>();
        let items = storage
            .latest_usage_snapshots_for_accounts(&requested)
            .expect("list snapshots");

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].account_id, target);
        assert_eq!(items[0].used_percent, Some(55.0));
    }
    #[test]
    fn usage_snapshot_prune_helpers_use_existing_indexes() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let account_prune_plan = collect_query_plan_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                prune_usage_snapshots_for_account_sql()
            ),
            vec![
                rusqlite::types::Value::Text("acc-a".to_string()),
                rusqlite::types::Value::Integer(1),
            ],
        );
        let global_prune_plan = collect_query_plan_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                prune_usage_snapshots_all_accounts_sql()
            ),
            vec![rusqlite::types::Value::Integer(1)],
        );

        assert!(
            account_prune_plan.contains("idx_usage_snapshots_account_captured_id"),
            "account usage snapshot prune should use account captured lookup index, got {account_prune_plan}"
        );
        assert!(
            global_prune_plan.contains("idx_usage_snapshots_account_captured_id"),
            "global usage snapshot prune should use account captured ordering index, got {global_prune_plan}"
        );
    }

    #[test]
    fn usage_snapshot_delete_for_account_uses_account_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let plan = collect_query_plan_with_params(
            &storage,
            &format!(
                "EXPLAIN QUERY PLAN {}",
                delete_usage_snapshots_for_account_sql()
            ),
            vec![rusqlite::types::Value::Text("acc-a".to_string())],
        );

        assert!(
            plan.contains("idx_usage_snapshots_account_captured_id"),
            "usage snapshot delete should use account captured lookup index, got {plan}"
        );
    }
}
