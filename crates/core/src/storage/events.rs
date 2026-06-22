use rusqlite::{params_from_iter, Result};
use std::collections::HashMap;

use super::key_id_filters::{normalize_text_ids, text_id_in_clause, SQLITE_IN_CLAUSE_BATCH_SIZE};
use super::{Event, Storage};

impl Storage {
    /// 函数 `insert_event`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - event: 参数 event
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn insert_event(&self, event: &Event) -> Result<()> {
        self.conn.execute(
            "INSERT INTO events (account_id, type, message, created_at) VALUES (?1, ?2, ?3, ?4)",
            (
                &event.account_id,
                &event.event_type,
                &event.message,
                event.created_at,
            ),
        )?;
        Ok(())
    }

    /// 函数 `event_count`
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
    pub fn event_count(&self) -> Result<i64> {
        self.conn
            .query_row("SELECT COUNT(1) FROM events", [], |row| row.get(0))
    }

    /// 函数 `latest_account_status_reasons`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - account_ids: 参数 account_ids
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn latest_account_status_reasons(
        &self,
        account_ids: &[String],
    ) -> Result<HashMap<String, String>> {
        let account_ids = normalize_text_ids(account_ids);
        if account_ids.is_empty() {
            return Ok(HashMap::new());
        }

        let mut out = HashMap::new();
        for chunk in account_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(latest_account_status_reasons_chunk(self, chunk)?);
        }
        Ok(out)
    }

    pub fn latest_account_status_blocked_ids(&self, account_ids: &[String]) -> Result<Vec<String>> {
        let account_ids = normalize_text_ids(account_ids);
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for chunk in account_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(latest_account_status_blocked_ids_chunk(self, chunk)?);
        }
        out.sort();
        out.dedup();
        Ok(out)
    }

    pub(super) fn ensure_events_account_status_lookup_index(&self) -> Result<()> {
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_account_status_lookup
             ON events(type, account_id, created_at DESC, id DESC)",
            [],
        )?;
        Ok(())
    }

    pub(super) fn ensure_events_account_cleanup_index(&self) -> Result<()> {
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_account_cleanup
             ON events(account_id)",
            [],
        )?;
        Ok(())
    }
}

pub(super) fn delete_events_for_account_sql() -> &'static str {
    "DELETE FROM events WHERE account_id = ?1"
}

fn latest_account_status_reasons_chunk(
    storage: &Storage,
    account_ids: &[String],
) -> Result<HashMap<String, String>> {
    let Some((condition, params)) = text_id_in_clause("account_id", account_ids) else {
        return Ok(HashMap::new());
    };
    let sql = latest_account_status_reasons_chunk_sql(&condition);

    let mut stmt = storage.conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(params))?;
    let mut out = HashMap::new();
    while let Some(row) = rows.next()? {
        let account_id: String = row.get(0)?;
        let message: String = row.get(1)?;
        if let Some(reason) = extract_status_reason_from_event_message(&message) {
            out.insert(account_id, reason.to_string());
        }
    }
    Ok(out)
}

fn latest_account_status_blocked_ids_chunk(
    storage: &Storage,
    account_ids: &[String],
) -> Result<Vec<String>> {
    let Some((condition, params)) = text_id_in_clause("account_id", account_ids) else {
        return Ok(Vec::new());
    };
    let sql = latest_account_status_blocked_ids_chunk_sql(&condition);

    let mut stmt = storage.conn.prepare(&sql)?;
    let rows = stmt.query_map(params_from_iter(params), |row| row.get(0))?;
    rows.collect()
}

fn latest_account_status_reasons_chunk_sql(account_condition: &str) -> String {
    format!(
        "{cte}
        SELECT account_id, message
        FROM ranked
        WHERE rn = 1",
        cte = latest_account_status_ranked_cte_sql("message", account_condition, false),
    )
}

fn latest_account_status_blocked_ids_chunk_sql(account_condition: &str) -> String {
    let sql = format!(
        "{cte}
        SELECT account_id
        FROM ranked
        WHERE rn = 1
          AND reason IN (
              'account_deactivated',
              'workspace_deactivated',
              'deactivated_workspace',
              'refresh_token_region_blocked'
          )",
        cte = latest_account_status_ranked_cte_sql(
            "LOWER(TRIM(SUBSTR(message, INSTR(message, ' reason=') + LENGTH(' reason=')))) AS reason",
            account_condition,
            true,
        ),
    );
    sql
}

fn latest_account_status_ranked_cte_sql(
    select_columns: &str,
    account_condition: &str,
    require_reason_marker: bool,
) -> String {
    let reason_filter = if require_reason_marker {
        "AND INSTR(message, ' reason=') > 0"
    } else {
        ""
    };
    format!(
        "WITH ranked AS (
            SELECT
                account_id,
                {select_columns},
                ROW_NUMBER() OVER (
                    PARTITION BY account_id
                    ORDER BY created_at DESC, id DESC
                ) AS rn
            FROM events
            WHERE type = 'account_status_update'
              {reason_filter}
              AND {account_condition}
        )"
    )
}

/// 函数 `extract_status_reason_from_event_message`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - message: 参数 message
///
/// # 返回
/// 返回函数执行结果
fn extract_status_reason_from_event_message(message: &str) -> Option<&str> {
    let marker = " reason=";
    let start = message.find(marker)? + marker.len();
    let reason = message.get(start..)?.trim();
    if reason.is_empty() {
        None
    } else {
        Some(reason)
    }
}

#[cfg(test)]
#[path = "tests/events_tests.rs"]
mod tests;
