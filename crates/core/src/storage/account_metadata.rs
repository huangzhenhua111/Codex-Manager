use rusqlite::{params_from_iter, Result, Row};

use super::key_id_filters::{normalize_text_ids, text_id_in_clause, SQLITE_IN_CLAUSE_BATCH_SIZE};
use super::{now_ts, AccountMetadata, Storage};

fn account_metadata_select_columns() -> &'static str {
    "account_id, note, tags, updated_at"
}

fn account_metadata_by_account_sql() -> String {
    format!(
        "SELECT {columns}
         FROM account_metadata
         WHERE account_id = ?1
         LIMIT 1",
        columns = account_metadata_select_columns(),
    )
}

fn account_metadata_list_sql() -> String {
    format!(
        "SELECT {columns}
         FROM account_metadata
         ORDER BY updated_at DESC, account_id ASC",
        columns = account_metadata_select_columns(),
    )
}

pub(super) fn delete_account_metadata_for_account_sql() -> &'static str {
    "DELETE FROM account_metadata WHERE account_id = ?1"
}

impl Storage {
    /// 函数 `upsert_account_metadata`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - account_id: 参数 account_id
    /// - note: 参数 note
    /// - tags: 参数 tags
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn upsert_account_metadata(
        &self,
        account_id: &str,
        note: Option<&str>,
        tags: Option<&str>,
    ) -> Result<()> {
        let normalized_note = normalize_optional_text(note);
        let normalized_tags = normalize_optional_text(tags);
        if normalized_note.is_none() && normalized_tags.is_none() {
            self.conn
                .execute(delete_account_metadata_for_account_sql(), [account_id])?;
            return Ok(());
        }

        self.conn.execute(
            "INSERT INTO account_metadata (account_id, note, tags, updated_at)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(account_id) DO UPDATE SET
                note = excluded.note,
                tags = excluded.tags,
                updated_at = excluded.updated_at",
            (account_id, normalized_note, normalized_tags, now_ts()),
        )?;
        Ok(())
    }

    /// 函数 `find_account_metadata`
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
    pub fn find_account_metadata(&self, account_id: &str) -> Result<Option<AccountMetadata>> {
        let mut stmt = self.conn.prepare(&account_metadata_by_account_sql())?;
        let mut rows = stmt.query([account_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(map_account_metadata_row(row)?))
        } else {
            Ok(None)
        }
    }

    /// 函数 `list_account_metadata`
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
    pub fn list_account_metadata(&self) -> Result<Vec<AccountMetadata>> {
        let mut stmt = self.conn.prepare(&account_metadata_list_sql())?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(map_account_metadata_row(row)?);
        }
        Ok(out)
    }

    pub fn list_account_metadata_for_accounts(
        &self,
        account_ids: &[String],
    ) -> Result<Vec<AccountMetadata>> {
        let account_ids = normalize_text_ids(account_ids);
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for chunk in account_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(list_account_metadata_for_accounts_chunk(self, chunk)?);
        }
        out.sort_by(|a, b| a.account_id.cmp(&b.account_id));
        Ok(out)
    }
}

/// 函数 `normalize_optional_text`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - value: 参数 value
///
/// # 返回
/// 返回函数执行结果
fn normalize_optional_text(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(ToString::to_string)
}

/// 函数 `map_account_metadata_row`
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
fn map_account_metadata_row(row: &Row<'_>) -> Result<AccountMetadata> {
    Ok(AccountMetadata {
        account_id: row.get(0)?,
        note: row.get(1)?,
        tags: row.get(2)?,
        updated_at: row.get(3)?,
    })
}

fn list_account_metadata_for_accounts_chunk(
    storage: &Storage,
    account_ids: &[String],
) -> Result<Vec<AccountMetadata>> {
    let Some((condition, params)) = text_id_in_clause("account_id", account_ids) else {
        return Ok(Vec::new());
    };
    let sql = account_metadata_for_accounts_chunk_sql(&condition);
    let mut stmt = storage.conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(params))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(map_account_metadata_row(row)?);
    }
    Ok(out)
}

fn account_metadata_for_accounts_chunk_sql(account_condition: &str) -> String {
    format!(
        "SELECT {columns}
         FROM account_metadata
         WHERE {account_condition}",
        columns = account_metadata_select_columns(),
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

    #[test]
    fn list_account_metadata_for_accounts_filters_to_requested_ids() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        for account_id in ["acc-a", "acc-b"] {
            storage
                .insert_account(&sample_account(account_id, now))
                .expect("insert account");
            storage
                .upsert_account_metadata(account_id, Some(account_id), None)
                .expect("upsert metadata");
        }

        let requested = vec!["acc-b".to_string(), "missing".to_string()];
        let items = storage
            .list_account_metadata_for_accounts(&requested)
            .expect("list metadata");

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].account_id, "acc-b");
        assert_eq!(items[0].note.as_deref(), Some("acc-b"));
    }

    #[test]
    fn list_account_metadata_for_accounts_chunks_large_account_sets() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        let target = "acc-0949";
        storage
            .insert_account(&sample_account(target, now))
            .expect("insert target account");
        storage
            .upsert_account_metadata(target, Some("target"), None)
            .expect("upsert metadata");

        let requested = (0..950)
            .map(|index| format!("acc-{index:04}"))
            .collect::<Vec<_>>();
        let items = storage
            .list_account_metadata_for_accounts(&requested)
            .expect("list metadata");

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].account_id, target);
        assert_eq!(items[0].note.as_deref(), Some("target"));
    }

    #[test]
    fn account_metadata_chunk_query_defers_final_ordering_to_rust() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let sql = account_metadata_for_accounts_chunk_sql("account_id IN ('acc-a', 'acc-b')");
        let mut stmt = storage
            .conn
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .expect("prepare explain");
        let mut rows = stmt.query([]).expect("query explain");
        let mut plan = String::new();
        while let Some(row) = rows.next().expect("read explain row") {
            let detail: String = row.get(3).expect("plan detail");
            plan.push_str(&detail);
            plan.push('\n');
        }

        assert!(
            plan.contains("sqlite_autoindex_account_metadata_1") || plan.contains("USING INDEX"),
            "metadata chunk query should use account_id lookup index, got {plan}"
        );
        assert!(
            !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "metadata chunk query should avoid per-chunk ORDER BY temp sorting, got {plan}"
        );
    }

    #[test]
    fn account_metadata_lookup_uses_primary_key_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let mut stmt = storage
            .conn
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {}",
                account_metadata_by_account_sql()
            ))
            .expect("prepare explain");
        let mut rows = stmt.query(["acc-a"]).expect("query explain");
        let mut plan = String::new();
        while let Some(row) = rows.next().expect("read explain row") {
            let detail: String = row.get(3).expect("plan detail");
            plan.push_str(&detail);
            plan.push('\n');
        }

        assert!(
            plan.contains("sqlite_autoindex_account_metadata_1") || plan.contains("USING INDEX"),
            "metadata lookup should use account_id primary-key index, got {plan}"
        );
    }
    #[test]
    fn account_metadata_delete_uses_primary_key_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let mut stmt = storage
            .conn
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {}",
                delete_account_metadata_for_account_sql()
            ))
            .expect("prepare explain");
        let mut rows = stmt.query(["acc-a"]).expect("query explain");
        let mut plan = String::new();
        while let Some(row) = rows.next().expect("read explain row") {
            let detail: String = row.get(3).expect("plan detail");
            plan.push_str(&detail);
            plan.push('\n');
        }

        assert!(
            plan.contains("sqlite_autoindex_account_metadata_1") || plan.contains("USING INDEX"),
            "metadata delete should use account_id primary-key index, got {plan}"
        );
    }
}
