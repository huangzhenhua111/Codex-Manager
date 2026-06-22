use rusqlite::{params_from_iter, Result, Row};

use super::key_id_filters::{normalize_text_ids, text_id_in_clause, SQLITE_IN_CLAUSE_BATCH_SIZE};
use super::{now_ts, AccountSubscription, Storage};

fn account_subscription_select_columns() -> &'static str {
    "account_id, has_subscription, account_plan_type, plan_type, expires_at, renews_at, updated_at"
}

fn account_subscription_by_account_sql() -> String {
    format!(
        "SELECT {columns}
         FROM account_subscriptions
         WHERE account_id = ?1
         LIMIT 1",
        columns = account_subscription_select_columns(),
    )
}

fn account_subscription_list_sql() -> String {
    format!(
        "SELECT {columns}
         FROM account_subscriptions
         ORDER BY updated_at DESC, account_id ASC",
        columns = account_subscription_select_columns(),
    )
}

pub(super) fn delete_account_subscription_for_account_sql() -> &'static str {
    "DELETE FROM account_subscriptions WHERE account_id = ?1"
}

impl Storage {
    /// 函数 `upsert_account_subscription`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-17
    ///
    /// # 参数
    /// - self: 参数 self
    /// - account_id: 参数 account_id
    /// - has_subscription: 参数 has_subscription
    /// - account_plan_type: 参数 account_plan_type
    /// - plan_type: 参数 plan_type
    /// - expires_at: 参数 expires_at
    /// - renews_at: 参数 renews_at
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn upsert_account_subscription(
        &self,
        account_id: &str,
        has_subscription: bool,
        account_plan_type: Option<&str>,
        plan_type: Option<&str>,
        expires_at: Option<i64>,
        renews_at: Option<i64>,
    ) -> Result<()> {
        self.conn.execute(
            "INSERT INTO account_subscriptions (
                account_id,
                has_subscription,
                account_plan_type,
                plan_type,
                expires_at,
                renews_at,
                updated_at
            ) VALUES (
                ?1,
                ?2,
                ?3,
                ?4,
                ?5,
                ?6,
                ?7
            )
            ON CONFLICT(account_id) DO UPDATE SET
                has_subscription = excluded.has_subscription,
                account_plan_type = excluded.account_plan_type,
                plan_type = excluded.plan_type,
                expires_at = excluded.expires_at,
                renews_at = excluded.renews_at,
                updated_at = excluded.updated_at",
            (
                account_id,
                if has_subscription { 1 } else { 0 },
                normalize_optional_text(account_plan_type),
                normalize_optional_text(plan_type),
                expires_at,
                renews_at,
                now_ts(),
            ),
        )?;
        Ok(())
    }

    /// 函数 `delete_account_subscription`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-17
    ///
    /// # 参数
    /// - self: 参数 self
    /// - account_id: 参数 account_id
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn delete_account_subscription(&self, account_id: &str) -> Result<()> {
        self.conn
            .execute(delete_account_subscription_for_account_sql(), [account_id])?;
        Ok(())
    }

    /// 函数 `find_account_subscription`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-17
    ///
    /// # 参数
    /// - self: 参数 self
    /// - account_id: 参数 account_id
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn find_account_subscription(
        &self,
        account_id: &str,
    ) -> Result<Option<AccountSubscription>> {
        let mut stmt = self.conn.prepare(&account_subscription_by_account_sql())?;
        let mut rows = stmt.query([account_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(map_account_subscription_row(row)?))
        } else {
            Ok(None)
        }
    }

    /// 函数 `list_account_subscriptions`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-17
    ///
    /// # 参数
    /// - self: 参数 self
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn list_account_subscriptions(&self) -> Result<Vec<AccountSubscription>> {
        let mut stmt = self.conn.prepare(&account_subscription_list_sql())?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(map_account_subscription_row(row)?);
        }
        Ok(out)
    }

    pub fn list_account_subscriptions_for_accounts(
        &self,
        account_ids: &[String],
    ) -> Result<Vec<AccountSubscription>> {
        let account_ids = normalize_text_ids(account_ids);
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for chunk in account_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(list_account_subscriptions_for_accounts_chunk(self, chunk)?);
        }
        out.sort_by(|a, b| a.account_id.cmp(&b.account_id));
        Ok(out)
    }

    /// 函数 `ensure_account_subscriptions_table`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-17
    ///
    /// # 参数
    /// - self: 参数 self
    ///
    /// # 返回
    /// 返回函数执行结果
    pub(super) fn ensure_account_subscriptions_table(&self) -> Result<()> {
        self.conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS account_subscriptions (
                account_id TEXT PRIMARY KEY REFERENCES accounts(id) ON DELETE CASCADE,
                has_subscription INTEGER NOT NULL DEFAULT 0,
                account_plan_type TEXT,
                plan_type TEXT,
                expires_at INTEGER,
                renews_at INTEGER,
                updated_at INTEGER NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_account_subscriptions_updated_at
                ON account_subscriptions(updated_at DESC, account_id ASC);",
        )?;
        self.ensure_column("account_subscriptions", "account_plan_type", "TEXT")?;
        Ok(())
    }
}

fn normalize_optional_text(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|text| !text.is_empty())
        .map(ToString::to_string)
}

fn map_account_subscription_row(row: &Row<'_>) -> Result<AccountSubscription> {
    Ok(AccountSubscription {
        account_id: row.get(0)?,
        has_subscription: row.get::<_, i64>(1)? != 0,
        account_plan_type: row.get(2)?,
        plan_type: row.get(3)?,
        expires_at: row.get(4)?,
        renews_at: row.get(5)?,
        updated_at: row.get(6)?,
    })
}

fn list_account_subscriptions_for_accounts_chunk(
    storage: &Storage,
    account_ids: &[String],
) -> Result<Vec<AccountSubscription>> {
    let Some((condition, params)) = text_id_in_clause("account_id", account_ids) else {
        return Ok(Vec::new());
    };
    let sql = account_subscriptions_for_accounts_chunk_sql(&condition);
    let mut stmt = storage.conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(params))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(map_account_subscription_row(row)?);
    }
    Ok(out)
}

fn account_subscriptions_for_accounts_chunk_sql(account_condition: &str) -> String {
    format!(
        "SELECT {columns}
         FROM account_subscriptions
         WHERE {account_condition}",
        columns = account_subscription_select_columns(),
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
    fn list_account_subscriptions_for_accounts_filters_to_requested_ids() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        for (account_id, plan) in [("acc-a", "free"), ("acc-b", "plus")] {
            storage
                .insert_account(&sample_account(account_id, now))
                .expect("insert account");
            storage
                .upsert_account_subscription(account_id, true, Some(plan), Some(plan), None, None)
                .expect("upsert subscription");
        }

        let requested = vec!["acc-b".to_string(), "missing".to_string()];
        let items = storage
            .list_account_subscriptions_for_accounts(&requested)
            .expect("list subscriptions");

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].account_id, "acc-b");
        assert_eq!(items[0].account_plan_type.as_deref(), Some("plus"));
    }

    #[test]
    fn list_account_subscriptions_for_accounts_chunks_large_account_sets() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        let target = "acc-0949";
        storage
            .insert_account(&sample_account(target, now))
            .expect("insert target account");
        storage
            .upsert_account_subscription(target, true, Some("team"), Some("team"), None, None)
            .expect("upsert subscription");

        let requested = (0..950)
            .map(|index| format!("acc-{index:04}"))
            .collect::<Vec<_>>();
        let items = storage
            .list_account_subscriptions_for_accounts(&requested)
            .expect("list subscriptions");

        assert_eq!(items.len(), 1);
        assert_eq!(items[0].account_id, target);
        assert_eq!(items[0].account_plan_type.as_deref(), Some("team"));
    }

    #[test]
    fn account_subscription_chunk_query_defers_final_ordering_to_rust() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let sql = account_subscriptions_for_accounts_chunk_sql("account_id IN ('acc-a', 'acc-b')");
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
            plan.contains("sqlite_autoindex_account_subscriptions_1")
                || plan.contains("USING INDEX"),
            "subscription chunk query should use account_id lookup index, got {plan}"
        );
        assert!(
            !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "subscription chunk query should avoid per-chunk ORDER BY temp sorting, got {plan}"
        );
    }

    #[test]
    fn account_subscription_lookup_uses_primary_key_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let mut stmt = storage
            .conn
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {}",
                account_subscription_by_account_sql()
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
            plan.contains("sqlite_autoindex_account_subscriptions_1")
                || plan.contains("USING INDEX"),
            "subscription lookup should use account_id primary-key index, got {plan}"
        );
    }

    #[test]
    fn account_subscription_list_uses_updated_at_order_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let mut stmt = storage
            .conn
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {}",
                account_subscription_list_sql()
            ))
            .expect("prepare explain");
        let mut rows = stmt.query([]).expect("query explain");
        let mut plan = String::new();
        while let Some(row) = rows.next().expect("read explain row") {
            let detail: String = row.get(3).expect("plan detail");
            plan.push_str(&detail);
            plan.push('\n');
        }

        assert!(
            plan.contains("idx_account_subscriptions_updated_at"),
            "subscription list should use updated-at order index, got {plan}"
        );
        assert!(
            !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "subscription list should avoid temp ORDER BY sorting, got {plan}"
        );
    }
    #[test]
    fn account_subscription_delete_uses_primary_key_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let mut stmt = storage
            .conn
            .prepare(&format!(
                "EXPLAIN QUERY PLAN {}",
                delete_account_subscription_for_account_sql()
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
            plan.contains("sqlite_autoindex_account_subscriptions_1")
                || plan.contains("USING INDEX"),
            "subscription delete should use account_id primary-key index, got {plan}"
        );
    }
}
