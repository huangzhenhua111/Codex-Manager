use rusqlite::{params_from_iter, Result, Row};

use super::key_id_filters::{normalize_text_ids, text_id_in_clause, SQLITE_IN_CLAUSE_BATCH_SIZE};
use super::{AccountImportTokenSubject, AccountTokenCandidate, AccountTokenPlan, Storage, Token};

pub(super) fn delete_token_for_account_sql() -> &'static str {
    "DELETE FROM tokens WHERE account_id = ?1"
}

impl Storage {
    /// 函数 `insert_token`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - token: 参数 token
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn insert_token(&self, token: &Token) -> Result<()> {
        self.conn.execute(
            "INSERT INTO tokens (account_id, id_token, access_token, refresh_token, api_key_access_token, last_refresh)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             ON CONFLICT(account_id) DO UPDATE SET
                id_token = excluded.id_token,
                access_token = excluded.access_token,
                refresh_token = excluded.refresh_token,
                api_key_access_token = excluded.api_key_access_token,
                last_refresh = excluded.last_refresh",
            (
                &token.account_id,
                &token.id_token,
                &token.access_token,
                &token.refresh_token,
                &token.api_key_access_token,
                token.last_refresh,
            ),
        )?;
        Ok(())
    }

    /// 函数 `list_tokens_due_for_refresh`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - refresh_due_cutoff_ts: 参数 refresh_due_cutoff_ts
    /// - access_exp_cutoff_ts: 参数 access_exp_cutoff_ts
    /// - limit: 参数 limit
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn list_tokens_due_for_refresh(
        &self,
        refresh_due_cutoff_ts: i64,
        access_exp_cutoff_ts: i64,
        limit: usize,
    ) -> Result<Vec<Token>> {
        if limit == 0 {
            return Ok(Vec::new());
        }

        let sql = tokens_due_for_refresh_sql();
        let mut stmt = self.conn.prepare(sql)?;
        let mut rows = stmt.query((refresh_due_cutoff_ts, access_exp_cutoff_ts, limit as i64))?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(map_token_row(row)?);
        }
        Ok(out)
    }

    /// 函数 `update_token_refresh_schedule`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - account_id: 参数 account_id
    /// - access_token_exp: 参数 access_token_exp
    /// - next_refresh_at: 参数 next_refresh_at
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn update_token_refresh_schedule(
        &self,
        account_id: &str,
        access_token_exp: Option<i64>,
        next_refresh_at: Option<i64>,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE tokens
             SET access_token_exp = ?1,
                 next_refresh_at = ?2
             WHERE account_id = ?3",
            (access_token_exp, next_refresh_at, account_id),
        )?;
        Ok(())
    }

    /// 函数 `touch_token_refresh_attempt`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - account_id: 参数 account_id
    /// - attempt_ts: 参数 attempt_ts
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn touch_token_refresh_attempt(&self, account_id: &str, attempt_ts: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE tokens
             SET last_refresh_attempt_at = ?1
             WHERE account_id = ?2",
            (attempt_ts, account_id),
        )?;
        Ok(())
    }

    /// 函数 `token_count`
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
    pub fn token_count(&self) -> Result<i64> {
        self.conn.query_row(token_count_sql(), [], |row| row.get(0))
    }

    pub fn token_account_count(&self) -> Result<i64> {
        self.conn
            .query_row(token_account_count_sql(), [], |row| row.get(0))
    }

    /// 函数 `list_tokens`
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
    pub fn list_tokens(&self) -> Result<Vec<Token>> {
        let mut stmt = self.conn.prepare(token_list_sql())?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(map_token_row(row)?);
        }
        Ok(out)
    }

    pub fn list_account_token_candidates(&self) -> Result<Vec<AccountTokenCandidate>> {
        let mut stmt = self.conn.prepare(account_token_candidates_sql())?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(map_account_token_candidate_row(row)?);
        }
        Ok(out)
    }

    pub fn list_usable_account_token_candidates(&self) -> Result<Vec<AccountTokenCandidate>> {
        let mut stmt = self.conn.prepare(usable_account_token_candidates_sql())?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(map_account_token_candidate_row(row)?);
        }
        Ok(out)
    }

    pub fn list_account_token_candidates_for_accounts(
        &self,
        account_ids: &[String],
    ) -> Result<Vec<AccountTokenCandidate>> {
        let account_ids = normalize_text_ids(account_ids);
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for chunk in account_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(list_account_token_candidates_for_accounts_chunk(
                self, chunk,
            )?);
        }
        out.sort_by(|a, b| a.account_id.cmp(&b.account_id));
        Ok(out)
    }

    pub fn list_usable_account_token_candidates_for_accounts(
        &self,
        account_ids: &[String],
    ) -> Result<Vec<AccountTokenCandidate>> {
        let account_ids = normalize_text_ids(account_ids);
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for chunk in account_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(list_usable_account_token_candidates_for_accounts_chunk(
                self, chunk,
            )?);
        }
        out.sort_by(|a, b| a.account_id.cmp(&b.account_id));
        Ok(out)
    }

    pub fn list_account_import_token_subjects(&self) -> Result<Vec<AccountImportTokenSubject>> {
        let mut stmt = self.conn.prepare(account_import_token_subjects_sql())?;
        let mut rows = stmt.query([])?;
        let mut out = Vec::new();
        while let Some(row) = rows.next()? {
            out.push(AccountImportTokenSubject {
                account_id: row.get(0)?,
                id_token: row.get(1)?,
                access_token: row.get(2)?,
                refresh_token: row.get(3)?,
            });
        }
        Ok(out)
    }

    pub fn list_tokens_for_accounts(&self, account_ids: &[String]) -> Result<Vec<Token>> {
        let account_ids = normalize_text_ids(account_ids);
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for chunk in account_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(list_tokens_for_accounts_chunk(self, chunk)?);
        }
        out.sort_by(|a, b| a.account_id.cmp(&b.account_id));
        Ok(out)
    }

    pub fn list_account_token_plans_for_accounts(
        &self,
        account_ids: &[String],
    ) -> Result<Vec<AccountTokenPlan>> {
        let account_ids = normalize_text_ids(account_ids);
        if account_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut out = Vec::new();
        for chunk in account_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            out.extend(list_account_token_plans_for_accounts_chunk(self, chunk)?);
        }
        out.sort_by(|a, b| a.account_id.cmp(&b.account_id));
        Ok(out)
    }

    /// 函数 `find_token_by_account_id`
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
    pub fn find_token_by_account_id(&self, account_id: &str) -> Result<Option<Token>> {
        let mut stmt = self.conn.prepare(token_by_account_sql())?;
        let mut rows = stmt.query([account_id])?;
        if let Some(row) = rows.next()? {
            Ok(Some(map_token_row(row)?))
        } else {
            Ok(None)
        }
    }

    /// 函数 `ensure_token_api_key_column`
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
    pub(super) fn ensure_token_api_key_column(&self) -> Result<()> {
        if self.has_column("tokens", "api_key_access_token")? {
            return Ok(());
        }
        self.conn.execute(
            "ALTER TABLE tokens ADD COLUMN api_key_access_token TEXT",
            [],
        )?;
        Ok(())
    }

    /// 函数 `ensure_token_refresh_schedule_columns`
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
    pub(super) fn ensure_token_refresh_schedule_columns(&self) -> Result<()> {
        self.ensure_column("tokens", "access_token_exp", "INTEGER")?;
        self.ensure_column("tokens", "next_refresh_at", "INTEGER")?;
        self.ensure_column("tokens", "last_refresh_attempt_at", "INTEGER")?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tokens_next_refresh_at ON tokens(next_refresh_at)",
            [],
        )?;
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_tokens_refresh_due_order
             ON tokens(COALESCE(next_refresh_at, 0) ASC, account_id ASC)",
            [],
        )?;
        Ok(())
    }
}

fn token_count_sql() -> &'static str {
    "SELECT COUNT(1) FROM tokens"
}

fn token_account_count_sql() -> &'static str {
    "SELECT COUNT(DISTINCT account_id) FROM tokens"
}

fn token_list_sql() -> &'static str {
    "SELECT account_id, id_token, access_token, refresh_token, api_key_access_token, last_refresh
     FROM tokens"
}

fn account_token_candidates_sql() -> &'static str {
    "SELECT
        account_id,
        TRIM(COALESCE(access_token, '')) <> '',
        TRIM(COALESCE(refresh_token, '')) <> '',
        last_refresh
     FROM tokens
     ORDER BY account_id ASC"
}

fn usable_account_token_candidates_sql() -> &'static str {
    "SELECT
        account_id,
        1,
        1,
        last_refresh
     FROM tokens
     WHERE TRIM(COALESCE(access_token, '')) <> ''
       AND TRIM(COALESCE(refresh_token, '')) <> ''
     ORDER BY account_id ASC"
}

fn account_import_token_subjects_sql() -> &'static str {
    "SELECT account_id, id_token, access_token, refresh_token
     FROM tokens
     ORDER BY account_id ASC"
}

fn token_by_account_sql() -> &'static str {
    "SELECT account_id, id_token, access_token, refresh_token, api_key_access_token, last_refresh
     FROM tokens
     WHERE account_id = ?1
     LIMIT 1"
}

fn tokens_due_for_refresh_sql() -> &'static str {
    "WITH latest_status AS (
        SELECT
            e.account_id,
            e.message,
            ROW_NUMBER() OVER (
                PARTITION BY e.account_id
                ORDER BY e.created_at DESC, e.id DESC
            ) AS rn
        FROM tokens target_tokens
        INNER JOIN events e
          ON e.account_id = target_tokens.account_id
        WHERE e.type = 'account_status_update'
          AND TRIM(COALESCE(target_tokens.refresh_token, '')) <> ''
          AND (
                target_tokens.next_refresh_at IS NULL
                OR target_tokens.next_refresh_at <= ?1
                OR (
                    target_tokens.access_token_exp IS NOT NULL
                    AND target_tokens.access_token_exp <= ?2
                )
          )
     )
     SELECT tokens.account_id, tokens.id_token, tokens.access_token, tokens.refresh_token, tokens.api_key_access_token, tokens.last_refresh
     FROM tokens
     LEFT JOIN latest_status
       ON latest_status.account_id = tokens.account_id
      AND latest_status.rn = 1
     WHERE TRIM(COALESCE(refresh_token, '')) <> ''
       AND (
            latest_status.message IS NULL
            OR (
                latest_status.message NOT LIKE '% reason=account_deactivated'
                AND latest_status.message NOT LIKE '% reason=workspace_deactivated'
                AND latest_status.message NOT LIKE '% reason=refresh_token_region_blocked'
            )
       )
       AND (
            next_refresh_at IS NULL
            OR next_refresh_at <= ?1
            OR (
                access_token_exp IS NOT NULL
                AND access_token_exp <= ?2
            )
       )
     ORDER BY COALESCE(tokens.next_refresh_at, 0) ASC, tokens.account_id ASC
     LIMIT ?3"
}

/// 函数 `map_token_row`
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
fn map_token_row(row: &Row<'_>) -> Result<Token> {
    Ok(Token {
        account_id: row.get(0)?,
        id_token: row.get(1)?,
        access_token: row.get(2)?,
        refresh_token: row.get(3)?,
        api_key_access_token: row.get(4)?,
        last_refresh: row.get(5)?,
    })
}

fn map_account_token_plan_row(row: &Row<'_>) -> Result<AccountTokenPlan> {
    Ok(AccountTokenPlan {
        account_id: row.get(0)?,
        id_token: row.get(1)?,
        access_token: row.get(2)?,
    })
}

fn map_account_token_candidate_row(row: &Row<'_>) -> Result<AccountTokenCandidate> {
    Ok(AccountTokenCandidate {
        account_id: row.get(0)?,
        has_access_token: row.get(1)?,
        has_refresh_token: row.get(2)?,
        last_refresh: row.get(3)?,
    })
}

fn list_tokens_for_accounts_chunk(storage: &Storage, account_ids: &[String]) -> Result<Vec<Token>> {
    let Some((condition, params)) = text_id_in_clause("account_id", account_ids) else {
        return Ok(Vec::new());
    };
    let sql = tokens_for_accounts_chunk_sql(&condition);
    let mut stmt = storage.conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(params))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(map_token_row(row)?);
    }
    Ok(out)
}

fn tokens_for_accounts_chunk_sql(account_condition: &str) -> String {
    format!(
        "SELECT account_id, id_token, access_token, refresh_token, api_key_access_token, last_refresh
         FROM tokens
         WHERE {account_condition}"
    )
}

fn list_account_token_candidates_for_accounts_chunk(
    storage: &Storage,
    account_ids: &[String],
) -> Result<Vec<AccountTokenCandidate>> {
    let Some((condition, params)) = text_id_in_clause("account_id", account_ids) else {
        return Ok(Vec::new());
    };
    let sql = account_token_candidates_for_accounts_chunk_sql(&condition);
    let mut stmt = storage.conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(params))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(map_account_token_candidate_row(row)?);
    }
    Ok(out)
}

fn account_token_candidates_for_accounts_chunk_sql(account_condition: &str) -> String {
    format!(
        "SELECT
            account_id,
            TRIM(COALESCE(access_token, '')) <> '',
            TRIM(COALESCE(refresh_token, '')) <> '',
            last_refresh
         FROM tokens
         WHERE {account_condition}"
    )
}

fn list_usable_account_token_candidates_for_accounts_chunk(
    storage: &Storage,
    account_ids: &[String],
) -> Result<Vec<AccountTokenCandidate>> {
    let Some((condition, params)) = text_id_in_clause("account_id", account_ids) else {
        return Ok(Vec::new());
    };
    let sql = usable_account_token_candidates_for_accounts_chunk_sql(&condition);
    let mut stmt = storage.conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(params))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(map_account_token_candidate_row(row)?);
    }
    Ok(out)
}

fn usable_account_token_candidates_for_accounts_chunk_sql(account_condition: &str) -> String {
    format!(
        "SELECT
            account_id,
            1,
            1,
            last_refresh
         FROM tokens
         WHERE {account_condition}
           AND TRIM(COALESCE(access_token, '')) <> ''
           AND TRIM(COALESCE(refresh_token, '')) <> ''"
    )
}

fn list_account_token_plans_for_accounts_chunk(
    storage: &Storage,
    account_ids: &[String],
) -> Result<Vec<AccountTokenPlan>> {
    let Some((condition, params)) = text_id_in_clause("account_id", account_ids) else {
        return Ok(Vec::new());
    };
    let sql = account_token_plans_for_accounts_chunk_sql(&condition);
    let mut stmt = storage.conn.prepare(&sql)?;
    let mut rows = stmt.query(params_from_iter(params))?;
    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        out.push(map_account_token_plan_row(row)?);
    }
    Ok(out)
}

fn account_token_plans_for_accounts_chunk_sql(account_condition: &str) -> String {
    format!(
        "SELECT account_id, id_token, access_token
         FROM tokens
         WHERE {account_condition}"
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

    fn sample_token(account_id: &str, now: i64) -> Token {
        Token {
            account_id: account_id.to_string(),
            id_token: format!("{account_id}.id"),
            access_token: format!("{account_id}.access"),
            refresh_token: format!("{account_id}.refresh"),
            api_key_access_token: Some(format!("{account_id}.api")),
            last_refresh: now,
        }
    }

    fn collect_query_plan(storage: &Storage, sql: &str) -> String {
        let mut stmt = storage.conn.prepare(sql).expect("prepare explain");
        let mut rows = stmt.query([]).expect("query explain");
        let mut plan = String::new();
        while let Some(row) = rows.next().expect("read explain row") {
            let detail: String = row.get(3).expect("plan detail");
            plan.push_str(&detail);
            plan.push('\n');
        }
        plan
    }

    fn collect_query_plan_with_params<P>(storage: &Storage, sql: &str, params: P) -> String
    where
        P: rusqlite::Params,
    {
        let mut stmt = storage.conn.prepare(sql).expect("prepare explain");
        let mut rows = stmt.query(params).expect("query explain");
        let mut plan = String::new();
        while let Some(row) = rows.next().expect("read explain row") {
            let detail: String = row.get(3).expect("plan detail");
            plan.push_str(&detail);
            plan.push('\n');
        }
        plan
    }
    #[test]
    fn list_account_token_plans_for_accounts_reads_only_requested_plan_fields() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        for account_id in ["acc-a", "acc-b", "acc-c"] {
            storage
                .insert_account(&sample_account(account_id, now))
                .expect("insert account");
            storage
                .insert_token(&sample_token(account_id, now))
                .expect("insert token");
        }

        let requested = vec!["acc-b".to_string(), "missing".to_string()];
        let plans = storage
            .list_account_token_plans_for_accounts(&requested)
            .expect("list token plans");

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].account_id, "acc-b");
        assert_eq!(plans[0].id_token, "acc-b.id");
        assert_eq!(plans[0].access_token, "acc-b.access");
    }

    #[test]
    fn list_account_token_candidates_reads_only_candidate_fields() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        storage
            .insert_account(&sample_account("acc-a", now))
            .expect("insert account");
        storage
            .insert_token(&sample_token("acc-a", now))
            .expect("insert token");

        storage
            .insert_account(&sample_account("acc-empty", now))
            .expect("insert empty account");
        storage
            .insert_token(&Token {
                account_id: "acc-empty".to_string(),
                access_token: " ".to_string(),
                refresh_token: String::new(),
                ..sample_token("acc-empty", now + 1)
            })
            .expect("insert empty token");

        let candidates = storage
            .list_account_token_candidates()
            .expect("list token candidates");

        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0].account_id, "acc-a");
        assert!(candidates[0].has_access_token);
        assert!(candidates[0].has_refresh_token);
        assert_eq!(candidates[0].last_refresh, now);
        assert_eq!(candidates[1].account_id, "acc-empty");
        assert!(!candidates[1].has_access_token);
        assert!(!candidates[1].has_refresh_token);
        assert_eq!(candidates[1].last_refresh, now + 1);
    }

    #[test]
    fn list_usable_account_token_candidates_filters_empty_tokens_in_sql() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        for account_id in ["acc-ready", "acc-no-access", "acc-no-refresh"] {
            storage
                .insert_account(&sample_account(account_id, now))
                .expect("insert account");
        }
        storage
            .insert_token(&sample_token("acc-ready", now))
            .expect("insert ready token");
        storage
            .insert_token(&Token {
                account_id: "acc-no-access".to_string(),
                access_token: " ".to_string(),
                ..sample_token("acc-no-access", now + 1)
            })
            .expect("insert no access token");
        storage
            .insert_token(&Token {
                account_id: "acc-no-refresh".to_string(),
                refresh_token: String::new(),
                ..sample_token("acc-no-refresh", now + 2)
            })
            .expect("insert no refresh token");

        let candidates = storage
            .list_usable_account_token_candidates()
            .expect("list usable token candidates");

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].account_id, "acc-ready");
        assert!(candidates[0].has_access_token);
        assert!(candidates[0].has_refresh_token);
        assert_eq!(candidates[0].last_refresh, now);
    }

    #[test]
    fn token_account_count_counts_distinct_token_accounts() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        for account_id in ["acc-a", "acc-b"] {
            storage
                .insert_account(&sample_account(account_id, now))
                .expect("insert account");
            storage
                .insert_token(&sample_token(account_id, now))
                .expect("insert token");
        }

        assert_eq!(
            storage.token_account_count().expect("count token accounts"),
            2
        );
    }

    #[test]
    fn list_account_token_candidates_for_accounts_filters_requested_ids() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        for account_id in ["acc-a", "acc-b", "acc-c"] {
            storage
                .insert_account(&sample_account(account_id, now))
                .expect("insert account");
            storage
                .insert_token(&sample_token(account_id, now))
                .expect("insert token");
        }

        let requested = vec![
            "acc-c".to_string(),
            "missing".to_string(),
            "acc-c".to_string(),
            " ".to_string(),
        ];
        let candidates = storage
            .list_account_token_candidates_for_accounts(&requested)
            .expect("list token candidates");

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].account_id, "acc-c");
        assert!(candidates[0].has_access_token);
        assert!(candidates[0].has_refresh_token);
    }

    #[test]
    fn list_usable_account_token_candidates_for_accounts_filters_requested_and_empty_tokens() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        for account_id in ["acc-ready", "acc-no-access", "acc-no-refresh", "acc-other"] {
            storage
                .insert_account(&sample_account(account_id, now))
                .expect("insert account");
        }
        storage
            .insert_token(&sample_token("acc-ready", now))
            .expect("insert ready token");
        storage
            .insert_token(&Token {
                account_id: "acc-no-access".to_string(),
                access_token: " ".to_string(),
                ..sample_token("acc-no-access", now + 1)
            })
            .expect("insert no access token");
        storage
            .insert_token(&Token {
                account_id: "acc-no-refresh".to_string(),
                refresh_token: String::new(),
                ..sample_token("acc-no-refresh", now + 2)
            })
            .expect("insert no refresh token");
        storage
            .insert_token(&sample_token("acc-other", now + 3))
            .expect("insert unrequested token");

        let requested = vec![
            "acc-no-access".to_string(),
            "acc-ready".to_string(),
            "missing".to_string(),
            "acc-no-refresh".to_string(),
            "acc-ready".to_string(),
            " ".to_string(),
        ];
        let candidates = storage
            .list_usable_account_token_candidates_for_accounts(&requested)
            .expect("list usable token candidates");

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].account_id, "acc-ready");
        assert!(candidates[0].has_access_token);
        assert!(candidates[0].has_refresh_token);
    }

    #[test]
    fn list_account_token_candidates_for_accounts_chunks_large_account_sets() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        let target = "acc-0949";
        storage
            .insert_account(&sample_account(target, now))
            .expect("insert target account");
        storage
            .insert_token(&sample_token(target, now))
            .expect("insert target token");

        let requested = (0..950)
            .map(|index| format!("acc-{index:04}"))
            .collect::<Vec<_>>();
        let candidates = storage
            .list_account_token_candidates_for_accounts(&requested)
            .expect("list token candidates");

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].account_id, target);
    }

    #[test]
    fn account_token_full_list_helpers_use_primary_key_ordering() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        for sql in [
            account_token_candidates_sql(),
            usable_account_token_candidates_sql(),
            account_import_token_subjects_sql(),
        ] {
            let plan = collect_query_plan(&storage, &format!("EXPLAIN QUERY PLAN {sql}"));
            assert!(
                plan.contains("sqlite_autoindex_tokens_1") || plan.contains("USING INDEX"),
                "expected full token list query to scan token primary-key order, got {plan}"
            );
            assert!(
                !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
                "full token list query should avoid ORDER BY temp sorting, got {plan}"
            );
        }
    }

    #[test]
    fn token_lookup_helpers_use_primary_key_indexes() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let token_plan = collect_query_plan_with_params(
            &storage,
            &format!("EXPLAIN QUERY PLAN {}", token_by_account_sql()),
            ["acc-a"],
        );
        assert!(
            token_plan.contains("sqlite_autoindex_tokens_1") || token_plan.contains("USING INDEX"),
            "expected token lookup by account primary key, got {token_plan}"
        );

        let account_count_plan = collect_query_plan(
            &storage,
            &format!("EXPLAIN QUERY PLAN {}", token_account_count_sql()),
        );
        assert!(
            account_count_plan.contains("sqlite_autoindex_tokens_1")
                || account_count_plan.contains("USING COVERING INDEX"),
            "expected token account count to use account primary key, got {account_count_plan}"
        );
    }
    #[test]
    fn list_account_token_candidates_for_accounts_uses_account_lookup_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let sql = account_token_candidates_for_accounts_chunk_sql("account_id IN ('acc-a')");
        let plan = collect_query_plan(&storage, &format!("EXPLAIN QUERY PLAN {sql}"));

        assert!(
            plan.contains("sqlite_autoindex_tokens_1") || plan.contains("USING INDEX"),
            "expected token account lookup index in plan, got {plan}"
        );
        assert!(
            !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "token candidate chunk query should avoid per-chunk ORDER BY temp sorting, got {plan}"
        );
    }

    #[test]
    fn list_tokens_for_accounts_uses_account_lookup_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let sql = tokens_for_accounts_chunk_sql("account_id IN ('acc-a')");
        let plan = collect_query_plan(&storage, &format!("EXPLAIN QUERY PLAN {sql}"));

        assert!(
            plan.contains("sqlite_autoindex_tokens_1") || plan.contains("USING INDEX"),
            "expected token row lookup by account index in plan, got {plan}"
        );
        assert!(
            !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "token row chunk query should avoid per-chunk ORDER BY temp sorting, got {plan}"
        );
    }

    #[test]
    fn list_account_token_plans_for_accounts_uses_account_lookup_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let sql = account_token_plans_for_accounts_chunk_sql("account_id IN ('acc-a')");
        let plan = collect_query_plan(&storage, &format!("EXPLAIN QUERY PLAN {sql}"));

        assert!(
            plan.contains("sqlite_autoindex_tokens_1") || plan.contains("USING INDEX"),
            "expected token plan lookup by account index in plan, got {plan}"
        );
        assert!(
            !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "token plan chunk query should avoid per-chunk ORDER BY temp sorting, got {plan}"
        );
    }

    #[test]
    fn list_usable_account_token_candidates_for_accounts_uses_account_lookup_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let sql = usable_account_token_candidates_for_accounts_chunk_sql("account_id IN ('acc-a')");
        let plan = collect_query_plan(&storage, &format!("EXPLAIN QUERY PLAN {sql}"));

        assert!(
            plan.contains("sqlite_autoindex_tokens_1") || plan.contains("USING INDEX"),
            "expected token account lookup index in plan, got {plan}"
        );
        assert!(
            !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "usable token candidate chunk query should avoid per-chunk ORDER BY temp sorting, got {plan}"
        );
    }

    #[test]
    fn list_tokens_due_for_refresh_uses_due_order_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let sql = tokens_due_for_refresh_sql();
        let mut stmt = storage
            .conn
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .expect("prepare explain");
        let mut rows = stmt
            .query((100_i64, 100_i64, 10_i64))
            .expect("query explain");
        let mut plan = String::new();
        while let Some(row) = rows.next().expect("read explain row") {
            let detail: String = row.get(3).expect("plan detail");
            plan.push_str(&detail);
            plan.push('\n');
        }

        assert!(
            sql.contains("FROM tokens target_tokens"),
            "expected latest status CTE to scope events through due tokens, got {sql}"
        );
        assert!(
            plan.contains("idx_tokens_refresh_due_order"),
            "expected token refresh due order index in plan, got {plan}"
        );
        assert!(
            !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "expected refresh due query to avoid a temp sort, got {plan}"
        );
    }

    #[test]
    fn list_tokens_due_for_refresh_short_circuits_zero_limit() {
        let storage = Storage::open_in_memory().expect("open storage");

        let tokens = storage
            .list_tokens_due_for_refresh(100, 200, 0)
            .expect("zero limit should not query storage");

        assert!(tokens.is_empty());
    }

    #[test]
    fn list_account_import_token_subjects_reads_only_subject_fields() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        for account_id in ["acc-b", "acc-a"] {
            storage
                .insert_account(&sample_account(account_id, now))
                .expect("insert account");
            storage
                .insert_token(&sample_token(account_id, now))
                .expect("insert token");
        }

        let subjects = storage
            .list_account_import_token_subjects()
            .expect("list import token subjects");

        assert_eq!(subjects.len(), 2);
        assert_eq!(subjects[0].account_id, "acc-a");
        assert_eq!(subjects[0].id_token, "acc-a.id");
        assert_eq!(subjects[0].access_token, "acc-a.access");
        assert_eq!(subjects[0].refresh_token, "acc-a.refresh");
        assert_eq!(subjects[1].account_id, "acc-b");
    }

    #[test]
    fn list_tokens_for_accounts_filters_requested_ids() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        for account_id in ["acc-a", "acc-b", "acc-c"] {
            storage
                .insert_account(&sample_account(account_id, now))
                .expect("insert account");
            storage
                .insert_token(&sample_token(account_id, now))
                .expect("insert token");
        }

        let requested = vec!["acc-c".to_string(), "missing".to_string()];
        let tokens = storage
            .list_tokens_for_accounts(&requested)
            .expect("list tokens");

        assert_eq!(tokens.len(), 1);
        assert_eq!(tokens[0].account_id, "acc-c");
        assert_eq!(tokens[0].refresh_token, "acc-c.refresh");
    }

    #[test]
    fn list_account_token_plans_for_accounts_chunks_large_account_sets() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");
        let now = now_ts();

        let target = "acc-0949";
        storage
            .insert_account(&sample_account(target, now))
            .expect("insert target account");
        storage
            .insert_token(&sample_token(target, now))
            .expect("insert target token");

        let requested = (0..950)
            .map(|index| format!("acc-{index:04}"))
            .collect::<Vec<_>>();
        let plans = storage
            .list_account_token_plans_for_accounts(&requested)
            .expect("list token plans");

        assert_eq!(plans.len(), 1);
        assert_eq!(plans[0].account_id, target);
        assert_eq!(plans[0].id_token, "acc-0949.id");
        assert_eq!(plans[0].access_token, "acc-0949.access");
    }
    #[test]
    fn token_delete_uses_primary_key_index() {
        let storage = Storage::open_in_memory().expect("open");
        storage.init().expect("init");

        let plan = collect_query_plan_with_params(
            &storage,
            &format!("EXPLAIN QUERY PLAN {}", delete_token_for_account_sql()),
            ["acc-a"],
        );

        assert!(
            plan.contains("sqlite_autoindex_tokens_1") || plan.contains("USING INDEX"),
            "token delete should use account_id primary-key index, got {plan}"
        );
    }
}
