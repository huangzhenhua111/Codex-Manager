use rusqlite::params;

use super::Storage;

fn app_settings_list_sql() -> &'static str {
    "SELECT key, value
     FROM app_settings
     ORDER BY key ASC"
}

fn app_setting_value_by_key_sql() -> &'static str {
    "SELECT value
     FROM app_settings
     WHERE key = ?1
     LIMIT 1"
}

fn delete_app_setting_sql() -> &'static str {
    "DELETE FROM app_settings WHERE key = ?1"
}

impl Storage {
    /// 函数 `list_app_settings`
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
    pub fn list_app_settings(&self) -> rusqlite::Result<Vec<(String, String)>> {
        let mut stmt = self.conn.prepare(app_settings_list_sql())?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        let mut items = Vec::new();
        for row in rows {
            items.push(row?);
        }
        Ok(items)
    }

    /// 函数 `get_app_setting`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - key: 参数 key
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn get_app_setting(&self, key: &str) -> rusqlite::Result<Option<String>> {
        let mut stmt = self.conn.prepare(app_setting_value_by_key_sql())?;
        let mut rows = stmt.query([key])?;
        if let Some(row) = rows.next()? {
            return Ok(Some(row.get(0)?));
        }
        Ok(None)
    }

    /// 函数 `set_app_setting`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - key: 参数 key
    /// - value: 参数 value
    /// - updated_at: 参数 updated_at
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn set_app_setting(&self, key: &str, value: &str, updated_at: i64) -> rusqlite::Result<()> {
        self.conn.execute(
            "INSERT INTO app_settings (key, value, updated_at)
             VALUES (?1, ?2, ?3)
             ON CONFLICT(key) DO UPDATE SET
               value = excluded.value,
               updated_at = excluded.updated_at",
            params![key, value, updated_at],
        )?;
        Ok(())
    }

    /// 函数 `delete_app_setting`
    ///
    /// 作者: gaohongshun
    ///
    /// 时间: 2026-04-02
    ///
    /// # 参数
    /// - self: 参数 self
    /// - key: 参数 key
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn delete_app_setting(&self, key: &str) -> rusqlite::Result<()> {
        self.conn.execute(delete_app_setting_sql(), [key])?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn app_setting_lookup_uses_primary_key_index() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let sql = app_setting_value_by_key_sql().replace("?1", "'gateway.routeStrategy'");
        let plan = collect_query_plan(&storage, &format!("EXPLAIN QUERY PLAN {sql}"));

        assert!(
            plan.contains("sqlite_autoindex_app_settings_1"),
            "expected app setting lookup to use primary-key index, got {plan}"
        );
    }

    #[test]
    fn app_settings_list_uses_primary_key_order_without_temp_sort() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let plan = collect_query_plan(
            &storage,
            &format!("EXPLAIN QUERY PLAN {}", app_settings_list_sql()),
        );

        assert!(
            plan.contains("sqlite_autoindex_app_settings_1"),
            "expected app settings list to scan primary-key order, got {plan}"
        );
        assert!(
            !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "expected app settings list to avoid temp ordering, got {plan}"
        );
    }
}
