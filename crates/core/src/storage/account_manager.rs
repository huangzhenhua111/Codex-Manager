use std::collections::HashMap;

use rusqlite::{params, params_from_iter, OptionalExtension, Result, Row};

use super::{
    now_ts, ApiKeyOwner, AppProject, AppSessionUserWithWallet, AppUser, AppUserAccessSummary,
    AppUserSession, AppWallet, AppWalletLedgerEntry, BillingRule, DashboardAppUserSummary,
    PublicAppUserWithWallet, Storage,
};
use crate::storage::key_id_filters::{
    normalize_text_ids, text_id_in_clause, SQLITE_IN_CLAUSE_BATCH_SIZE,
};

fn map_app_user(row: &Row<'_>) -> Result<AppUser> {
    Ok(AppUser {
        id: row.get(0)?,
        username: row.get(1)?,
        display_name: row.get(2)?,
        password_hash: row.get(3)?,
        role: row.get(4)?,
        status: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
        last_login_at: row.get(8)?,
    })
}

fn map_dashboard_app_user_summary(row: &Row<'_>) -> Result<DashboardAppUserSummary> {
    Ok(DashboardAppUserSummary {
        id: row.get(0)?,
        username: row.get(1)?,
        display_name: row.get(2)?,
        role: row.get(3)?,
        status: row.get(4)?,
        wallet_available_credit_micros: row.get(5)?,
    })
}

fn map_app_user_access_summary(row: &Row<'_>) -> Result<AppUserAccessSummary> {
    Ok(AppUserAccessSummary {
        id: row.get(0)?,
        username: row.get(1)?,
        role: row.get(2)?,
        status: row.get(3)?,
    })
}

fn map_public_app_user_with_wallet(row: &Row<'_>) -> Result<PublicAppUserWithWallet> {
    Ok(PublicAppUserWithWallet {
        id: row.get(0)?,
        username: row.get(1)?,
        display_name: row.get(2)?,
        role: row.get(3)?,
        status: row.get(4)?,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
        last_login_at: row.get(7)?,
        wallet_id: row.get(8)?,
        wallet_owner_kind: row.get(9)?,
        wallet_owner_id: row.get(10)?,
        wallet_balance_credit_micros: row.get(11)?,
        wallet_frozen_credit_micros: row.get(12)?,
        wallet_status: row.get(13)?,
        wallet_created_at: row.get(14)?,
        wallet_updated_at: row.get(15)?,
    })
}

fn map_public_app_user_with_wallet_from_offset(
    row: &Row<'_>,
    offset: usize,
) -> Result<PublicAppUserWithWallet> {
    Ok(PublicAppUserWithWallet {
        id: row.get(offset)?,
        username: row.get(offset + 1)?,
        display_name: row.get(offset + 2)?,
        role: row.get(offset + 3)?,
        status: row.get(offset + 4)?,
        created_at: row.get(offset + 5)?,
        updated_at: row.get(offset + 6)?,
        last_login_at: row.get(offset + 7)?,
        wallet_id: row.get(offset + 8)?,
        wallet_owner_kind: row.get(offset + 9)?,
        wallet_owner_id: row.get(offset + 10)?,
        wallet_balance_credit_micros: row.get(offset + 11)?,
        wallet_frozen_credit_micros: row.get(offset + 12)?,
        wallet_status: row.get(offset + 13)?,
        wallet_created_at: row.get(offset + 14)?,
        wallet_updated_at: row.get(offset + 15)?,
    })
}

fn map_app_session_user_with_wallet(row: &Row<'_>) -> Result<AppSessionUserWithWallet> {
    Ok(AppSessionUserWithWallet {
        session_id: row.get(0)?,
        expires_at: row.get(1)?,
        user: map_public_app_user_with_wallet_from_offset(row, 2)?,
    })
}

fn map_app_session(row: &Row<'_>) -> Result<AppUserSession> {
    Ok(AppUserSession {
        id: row.get(0)?,
        user_id: row.get(1)?,
        token_hash: row.get(2)?,
        expires_at: row.get(3)?,
        created_at: row.get(4)?,
        last_seen_at: row.get(5)?,
        revoked_at: row.get(6)?,
    })
}

fn map_app_wallet(row: &Row<'_>) -> Result<AppWallet> {
    Ok(AppWallet {
        id: row.get(0)?,
        owner_kind: row.get(1)?,
        owner_id: row.get(2)?,
        balance_credit_micros: row.get(3)?,
        frozen_credit_micros: row.get(4)?,
        status: row.get(5)?,
        created_at: row.get(6)?,
        updated_at: row.get(7)?,
    })
}

fn map_api_key_owner(row: &Row<'_>) -> Result<ApiKeyOwner> {
    Ok(ApiKeyOwner {
        key_id: row.get(0)?,
        owner_kind: row.get(1)?,
        owner_user_id: row.get(2)?,
        project_id: row.get(3)?,
        updated_at: row.get(4)?,
    })
}

fn map_billing_rule(row: &Row<'_>) -> Result<BillingRule> {
    Ok(BillingRule {
        id: row.get(0)?,
        name: row.get(1)?,
        status: row.get(2)?,
        priority: row.get(3)?,
        multiplier_millis: row.get(4)?,
        model_pattern: row.get(5)?,
        service_tier: row.get(6)?,
        user_id: row.get(7)?,
        project_id: row.get(8)?,
        api_key_id: row.get(9)?,
        starts_at: row.get(10)?,
        ends_at: row.get(11)?,
        created_at: row.get(12)?,
        updated_at: row.get(13)?,
    })
}

impl Storage {
    pub fn app_user_count(&self) -> Result<i64> {
        self.conn
            .query_row("SELECT COUNT(*) FROM app_users", [], |row| row.get(0))
    }

    pub fn member_app_user_count(&self) -> Result<i64> {
        self.conn.query_row(
            "SELECT COUNT(*) FROM app_users WHERE role = 'member'",
            [],
            |row| row.get(0),
        )
    }

    pub fn active_admin_count(&self) -> Result<i64> {
        self.conn.query_row(
            "SELECT COUNT(*) FROM app_users WHERE role = 'admin' AND status = 'active'",
            [],
            |row| row.get(0),
        )
    }

    pub fn insert_app_user(&self, user: &AppUser) -> Result<()> {
        self.conn.execute(
            "INSERT INTO app_users (
                id, username, display_name, password_hash, role, status,
                created_at, updated_at, last_login_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            (
                &user.id,
                &user.username,
                &user.display_name,
                &user.password_hash,
                &user.role,
                &user.status,
                user.created_at,
                user.updated_at,
                user.last_login_at,
            ),
        )?;
        Ok(())
    }

    pub fn delete_app_user(&self, user_id: &str) -> Result<usize> {
        let tx = self.conn.unchecked_transaction()?;
        tx.execute(
            "DELETE FROM api_key_owners
             WHERE owner_kind = 'user' AND owner_user_id = ?1",
            [user_id],
        )?;
        tx.execute(
            "DELETE FROM app_user_sessions WHERE user_id = ?1",
            [user_id],
        )?;
        tx.execute(
            "DELETE FROM user_model_groups WHERE user_id = ?1",
            [user_id],
        )?;
        tx.execute(
            "DELETE FROM app_wallet_ledger_entries
             WHERE wallet_id IN (
                SELECT id FROM app_wallets WHERE owner_kind = 'user' AND owner_id = ?1
             )",
            [user_id],
        )?;
        tx.execute(
            "DELETE FROM app_wallets WHERE owner_kind = 'user' AND owner_id = ?1",
            [user_id],
        )?;
        let deleted = tx.execute("DELETE FROM app_users WHERE id = ?1", [user_id])?;
        tx.commit()?;
        Ok(deleted)
    }

    pub fn list_app_users(&self) -> Result<Vec<AppUser>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, username, display_name, password_hash, role, status,
                    created_at, updated_at, last_login_at
             FROM app_users
             ORDER BY created_at ASC, username ASC",
        )?;
        let rows = stmt.query_map([], map_app_user)?;
        rows.collect()
    }

    pub fn list_dashboard_app_user_summaries(&self) -> Result<Vec<DashboardAppUserSummary>> {
        let mut stmt = self.conn.prepare(
            "SELECT
                u.id,
                u.username,
                u.display_name,
                u.role,
                u.status,
                w.balance_credit_micros - w.frozen_credit_micros
             FROM app_users u
             LEFT JOIN app_wallets w
               ON w.owner_kind = 'user'
              AND w.owner_id = u.id
             ORDER BY u.created_at ASC, u.username ASC",
        )?;
        let rows = stmt.query_map([], map_dashboard_app_user_summary)?;
        rows.collect()
    }

    pub fn list_dashboard_app_user_summaries_for_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<DashboardAppUserSummary>> {
        let user_ids = normalize_text_ids(user_ids);
        if user_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut users = Vec::new();
        for chunk in user_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            let Some((condition, params)) = text_id_in_clause("u.id", chunk) else {
                continue;
            };
            let sql = format!(
                "SELECT
                    u.id,
                    u.username,
                    u.display_name,
                    u.role,
                    u.status,
                    w.balance_credit_micros - w.frozen_credit_micros
                 FROM app_users u
                 LEFT JOIN app_wallets w
                   ON w.owner_kind = 'user'
                  AND w.owner_id = u.id
                 WHERE {condition}"
            );
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(params), map_dashboard_app_user_summary)?;
            for row in rows {
                users.push(row?);
            }
        }
        users.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(users)
    }

    pub fn list_public_app_users_with_wallets(&self) -> Result<Vec<PublicAppUserWithWallet>> {
        let mut stmt = self.conn.prepare(
            "SELECT
                u.id,
                u.username,
                u.display_name,
                u.role,
                u.status,
                u.created_at,
                u.updated_at,
                u.last_login_at,
                w.id,
                w.owner_kind,
                w.owner_id,
                w.balance_credit_micros,
                w.frozen_credit_micros,
                w.status,
                w.created_at,
                w.updated_at
             FROM app_users u
             LEFT JOIN app_wallets w
               ON u.role <> 'admin'
              AND w.owner_kind = 'user'
              AND w.owner_id = u.id
             ORDER BY u.created_at ASC, u.username ASC",
        )?;
        let rows = stmt.query_map([], map_public_app_user_with_wallet)?;
        rows.collect()
    }

    pub fn find_public_app_user_with_wallet_by_id(
        &self,
        id: &str,
    ) -> Result<Option<PublicAppUserWithWallet>> {
        self.conn
            .query_row(
                "SELECT
                    u.id,
                    u.username,
                    u.display_name,
                    u.role,
                    u.status,
                    u.created_at,
                    u.updated_at,
                    u.last_login_at,
                    w.id,
                    w.owner_kind,
                    w.owner_id,
                    w.balance_credit_micros,
                    w.frozen_credit_micros,
                    w.status,
                    w.created_at,
                    w.updated_at
                 FROM app_users u
                 LEFT JOIN app_wallets w
                   ON u.role <> 'admin'
                  AND w.owner_kind = 'user'
                  AND w.owner_id = u.id
                 WHERE u.id = ?1
                 LIMIT 1",
                [id],
                map_public_app_user_with_wallet,
            )
            .optional()
    }

    pub fn find_app_user_by_username(&self, username: &str) -> Result<Option<AppUser>> {
        self.conn
            .query_row(
                "SELECT id, username, display_name, password_hash, role, status,
                        created_at, updated_at, last_login_at
                 FROM app_users
                 WHERE lower(username) = lower(?1)
                 LIMIT 1",
                [username],
                map_app_user,
            )
            .optional()
    }

    pub fn find_app_user_by_id(&self, id: &str) -> Result<Option<AppUser>> {
        self.conn
            .query_row(
                "SELECT id, username, display_name, password_hash, role, status,
                        created_at, updated_at, last_login_at
                 FROM app_users
                 WHERE id = ?1
                 LIMIT 1",
                [id],
                map_app_user,
            )
            .optional()
    }

    pub fn list_app_user_access_summaries_for_ids(
        &self,
        user_ids: &[String],
    ) -> Result<Vec<AppUserAccessSummary>> {
        let user_ids = normalize_text_ids(user_ids);
        if user_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut users = Vec::new();
        for chunk in user_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            let Some((condition, params)) = text_id_in_clause("id", chunk) else {
                continue;
            };
            let sql = format!(
                "SELECT id, username, role, status
                 FROM app_users
                 WHERE {condition}"
            );
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(params), map_app_user_access_summary)?;
            for row in rows {
                users.push(row?);
            }
        }
        users.sort_by(|left, right| left.id.cmp(&right.id));
        Ok(users)
    }

    pub fn find_app_user_access_summary_by_id(
        &self,
        id: &str,
    ) -> Result<Option<AppUserAccessSummary>> {
        self.conn
            .query_row(
                "SELECT id, username, role, status
                 FROM app_users
                 WHERE id = ?1
                 LIMIT 1",
                [id],
                map_app_user_access_summary,
            )
            .optional()
    }

    pub fn app_user_exists(&self, id: &str) -> Result<bool> {
        self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM app_users WHERE id = ?1)",
            [id],
            |row| row.get(0),
        )
    }

    pub fn app_username_exists(&self, username: &str) -> Result<bool> {
        self.conn.query_row(
            "SELECT EXISTS(SELECT 1 FROM app_users WHERE lower(username) = lower(?1))",
            [username],
            |row| row.get(0),
        )
    }

    pub fn update_app_user_last_login(&self, id: &str, ts: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE app_users SET last_login_at = ?1, updated_at = ?1 WHERE id = ?2",
            (ts, id),
        )?;
        Ok(())
    }

    pub fn update_app_user_status(&self, id: &str, status: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE app_users SET status = ?1, updated_at = ?2 WHERE id = ?3",
            (status, now_ts(), id),
        )?;
        Ok(())
    }

    pub fn update_app_user_role(&self, id: &str, role: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE app_users SET role = ?1, updated_at = ?2 WHERE id = ?3",
            (role, now_ts(), id),
        )?;
        Ok(())
    }

    pub fn update_app_user_display_name(
        &self,
        id: &str,
        display_name: Option<String>,
    ) -> Result<()> {
        self.conn.execute(
            "UPDATE app_users SET display_name = ?1, updated_at = ?2 WHERE id = ?3",
            (display_name, now_ts(), id),
        )?;
        Ok(())
    }

    pub fn update_app_user_password_hash(&self, id: &str, password_hash: &str) -> Result<()> {
        self.conn.execute(
            "UPDATE app_users SET password_hash = ?1, updated_at = ?2 WHERE id = ?3",
            (password_hash, now_ts(), id),
        )?;
        Ok(())
    }

    pub fn insert_app_user_session(&self, session: &AppUserSession) -> Result<()> {
        self.conn.execute(
            "INSERT INTO app_user_sessions (
                id, user_id, token_hash, expires_at, created_at, last_seen_at, revoked_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            (
                &session.id,
                &session.user_id,
                &session.token_hash,
                session.expires_at,
                session.created_at,
                session.last_seen_at,
                session.revoked_at,
            ),
        )?;
        Ok(())
    }

    pub fn find_active_app_session_by_token_hash(
        &self,
        token_hash: &str,
        now: i64,
    ) -> Result<Option<AppUserSession>> {
        self.conn
            .query_row(
                "SELECT id, user_id, token_hash, expires_at, created_at, last_seen_at, revoked_at
                 FROM app_user_sessions
                 WHERE token_hash = ?1 AND revoked_at IS NULL AND expires_at > ?2
                 LIMIT 1",
                (token_hash, now),
                map_app_session,
            )
            .optional()
    }

    pub fn find_active_app_session_user_by_token_hash(
        &self,
        token_hash: &str,
        now: i64,
    ) -> Result<Option<AppSessionUserWithWallet>> {
        self.conn
            .query_row(
                "SELECT
                    s.id,
                    s.expires_at,
                    u.id,
                    u.username,
                    u.display_name,
                    u.role,
                    u.status,
                    u.created_at,
                    u.updated_at,
                    u.last_login_at,
                    w.id,
                    w.owner_kind,
                    w.owner_id,
                    w.balance_credit_micros,
                    w.frozen_credit_micros,
                    w.status,
                    w.created_at,
                    w.updated_at
                 FROM app_user_sessions s
                 INNER JOIN app_users u ON u.id = s.user_id
                 LEFT JOIN app_wallets w
                   ON u.role <> 'admin'
                  AND w.owner_kind = 'user'
                  AND w.owner_id = u.id
                 WHERE s.token_hash = ?1
                   AND s.revoked_at IS NULL
                   AND s.expires_at > ?2
                   AND u.status = 'active'
                 LIMIT 1",
                (token_hash, now),
                map_app_session_user_with_wallet,
            )
            .optional()
    }

    pub fn touch_app_user_session(&self, session_id: &str, ts: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE app_user_sessions SET last_seen_at = ?1 WHERE id = ?2",
            (ts, session_id),
        )?;
        Ok(())
    }

    pub fn revoke_app_user_session_by_token_hash(&self, token_hash: &str, ts: i64) -> Result<()> {
        self.conn.execute(
            "UPDATE app_user_sessions
             SET revoked_at = ?1
             WHERE token_hash = ?2 AND revoked_at IS NULL",
            (ts, token_hash),
        )?;
        Ok(())
    }

    pub fn insert_app_project(&self, project: &AppProject) -> Result<()> {
        self.conn.execute(
            "INSERT INTO app_projects (
                id, name, owner_user_id, status, created_at, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            (
                &project.id,
                &project.name,
                &project.owner_user_id,
                &project.status,
                project.created_at,
                project.updated_at,
            ),
        )?;
        Ok(())
    }

    pub fn ensure_wallet_for_owner(
        &self,
        id: &str,
        owner_kind: &str,
        owner_id: &str,
    ) -> Result<AppWallet> {
        if let Some(wallet) = self.find_wallet_by_owner(owner_kind, owner_id)? {
            return Ok(wallet);
        }
        let now = now_ts();
        self.conn.execute(
            "INSERT INTO app_wallets (
                id, owner_kind, owner_id, balance_credit_micros, frozen_credit_micros,
                status, created_at, updated_at
             ) VALUES (?1, ?2, ?3, 0, 0, 'active', ?4, ?4)",
            (id, owner_kind, owner_id, now),
        )?;
        self.find_wallet_by_owner(owner_kind, owner_id)?
            .ok_or_else(|| rusqlite::Error::QueryReturnedNoRows)
    }

    pub fn find_wallet_by_owner(
        &self,
        owner_kind: &str,
        owner_id: &str,
    ) -> Result<Option<AppWallet>> {
        self.conn
            .query_row(
                "SELECT id, owner_kind, owner_id, balance_credit_micros, frozen_credit_micros,
                        status, created_at, updated_at
                 FROM app_wallets
                 WHERE owner_kind = ?1 AND owner_id = ?2
                 LIMIT 1",
                (owner_kind, owner_id),
                map_app_wallet,
            )
            .optional()
    }

    pub fn user_wallets_for_users(&self, user_ids: &[String]) -> Result<Vec<AppWallet>> {
        let user_ids = normalize_text_ids(user_ids);
        if user_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut wallets = Vec::new();
        for chunk in user_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            let Some((condition, params)) = text_id_in_clause("owner_id", chunk) else {
                continue;
            };
            let sql = format!(
                "SELECT id, owner_kind, owner_id, balance_credit_micros, frozen_credit_micros,
                        status, created_at, updated_at
                 FROM app_wallets
                 WHERE owner_kind = 'user'
                   AND {condition}"
            );
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(params), map_app_wallet)?;
            for row in rows {
                wallets.push(row?);
            }
        }
        Ok(wallets)
    }

    pub fn list_wallets(&self) -> Result<Vec<AppWallet>> {
        let mut stmt = self.conn.prepare(
            "SELECT id, owner_kind, owner_id, balance_credit_micros, frozen_credit_micros,
                    status, created_at, updated_at
             FROM app_wallets
             ORDER BY created_at ASC",
        )?;
        let rows = stmt.query_map([], map_app_wallet)?;
        rows.collect()
    }

    pub fn user_wallet_available_credit_micros(&self) -> Result<Vec<(String, i64)>> {
        let mut stmt = self.conn.prepare(
            "SELECT owner_id, balance_credit_micros - frozen_credit_micros
             FROM app_wallets
             WHERE owner_kind = 'user'",
        )?;
        let rows = stmt.query_map([], |row| Ok((row.get(0)?, row.get(1)?)))?;
        rows.collect()
    }

    pub fn nonzero_wallet_count(&self) -> Result<i64> {
        self.conn.query_row(
            "SELECT COUNT(*)
             FROM app_wallets
             WHERE balance_credit_micros <> 0 OR frozen_credit_micros <> 0",
            [],
            |row| row.get(0),
        )
    }

    pub fn wallet_ledger_entry_count(&self) -> Result<i64> {
        self.conn.query_row(
            "SELECT COUNT(*) FROM app_wallet_ledger_entries",
            [],
            |row| row.get(0),
        )
    }

    pub fn request_charge_ledger_entry_count(&self) -> Result<i64> {
        self.conn.query_row(
            "SELECT COUNT(*) FROM app_wallet_ledger_entries WHERE entry_kind = 'request_charge'",
            [],
            |row| row.get(0),
        )
    }

    pub fn adjust_wallet_balance(
        &self,
        ledger: &AppWalletLedgerEntry,
    ) -> Result<AppWalletLedgerEntry> {
        let tx = self.conn.unchecked_transaction()?;
        let balance_after = tx.query_row(
            "SELECT balance_credit_micros + ?2
             FROM app_wallets
             WHERE id = ?1 AND status = 'active'",
            (&ledger.wallet_id, ledger.amount_credit_micros),
            |row| row.get::<_, i64>(0),
        )?;
        tx.execute(
            "UPDATE app_wallets
             SET balance_credit_micros = balance_credit_micros + ?2, updated_at = ?3
             WHERE id = ?1",
            (
                &ledger.wallet_id,
                ledger.amount_credit_micros,
                ledger.created_at,
            ),
        )?;
        tx.execute(
            "INSERT INTO app_wallet_ledger_entries (
                id, wallet_id, entry_kind, amount_credit_micros, balance_after_credit_micros,
                request_log_id, api_key_id, pricing_rule_id, raw_usage_json, note,
                created_by_user_id, created_at
             ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12)",
            (
                &ledger.id,
                &ledger.wallet_id,
                &ledger.entry_kind,
                ledger.amount_credit_micros,
                balance_after,
                ledger.request_log_id,
                &ledger.api_key_id,
                &ledger.pricing_rule_id,
                &ledger.raw_usage_json,
                &ledger.note,
                &ledger.created_by_user_id,
                ledger.created_at,
            ),
        )?;
        tx.commit()?;
        let mut next = ledger.clone();
        next.balance_after_credit_micros = balance_after;
        Ok(next)
    }

    pub fn upsert_api_key_owner(&self, owner: &ApiKeyOwner) -> Result<()> {
        self.conn.execute(
            "INSERT INTO api_key_owners (
                key_id, owner_kind, owner_user_id, project_id, updated_at
             ) VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(key_id) DO UPDATE SET
                owner_kind = excluded.owner_kind,
                owner_user_id = excluded.owner_user_id,
                project_id = excluded.project_id,
                updated_at = excluded.updated_at",
            (
                &owner.key_id,
                &owner.owner_kind,
                &owner.owner_user_id,
                &owner.project_id,
                owner.updated_at,
            ),
        )?;
        Ok(())
    }

    pub fn find_api_key_owner(&self, key_id: &str) -> Result<Option<ApiKeyOwner>> {
        self.conn
            .query_row(
                "SELECT key_id, owner_kind, owner_user_id, project_id, updated_at
                 FROM api_key_owners
                 WHERE key_id = ?1
                 LIMIT 1",
                [key_id],
                map_api_key_owner,
            )
            .optional()
    }

    pub fn list_api_key_owners_for_ids(&self, key_ids: &[String]) -> Result<Vec<ApiKeyOwner>> {
        let key_ids = normalize_text_ids(key_ids);
        if key_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut owners = Vec::new();
        for chunk in key_ids.chunks(SQLITE_IN_CLAUSE_BATCH_SIZE) {
            let Some((condition, params)) = text_id_in_clause("key_id", chunk) else {
                continue;
            };
            let sql = format!(
                "SELECT key_id, owner_kind, owner_user_id, project_id, updated_at
                 FROM api_key_owners
                 WHERE {condition}"
            );
            let mut stmt = self.conn.prepare(&sql)?;
            let rows = stmt.query_map(params_from_iter(params), map_api_key_owner)?;
            for row in rows {
                owners.push(row?);
            }
        }
        owners.sort_by(|left, right| left.key_id.cmp(&right.key_id));
        Ok(owners)
    }

    pub fn list_api_key_owners(&self) -> Result<HashMap<String, ApiKeyOwner>> {
        let mut stmt = self.conn.prepare(
            "SELECT key_id, owner_kind, owner_user_id, project_id, updated_at
             FROM api_key_owners",
        )?;
        let rows = stmt.query_map([], map_api_key_owner)?;
        let mut out = HashMap::new();
        for row in rows {
            let owner = row?;
            out.insert(owner.key_id.clone(), owner);
        }
        Ok(out)
    }

    pub fn list_api_key_owner_rows(&self) -> Result<Vec<ApiKeyOwner>> {
        let mut stmt = self.conn.prepare(
            "SELECT key_id, owner_kind, owner_user_id, project_id, updated_at
             FROM api_key_owners
             ORDER BY key_id ASC",
        )?;
        let rows = stmt.query_map([], map_api_key_owner)?;
        rows.collect()
    }

    /// 函数 `list_api_key_ids_for_user`
    ///
    ///
    /// 时间: 2026-05-28
    ///
    /// # 参数
    /// - self: 参数 self
    /// - user_id: 参数 user_id
    ///
    /// # 返回
    /// 返回函数执行结果
    pub fn list_api_key_ids_for_user(&self, user_id: &str) -> Result<Vec<String>> {
        let normalized_user_id = user_id.trim();
        if normalized_user_id.is_empty() {
            return Ok(Vec::new());
        }

        // 中文注释：这里直接按 owner_user_id 走索引取 key_id，避免把整张 api_key_owners 表拉回 Rust 再过滤。
        let mut stmt = self.conn.prepare(
            "SELECT key_id
             FROM api_key_owners
             WHERE owner_kind = 'user' AND owner_user_id = ?1
             ORDER BY key_id ASC",
        )?;
        let rows = stmt.query_map([normalized_user_id], |row| row.get(0))?;
        rows.collect()
    }

    pub fn api_key_owner_count(&self) -> Result<i64> {
        self.conn
            .query_row("SELECT COUNT(*) FROM api_key_owners", [], |row| row.get(0))
    }

    pub fn list_billing_rules(&self) -> Result<Vec<BillingRule>> {
        let mut stmt = self.conn.prepare(
            "SELECT
                id, name, status, priority, multiplier_millis, model_pattern, service_tier,
                user_id, project_id, api_key_id, starts_at, ends_at, created_at, updated_at
             FROM billing_rules
             ORDER BY status ASC, priority DESC, updated_at DESC, name ASC",
        )?;
        let rows = stmt.query_map([], map_billing_rule)?;
        rows.collect()
    }

    pub fn list_active_billing_rules(&self, now: i64) -> Result<Vec<BillingRule>> {
        let mut stmt = self.conn.prepare(
            "SELECT
                id, name, status, priority, multiplier_millis, model_pattern, service_tier,
                user_id, project_id, api_key_id, starts_at, ends_at, created_at, updated_at
             FROM billing_rules
             WHERE status = 'active'
               AND (starts_at IS NULL OR starts_at <= ?1)
               AND (ends_at IS NULL OR ends_at > ?1)
             ORDER BY priority DESC, updated_at DESC, name ASC",
        )?;
        let rows = stmt.query_map([now], map_billing_rule)?;
        rows.collect()
    }

    pub fn list_active_billing_rules_for_context(
        &self,
        now: i64,
        key_id: &str,
        user_id: Option<&str>,
        project_id: Option<&str>,
        service_tier: Option<&str>,
    ) -> Result<Vec<BillingRule>> {
        let normalized_key_id = key_id.trim();
        if normalized_key_id.is_empty() {
            return Ok(Vec::new());
        }
        let normalized_user_id = normalize_optional_context_text(user_id);
        let normalized_project_id = normalize_optional_context_text(project_id);
        let normalized_service_tier = normalize_optional_context_text(service_tier);
        let mut stmt = self.conn.prepare(
            "SELECT
                id, name, status, priority, multiplier_millis, model_pattern, service_tier,
                user_id, project_id, api_key_id, starts_at, ends_at, created_at, updated_at
             FROM billing_rules
             WHERE status = 'active'
               AND (starts_at IS NULL OR starts_at <= ?1)
               AND (ends_at IS NULL OR ends_at > ?1)
               AND (api_key_id IS NULL OR TRIM(api_key_id) = '' OR api_key_id = ?2)
               AND (user_id IS NULL OR TRIM(user_id) = '' OR user_id = ?3)
               AND (project_id IS NULL OR TRIM(project_id) = '' OR project_id = ?4)
               AND (
                    service_tier IS NULL
                    OR TRIM(service_tier) = ''
                    OR LOWER(TRIM(service_tier)) = LOWER(TRIM(?5))
               )
             ORDER BY priority DESC, updated_at DESC, name ASC",
        )?;
        let rows = stmt.query_map(
            (
                now,
                normalized_key_id,
                normalized_user_id.as_deref().unwrap_or(""),
                normalized_project_id.as_deref().unwrap_or(""),
                normalized_service_tier.as_deref().unwrap_or(""),
            ),
            map_billing_rule,
        )?;
        rows.collect()
    }

    pub fn list_active_billing_rules_for_request_candidate(
        &self,
        now: i64,
        key_id: &str,
        user_id: Option<&str>,
        project_id: Option<&str>,
        service_tier: Option<&str>,
        model: Option<&str>,
    ) -> Result<Vec<BillingRule>> {
        let normalized_key_id = key_id.trim();
        if normalized_key_id.is_empty() {
            return Ok(Vec::new());
        }
        let normalized_user_id = normalize_optional_context_text(user_id);
        let normalized_project_id = normalize_optional_context_text(project_id);
        let normalized_service_tier = normalize_optional_context_text(service_tier);
        let normalized_model = normalize_optional_context_text(model)
            .filter(|value| !value.eq_ignore_ascii_case("unknown"));
        let mut stmt = self.conn.prepare(
            "SELECT
                id, name, status, priority, multiplier_millis, model_pattern, service_tier,
                user_id, project_id, api_key_id, starts_at, ends_at, created_at, updated_at
             FROM billing_rules
             WHERE status = 'active'
               AND (starts_at IS NULL OR starts_at <= ?1)
               AND (ends_at IS NULL OR ends_at > ?1)
               AND (api_key_id IS NULL OR TRIM(api_key_id) = '' OR api_key_id = ?2)
               AND (user_id IS NULL OR TRIM(user_id) = '' OR user_id = ?3)
               AND (project_id IS NULL OR TRIM(project_id) = '' OR project_id = ?4)
               AND (
                    service_tier IS NULL
                    OR TRIM(service_tier) = ''
                    OR LOWER(TRIM(service_tier)) = LOWER(TRIM(?5))
               )
               AND (
                    model_pattern IS NULL
                    OR TRIM(model_pattern) = ''
                    OR TRIM(model_pattern) = '*'
                    OR (
                        ?6 <> ''
                        AND (
                            INSTR(model_pattern, '*') > 0
                            OR LOWER(TRIM(model_pattern)) = LOWER(TRIM(?6))
                            OR LOWER(TRIM(?6)) LIKE LOWER(TRIM(model_pattern)) || '%'
                        )
                    )
               )
             ORDER BY
                priority DESC,
                (
                    CASE WHEN api_key_id IS NOT NULL AND TRIM(api_key_id) <> '' THEN 1 ELSE 0 END
                  + CASE WHEN user_id IS NOT NULL AND TRIM(user_id) <> '' THEN 1 ELSE 0 END
                  + CASE WHEN project_id IS NOT NULL AND TRIM(project_id) <> '' THEN 1 ELSE 0 END
                  + CASE WHEN service_tier IS NOT NULL AND TRIM(service_tier) <> '' THEN 1 ELSE 0 END
                  + CASE WHEN model_pattern IS NOT NULL AND TRIM(model_pattern) <> '' THEN 1 ELSE 0 END
                ) DESC,
                LENGTH(IFNULL(model_pattern, '')) DESC,
                updated_at DESC",
        )?;
        let rows = stmt.query_map(
            (
                now,
                normalized_key_id,
                normalized_user_id.as_deref().unwrap_or(""),
                normalized_project_id.as_deref().unwrap_or(""),
                normalized_service_tier.as_deref().unwrap_or(""),
                normalized_model.as_deref().unwrap_or(""),
            ),
            map_billing_rule,
        )?;
        rows.collect()
    }

    pub fn upsert_billing_rule(&self, rule: &BillingRule) -> Result<()> {
        self.conn.execute(
            "INSERT INTO billing_rules (
                id, name, status, priority, multiplier_millis, model_pattern, service_tier,
                user_id, project_id, api_key_id, starts_at, ends_at, created_at, updated_at
             ) VALUES (
                ?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14
             )
             ON CONFLICT(id) DO UPDATE SET
                name = excluded.name,
                status = excluded.status,
                priority = excluded.priority,
                multiplier_millis = excluded.multiplier_millis,
                model_pattern = excluded.model_pattern,
                service_tier = excluded.service_tier,
                user_id = excluded.user_id,
                project_id = excluded.project_id,
                api_key_id = excluded.api_key_id,
                starts_at = excluded.starts_at,
                ends_at = excluded.ends_at,
                updated_at = excluded.updated_at",
            params![
                &rule.id,
                &rule.name,
                &rule.status,
                rule.priority,
                rule.multiplier_millis,
                &rule.model_pattern,
                &rule.service_tier,
                &rule.user_id,
                &rule.project_id,
                &rule.api_key_id,
                rule.starts_at,
                rule.ends_at,
                rule.created_at,
                rule.updated_at,
            ],
        )?;
        Ok(())
    }

    pub fn delete_billing_rule(&self, id: &str) -> Result<()> {
        self.conn
            .execute("DELETE FROM billing_rules WHERE id = ?1", [id])?;
        Ok(())
    }

    pub(super) fn ensure_account_manager_tables(&self) -> Result<()> {
        self.conn
            .execute_batch(include_str!("../../migrations/057_account_manager.sql"))?;
        self.conn.execute_batch(include_str!(
            "../../migrations/102_app_users_list_order_index.sql"
        ))?;
        Ok(())
    }
}

fn normalize_optional_context_text(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::super::Storage;
    use super::super::{ApiKey, ApiKeyOwner, BillingRule};

    fn seed_api_key(storage: &Storage, key_id: &str) {
        storage
            .insert_api_key(&ApiKey {
                id: key_id.to_string(),
                name: Some(key_id.to_string()),
                model_slug: None,
                reasoning_effort: None,
                service_tier: None,
                rotation_strategy: "account_rotation".to_string(),
                aggregate_api_id: None,
                account_plan_filter: None,
                aggregate_api_url: None,
                client_type: "codex".to_string(),
                protocol_type: "openai_compat".to_string(),
                auth_scheme: "authorization_bearer".to_string(),
                upstream_base_url: None,
                static_headers_json: None,
                key_hash: format!("hash-{key_id}"),
                status: "active".to_string(),
                created_at: 1,
                last_used_at: None,
            })
            .expect("seed api key");
    }

    fn seed_app_user(storage: &Storage, user_id: &str) {
        storage
            .conn
            .execute(
                "INSERT INTO app_users (
                    id, username, display_name, password_hash, role, status,
                    created_at, updated_at, last_login_at
                 ) VALUES (?1, ?2, NULL, 'hash', 'member', 'active', 1, 1, NULL)",
                (user_id, format!("{user_id}@example.com")),
            )
            .expect("seed app user");
    }

    fn seed_app_project(storage: &Storage, project_id: &str, owner_user_id: &str) {
        storage
            .conn
            .execute(
                "INSERT INTO app_projects (
                    id, name, owner_user_id, status, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, 'active', 1, 1)",
                (project_id, project_id, owner_user_id),
            )
            .expect("seed app project");
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

    #[test]
    fn active_billing_rules_query_uses_order_index() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let mut stmt = storage
            .conn
            .prepare(
                "EXPLAIN QUERY PLAN
                 SELECT id
                 FROM billing_rules
                 WHERE status = 'active'
                   AND (starts_at IS NULL OR starts_at <= ?1)
                   AND (ends_at IS NULL OR ends_at > ?1)
                 ORDER BY priority DESC, updated_at DESC, name ASC",
            )
            .expect("prepare explain");
        let mut rows = stmt.query([100_i64]).expect("query explain");
        let mut plan = String::new();
        while let Some(row) = rows.next().expect("read explain row") {
            let detail: String = row.get(3).expect("plan detail");
            plan.push_str(&detail);
            plan.push('\n');
        }

        assert!(
            plan.contains("idx_billing_rules_active_order"),
            "expected active billing rules order index in plan, got {plan}"
        );
    }

    fn billing_rule(id: &str) -> BillingRule {
        BillingRule {
            id: id.to_string(),
            name: id.to_string(),
            status: "active".to_string(),
            priority: 1,
            multiplier_millis: 1000,
            model_pattern: None,
            service_tier: None,
            user_id: None,
            project_id: None,
            api_key_id: None,
            starts_at: None,
            ends_at: None,
            created_at: 1,
            updated_at: 1,
        }
    }

    #[test]
    fn active_billing_rules_for_context_filters_scope_columns_in_sql() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        storage
            .conn
            .execute(
                "INSERT INTO api_keys (id, name, key_hash, status, created_at, last_used_at)
                 VALUES
                    ('key-1', 'Key 1', 'hash-1', 'enabled', 1, NULL),
                    ('key-2', 'Key 2', 'hash-2', 'enabled', 1, NULL)",
                [],
            )
            .expect("seed api keys");
        storage
            .conn
            .execute(
                "INSERT INTO app_users (
                    id, username, display_name, password_hash, role, status,
                    created_at, updated_at, last_login_at
                 ) VALUES
                    ('user-1', 'user-1@example.com', NULL, 'hash', 'member', 'active', 1, 1, NULL),
                    ('user-2', 'user-2@example.com', NULL, 'hash', 'member', 'active', 1, 1, NULL)",
                [],
            )
            .expect("seed users");
        storage
            .conn
            .execute(
                "INSERT INTO app_projects (id, name, owner_user_id, status, created_at, updated_at)
                 VALUES
                    ('project-1', 'Project 1', 'user-1', 'active', 1, 1),
                    ('project-2', 'Project 2', 'user-2', 'active', 1, 1)",
                [],
            )
            .expect("seed projects");

        let global = billing_rule("global");
        let mut matching = billing_rule("matching");
        matching.api_key_id = Some("key-1".to_string());
        matching.user_id = Some("user-1".to_string());
        matching.project_id = Some("project-1".to_string());
        matching.service_tier = Some("Premium".to_string());
        matching.priority = 10;
        let mut wrong_key = billing_rule("wrong-key");
        wrong_key.api_key_id = Some("key-2".to_string());
        let mut wrong_user = billing_rule("wrong-user");
        wrong_user.user_id = Some("user-2".to_string());
        let mut wrong_project = billing_rule("wrong-project");
        wrong_project.project_id = Some("project-2".to_string());
        let mut wrong_tier = billing_rule("wrong-tier");
        wrong_tier.service_tier = Some("standard".to_string());
        let mut expired = billing_rule("expired");
        expired.ends_at = Some(50);
        let mut disabled = billing_rule("disabled");
        disabled.status = "disabled".to_string();

        for rule in [
            &global,
            &matching,
            &wrong_key,
            &wrong_user,
            &wrong_project,
            &wrong_tier,
            &expired,
            &disabled,
        ] {
            storage.upsert_billing_rule(rule).expect("insert rule");
        }

        let rules = storage
            .list_active_billing_rules_for_context(
                100,
                "key-1",
                Some("user-1"),
                Some("project-1"),
                Some("premium"),
            )
            .expect("list context rules");
        let ids = rules.into_iter().map(|rule| rule.id).collect::<Vec<_>>();

        assert_eq!(ids, vec!["matching".to_string(), "global".to_string()]);
    }

    #[test]
    fn active_billing_rules_for_request_candidates_prefilter_model_rules() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let global = billing_rule("global");
        let mut exact = billing_rule("exact");
        exact.model_pattern = Some("gpt-4.1".to_string());
        exact.priority = 8;
        exact.updated_at = 8;
        let mut prefix = billing_rule("prefix");
        prefix.model_pattern = Some("gpt-4".to_string());
        prefix.priority = 7;
        prefix.updated_at = 7;
        let mut wildcard = billing_rule("wildcard");
        wildcard.model_pattern = Some("gpt-*".to_string());
        wildcard.priority = 6;
        wildcard.updated_at = 6;
        let mut unrelated = billing_rule("unrelated");
        unrelated.model_pattern = Some("claude".to_string());
        unrelated.priority = 20;
        unrelated.updated_at = 20;

        for rule in [&global, &exact, &prefix, &wildcard, &unrelated] {
            storage.upsert_billing_rule(rule).expect("insert rule");
        }

        let rules = storage
            .list_active_billing_rules_for_request_candidate(
                100,
                "key-1",
                None,
                None,
                None,
                Some("gpt-4.1"),
            )
            .expect("list request candidates");
        let ids = rules.into_iter().map(|rule| rule.id).collect::<Vec<_>>();

        assert_eq!(
            ids,
            vec![
                "exact".to_string(),
                "prefix".to_string(),
                "wildcard".to_string(),
                "global".to_string()
            ]
        );
    }

    #[test]
    fn username_lookup_uses_lower_username_index() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let plan = storage
            .conn
            .query_row(
                "EXPLAIN QUERY PLAN
                 SELECT id
                 FROM app_users
                 WHERE lower(username) = lower(?1)
                 LIMIT 1",
                ["member@example.com"],
                |row| row.get::<_, String>(3),
            )
            .expect("explain plan");

        assert!(
            plan.contains("idx_app_users_lower_username"),
            "expected lower username index in plan, got {plan}"
        );
    }

    #[test]
    fn user_api_key_lookup_uses_owner_key_index() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let plan = storage
            .conn
            .query_row(
                "EXPLAIN QUERY PLAN
                 SELECT key_id
                 FROM api_key_owners
                 WHERE owner_kind = 'user' AND owner_user_id = ?1
                 ORDER BY key_id ASC",
                ["user-1"],
                |row| row.get::<_, String>(3),
            )
            .expect("explain plan");

        assert!(
            plan.contains("idx_api_key_owners_user_key_lookup"),
            "expected user key owner lookup index in plan, got {plan}"
        );
    }

    #[test]
    fn app_user_lists_use_list_order_index() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let plan = collect_query_plan(
            &storage,
            "EXPLAIN QUERY PLAN
             SELECT id, username, display_name, password_hash, role, status,
                    created_at, updated_at, last_login_at
             FROM app_users
             ORDER BY created_at ASC, username ASC",
        );

        assert!(
            plan.contains("idx_app_users_list_order"),
            "expected app user list query to use idx_app_users_list_order, got {plan}"
        );
        assert!(
            !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "app user list query should avoid temp sorting, got {plan}"
        );
    }

    #[test]
    fn account_manager_chunk_queries_defer_final_ordering_to_rust() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let dashboard_users_plan = collect_query_plan(
            &storage,
            "EXPLAIN QUERY PLAN
             SELECT
                u.id,
                u.username,
                u.display_name,
                u.role,
                u.status
             FROM app_users u
             WHERE u.id IN ('user-a', 'user-b')",
        );
        let access_users_plan = collect_query_plan(
            &storage,
            "EXPLAIN QUERY PLAN
             SELECT id, username, role, status
             FROM app_users
             WHERE id IN ('user-a', 'user-b')",
        );
        let api_key_owners_plan = collect_query_plan(
            &storage,
            "EXPLAIN QUERY PLAN
             SELECT key_id, owner_kind, owner_user_id, project_id, updated_at
             FROM api_key_owners
             WHERE key_id IN ('key-a', 'key-b')",
        );

        assert!(
            !dashboard_users_plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "dashboard user chunk query should avoid per-chunk ORDER BY temp sorting, got {dashboard_users_plan}"
        );
        assert!(
            !access_users_plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "access user chunk query should avoid per-chunk ORDER BY temp sorting, got {access_users_plan}"
        );
        assert!(
            !api_key_owners_plan.contains("USE TEMP B-TREE FOR ORDER BY"),
            "API key owner chunk query should avoid per-chunk ORDER BY temp sorting, got {api_key_owners_plan}"
        );
    }

    #[test]
    fn api_key_owner_rows_return_key_ordered_rows() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        storage
            .conn
            .execute(
                "INSERT INTO api_keys (id, name, key_hash, status, created_at, last_used_at)
                 VALUES
                    ('key-a', 'Key A', 'hash-a', 'enabled', 1, NULL),
                    ('key-b', 'Key B', 'hash-b', 'enabled', 2, NULL)",
                [],
            )
            .expect("seed api keys");
        storage
            .conn
            .execute(
                "INSERT INTO app_users (
                    id, username, display_name, password_hash, role, status,
                    created_at, updated_at, last_login_at
                 ) VALUES ('user-b', 'user-b@example.com', NULL, 'hash', 'member', 'active', 1, 1, NULL)",
                [],
            )
            .expect("seed user");
        storage
            .conn
            .execute(
                "INSERT INTO app_projects (id, name, owner_user_id, status, created_at, updated_at)
                 VALUES ('project-a', 'Project A', NULL, 'active', 1, 1)",
                [],
            )
            .expect("seed project");
        storage
            .conn
            .execute(
                "INSERT INTO api_key_owners (
                    key_id, owner_kind, owner_user_id, project_id, updated_at
                 ) VALUES
                    ('key-b', 'user', 'user-b', NULL, 2),
                    ('key-a', 'project', NULL, 'project-a', 1)",
                [],
            )
            .expect("seed owners");

        let owners = storage.list_api_key_owner_rows().expect("read owner rows");

        assert_eq!(owners.len(), 2);
        assert_eq!(owners[0].key_id, "key-a");
        assert_eq!(owners[0].owner_kind, "project");
        assert_eq!(owners[0].project_id.as_deref(), Some("project-a"));
        assert_eq!(owners[1].key_id, "key-b");
        assert_eq!(owners[1].owner_kind, "user");
        assert_eq!(owners[1].owner_user_id.as_deref(), Some("user-b"));
    }

    #[test]
    fn request_charge_count_uses_entry_kind_index() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let plan = storage
            .conn
            .query_row(
                "EXPLAIN QUERY PLAN
                 SELECT COUNT(*)
                 FROM app_wallet_ledger_entries
                 WHERE entry_kind = 'request_charge'",
                [],
                |row| row.get::<_, String>(3),
            )
            .expect("explain plan");

        assert!(
            plan.contains("idx_app_wallet_ledger_entry_kind"),
            "expected wallet ledger entry kind index in plan, got {plan}"
        );
    }

    #[test]
    fn user_wallet_available_credit_filters_to_user_wallets() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        storage
            .conn
            .execute(
                "INSERT INTO app_wallets (
                    id, owner_kind, owner_id, balance_credit_micros, frozen_credit_micros,
                    status, created_at, updated_at
                 ) VALUES
                    ('wallet-user-1', 'user', 'user-1', 1000, 250, 'active', 1, 1),
                    ('wallet-project-1', 'project', 'project-1', 5000, 0, 'active', 1, 1)",
                [],
            )
            .expect("seed wallets");

        let balances = storage
            .user_wallet_available_credit_micros()
            .expect("read user wallet balances");

        assert_eq!(balances, vec![("user-1".to_string(), 750)]);
    }

    #[test]
    fn dashboard_app_user_summaries_join_user_wallets() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        storage
            .conn
            .execute(
                "INSERT INTO app_users (
                    id, username, display_name, password_hash, role, status,
                    created_at, updated_at, last_login_at
                 ) VALUES
                    ('user-1', 'member@example.com', 'Member', 'secret-hash', 'member', 'active', 1, 1, NULL),
                    ('user-2', 'admin@example.com', NULL, 'admin-hash', 'admin', 'disabled', 2, 2, NULL)",
                [],
            )
            .expect("seed app users");
        storage
            .conn
            .execute(
                "INSERT INTO app_wallets (
                    id, owner_kind, owner_id, balance_credit_micros, frozen_credit_micros,
                    status, created_at, updated_at
                 ) VALUES
                    ('wallet-user-1', 'user', 'user-1', 1000, 250, 'active', 1, 1),
                    ('wallet-project-1', 'project', 'user-2', 5000, 0, 'active', 1, 1)",
                [],
            )
            .expect("seed wallets");

        let users = storage
            .list_dashboard_app_user_summaries()
            .expect("read dashboard users");

        assert_eq!(users.len(), 2);
        assert_eq!(users[0].id, "user-1");
        assert_eq!(users[0].username, "member@example.com");
        assert_eq!(users[0].display_name.as_deref(), Some("Member"));
        assert_eq!(users[0].role, "member");
        assert_eq!(users[0].status, "active");
        assert_eq!(users[0].wallet_available_credit_micros, Some(750));
        assert_eq!(users[1].id, "user-2");
        assert_eq!(users[1].wallet_available_credit_micros, None);
    }

    #[test]
    fn dashboard_app_user_summaries_for_ids_filters_requested_users() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        storage
            .conn
            .execute(
                "INSERT INTO app_users (
                    id, username, display_name, password_hash, role, status,
                    created_at, updated_at, last_login_at
                 ) VALUES
                    ('user-1', 'member@example.com', 'Member', 'secret-hash', 'member', 'active', 1, 1, NULL),
                    ('user-2', 'admin@example.com', NULL, 'admin-hash', 'admin', 'disabled', 2, 2, NULL),
                    ('user-3', 'unused@example.com', NULL, 'unused-hash', 'member', 'active', 3, 3, NULL)",
                [],
            )
            .expect("seed app users");
        storage
            .conn
            .execute(
                "INSERT INTO app_wallets (
                    id, owner_kind, owner_id, balance_credit_micros, frozen_credit_micros,
                    status, created_at, updated_at
                 ) VALUES
                    ('wallet-user-2', 'user', 'user-2', 2000, 500, 'active', 1, 1)",
                [],
            )
            .expect("seed wallets");

        let users = storage
            .list_dashboard_app_user_summaries_for_ids(&[
                "user-2".to_string(),
                "missing".to_string(),
                "user-1".to_string(),
                "user-2".to_string(),
            ])
            .expect("read dashboard users by ids");

        assert_eq!(users.len(), 2);
        assert_eq!(users[0].id, "user-1");
        assert_eq!(users[0].wallet_available_credit_micros, None);
        assert_eq!(users[1].id, "user-2");
        assert_eq!(users[1].wallet_available_credit_micros, Some(1500));
        assert!(!users.iter().any(|user| user.id == "user-3"));
    }

    #[test]
    fn app_user_access_summaries_for_ids_project_access_fields_only() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        storage
            .conn
            .execute(
                "INSERT INTO app_users (
                    id, username, display_name, password_hash, role, status,
                    created_at, updated_at, last_login_at
                 ) VALUES
                    ('user-1', 'member@example.com', 'Member', 'secret-hash', 'member', 'active', 1, 1, NULL),
                    ('user-2', 'admin@example.com', NULL, 'admin-hash', 'admin', 'disabled', 2, 2, NULL),
                    ('user-3', 'unused@example.com', NULL, 'unused-hash', 'member', 'active', 3, 3, NULL)",
                [],
            )
            .expect("seed app users");

        let users = storage
            .list_app_user_access_summaries_for_ids(&[
                "user-2".to_string(),
                "missing".to_string(),
                "user-1".to_string(),
                "user-2".to_string(),
                " ".to_string(),
            ])
            .expect("read access users by ids");

        assert_eq!(users.len(), 2);
        assert_eq!(users[0].id, "user-1");
        assert_eq!(users[0].username, "member@example.com");
        assert_eq!(users[0].role, "member");
        assert_eq!(users[0].status, "active");
        assert_eq!(users[1].id, "user-2");
        assert_eq!(users[1].username, "admin@example.com");
        assert_eq!(users[1].role, "admin");
        assert_eq!(users[1].status, "disabled");
        assert!(!users.iter().any(|user| user.id == "user-3"));
    }

    #[test]
    fn api_key_owners_for_ids_filters_requested_keys() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        for key_id in ["key-1", "key-2", "key-unused"] {
            seed_api_key(&storage, key_id);
        }
        seed_app_user(&storage, "user-1");
        seed_app_user(&storage, "user-unused");
        seed_app_user(&storage, "project-owner");
        seed_app_project(&storage, "project-1", "project-owner");

        for owner in [
            ApiKeyOwner {
                key_id: "key-1".to_string(),
                owner_kind: "user".to_string(),
                owner_user_id: Some("user-1".to_string()),
                project_id: None,
                updated_at: 1,
            },
            ApiKeyOwner {
                key_id: "key-2".to_string(),
                owner_kind: "project".to_string(),
                owner_user_id: None,
                project_id: Some("project-1".to_string()),
                updated_at: 2,
            },
            ApiKeyOwner {
                key_id: "key-unused".to_string(),
                owner_kind: "user".to_string(),
                owner_user_id: Some("user-unused".to_string()),
                project_id: None,
                updated_at: 3,
            },
        ] {
            storage
                .upsert_api_key_owner(&owner)
                .expect("seed api key owner");
        }

        let owners = storage
            .list_api_key_owners_for_ids(&[
                " key-2 ".to_string(),
                "missing".to_string(),
                "key-1".to_string(),
                "key-2".to_string(),
                " ".to_string(),
            ])
            .expect("read owners by ids");

        assert_eq!(
            owners
                .iter()
                .map(|owner| owner.key_id.as_str())
                .collect::<Vec<_>>(),
            vec!["key-1", "key-2"]
        );
        assert_eq!(owners[0].owner_user_id.as_deref(), Some("user-1"));
        assert_eq!(owners[1].project_id.as_deref(), Some("project-1"));
        assert!(!owners.iter().any(|owner| owner.key_id == "key-unused"));
    }

    #[test]
    fn api_key_owners_for_ids_chunks_large_key_sets() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        for index in 0..950 {
            let key_id = format!("key-{index:04}");
            let user_id = format!("user-{index:04}");
            seed_api_key(&storage, &key_id);
            seed_app_user(&storage, &user_id);
            let owner = ApiKeyOwner {
                key_id,
                owner_kind: "user".to_string(),
                owner_user_id: Some(user_id),
                project_id: None,
                updated_at: i64::from(index),
            };
            storage
                .upsert_api_key_owner(&owner)
                .expect("seed api key owner");
        }

        let key_ids = (0..950)
            .map(|index| format!("key-{index:04}"))
            .collect::<Vec<_>>();
        let owners = storage
            .list_api_key_owners_for_ids(&key_ids)
            .expect("read chunked owners");

        assert_eq!(owners.len(), 950);
        assert!(owners.iter().any(|owner| owner.key_id == "key-0949"));
    }

    #[test]
    fn app_user_access_summary_by_id_projects_access_fields_only() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        storage
            .conn
            .execute(
                "INSERT INTO app_users (
                    id, username, display_name, password_hash, role, status,
                    created_at, updated_at, last_login_at
                 ) VALUES
                    ('user-1', 'member@example.com', 'Member', 'secret-hash', 'member', 'active', 1, 1, NULL)",
                [],
            )
            .expect("seed app user");

        let user = storage
            .find_app_user_access_summary_by_id("user-1")
            .expect("read access user")
            .expect("user exists");
        assert_eq!(user.id, "user-1");
        assert_eq!(user.username, "member@example.com");
        assert_eq!(user.role, "member");
        assert_eq!(user.status, "active");

        assert!(storage
            .find_app_user_access_summary_by_id("missing")
            .expect("read missing access user")
            .is_none());
    }

    #[test]
    fn public_app_users_with_wallets_project_public_fields_without_password_hash() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        storage
            .conn
            .execute(
                "INSERT INTO app_users (
                    id, username, display_name, password_hash, role, status,
                    created_at, updated_at, last_login_at
                 ) VALUES
                    ('user-1', 'member@example.com', 'Member', 'secret-hash', 'member', 'active', 1, 2, 3),
                    ('user-2', 'admin@example.com', NULL, 'admin-hash', 'admin', 'active', 4, 5, NULL)",
                [],
            )
            .expect("seed app users");
        storage
            .conn
            .execute(
                "INSERT INTO app_wallets (
                    id, owner_kind, owner_id, balance_credit_micros, frozen_credit_micros,
                    status, created_at, updated_at
                 ) VALUES
                    ('wallet-user-1', 'user', 'user-1', 2000, 125, 'active', 6, 7),
                    ('wallet-admin-1', 'user', 'user-2', 9000, 0, 'active', 8, 9),
                    ('wallet-project-1', 'project', 'user-1', 5000, 0, 'active', 10, 11)",
                [],
            )
            .expect("seed wallets");

        let users = storage
            .list_public_app_users_with_wallets()
            .expect("read public users");

        assert_eq!(users.len(), 2);
        assert_eq!(users[0].id, "user-1");
        assert_eq!(users[0].username, "member@example.com");
        assert_eq!(users[0].created_at, 1);
        assert_eq!(users[0].updated_at, 2);
        assert_eq!(users[0].last_login_at, Some(3));
        assert_eq!(users[0].wallet_id.as_deref(), Some("wallet-user-1"));
        assert_eq!(users[0].wallet_owner_kind.as_deref(), Some("user"));
        assert_eq!(users[0].wallet_balance_credit_micros, Some(2000));
        assert_eq!(users[0].wallet_frozen_credit_micros, Some(125));
        assert_eq!(users[1].id, "user-2");
        assert_eq!(users[1].wallet_id, None);
    }

    #[test]
    fn public_app_user_with_wallet_by_id_filters_single_public_user() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        storage
            .conn
            .execute(
                "INSERT INTO app_users (
                    id, username, display_name, password_hash, role, status,
                    created_at, updated_at, last_login_at
                 ) VALUES
                    ('user-1', 'member@example.com', 'Member', 'secret-hash', 'member', 'active', 1, 2, 3),
                    ('user-2', 'admin@example.com', NULL, 'admin-hash', 'admin', 'active', 4, 5, NULL)",
                [],
            )
            .expect("seed app users");
        storage
            .conn
            .execute(
                "INSERT INTO app_wallets (
                    id, owner_kind, owner_id, balance_credit_micros, frozen_credit_micros,
                    status, created_at, updated_at
                 ) VALUES
                    ('wallet-user-1', 'user', 'user-1', 2000, 125, 'active', 6, 7),
                    ('wallet-admin-1', 'user', 'user-2', 9000, 0, 'active', 8, 9)",
                [],
            )
            .expect("seed wallets");

        let member = storage
            .find_public_app_user_with_wallet_by_id("user-1")
            .expect("read member public user")
            .expect("member exists");
        assert_eq!(member.username, "member@example.com");
        assert_eq!(member.wallet_id.as_deref(), Some("wallet-user-1"));
        assert_eq!(member.wallet_balance_credit_micros, Some(2000));
        assert_eq!(member.wallet_frozen_credit_micros, Some(125));

        let admin = storage
            .find_public_app_user_with_wallet_by_id("user-2")
            .expect("read admin public user")
            .expect("admin exists");
        assert_eq!(admin.username, "admin@example.com");
        assert_eq!(admin.wallet_id, None);

        assert!(storage
            .find_public_app_user_with_wallet_by_id("missing")
            .expect("read missing public user")
            .is_none());
    }

    #[test]
    fn active_app_session_user_by_token_hash_joins_public_user_and_wallet() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        storage
            .conn
            .execute(
                "INSERT INTO app_users (
                    id, username, display_name, password_hash, role, status,
                    created_at, updated_at, last_login_at
                 ) VALUES
                    ('user-active', 'member@example.com', 'Member', 'secret-hash', 'member', 'active', 1, 2, 3),
                    ('user-admin', 'admin@example.com', NULL, 'admin-hash', 'admin', 'active', 4, 5, NULL),
                    ('user-disabled', 'disabled@example.com', NULL, 'disabled-hash', 'member', 'disabled', 6, 7, NULL)",
                [],
            )
            .expect("seed app users");
        storage
            .conn
            .execute(
                "INSERT INTO app_user_sessions (
                    id, user_id, token_hash, expires_at, created_at, last_seen_at, revoked_at
                 ) VALUES
                    ('session-active', 'user-active', 'hash-active', 100, 1, NULL, NULL),
                    ('session-admin', 'user-admin', 'hash-admin', 100, 1, NULL, NULL),
                    ('session-disabled', 'user-disabled', 'hash-disabled', 100, 1, NULL, NULL),
                    ('session-expired', 'user-active', 'hash-expired', 10, 1, NULL, NULL),
                    ('session-revoked', 'user-active', 'hash-revoked', 100, 1, NULL, 2)",
                [],
            )
            .expect("seed sessions");
        storage
            .conn
            .execute(
                "INSERT INTO app_wallets (
                    id, owner_kind, owner_id, balance_credit_micros, frozen_credit_micros,
                    status, created_at, updated_at
                 ) VALUES
                    ('wallet-active', 'user', 'user-active', 2000, 125, 'active', 8, 9),
                    ('wallet-admin', 'user', 'user-admin', 9000, 0, 'active', 10, 11),
                    ('wallet-project', 'project', 'user-active', 5000, 0, 'active', 12, 13)",
                [],
            )
            .expect("seed wallets");

        let active = storage
            .find_active_app_session_user_by_token_hash("hash-active", 50)
            .expect("find active session")
            .expect("active session exists");

        assert_eq!(active.session_id, "session-active");
        assert_eq!(active.expires_at, 100);
        assert_eq!(active.user.id, "user-active");
        assert_eq!(active.user.username, "member@example.com");
        assert_eq!(active.user.wallet_id.as_deref(), Some("wallet-active"));
        assert_eq!(active.user.wallet_balance_credit_micros, Some(2000));
        assert_eq!(active.user.wallet_frozen_credit_micros, Some(125));

        let admin = storage
            .find_active_app_session_user_by_token_hash("hash-admin", 50)
            .expect("find admin session")
            .expect("admin session exists");
        assert_eq!(admin.user.id, "user-admin");
        assert_eq!(admin.user.wallet_id, None);

        for token_hash in [
            "hash-disabled",
            "hash-expired",
            "hash-revoked",
            "hash-missing",
        ] {
            assert!(storage
                .find_active_app_session_user_by_token_hash(token_hash, 50)
                .expect("find inactive session")
                .is_none());
        }
    }

    #[test]
    fn user_wallets_for_users_filters_requested_user_wallets() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        storage
            .conn
            .execute(
                "INSERT INTO app_wallets (
                    id, owner_kind, owner_id, balance_credit_micros, frozen_credit_micros,
                    status, created_at, updated_at
                 ) VALUES
                    ('wallet-user-1', 'user', 'user-1', 1000, 100, 'active', 1, 1),
                    ('wallet-user-2', 'user', 'user-2', 2000, 0, 'active', 1, 1),
                    ('wallet-project-1', 'project', 'user-1', 5000, 0, 'active', 1, 1)",
                [],
            )
            .expect("seed wallets");

        let wallets = storage
            .user_wallets_for_users(&[
                "user-1".to_string(),
                "user-1".to_string(),
                "missing-user".to_string(),
                " ".to_string(),
            ])
            .expect("read user wallets");

        assert_eq!(wallets.len(), 1);
        assert_eq!(wallets[0].owner_id, "user-1");
        assert_eq!(wallets[0].owner_kind, "user");
        assert_eq!(wallets[0].balance_credit_micros, 1000);
    }

    #[test]
    fn user_wallets_for_users_chunks_large_user_sets() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let user_ids = (0..950)
            .map(|idx| format!("user-{idx}"))
            .collect::<Vec<_>>();
        let tx = storage.conn.unchecked_transaction().expect("begin tx");
        for (idx, user_id) in user_ids.iter().enumerate() {
            tx.execute(
                "INSERT INTO app_wallets (
                    id, owner_kind, owner_id, balance_credit_micros, frozen_credit_micros,
                    status, created_at, updated_at
                 ) VALUES (?1, 'user', ?2, ?3, 0, 'active', 1, 1)",
                (
                    format!("wallet-{idx}"),
                    user_id,
                    i64::try_from(idx).expect("idx fits i64"),
                ),
            )
            .expect("seed wallet");
        }
        tx.commit().expect("commit wallets");

        let wallets = storage
            .user_wallets_for_users(&user_ids)
            .expect("read chunked wallets");

        assert_eq!(wallets.len(), 950);
        assert!(wallets.iter().any(|wallet| wallet.owner_id == "user-949"));
    }

    #[test]
    fn user_wallet_available_credit_uses_owner_index() {
        let storage = Storage::open_in_memory().expect("open storage");
        storage.init().expect("init storage");

        let plan = storage
            .conn
            .query_row(
                "EXPLAIN QUERY PLAN
                 SELECT owner_id, balance_credit_micros - frozen_credit_micros
                 FROM app_wallets
                 WHERE owner_kind = 'user'",
                [],
                |row| row.get::<_, String>(3),
            )
            .expect("explain plan");

        assert!(
            plan.contains("sqlite_autoindex_app_wallets_2"),
            "expected wallet unique owner index in plan, got {plan}"
        );
    }
}
