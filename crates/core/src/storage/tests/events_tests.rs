use super::{Event, Storage};

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

/// 函数 `latest_account_status_reasons_returns_latest_reason_per_account`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// 无
///
/// # 返回
/// 无
#[test]
fn latest_account_status_reasons_returns_latest_reason_per_account() {
    let storage = Storage::open_in_memory().expect("open");
    storage.init().expect("init");
    storage
        .insert_event(&Event {
            account_id: Some("acc-1".to_string()),
            event_type: "account_status_update".to_string(),
            message: "status=unavailable reason=usage_http_401".to_string(),
            created_at: 10,
        })
        .expect("insert first");
    storage
        .insert_event(&Event {
            account_id: Some("acc-1".to_string()),
            event_type: "account_status_update".to_string(),
            message: "status=unavailable reason=account_deactivated".to_string(),
            created_at: 20,
        })
        .expect("insert second");
    storage
        .insert_event(&Event {
            account_id: Some("acc-2".to_string()),
            event_type: "account_status_update".to_string(),
            message: "status=unavailable reason=workspace_deactivated".to_string(),
            created_at: 15,
        })
        .expect("insert third");

    let reasons = storage
        .latest_account_status_reasons(&[
            "acc-1".to_string(),
            "acc-2".to_string(),
            "missing".to_string(),
        ])
        .expect("load reasons");

    assert_eq!(
        reasons.get("acc-1").map(String::as_str),
        Some("account_deactivated")
    );
    assert_eq!(
        reasons.get("acc-2").map(String::as_str),
        Some("workspace_deactivated")
    );
    assert!(!reasons.contains_key("missing"));
}

#[test]
fn latest_account_status_reasons_chunks_large_account_sets() {
    let storage = Storage::open_in_memory().expect("open");
    storage.init().expect("init");

    storage
        .insert_event(&Event {
            account_id: Some("acc-0949".to_string()),
            event_type: "account_status_update".to_string(),
            message: "status=unavailable reason=usage_limit".to_string(),
            created_at: 10,
        })
        .expect("insert old target");
    storage
        .insert_event(&Event {
            account_id: Some("acc-0949".to_string()),
            event_type: "account_status_update".to_string(),
            message: "status=unavailable reason=account_deactivated".to_string(),
            created_at: 20,
        })
        .expect("insert new target");

    let account_ids = (0..950)
        .map(|index| format!("acc-{index:04}"))
        .collect::<Vec<_>>();
    let reasons = storage
        .latest_account_status_reasons(&account_ids)
        .expect("load reasons");

    assert_eq!(reasons.len(), 1);
    assert_eq!(
        reasons.get("acc-0949").map(String::as_str),
        Some("account_deactivated")
    );
}

#[test]
fn latest_account_status_blocked_ids_filters_latest_reason_in_sql() {
    let storage = Storage::open_in_memory().expect("open");
    storage.init().expect("init");

    for (account_id, reason, created_at) in [
        ("acc-blocked", "usage_http_401", 10),
        ("acc-blocked", "refresh_token_region_blocked", 20),
        ("acc-cleared", "account_deactivated", 10),
        ("acc-cleared", "usage_limit_exhausted", 20),
        ("acc-workspace", "workspace_deactivated", 30),
    ] {
        storage
            .insert_event(&Event {
                account_id: Some(account_id.to_string()),
                event_type: "account_status_update".to_string(),
                message: format!("status=unavailable reason={reason}"),
                created_at,
            })
            .expect("insert status event");
    }

    let account_ids = (0..950)
        .map(|index| format!("acc-{index:04}"))
        .chain([
            "acc-blocked".to_string(),
            "acc-cleared".to_string(),
            "acc-workspace".to_string(),
        ])
        .collect::<Vec<_>>();
    let blocked_ids = storage
        .latest_account_status_blocked_ids(&account_ids)
        .expect("load blocked ids");

    assert_eq!(
        blocked_ids,
        vec!["acc-blocked".to_string(), "acc-workspace".to_string()]
    );
}

#[test]
fn latest_account_status_blocked_ids_defers_final_ordering_to_rust() {
    let storage = Storage::open_in_memory().expect("open");
    storage.init().expect("init");

    let sql =
        super::latest_account_status_blocked_ids_chunk_sql("account_id IN ('acc-a', 'acc-b')");
    let plan = collect_query_plan(&storage, &format!("EXPLAIN QUERY PLAN {sql}"));

    assert!(
        plan.contains("idx_events_account_status_lookup"),
        "expected blocked status chunk lookup to use account status index, got {plan}"
    );
    assert!(
        !plan.contains("USE TEMP B-TREE FOR ORDER BY"),
        "blocked status chunk output should not require an outer per-chunk sort, got {plan}"
    );
}

#[test]
fn latest_account_status_reasons_uses_lookup_index() {
    let storage = Storage::open_in_memory().expect("open");
    storage.init().expect("init");

    let sql = super::latest_account_status_reasons_chunk_sql("account_id = 'acc-index'");
    let plan = collect_query_plan(&storage, &format!("EXPLAIN QUERY PLAN {sql}"));

    assert!(
        plan.contains("idx_events_account_status_lookup"),
        "expected account status lookup index in plan, got {plan}"
    );
}

#[test]
fn account_event_cleanup_uses_account_cleanup_index() {
    let storage = Storage::open_in_memory().expect("open");
    storage.init().expect("init");

    let plan = storage
        .conn
        .query_row(
            &format!(
                "EXPLAIN QUERY PLAN {}",
                super::delete_events_for_account_sql()
            ),
            ["acc-index"],
            |row| row.get::<_, String>(3),
        )
        .expect("explain plan");

    assert!(
        plan.contains("idx_events_account_cleanup"),
        "expected event account cleanup index in plan, got {plan}"
    );
}
