use codexmanager_core::rpc::types::{JsonRpcRequest, JsonRpcResponse};

use crate::startup_snapshot;
use crate::RpcActor;

/// 函数 `try_handle`
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
pub(super) fn try_handle(req: &JsonRpcRequest, actor: &RpcActor) -> Option<JsonRpcResponse> {
    let result = match req.method.as_str() {
        "startup/snapshot" => {
            let request_log_limit = super::i64_param(req, "requestLogLimit");
            let day_start_ts = super::i64_param(req, "dayStartTs");
            let day_end_ts = super::i64_param(req, "dayEndTs");
            let include_api_models = super::bool_param(req, "includeApiModels").unwrap_or(true);
            let include_api_keys = super::bool_param(req, "includeApiKeys").unwrap_or(true);
            let include_accounts = super::bool_param(req, "includeAccounts").unwrap_or(true);
            let include_usage_snapshots =
                super::bool_param(req, "includeUsageSnapshots").unwrap_or(true);
            let include_account_runtime =
                super::bool_param(req, "includeAccountRuntime").unwrap_or(true);
            let include_account_details =
                super::bool_param(req, "includeAccountDetails").unwrap_or(true);
            super::value_or_error(startup_snapshot::read_startup_snapshot_for_actor(
                actor,
                request_log_limit,
                day_start_ts,
                day_end_ts,
                include_api_models,
                include_api_keys,
                include_accounts,
                include_usage_snapshots,
                include_account_runtime,
                include_account_details,
            ))
        }
        _ => return None,
    };

    Some(super::response(req, result))
}
