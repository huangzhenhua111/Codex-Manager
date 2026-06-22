use serde_json::Value;
use std::sync::{Arc, Mutex};
use tiny_http::{Header, Request, Response, StatusCode};

use crate::gateway::upstream::GatewayStreamResponse;

use super::super::{GeminiStreamOutputMode, ResponseAdapter, ToolNameRestoreMap};
use super::body_conversion::{
    chat_completion_body_to_single_sse, compatibility_stream_content_type,
    convert_chat_completions_body_to_compact, convert_error_body_for_adapter,
    convert_responses_body_to_chat_completions, convert_success_body_for_adapter,
    gemini_cli_wrap_response_envelope, images_response_body_to_sse,
    merge_usage_from_body_without_output_text,
};
use super::compact_delivery::{
    respond_compact_success_body, respond_invalid_compact_non_success_body,
    respond_invalid_compact_success_body, respond_normalized_passthrough_non_success_body,
};
use super::compact_errors::{
    compact_non_success_body_should_be_normalized, compact_success_body_is_valid,
    extract_error_hint_from_body_or_headers, non_success_body_should_be_normalized,
    with_upstream_debug_suffix,
};
use super::manual_chunked::respond_streaming_chunked;
#[cfg(test)]
use super::manual_chunked::write_streaming_chunked_response;
use super::metadata::{
    copy_upstream_response_headers, log_bridge_stream_diagnostics,
    terminal_bridge_result_with_debug_meta, upstream_response_metadata, with_bridge_debug_meta,
};
use super::response_helpers::{
    extract_error_message_from_json_bytes, force_openai_responses_stream_content_type,
    replace_content_type_header, respond_json_bytes,
};
use super::{
    collect_non_stream_json_from_sse_bytes, extract_error_hint_from_body, looks_like_sse_payload,
    parse_usage_from_json, usage_has_signal, AnthropicSseReader,
    ChatCompletionsFromResponsesSseReader, GeminiSseReader, ImagesFromResponsesSseReader,
    ImagesResponseFormat, OpenAIResponsesPassthroughSseReader, PassthroughSseCollector,
    PassthroughSseProtocol, PassthroughSseUsageReader, ResponsesFromAnthropicSseReader,
    SseKeepAliveFrame, UpstreamResponseBridgeResult, UpstreamResponseUsage,
};

/// 函数 `is_compact_request_path`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - path: 参数 path
///
/// # 返回
/// 返回函数执行结果
fn is_compact_request_path(path: &str) -> bool {
    path == "/v1/responses/compact" || path.starts_with("/v1/responses/compact?")
}

#[cfg(test)]
fn response_adapter_uses_manual_chunked_streaming(response_adapter: ResponseAdapter) -> bool {
    matches!(
        response_adapter,
        ResponseAdapter::ResponsesFromAnthropicMessages
    )
}

/// 函数 `should_suppress_deactivation_delivery`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - upstream_error_hint: 参数 upstream_error_hint
/// - allow_failover_for_deactivation: 参数 allow_failover_for_deactivation
///
/// # 返回
/// 返回函数执行结果
fn should_suppress_deactivation_delivery(
    upstream_error_hint: Option<&str>,
    allow_failover_for_deactivation: bool,
) -> bool {
    allow_failover_for_deactivation
        && upstream_error_hint.is_some_and(|message| {
            crate::account_status::deactivation_reason_from_message(message).is_some()
        })
}

struct UpstreamDebugMetaRefs<'a> {
    request_id: &'a Option<String>,
    cf_ray: &'a Option<String>,
    auth_error: &'a Option<String>,
    identity_error_code: &'a Option<String>,
    content_type: &'a Option<String>,
}

fn respond_usage_collector_stream(
    request: Request,
    status: StatusCode,
    headers: Vec<Header>,
    response_body: Box<dyn std::io::Read + Send>,
    usage_collector: Arc<Mutex<UpstreamResponseUsage>>,
    meta: UpstreamDebugMetaRefs<'_>,
) -> UpstreamResponseBridgeResult {
    let delivery_error = respond_streaming_chunked(request, status, headers, response_body)
        .err()
        .map(|err| err.to_string());
    let usage = usage_collector
        .lock()
        .map(|guard| guard.clone())
        .unwrap_or_default();
    terminal_bridge_result_with_debug_meta(
        usage,
        delivery_error,
        None,
        meta.request_id,
        meta.cf_ray,
        meta.auth_error,
        meta.identity_error_code,
        meta.content_type,
    )
}

fn respond_passthrough_collector_stream(
    request: Request,
    status: StatusCode,
    headers: Vec<Header>,
    response_body: Box<dyn std::io::Read + Send>,
    usage_collector: Arc<Mutex<PassthroughSseCollector>>,
    meta: UpstreamDebugMetaRefs<'_>,
) -> UpstreamResponseBridgeResult {
    let delivery_error = respond_streaming_chunked(request, status, headers, response_body)
        .err()
        .map(|err| err.to_string());
    let collector = usage_collector
        .lock()
        .map(|guard| guard.clone())
        .unwrap_or_default();
    with_bridge_debug_meta(
        UpstreamResponseBridgeResult {
            usage: collector.usage,
            stream_terminal_seen: collector.saw_terminal,
            stream_terminal_error: collector.terminal_error,
            delivery_error,
            upstream_error_hint: collector.upstream_error_hint,
            delivered_status_code: None,
            upstream_request_id: None,
            upstream_cf_ray: None,
            upstream_auth_error: None,
            upstream_identity_error_code: None,
            upstream_content_type: None,
            last_sse_event_type: collector.last_event_type,
        },
        meta.request_id,
        meta.cf_ray,
        meta.auth_error,
        meta.identity_error_code,
        meta.content_type,
        None,
    )
}

/// 函数 `respond_with_upstream`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - crate: 参数 crate
///
/// # 返回
/// 返回函数执行结果
pub(crate) fn respond_with_upstream(
    request: Request,
    upstream: reqwest::blocking::Response,
    _inflight_guard: super::super::AccountInFlightGuard,
    response_adapter: ResponseAdapter,
    passthrough_sse_protocol: Option<PassthroughSseProtocol>,
    gemini_stream_output_mode: Option<GeminiStreamOutputMode>,
    request_path: &str,
    tool_name_restore_map: Option<&ToolNameRestoreMap>,
    is_stream: bool,
    allow_failover_for_deactivation: bool,
    trace_id: Option<&str>,
    fallback_model: Option<&str>,
    request_started_at: std::time::Instant,
) -> Result<UpstreamResponseBridgeResult, String> {
    let keepalive_frame = resolve_stream_keepalive_frame(response_adapter, request_path);
    let passthrough_sse_protocol =
        passthrough_sse_protocol.unwrap_or(PassthroughSseProtocol::Generic);
    let upstream_meta = upstream_response_metadata(upstream.headers());
    let upstream_request_id = upstream_meta.request_id;
    let upstream_cf_ray = upstream_meta.cf_ray;
    let upstream_auth_error = upstream_meta.auth_error;
    let upstream_identity_error_code = upstream_meta.identity_error_code;
    let upstream_content_type = upstream_meta.content_type;
    let is_sse = upstream_meta.is_sse;
    let is_json = upstream_meta.is_json;
    if response_adapter != ResponseAdapter::Passthrough {
        let status = StatusCode(upstream.status().as_u16());
        let mut headers = copy_upstream_response_headers(upstream.headers(), trace_id);

        if !is_stream {
            let upstream_body = upstream
                .bytes()
                .map_err(|err| format!("read upstream body failed: {err}"))?;
            let detected_sse =
                is_sse || (!is_json && looks_like_sse_payload(upstream_body.as_ref()));
            let (body, usage) = if detected_sse {
                let (synthesized, mut usage) =
                    collect_non_stream_json_from_sse_bytes(upstream_body.as_ref());
                let body = synthesized.unwrap_or_else(|| upstream_body.to_vec());
                merge_usage_from_body_without_output_text(&mut usage, &body);
                (body, usage)
            } else {
                let usage = serde_json::from_slice::<Value>(upstream_body.as_ref())
                    .ok()
                    .map(|value| parse_usage_from_json(&value))
                    .unwrap_or_default();
                (upstream_body.to_vec(), usage)
            };
            let response_body = if status.0 >= 400 {
                let message = with_upstream_debug_suffix(
                    extract_error_hint_from_body_or_headers(
                        status.0,
                        upstream_content_type.as_deref(),
                        &body,
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    )
                    .or_else(|| extract_error_message_from_json_bytes(&body)),
                    None,
                    upstream_request_id.as_deref(),
                    upstream_cf_ray.as_deref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                )
                .unwrap_or_else(|| "upstream compatibility bridge failed".to_string());
                convert_error_body_for_adapter(response_adapter, &message)
            } else {
                convert_success_body_for_adapter(
                    response_adapter,
                    &body,
                    request_path,
                    tool_name_restore_map,
                )
                .unwrap_or_else(|| body.clone())
            };
            let delivery_error = respond_json_bytes(request, status, headers, response_body);
            return Ok(terminal_bridge_result_with_debug_meta(
                usage,
                delivery_error,
                None,
                &upstream_request_id,
                &upstream_cf_ray,
                &upstream_auth_error,
                &upstream_identity_error_code,
                &upstream_content_type,
            ));
        }

        if status.0 >= 400 && !is_sse {
            let upstream_body = upstream
                .bytes()
                .map_err(|err| format!("read upstream body failed: {err}"))?;
            let message = with_upstream_debug_suffix(
                extract_error_hint_from_body_or_headers(
                    status.0,
                    upstream_content_type.as_deref(),
                    upstream_body.as_ref(),
                    upstream_cf_ray.as_deref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                )
                .or_else(|| extract_error_message_from_json_bytes(upstream_body.as_ref())),
                None,
                upstream_request_id.as_deref(),
                upstream_cf_ray.as_deref(),
                upstream_auth_error.as_deref(),
                upstream_identity_error_code.as_deref(),
            )
            .unwrap_or_else(|| "upstream compatibility bridge failed".to_string());
            let response_body = convert_error_body_for_adapter(response_adapter, &message);
            let delivery_error = respond_json_bytes(request, status, headers, response_body);
            return Ok(terminal_bridge_result_with_debug_meta(
                UpstreamResponseUsage::default(),
                delivery_error,
                Some(message),
                &upstream_request_id,
                &upstream_cf_ray,
                &upstream_auth_error,
                &upstream_identity_error_code,
                &upstream_content_type,
            ));
        }

        replace_content_type_header(
            &mut headers,
            compatibility_stream_content_type(response_adapter, gemini_stream_output_mode),
        );
        match response_adapter {
            ResponseAdapter::AnthropicMessagesFromResponses => {
                let usage_collector = Arc::new(Mutex::new(UpstreamResponseUsage::default()));
                let response_body: Box<dyn std::io::Read + Send> =
                    Box::new(AnthropicSseReader::new(
                        upstream,
                        Arc::clone(&usage_collector),
                        fallback_model,
                        tool_name_restore_map.cloned(),
                        request_started_at,
                    ));
                return Ok(respond_usage_collector_stream(
                    request,
                    status,
                    headers,
                    response_body,
                    usage_collector,
                    UpstreamDebugMetaRefs {
                        request_id: &upstream_request_id,
                        cf_ray: &upstream_cf_ray,
                        auth_error: &upstream_auth_error,
                        identity_error_code: &upstream_identity_error_code,
                        content_type: &upstream_content_type,
                    },
                ));
            }
            ResponseAdapter::ResponsesFromAnthropicMessages => {
                let usage_collector = Arc::new(Mutex::new(UpstreamResponseUsage::default()));
                let response_body: Box<dyn std::io::Read + Send> =
                    Box::new(ResponsesFromAnthropicSseReader::new(
                        upstream,
                        Arc::clone(&usage_collector),
                        fallback_model,
                        request_started_at,
                    ));
                return Ok(respond_usage_collector_stream(
                    request,
                    status,
                    headers,
                    response_body,
                    usage_collector,
                    UpstreamDebugMetaRefs {
                        request_id: &upstream_request_id,
                        cf_ray: &upstream_cf_ray,
                        auth_error: &upstream_auth_error,
                        identity_error_code: &upstream_identity_error_code,
                        content_type: &upstream_content_type,
                    },
                ));
            }
            ResponseAdapter::ChatCompletionsFromResponses => {
                let usage_collector = Arc::new(Mutex::new(PassthroughSseCollector::default()));
                let response_body: Box<dyn std::io::Read + Send> =
                    Box::new(ChatCompletionsFromResponsesSseReader::new(
                        upstream,
                        Arc::clone(&usage_collector),
                        request_started_at,
                    ));
                return Ok(respond_passthrough_collector_stream(
                    request,
                    status,
                    headers,
                    response_body,
                    usage_collector,
                    UpstreamDebugMetaRefs {
                        request_id: &upstream_request_id,
                        cf_ray: &upstream_cf_ray,
                        auth_error: &upstream_auth_error,
                        identity_error_code: &upstream_identity_error_code,
                        content_type: &upstream_content_type,
                    },
                ));
            }
            ResponseAdapter::CompactFromChatCompletions => unreachable!(),
            ResponseAdapter::ImagesB64JsonFromResponses
            | ResponseAdapter::ImagesUrlFromResponses => {
                let response_format = if response_adapter == ResponseAdapter::ImagesUrlFromResponses
                {
                    ImagesResponseFormat::Url
                } else {
                    ImagesResponseFormat::B64Json
                };
                let usage_collector = Arc::new(Mutex::new(PassthroughSseCollector::default()));
                let response_body: Box<dyn std::io::Read + Send> =
                    Box::new(ImagesFromResponsesSseReader::new(
                        upstream,
                        Arc::clone(&usage_collector),
                        request_started_at,
                        response_format,
                    ));
                return Ok(respond_passthrough_collector_stream(
                    request,
                    status,
                    headers,
                    response_body,
                    usage_collector,
                    UpstreamDebugMetaRefs {
                        request_id: &upstream_request_id,
                        cf_ray: &upstream_cf_ray,
                        auth_error: &upstream_auth_error,
                        identity_error_code: &upstream_identity_error_code,
                        content_type: &upstream_content_type,
                    },
                ));
            }
            ResponseAdapter::GeminiJson | ResponseAdapter::GeminiCliJson => unreachable!(),
            ResponseAdapter::GeminiSse | ResponseAdapter::GeminiCliSse => {
                let usage_collector = Arc::new(Mutex::new(PassthroughSseCollector::default()));
                let response_body: Box<dyn std::io::Read + Send> = Box::new(GeminiSseReader::new(
                    upstream,
                    Arc::clone(&usage_collector),
                    tool_name_restore_map.cloned(),
                    gemini_stream_output_mode.unwrap_or(GeminiStreamOutputMode::Sse),
                    gemini_cli_wrap_response_envelope(response_adapter),
                    request_started_at,
                ));
                return Ok(respond_passthrough_collector_stream(
                    request,
                    status,
                    headers,
                    response_body,
                    usage_collector,
                    UpstreamDebugMetaRefs {
                        request_id: &upstream_request_id,
                        cf_ray: &upstream_cf_ray,
                        auth_error: &upstream_auth_error,
                        identity_error_code: &upstream_identity_error_code,
                        content_type: &upstream_content_type,
                    },
                ));
            }
            ResponseAdapter::Passthrough => {}
        }
    }
    match response_adapter {
        ResponseAdapter::Passthrough => {
            let status = StatusCode(upstream.status().as_u16());
            let mut headers = copy_upstream_response_headers(upstream.headers(), trace_id);
            if !is_stream {
                let upstream_body = upstream
                    .bytes()
                    .map_err(|err| format!("read upstream body failed: {err}"))?;
                let detected_sse =
                    is_sse || (!is_json && looks_like_sse_payload(upstream_body.as_ref()));
                let is_compact_request = is_compact_request_path(request_path);
                if detected_sse {
                    let (synthesized_body, mut usage) =
                        collect_non_stream_json_from_sse_bytes(upstream_body.as_ref());
                    let synthesized_response = synthesized_body.is_some();
                    let body = synthesized_body.unwrap_or_else(|| upstream_body.to_vec());
                    merge_usage_from_body_without_output_text(&mut usage, &body);
                    let upstream_error_hint = with_upstream_debug_suffix(
                        extract_error_hint_from_body_or_headers(
                            status.0,
                            upstream_content_type.as_deref(),
                            &body,
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                        ),
                        None,
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    );
                    if should_suppress_deactivation_delivery(
                        upstream_error_hint.as_deref(),
                        allow_failover_for_deactivation,
                    ) {
                        return Ok(terminal_bridge_result_with_debug_meta(
                            usage,
                            None,
                            upstream_error_hint,
                            &upstream_request_id,
                            &upstream_cf_ray,
                            &upstream_auth_error,
                            &upstream_identity_error_code,
                            &upstream_content_type,
                        ));
                    }
                    if synthesized_response {
                        replace_content_type_header(&mut headers, "application/json");
                    }
                    if status.0 < 400
                        && is_compact_request
                        && !compact_success_body_is_valid(body.as_ref())
                    {
                        return Ok(respond_invalid_compact_success_body(
                            request,
                            usage,
                            body.as_ref(),
                            upstream_request_id.as_deref(),
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                            trace_id,
                        ));
                    }
                    if is_compact_request
                        && compact_non_success_body_should_be_normalized(
                            status.0,
                            upstream_content_type.as_deref(),
                            body.as_ref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                        )
                    {
                        return Ok(respond_invalid_compact_non_success_body(
                            request,
                            status.0,
                            usage,
                            body.as_ref(),
                            upstream_content_type.as_deref(),
                            upstream_request_id.as_deref(),
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                            trace_id,
                        ));
                    }
                    if status.0 >= 400
                        && non_success_body_should_be_normalized(
                            status.0,
                            upstream_content_type.as_deref(),
                            body.as_ref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                        )
                    {
                        return Ok(respond_normalized_passthrough_non_success_body(
                            request,
                            usage,
                            body.as_ref(),
                            upstream_content_type.as_deref(),
                            upstream_request_id.as_deref(),
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                            trace_id,
                        ));
                    }
                    let len = Some(body.len());
                    let response =
                        Response::new(status, headers, std::io::Cursor::new(body), len, None);
                    let delivery_error = request.respond(response).err().map(|err| err.to_string());
                    return Ok(terminal_bridge_result_with_debug_meta(
                        usage,
                        delivery_error,
                        upstream_error_hint,
                        &upstream_request_id,
                        &upstream_cf_ray,
                        &upstream_auth_error,
                        &upstream_identity_error_code,
                        &upstream_content_type,
                    ));
                }

                let (_, sse_usage) = collect_non_stream_json_from_sse_bytes(upstream_body.as_ref());
                let usage = if is_json {
                    serde_json::from_slice::<Value>(upstream_body.as_ref())
                        .ok()
                        .map(|value| parse_usage_from_json(&value))
                        .unwrap_or_default()
                } else if usage_has_signal(&sse_usage) {
                    sse_usage
                } else {
                    UpstreamResponseUsage::default()
                };
                if status.0 < 400
                    && is_compact_request
                    && !compact_success_body_is_valid(upstream_body.as_ref())
                {
                    return Ok(respond_invalid_compact_success_body(
                        request,
                        usage,
                        upstream_body.as_ref(),
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                        trace_id,
                    ));
                }
                if is_compact_request
                    && compact_non_success_body_should_be_normalized(
                        status.0,
                        upstream_content_type.as_deref(),
                        upstream_body.as_ref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    )
                {
                    return Ok(respond_invalid_compact_non_success_body(
                        request,
                        status.0,
                        usage,
                        upstream_body.as_ref(),
                        upstream_content_type.as_deref(),
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                        trace_id,
                    ));
                }
                if status.0 >= 400
                    && non_success_body_should_be_normalized(
                        status.0,
                        upstream_content_type.as_deref(),
                        upstream_body.as_ref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    )
                {
                    return Ok(respond_normalized_passthrough_non_success_body(
                        request,
                        usage,
                        upstream_body.as_ref(),
                        upstream_content_type.as_deref(),
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                        trace_id,
                    ));
                }
                let upstream_error_hint = with_upstream_debug_suffix(
                    extract_error_hint_from_body_or_headers(
                        status.0,
                        upstream_content_type.as_deref(),
                        upstream_body.as_ref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    ),
                    None,
                    upstream_request_id.as_deref(),
                    upstream_cf_ray.as_deref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                );
                if should_suppress_deactivation_delivery(
                    upstream_error_hint.as_deref(),
                    allow_failover_for_deactivation,
                ) {
                    return Ok(terminal_bridge_result_with_debug_meta(
                        usage,
                        None,
                        upstream_error_hint,
                        &upstream_request_id,
                        &upstream_cf_ray,
                        &upstream_auth_error,
                        &upstream_identity_error_code,
                        &upstream_content_type,
                    ));
                }
                let len = Some(upstream_body.len());
                let response = Response::new(
                    status,
                    headers,
                    std::io::Cursor::new(upstream_body.to_vec()),
                    len,
                    None,
                );
                let delivery_error = request.respond(response).err().map(|err| err.to_string());
                return Ok(terminal_bridge_result_with_debug_meta(
                    usage,
                    delivery_error,
                    upstream_error_hint,
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                ));
            }
            if is_stream && !is_sse && status.0 >= 400 {
                let upstream_body = upstream
                    .bytes()
                    .map_err(|err| format!("read upstream body failed: {err}"))?;
                let usage = if is_json {
                    serde_json::from_slice::<Value>(upstream_body.as_ref())
                        .ok()
                        .map(|value| parse_usage_from_json(&value))
                        .unwrap_or_default()
                } else {
                    UpstreamResponseUsage::default()
                };
                if non_success_body_should_be_normalized(
                    status.0,
                    upstream_content_type.as_deref(),
                    upstream_body.as_ref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                ) {
                    return Ok(respond_normalized_passthrough_non_success_body(
                        request,
                        usage,
                        upstream_body.as_ref(),
                        upstream_content_type.as_deref(),
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                        trace_id,
                    ));
                }
                let upstream_error_hint = with_upstream_debug_suffix(
                    extract_error_hint_from_body_or_headers(
                        status.0,
                        upstream_content_type.as_deref(),
                        upstream_body.as_ref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    ),
                    None,
                    upstream_request_id.as_deref(),
                    upstream_cf_ray.as_deref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                );
                let len = Some(upstream_body.len());
                let response = Response::new(
                    status,
                    headers,
                    std::io::Cursor::new(upstream_body.to_vec()),
                    len,
                    None,
                );
                let delivery_error = request.respond(response).err().map(|err| err.to_string());
                return Ok(terminal_bridge_result_with_debug_meta(
                    usage,
                    delivery_error,
                    upstream_error_hint,
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                ));
            }
            if is_stream && !is_sse && status.0 < 400 && is_compact_request_path(request_path) {
                let upstream_body = upstream
                    .bytes()
                    .map_err(|err| format!("read upstream body failed: {err}"))?;
                let usage = if is_json {
                    serde_json::from_slice::<Value>(upstream_body.as_ref())
                        .ok()
                        .map(|value| parse_usage_from_json(&value))
                        .unwrap_or_default()
                } else {
                    UpstreamResponseUsage::default()
                };
                return Ok(respond_compact_success_body(
                    request,
                    status,
                    headers,
                    usage,
                    upstream_body.as_ref(),
                    upstream_request_id.as_deref(),
                    upstream_cf_ray.as_deref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                    &upstream_content_type,
                    trace_id,
                ));
            }
            if is_sse || is_stream {
                let usage_collector = Arc::new(Mutex::new(PassthroughSseCollector::default()));
                let response_body: Box<dyn std::io::Read + Send> =
                    if request_path.starts_with("/v1/responses") {
                        Box::new(OpenAIResponsesPassthroughSseReader::new(
                            upstream,
                            Arc::clone(&usage_collector),
                            keepalive_frame,
                            request_started_at,
                        ))
                    } else {
                        Box::new(PassthroughSseUsageReader::new(
                            upstream,
                            Arc::clone(&usage_collector),
                            keepalive_frame,
                            passthrough_sse_protocol,
                            request_started_at,
                        ))
                    };
                force_openai_responses_stream_content_type(&mut headers, request_path, is_stream);
                let delivery_error =
                    respond_streaming_chunked(request, status, headers, response_body)
                        .err()
                        .map(|err| err.to_string());
                let collector = usage_collector
                    .lock()
                    .map(|guard| guard.clone())
                    .unwrap_or_default();
                let last_sse_event_type = collector.last_event_type.clone();
                let result = with_bridge_debug_meta(
                    UpstreamResponseBridgeResult {
                        usage: collector.usage,
                        stream_terminal_seen: collector.saw_terminal,
                        stream_terminal_error: collector.terminal_error,
                        delivery_error,
                        upstream_error_hint: with_upstream_debug_suffix(
                            collector.upstream_error_hint,
                            None,
                            upstream_request_id.as_deref(),
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                        ),
                        delivered_status_code: None,
                        upstream_request_id: None,
                        upstream_cf_ray: None,
                        upstream_auth_error: None,
                        upstream_identity_error_code: None,
                        upstream_content_type: None,
                        last_sse_event_type: None,
                    },
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                    last_sse_event_type,
                );
                log_bridge_stream_diagnostics(response_adapter, request_path, &result);
                return Ok(result);
            }
            let len = upstream.content_length().map(|v| v as usize);
            let response = Response::new(status, headers, upstream, len, None);
            let delivery_error = request.respond(response).err().map(|err| err.to_string());
            Ok(terminal_bridge_result_with_debug_meta(
                UpstreamResponseUsage::default(),
                delivery_error,
                None,
                &upstream_request_id,
                &upstream_cf_ray,
                &upstream_auth_error,
                &upstream_identity_error_code,
                &upstream_content_type,
            ))
        }
        ResponseAdapter::CompactFromChatCompletions => {
            let status = StatusCode(upstream.status().as_u16());
            let headers = copy_upstream_response_headers(upstream.headers(), trace_id);
            let upstream_body = upstream
                .bytes()
                .map_err(|err| format!("read upstream body failed: {err}"))?;
            let usage = serde_json::from_slice::<Value>(upstream_body.as_ref())
                .ok()
                .map(|value| parse_usage_from_json(&value))
                .unwrap_or_default();
            let response_body = if status.0 < 400 {
                convert_chat_completions_body_to_compact(upstream_body.as_ref())
                    .unwrap_or_else(|| upstream_body.to_vec())
            } else {
                upstream_body.to_vec()
            };
            let upstream_error_hint = (status.0 >= 400)
                .then(|| {
                    with_upstream_debug_suffix(
                        extract_error_hint_from_body(status.0, upstream_body.as_ref()),
                        None,
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    )
                })
                .flatten();
            let delivery_error = respond_json_bytes(request, status, headers, response_body);
            Ok(terminal_bridge_result_with_debug_meta(
                usage,
                delivery_error,
                upstream_error_hint,
                &upstream_request_id,
                &upstream_cf_ray,
                &upstream_auth_error,
                &upstream_identity_error_code,
                &upstream_content_type,
            ))
        }
        ResponseAdapter::AnthropicMessagesFromResponses
        | ResponseAdapter::ResponsesFromAnthropicMessages
        | ResponseAdapter::ChatCompletionsFromResponses
        | ResponseAdapter::ImagesB64JsonFromResponses
        | ResponseAdapter::ImagesUrlFromResponses
        | ResponseAdapter::GeminiJson
        | ResponseAdapter::GeminiCliJson
        | ResponseAdapter::GeminiSse
        | ResponseAdapter::GeminiCliSse => unreachable!(),
    }
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn respond_with_stream_upstream(
    request: Request,
    upstream: GatewayStreamResponse,
    _inflight_guard: super::super::AccountInFlightGuard,
    response_adapter: ResponseAdapter,
    _passthrough_sse_protocol: Option<PassthroughSseProtocol>,
    gemini_stream_output_mode: Option<GeminiStreamOutputMode>,
    request_path: &str,
    tool_name_restore_map: Option<&ToolNameRestoreMap>,
    is_stream: bool,
    _allow_failover_for_deactivation: bool,
    trace_id: Option<&str>,
    fallback_model: Option<&str>,
    request_started_at: std::time::Instant,
) -> Result<UpstreamResponseBridgeResult, String> {
    let keepalive_frame = resolve_stream_keepalive_frame(response_adapter, request_path);
    let upstream_meta = upstream_response_metadata(upstream.headers());
    let upstream_request_id = upstream_meta.request_id;
    let upstream_cf_ray = upstream_meta.cf_ray;
    let upstream_auth_error = upstream_meta.auth_error;
    let upstream_identity_error_code = upstream_meta.identity_error_code;
    let upstream_content_type = upstream_meta.content_type;
    let is_sse = upstream_meta.is_sse;
    let is_json = upstream_meta.is_json;
    if response_adapter != ResponseAdapter::Passthrough {
        let status = StatusCode(upstream.status().as_u16());
        let mut headers = copy_upstream_response_headers(upstream.headers(), trace_id);

        if !is_stream {
            let upstream_body = upstream
                .read_all_bytes()
                .map_err(|err| format!("read upstream body failed: {err}"))?;
            let detected_sse =
                is_sse || (!is_json && looks_like_sse_payload(upstream_body.as_ref()));
            let (body, usage) = if detected_sse {
                let (synthesized, mut usage) =
                    collect_non_stream_json_from_sse_bytes(upstream_body.as_ref());
                let body = synthesized.unwrap_or_else(|| upstream_body.to_vec());
                merge_usage_from_body_without_output_text(&mut usage, &body);
                (body, usage)
            } else {
                let usage = serde_json::from_slice::<Value>(upstream_body.as_ref())
                    .ok()
                    .map(|value| parse_usage_from_json(&value))
                    .unwrap_or_default();
                (upstream_body.to_vec(), usage)
            };
            let response_body = if status.0 >= 400 {
                let message = with_upstream_debug_suffix(
                    extract_error_hint_from_body_or_headers(
                        status.0,
                        upstream_content_type.as_deref(),
                        &body,
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    )
                    .or_else(|| extract_error_message_from_json_bytes(&body)),
                    None,
                    upstream_request_id.as_deref(),
                    upstream_cf_ray.as_deref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                )
                .unwrap_or_else(|| "upstream compatibility bridge failed".to_string());
                convert_error_body_for_adapter(response_adapter, &message)
            } else {
                convert_success_body_for_adapter(
                    response_adapter,
                    &body,
                    request_path,
                    tool_name_restore_map,
                )
                .unwrap_or_else(|| body.clone())
            };
            let delivery_error = respond_json_bytes(request, status, headers, response_body);
            return Ok(terminal_bridge_result_with_debug_meta(
                usage,
                delivery_error,
                None,
                &upstream_request_id,
                &upstream_cf_ray,
                &upstream_auth_error,
                &upstream_identity_error_code,
                &upstream_content_type,
            ));
        }

        if status.0 >= 400 && !is_sse {
            let upstream_body = upstream
                .read_all_bytes()
                .map_err(|err| format!("read upstream body failed: {err}"))?;
            let message = with_upstream_debug_suffix(
                extract_error_hint_from_body_or_headers(
                    status.0,
                    upstream_content_type.as_deref(),
                    upstream_body.as_ref(),
                    upstream_cf_ray.as_deref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                )
                .or_else(|| extract_error_message_from_json_bytes(upstream_body.as_ref())),
                None,
                upstream_request_id.as_deref(),
                upstream_cf_ray.as_deref(),
                upstream_auth_error.as_deref(),
                upstream_identity_error_code.as_deref(),
            )
            .unwrap_or_else(|| "upstream compatibility bridge failed".to_string());
            let response_body = convert_error_body_for_adapter(response_adapter, &message);
            let delivery_error = respond_json_bytes(request, status, headers, response_body);
            return Ok(terminal_bridge_result_with_debug_meta(
                UpstreamResponseUsage::default(),
                delivery_error,
                Some(message),
                &upstream_request_id,
                &upstream_cf_ray,
                &upstream_auth_error,
                &upstream_identity_error_code,
                &upstream_content_type,
            ));
        }

        replace_content_type_header(
            &mut headers,
            compatibility_stream_content_type(response_adapter, gemini_stream_output_mode),
        );
        match response_adapter {
            ResponseAdapter::AnthropicMessagesFromResponses => {
                let upstream_body = upstream
                    .read_all_bytes()
                    .map_err(|err| format!("read upstream body failed: {err}"))?;
                let usage_collector = Arc::new(Mutex::new(UpstreamResponseUsage::default()));
                let response_body: Box<dyn std::io::Read + Send> =
                    Box::new(AnthropicSseReader::from_reader(
                        std::io::Cursor::new(upstream_body.to_vec()),
                        Arc::clone(&usage_collector),
                        fallback_model,
                        tool_name_restore_map.cloned(),
                        request_started_at,
                    ));
                return Ok(respond_usage_collector_stream(
                    request,
                    status,
                    headers,
                    response_body,
                    usage_collector,
                    UpstreamDebugMetaRefs {
                        request_id: &upstream_request_id,
                        cf_ray: &upstream_cf_ray,
                        auth_error: &upstream_auth_error,
                        identity_error_code: &upstream_identity_error_code,
                        content_type: &upstream_content_type,
                    },
                ));
            }
            ResponseAdapter::ResponsesFromAnthropicMessages => {
                let upstream_body = upstream
                    .read_all_bytes()
                    .map_err(|err| format!("read upstream body failed: {err}"))?;
                let usage_collector = Arc::new(Mutex::new(UpstreamResponseUsage::default()));
                let response_body: Box<dyn std::io::Read + Send> =
                    Box::new(ResponsesFromAnthropicSseReader::from_reader(
                        std::io::Cursor::new(upstream_body.to_vec()),
                        Arc::clone(&usage_collector),
                        fallback_model,
                        request_started_at,
                    ));
                return Ok(respond_usage_collector_stream(
                    request,
                    status,
                    headers,
                    response_body,
                    usage_collector,
                    UpstreamDebugMetaRefs {
                        request_id: &upstream_request_id,
                        cf_ray: &upstream_cf_ray,
                        auth_error: &upstream_auth_error,
                        identity_error_code: &upstream_identity_error_code,
                        content_type: &upstream_content_type,
                    },
                ));
            }
            ResponseAdapter::ChatCompletionsFromResponses => {
                let upstream_body = upstream
                    .read_all_bytes()
                    .map_err(|err| format!("read upstream body failed: {err}"))?;
                let (synthesized, mut usage) =
                    collect_non_stream_json_from_sse_bytes(upstream_body.as_ref());
                let body = synthesized.unwrap_or_else(|| upstream_body.to_vec());
                merge_usage_from_body_without_output_text(&mut usage, &body);
                let chat_body =
                    convert_responses_body_to_chat_completions(&body).unwrap_or_else(|| body);
                let response_body = chat_completion_body_to_single_sse(&chat_body);
                let len = Some(response_body.len());
                let response = Response::new(
                    status,
                    headers,
                    std::io::Cursor::new(response_body),
                    len,
                    None,
                );
                let delivery_error = request.respond(response).err().map(|err| err.to_string());
                return Ok(terminal_bridge_result_with_debug_meta(
                    usage,
                    delivery_error,
                    None,
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                ));
            }
            ResponseAdapter::CompactFromChatCompletions => unreachable!(),
            ResponseAdapter::ImagesB64JsonFromResponses
            | ResponseAdapter::ImagesUrlFromResponses => {
                let response_format = if response_adapter == ResponseAdapter::ImagesUrlFromResponses
                {
                    ImagesResponseFormat::Url
                } else {
                    ImagesResponseFormat::B64Json
                };
                let upstream_body = upstream
                    .read_all_bytes()
                    .map_err(|err| format!("read upstream body failed: {err}"))?;
                let (synthesized, mut usage) =
                    collect_non_stream_json_from_sse_bytes(upstream_body.as_ref());
                let body = synthesized.unwrap_or_else(|| upstream_body.to_vec());
                merge_usage_from_body_without_output_text(&mut usage, &body);
                let response_body = images_response_body_to_sse(&body, response_format);
                let len = Some(response_body.len());
                let response = Response::new(
                    status,
                    headers,
                    std::io::Cursor::new(response_body),
                    len,
                    None,
                );
                let delivery_error = request.respond(response).err().map(|err| err.to_string());
                return Ok(terminal_bridge_result_with_debug_meta(
                    usage,
                    delivery_error,
                    None,
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                ));
            }
            ResponseAdapter::GeminiJson | ResponseAdapter::GeminiCliJson => unreachable!(),
            ResponseAdapter::GeminiSse | ResponseAdapter::GeminiCliSse => {
                let upstream_body = upstream
                    .read_all_bytes()
                    .map_err(|err| format!("read upstream body failed: {err}"))?;
                let usage_collector = Arc::new(Mutex::new(PassthroughSseCollector::default()));
                let response_body: Box<dyn std::io::Read + Send> =
                    Box::new(GeminiSseReader::from_reader(
                        std::io::Cursor::new(upstream_body.to_vec()),
                        Arc::clone(&usage_collector),
                        tool_name_restore_map.cloned(),
                        gemini_stream_output_mode.unwrap_or(GeminiStreamOutputMode::Sse),
                        gemini_cli_wrap_response_envelope(response_adapter),
                        request_started_at,
                    ));
                return Ok(respond_passthrough_collector_stream(
                    request,
                    status,
                    headers,
                    response_body,
                    usage_collector,
                    UpstreamDebugMetaRefs {
                        request_id: &upstream_request_id,
                        cf_ray: &upstream_cf_ray,
                        auth_error: &upstream_auth_error,
                        identity_error_code: &upstream_identity_error_code,
                        content_type: &upstream_content_type,
                    },
                ));
            }
            ResponseAdapter::Passthrough => {}
        }
    }

    match response_adapter {
        ResponseAdapter::Passthrough => {
            let status = StatusCode(upstream.status().as_u16());
            let mut headers = copy_upstream_response_headers(upstream.headers(), trace_id);

            if !is_stream {
                let upstream_body = upstream
                    .read_all_bytes()
                    .map_err(|err| format!("read upstream body failed: {err}"))?;
                let detected_sse =
                    is_sse || (!is_json && looks_like_sse_payload(upstream_body.as_ref()));
                let is_compact_request = is_compact_request_path(request_path);
                if detected_sse {
                    let (synthesized_body, mut usage) =
                        collect_non_stream_json_from_sse_bytes(upstream_body.as_ref());
                    let synthesized_response = synthesized_body.is_some();
                    let body = synthesized_body.unwrap_or_else(|| upstream_body.to_vec());
                    merge_usage_from_body_without_output_text(&mut usage, &body);
                    let upstream_error_hint = with_upstream_debug_suffix(
                        extract_error_hint_from_body_or_headers(
                            status.0,
                            upstream_content_type.as_deref(),
                            &body,
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                        ),
                        None,
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    );
                    if should_suppress_deactivation_delivery(
                        upstream_error_hint.as_deref(),
                        _allow_failover_for_deactivation,
                    ) {
                        return Ok(terminal_bridge_result_with_debug_meta(
                            usage,
                            None,
                            upstream_error_hint,
                            &upstream_request_id,
                            &upstream_cf_ray,
                            &upstream_auth_error,
                            &upstream_identity_error_code,
                            &upstream_content_type,
                        ));
                    }
                    if synthesized_response {
                        replace_content_type_header(&mut headers, "application/json");
                    }
                    if status.0 < 400
                        && is_compact_request
                        && !compact_success_body_is_valid(body.as_ref())
                    {
                        return Ok(respond_invalid_compact_success_body(
                            request,
                            usage,
                            body.as_ref(),
                            upstream_request_id.as_deref(),
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                            trace_id,
                        ));
                    }
                    if is_compact_request
                        && compact_non_success_body_should_be_normalized(
                            status.0,
                            upstream_content_type.as_deref(),
                            body.as_ref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                        )
                    {
                        return Ok(respond_invalid_compact_non_success_body(
                            request,
                            status.0,
                            usage,
                            body.as_ref(),
                            upstream_content_type.as_deref(),
                            upstream_request_id.as_deref(),
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                            trace_id,
                        ));
                    }
                    if status.0 >= 400
                        && non_success_body_should_be_normalized(
                            status.0,
                            upstream_content_type.as_deref(),
                            body.as_ref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                        )
                    {
                        return Ok(respond_normalized_passthrough_non_success_body(
                            request,
                            usage,
                            body.as_ref(),
                            upstream_content_type.as_deref(),
                            upstream_request_id.as_deref(),
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                            trace_id,
                        ));
                    }
                    let len = Some(body.len());
                    let response =
                        Response::new(status, headers, std::io::Cursor::new(body), len, None);
                    let delivery_error = request.respond(response).err().map(|err| err.to_string());
                    return Ok(terminal_bridge_result_with_debug_meta(
                        usage,
                        delivery_error,
                        upstream_error_hint,
                        &upstream_request_id,
                        &upstream_cf_ray,
                        &upstream_auth_error,
                        &upstream_identity_error_code,
                        &upstream_content_type,
                    ));
                }

                let (_, sse_usage) = collect_non_stream_json_from_sse_bytes(upstream_body.as_ref());
                let usage = if is_json {
                    serde_json::from_slice::<Value>(upstream_body.as_ref())
                        .ok()
                        .map(|value| parse_usage_from_json(&value))
                        .unwrap_or_default()
                } else if usage_has_signal(&sse_usage) {
                    sse_usage
                } else {
                    UpstreamResponseUsage::default()
                };
                if status.0 >= 400
                    && non_success_body_should_be_normalized(
                        status.0,
                        upstream_content_type.as_deref(),
                        upstream_body.as_ref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    )
                {
                    return Ok(respond_normalized_passthrough_non_success_body(
                        request,
                        usage,
                        upstream_body.as_ref(),
                        upstream_content_type.as_deref(),
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                        trace_id,
                    ));
                }
                let upstream_error_hint = with_upstream_debug_suffix(
                    extract_error_hint_from_body_or_headers(
                        status.0,
                        upstream_content_type.as_deref(),
                        upstream_body.as_ref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    ),
                    None,
                    upstream_request_id.as_deref(),
                    upstream_cf_ray.as_deref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                );
                if should_suppress_deactivation_delivery(
                    upstream_error_hint.as_deref(),
                    _allow_failover_for_deactivation,
                ) {
                    return Ok(terminal_bridge_result_with_debug_meta(
                        usage,
                        None,
                        upstream_error_hint,
                        &upstream_request_id,
                        &upstream_cf_ray,
                        &upstream_auth_error,
                        &upstream_identity_error_code,
                        &upstream_content_type,
                    ));
                }
                let len = Some(upstream_body.len());
                let response = Response::new(
                    status,
                    headers,
                    std::io::Cursor::new(upstream_body.to_vec()),
                    len,
                    None,
                );
                let delivery_error = request.respond(response).err().map(|err| err.to_string());
                return Ok(terminal_bridge_result_with_debug_meta(
                    usage,
                    delivery_error,
                    upstream_error_hint,
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                ));
            }

            if is_stream && !is_sse && status.0 >= 400 {
                let upstream_body = upstream
                    .read_all_bytes()
                    .map_err(|err| format!("read upstream body failed: {err}"))?;
                let usage = UpstreamResponseUsage::default();
                if non_success_body_should_be_normalized(
                    status.0,
                    upstream_content_type.as_deref(),
                    upstream_body.as_ref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                ) {
                    return Ok(respond_normalized_passthrough_non_success_body(
                        request,
                        usage,
                        upstream_body.as_ref(),
                        upstream_content_type.as_deref(),
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                        trace_id,
                    ));
                }
                let upstream_error_hint = with_upstream_debug_suffix(
                    extract_error_hint_from_body_or_headers(
                        status.0,
                        upstream_content_type.as_deref(),
                        upstream_body.as_ref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    ),
                    None,
                    upstream_request_id.as_deref(),
                    upstream_cf_ray.as_deref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                );
                let len = Some(upstream_body.len());
                let response = Response::new(
                    status,
                    headers,
                    std::io::Cursor::new(upstream_body.to_vec()),
                    len,
                    None,
                );
                let delivery_error = request.respond(response).err().map(|err| err.to_string());
                return Ok(terminal_bridge_result_with_debug_meta(
                    usage,
                    delivery_error,
                    upstream_error_hint,
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                ));
            }

            if is_stream && !is_sse && status.0 < 400 && is_compact_request_path(request_path) {
                let upstream_body = upstream
                    .read_all_bytes()
                    .map_err(|err| format!("read upstream body failed: {err}"))?;
                let usage = UpstreamResponseUsage::default();
                return Ok(respond_compact_success_body(
                    request,
                    status,
                    headers,
                    usage,
                    upstream_body.as_ref(),
                    upstream_request_id.as_deref(),
                    upstream_cf_ray.as_deref(),
                    upstream_auth_error.as_deref(),
                    upstream_identity_error_code.as_deref(),
                    &upstream_content_type,
                    trace_id,
                ));
            }

            if is_sse || is_stream {
                let usage_collector = Arc::new(Mutex::new(PassthroughSseCollector::default()));
                let response_body: Box<dyn std::io::Read + Send> =
                    if request_path.starts_with("/v1/responses") {
                        Box::new(OpenAIResponsesPassthroughSseReader::from_stream_response(
                            upstream,
                            Arc::clone(&usage_collector),
                            keepalive_frame,
                            request_started_at,
                        ))
                    } else {
                        return Err(format!(
                            "stream upstream response is not supported for path {request_path}"
                        ));
                    };
                force_openai_responses_stream_content_type(&mut headers, request_path, is_stream);
                let delivery_error =
                    respond_streaming_chunked(request, status, headers, response_body)
                        .err()
                        .map(|err| err.to_string());
                let collector = usage_collector
                    .lock()
                    .map(|guard| guard.clone())
                    .unwrap_or_default();
                let last_sse_event_type = collector.last_event_type.clone();
                let result = with_bridge_debug_meta(
                    UpstreamResponseBridgeResult {
                        usage: collector.usage,
                        stream_terminal_seen: collector.saw_terminal,
                        stream_terminal_error: collector.terminal_error,
                        delivery_error,
                        upstream_error_hint: with_upstream_debug_suffix(
                            collector.upstream_error_hint,
                            None,
                            upstream_request_id.as_deref(),
                            upstream_cf_ray.as_deref(),
                            upstream_auth_error.as_deref(),
                            upstream_identity_error_code.as_deref(),
                        ),
                        delivered_status_code: None,
                        upstream_request_id: None,
                        upstream_cf_ray: None,
                        upstream_auth_error: None,
                        upstream_identity_error_code: None,
                        upstream_content_type: None,
                        last_sse_event_type: None,
                    },
                    &upstream_request_id,
                    &upstream_cf_ray,
                    &upstream_auth_error,
                    &upstream_identity_error_code,
                    &upstream_content_type,
                    last_sse_event_type,
                );
                log_bridge_stream_diagnostics(response_adapter, request_path, &result);
                return Ok(result);
            }

            let upstream_body = upstream
                .read_all_bytes()
                .map_err(|err| format!("read upstream body failed: {err}"))?;
            let len = Some(upstream_body.len());
            let response = Response::new(
                status,
                headers,
                std::io::Cursor::new(upstream_body.to_vec()),
                len,
                None,
            );
            let delivery_error = request.respond(response).err().map(|err| err.to_string());
            Ok(terminal_bridge_result_with_debug_meta(
                UpstreamResponseUsage::default(),
                delivery_error,
                None,
                &upstream_request_id,
                &upstream_cf_ray,
                &upstream_auth_error,
                &upstream_identity_error_code,
                &upstream_content_type,
            ))
        }
        ResponseAdapter::CompactFromChatCompletions => {
            let status = StatusCode(upstream.status().as_u16());
            let headers = copy_upstream_response_headers(upstream.headers(), trace_id);
            let upstream_body = upstream
                .read_all_bytes()
                .map_err(|err| format!("read upstream body failed: {err}"))?;
            let usage = serde_json::from_slice::<Value>(upstream_body.as_ref())
                .ok()
                .map(|value| parse_usage_from_json(&value))
                .unwrap_or_default();
            let response_body = if status.0 < 400 {
                convert_chat_completions_body_to_compact(upstream_body.as_ref())
                    .unwrap_or_else(|| upstream_body.to_vec())
            } else {
                upstream_body.to_vec()
            };
            let upstream_error_hint = (status.0 >= 400)
                .then(|| {
                    with_upstream_debug_suffix(
                        extract_error_hint_from_body(status.0, upstream_body.as_ref()),
                        None,
                        upstream_request_id.as_deref(),
                        upstream_cf_ray.as_deref(),
                        upstream_auth_error.as_deref(),
                        upstream_identity_error_code.as_deref(),
                    )
                })
                .flatten();
            let delivery_error = respond_json_bytes(request, status, headers, response_body);
            Ok(terminal_bridge_result_with_debug_meta(
                usage,
                delivery_error,
                upstream_error_hint,
                &upstream_request_id,
                &upstream_cf_ray,
                &upstream_auth_error,
                &upstream_identity_error_code,
                &upstream_content_type,
            ))
        }
        ResponseAdapter::AnthropicMessagesFromResponses
        | ResponseAdapter::ResponsesFromAnthropicMessages
        | ResponseAdapter::ChatCompletionsFromResponses
        | ResponseAdapter::ImagesB64JsonFromResponses
        | ResponseAdapter::ImagesUrlFromResponses
        | ResponseAdapter::GeminiJson
        | ResponseAdapter::GeminiCliJson
        | ResponseAdapter::GeminiSse
        | ResponseAdapter::GeminiCliSse => unreachable!(),
    }
}

/// 函数 `resolve_stream_keepalive_frame`
///
/// 作者: gaohongshun
///
/// 时间: 2026-04-02
///
/// # 参数
/// - response_adapter: 参数 response_adapter
/// - request_path: 参数 request_path
///
/// # 返回
/// 返回函数执行结果
fn resolve_stream_keepalive_frame(
    response_adapter: ResponseAdapter,
    request_path: &str,
) -> SseKeepAliveFrame {
    match response_adapter {
        ResponseAdapter::Passthrough => {
            if request_path.starts_with("/v1/responses") {
                SseKeepAliveFrame::OpenAIResponses
            } else {
                SseKeepAliveFrame::Comment
            }
        }
        ResponseAdapter::AnthropicMessagesFromResponses
        | ResponseAdapter::ResponsesFromAnthropicMessages
        | ResponseAdapter::ChatCompletionsFromResponses
        | ResponseAdapter::CompactFromChatCompletions
        | ResponseAdapter::ImagesB64JsonFromResponses
        | ResponseAdapter::ImagesUrlFromResponses
        | ResponseAdapter::GeminiJson
        | ResponseAdapter::GeminiCliJson
        | ResponseAdapter::GeminiSse
        | ResponseAdapter::GeminiCliSse => SseKeepAliveFrame::Comment,
    }
}

#[cfg(test)]
mod tests {
    use super::super::body_conversion::{
        convert_chat_completions_body_to_compact, convert_responses_body_to_chat_completions,
        convert_responses_body_to_gemini_generate_content, convert_responses_body_to_images,
        gemini_cli_wrap_response_envelope, merge_usage_from_body_without_output_text,
    };
    use super::super::compact_errors::{
        build_passthrough_non_success_message, classify_compact_non_success_kind,
        compact_non_success_body_should_be_normalized, compact_success_body_is_valid,
    };
    use super::{
        collect_non_stream_json_from_sse_bytes, force_openai_responses_stream_content_type,
        response_adapter_uses_manual_chunked_streaming, write_streaming_chunked_response,
        ImagesResponseFormat, ResponseAdapter, StatusCode,
    };
    use serde_json::json;
    use std::io::{Read, Write};
    use tiny_http::{HTTPVersion, Header};

    struct ChunkedTestReader {
        chunks: Vec<&'static [u8]>,
        index: usize,
    }

    impl ChunkedTestReader {
        fn new(chunks: Vec<&'static [u8]>) -> Self {
            Self { chunks, index: 0 }
        }
    }

    impl Read for ChunkedTestReader {
        fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
            let Some(chunk) = self.chunks.get(self.index) else {
                return Ok(0);
            };
            self.index += 1;
            let read = chunk.len().min(buf.len());
            buf[..read].copy_from_slice(&chunk[..read]);
            Ok(read)
        }
    }

    #[derive(Default)]
    struct FlushCountingWriter {
        bytes: Vec<u8>,
        flushes: usize,
    }

    impl Write for FlushCountingWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.bytes.extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            self.flushes += 1;
            Ok(())
        }
    }

    #[test]
    fn streaming_chunked_response_flushes_each_read_chunk() {
        let mut writer = FlushCountingWriter::default();
        let headers = vec![
            Header::from_bytes("Content-Type", "text/event-stream").expect("content-type header"),
            Header::from_bytes("Content-Length", "999").expect("content-length header"),
        ];
        let body = ChunkedTestReader::new(vec![b"data: a\n\n", b"data: b\n\n"]);

        write_streaming_chunked_response(
            &mut writer,
            &HTTPVersion(1, 1),
            StatusCode(200),
            &headers,
            body,
            false,
        )
        .expect("write streaming response");

        let output = String::from_utf8(writer.bytes).expect("utf8 response");
        assert!(output.contains("HTTP/1.1 200 OK\r\n"));
        assert!(output.contains("Content-Type: text/event-stream\r\n"));
        assert!(output.contains("X-Accel-Buffering: no\r\n"));
        assert!(output.contains("Transfer-Encoding: chunked\r\n"));
        assert!(!output.to_ascii_lowercase().contains("content-length: 999"));
        assert!(output.contains("9\r\ndata: a\n\n\r\n"));
        assert!(output.contains("9\r\ndata: b\n\n\r\n"));
        assert!(output.ends_with("0\r\n\r\n"));
        assert!(writer.flushes >= 4);
    }

    #[test]
    fn responses_from_anthropic_streaming_uses_manual_chunked_delivery() {
        assert!(response_adapter_uses_manual_chunked_streaming(
            ResponseAdapter::ResponsesFromAnthropicMessages
        ));
    }

    /// 函数 `compact_header_only_identity_error_is_normalized_and_classified`
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
    fn compact_header_only_identity_error_is_normalized_and_classified() {
        assert!(compact_non_success_body_should_be_normalized(
            403,
            Some("text/plain"),
            b"",
            None,
            Some("org_membership_required"),
        ));
        assert_eq!(
            classify_compact_non_success_kind(
                403,
                Some("text/plain"),
                b"",
                None,
                None,
                Some("org_membership_required"),
            ),
            "identity_error"
        );
    }

    /// 函数 `compact_header_only_cf_ray_is_classified_as_cloudflare_edge`
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
    fn compact_header_only_cf_ray_is_classified_as_cloudflare_edge() {
        assert_eq!(
            classify_compact_non_success_kind(
                502,
                Some("text/plain"),
                b"",
                Some("ray_compact_edge"),
                None,
                None,
            ),
            "cloudflare_edge"
        );
    }

    #[test]
    fn compact_success_body_matches_official_compact_response_shape() {
        assert!(compact_success_body_is_valid(
            json!({
                "output": [
                    {
                        "type": "message",
                        "role": "user",
                        "content": [{ "type": "input_text", "text": "keep context" }]
                    },
                    {
                        "type": "compaction",
                        "encrypted_content": "summary_payload"
                    }
                ]
            })
            .to_string()
            .as_bytes()
        ));
        assert!(compact_success_body_is_valid(
            json!({
                "output": [
                    {
                        "type": "context_compaction",
                        "encrypted_content": "summary_payload"
                    }
                ]
            })
            .to_string()
            .as_bytes()
        ));
        assert!(compact_success_body_is_valid(
            json!({ "output": [] }).to_string().as_bytes()
        ));
        assert!(compact_success_body_is_valid(
            json!({
                "output": [
                    {
                        "type": "message",
                        "role": "assistant",
                        "content": [{ "type": "output_text", "text": "done" }]
                    }
                ]
            })
            .to_string()
            .as_bytes()
        ));
        assert!(!compact_success_body_is_valid(
            json!({ "id": "resp_missing_output" })
                .to_string()
                .as_bytes()
        ));
        assert!(!compact_success_body_is_valid(
            json!({
                "output": [
                    {
                        "type": "message",
                        "role": "assistant"
                    }
                ]
            })
            .to_string()
            .as_bytes()
        ));
    }

    #[test]
    fn chat_completions_body_converts_to_compact_response_shape() {
        let body = json!({
            "id": "chatcmpl_custom_compact",
            "object": "chat.completion",
            "choices": [{
                "index": 0,
                "message": {
                    "role": "assistant",
                    "content": "压缩摘要"
                },
                "finish_reason": "stop"
            }]
        });
        let converted = convert_chat_completions_body_to_compact(body.to_string().as_bytes())
            .expect("convert chat completions response");

        assert!(compact_success_body_is_valid(converted.as_slice()));
        let value: serde_json::Value =
            serde_json::from_slice(converted.as_slice()).expect("compact json");
        assert_eq!(value["output"][0]["type"], "message");
        assert_eq!(value["output"][0]["role"], "assistant");
        assert_eq!(value["output"][0]["content"][0]["type"], "output_text");
        assert_eq!(value["output"][0]["content"][0]["text"], "压缩摘要");
    }

    #[test]
    fn header_only_cloudflare_challenge_uses_stable_hint() {
        let message = build_passthrough_non_success_message(
            502,
            Some("text/html; charset=utf-8"),
            b"",
            Some("req-header-only"),
            Some("ray-header-only"),
            None,
            None,
        );

        assert!(message.contains("Cloudflare 安全验证页"));
        assert!(message.contains("cf_ray=ray-header-only"));
    }

    #[test]
    fn cloudflare_html_preview_keeps_title_hint() {
        let message = build_passthrough_non_success_message(
            502,
            Some("text/html; charset=utf-8"),
            b"<html><head><title>Just a moment...</title></head><body>Cloudflare</body></html>",
            Some("req-preview"),
            Some("ray-preview"),
            None,
            None,
        );

        assert!(message.contains("Cloudflare 安全验证页（title=Just a moment...）"));
        assert!(message.contains("cf_ray=ray-preview"));
    }

    #[test]
    fn streaming_responses_passthrough_forces_sse_content_type() {
        let mut headers = vec![
            Header::from_bytes(
                b"Content-Type".as_slice(),
                b"application/json; charset=utf-8".as_slice(),
            )
            .expect("content-type header"),
            Header::from_bytes(b"x-request-id".as_slice(), b"req_test".as_slice())
                .expect("request id header"),
        ];

        force_openai_responses_stream_content_type(&mut headers, "/v1/responses", true);

        let content_type = headers
            .iter()
            .find(|header| {
                header
                    .field
                    .as_str()
                    .as_str()
                    .eq_ignore_ascii_case("Content-Type")
            })
            .map(|header| header.value.as_str());
        assert_eq!(content_type, Some("text/event-stream"));
        assert!(headers
            .iter()
            .any(|header| header.field.as_str().as_str() == "x-request-id"));
    }

    #[test]
    fn non_stream_gemini_response_preserves_function_call_id_and_top_level_function_calls() {
        let body = json!({
            "id": "resp_non_stream_tool",
            "model": "gpt-5.4",
            "output": [{
                "type": "function_call",
                "call_id": "call_non_stream_write",
                "name": "write_file",
                "arguments": "{\"path\":\"plan.md\"}"
            }],
            "usage": { "input_tokens": 1, "output_tokens": 1, "total_tokens": 2 }
        });

        let mapped = convert_responses_body_to_gemini_generate_content(
            serde_json::to_vec(&body).expect("body").as_slice(),
            false,
            None,
        )
        .expect("convert gemini body");
        let value: serde_json::Value = serde_json::from_slice(&mapped).expect("parse mapped body");

        assert_eq!(
            value["candidates"][0]["content"]["parts"][0]["functionCall"]["id"],
            "call_non_stream_write"
        );
        assert_eq!(value["functionCalls"][0]["id"], "call_non_stream_write");
        assert_eq!(value["functionCalls"][0]["args"]["path"], "plan.md");
    }

    #[test]
    fn non_stream_chat_completion_response_adds_image_generation_message_images() {
        let body = json!({
            "id": "resp_non_stream_image",
            "model": "gpt-5.4",
            "output": [{
                "type": "image_generation_call",
                "id": "ig_non_stream_1",
                "status": "completed",
                "output_format": "png",
                "result": "aGVsbG8="
            }],
            "usage": { "input_tokens": 2, "output_tokens": 1, "total_tokens": 3 }
        });

        let mapped = convert_responses_body_to_chat_completions(
            serde_json::to_vec(&body).expect("body").as_slice(),
        )
        .expect("convert chat completion body");
        let value: serde_json::Value = serde_json::from_slice(&mapped).expect("parse mapped body");

        assert_eq!(
            value["choices"][0]["message"]["images"][0]["type"],
            "image_url"
        );
        assert_eq!(
            value["choices"][0]["message"]["images"][0]["image_url"]["url"],
            "data:image/png;base64,aGVsbG8="
        );
        assert_eq!(
            value["usage"]["prompt_tokens"],
            serde_json::Value::Number(2.into())
        );
    }

    #[test]
    fn non_stream_chat_completion_response_preserves_reasoning_content() {
        let body = json!({
            "id": "resp_non_stream_reasoning",
            "model": "gpt-5.4",
            "output": [{
                "type": "reasoning",
                "id": "rs_non_stream_1",
                "summary": [{
                    "type": "summary_text",
                    "text": "先读配置"
                }]
            }],
            "usage": { "input_tokens": 4, "output_tokens": 2, "total_tokens": 6 }
        });

        let mapped = convert_responses_body_to_chat_completions(
            serde_json::to_vec(&body).expect("body").as_slice(),
        )
        .expect("convert chat completion body");
        let value: serde_json::Value = serde_json::from_slice(&mapped).expect("parse mapped body");

        assert_eq!(value["choices"][0]["message"]["content"], "");
        assert_eq!(
            value["choices"][0]["message"]["reasoning_content"],
            "先读配置"
        );
        assert_eq!(value["choices"][0]["message"]["reasoning"], "先读配置");
        assert_eq!(
            value["usage"]["prompt_tokens"],
            serde_json::Value::Number(4.into())
        );
    }

    #[test]
    fn non_stream_chat_completion_response_preserves_answer_and_reasoning_content() {
        let body = json!({
            "id": "resp_non_stream_text_and_reasoning",
            "model": "gpt-5.4",
            "output_text": "OK",
            "output": [{
                "type": "reasoning",
                "id": "rs_non_stream_1",
                "summary": [{
                    "type": "summary_text",
                    "text": "先想一下"
                }]
            }]
        });

        let mapped = convert_responses_body_to_chat_completions(
            serde_json::to_vec(&body).expect("body").as_slice(),
        )
        .expect("convert chat completion body");
        let value: serde_json::Value = serde_json::from_slice(&mapped).expect("parse mapped body");

        assert_eq!(value["choices"][0]["message"]["content"], "OK");
        assert_eq!(
            value["choices"][0]["message"]["reasoning_content"],
            "先想一下"
        );
    }

    #[test]
    fn non_stream_chat_responses_sse_json_mode_returns_single_parseable_content() {
        let sse = concat!(
            "event: response.output_text.delta\n",
            "data: {\"response_id\":\"resp_non_stream_json\",\"delta\":\"{\\\"answer\\\":true}\"}\n\n",
            "event: response.output_item.done\n",
            "data: {\"response_id\":\"resp_non_stream_json\",\"output_index\":0,\"item\":{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"{\\\"answer\\\":true}\"}]}}\n\n",
            "event: response.completed\n",
            "data: {\"response\":{\"id\":\"resp_non_stream_json\",\"created\":3,\"model\":\"gpt-5.3-codex\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"{\\\"answer\\\":true}\"}]}],\"usage\":{\"input_tokens\":3,\"output_tokens\":2,\"total_tokens\":5}}}\n\n",
            "data: [DONE]\n\n"
        );
        let (body, _) = collect_non_stream_json_from_sse_bytes(sse.as_bytes());
        let body = body.expect("synthesized response json");
        let mapped = convert_responses_body_to_chat_completions(body.as_slice())
            .expect("convert chat completion body");
        let value: serde_json::Value =
            serde_json::from_slice(&mapped).expect("parse chat completion body");
        let content = value["choices"][0]["message"]["content"]
            .as_str()
            .expect("chat message content");

        assert_eq!(content, r#"{"answer":true}"#);
        let parsed: serde_json::Value =
            serde_json::from_str(content).expect("chat content is a single json document");
        assert_eq!(parsed["answer"], true);
    }

    #[test]
    fn sse_synthesized_body_usage_merge_does_not_duplicate_output_text() {
        let sse = concat!(
            "event: response.output_text.delta\n",
            "data: {\"response_id\":\"resp_usage_no_dup\",\"delta\":\"{\\\"answer\\\":true}\"}\n\n",
            "event: response.completed\n",
            "data: {\"response\":{\"id\":\"resp_usage_no_dup\",\"created\":3,\"model\":\"gpt-5.3-codex\",\"output\":[{\"type\":\"message\",\"role\":\"assistant\",\"content\":[{\"type\":\"output_text\",\"text\":\"{\\\"answer\\\":true}\"}]}],\"usage\":{\"input_tokens\":3,\"output_tokens\":2,\"total_tokens\":5}}}\n\n",
            "data: [DONE]\n\n"
        );
        let (body, mut usage) = collect_non_stream_json_from_sse_bytes(sse.as_bytes());
        let body = body.expect("synthesized response json");

        merge_usage_from_body_without_output_text(&mut usage, body.as_slice());

        assert_eq!(usage.output_text.as_deref(), Some(r#"{"answer":true}"#));
        assert_eq!(usage.input_tokens, Some(3));
        assert_eq!(usage.output_tokens, Some(2));
        assert_eq!(usage.total_tokens, Some(5));
    }

    #[test]
    fn non_stream_images_response_builds_b64_json_payload() {
        let body = json!({
            "id": "resp_images_1",
            "created_at": 1775900000,
            "model": "gpt-5.4",
            "output": [{
                "type": "image_generation_call",
                "id": "ig_1",
                "status": "completed",
                "revised_prompt": "一只极简猫",
                "output_format": "png",
                "size": "1024x1024",
                "quality": "high",
                "background": "transparent",
                "result": "aGVsbG8="
            }],
            "tool_usage": {
                "image_gen": { "input_tokens": 1, "output_tokens": 2, "total_tokens": 3 }
            }
        });

        let mapped = convert_responses_body_to_images(
            serde_json::to_vec(&body).expect("body").as_slice(),
            ImagesResponseFormat::B64Json,
        )
        .expect("convert images body");
        let value: serde_json::Value = serde_json::from_slice(&mapped).expect("parse images body");

        assert_eq!(value["created"], 1775900000);
        assert_eq!(value["data"][0]["b64_json"], "aGVsbG8=");
        assert_eq!(value["data"][0]["revised_prompt"], "一只极简猫");
        assert_eq!(value["size"], "1024x1024");
        assert_eq!(value["quality"], "high");
        assert_eq!(value["background"], "transparent");
        assert_eq!(value["output_format"], "png");
        assert_eq!(value["usage"]["total_tokens"], 3);
    }

    #[test]
    fn non_stream_images_response_builds_url_payload() {
        let body = json!({
            "created": 1775900001,
            "output": [{
                "type": "image_generation_call",
                "output_format": "webp",
                "result": "aGVsbG8="
            }]
        });

        let mapped = convert_responses_body_to_images(
            serde_json::to_vec(&body).expect("body").as_slice(),
            ImagesResponseFormat::Url,
        )
        .expect("convert images body");
        let value: serde_json::Value = serde_json::from_slice(&mapped).expect("parse images body");

        assert_eq!(value["data"][0]["url"], "data:image/webp;base64,aGVsbG8=");
    }

    #[test]
    fn non_stream_gemini_response_decodes_double_encoded_function_call_arguments() {
        let body = json!({
            "id": "resp_non_stream_double_encoded_tool",
            "model": "gpt-5.4",
            "output": [{
                "type": "function_call",
                "call_id": "call_non_stream_double_encoded_write",
                "name": "write_file",
                "arguments": "\"{\\\"file_path\\\":\\\"C:/Users/test/Desktop/test/gemini/plan.md\\\",\\\"content\\\":\\\"plan\\\"}\""
            }],
            "usage": { "input_tokens": 1, "output_tokens": 1, "total_tokens": 2 }
        });

        let mapped = convert_responses_body_to_gemini_generate_content(
            serde_json::to_vec(&body).expect("body").as_slice(),
            false,
            None,
        )
        .expect("convert gemini body");
        let value: serde_json::Value = serde_json::from_slice(&mapped).expect("parse mapped body");

        assert_eq!(
            value["candidates"][0]["content"]["parts"][0]["functionCall"]["args"]["file_path"],
            "C:/Users/test/Desktop/test/gemini/plan.md"
        );
        assert_eq!(value["functionCalls"][0]["args"]["content"], "plan");
    }

    #[test]
    fn gemini_cli_wrap_response_envelope_is_enabled_for_gemini_adapter_only() {
        assert!(gemini_cli_wrap_response_envelope(
            ResponseAdapter::GeminiCliJson
        ));
        assert!(gemini_cli_wrap_response_envelope(
            ResponseAdapter::GeminiCliSse
        ));
        assert!(!gemini_cli_wrap_response_envelope(
            ResponseAdapter::AnthropicMessagesFromResponses
        ));
        assert!(!gemini_cli_wrap_response_envelope(
            ResponseAdapter::ChatCompletionsFromResponses
        ));
        assert!(!gemini_cli_wrap_response_envelope(
            ResponseAdapter::Passthrough
        ));
    }
}
