use super::super::ResponseAdapter;
use super::{UpstreamResponseBridgeResult, UpstreamResponseUsage};
use tiny_http::Header;

pub(super) const REQUEST_ID_HEADER_CANDIDATES: &[&str] = &["x-request-id", "x-oai-request-id"];
pub(super) const CF_RAY_HEADER_NAME: &str = "cf-ray";
pub(super) const AUTH_ERROR_HEADER_NAME: &str = "x-openai-authorization-error";

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct UpstreamResponseMetadata {
    pub(super) request_id: Option<String>,
    pub(super) cf_ray: Option<String>,
    pub(super) auth_error: Option<String>,
    pub(super) identity_error_code: Option<String>,
    pub(super) content_type: Option<String>,
    pub(super) is_sse: bool,
    pub(super) is_json: bool,
}

pub(super) fn upstream_response_metadata(
    headers: &reqwest::header::HeaderMap,
) -> UpstreamResponseMetadata {
    let content_type = headers
        .get(reqwest::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string);
    let normalized_content_type = content_type.as_deref().map(str::to_ascii_lowercase);

    UpstreamResponseMetadata {
        request_id: first_upstream_header(headers, REQUEST_ID_HEADER_CANDIDATES),
        cf_ray: first_upstream_header(headers, &[CF_RAY_HEADER_NAME]),
        auth_error: first_upstream_header(headers, &[AUTH_ERROR_HEADER_NAME]),
        identity_error_code: crate::gateway::extract_identity_error_code_from_headers(headers),
        content_type,
        is_sse: normalized_content_type
            .as_deref()
            .is_some_and(|value| value.starts_with("text/event-stream")),
        is_json: normalized_content_type
            .as_deref()
            .is_some_and(|value| value.contains("application/json")),
    }
}

pub(super) fn first_upstream_header(
    headers: &reqwest::header::HeaderMap,
    names: &[&str],
) -> Option<String> {
    names.iter().find_map(|name| {
        headers
            .get(*name)
            .and_then(|value| value.to_str().ok())
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(str::to_string)
    })
}

pub(super) fn copy_upstream_response_headers(
    upstream_headers: &reqwest::header::HeaderMap,
    trace_id: Option<&str>,
) -> Vec<Header> {
    let mut headers = Vec::new();
    for (name, value) in upstream_headers.iter() {
        let name_str = name.as_str();
        if name_str.eq_ignore_ascii_case("transfer-encoding")
            || name_str.eq_ignore_ascii_case("content-length")
            || name_str.eq_ignore_ascii_case("connection")
        {
            continue;
        }
        if let Ok(header) = Header::from_bytes(name_str.as_bytes(), value.as_bytes()) {
            headers.push(header);
        }
    }
    push_trace_id_header(&mut headers, trace_id);
    headers
}

fn push_trace_id_header(headers: &mut Vec<Header>, trace_id: Option<&str>) {
    let Some(trace_id) = trace_id.map(str::trim).filter(|value| !value.is_empty()) else {
        return;
    };
    if let Ok(header) = Header::from_bytes(
        crate::error_codes::TRACE_ID_HEADER_NAME.as_bytes(),
        trace_id.as_bytes(),
    ) {
        headers.push(header);
    }
}

pub(super) fn with_bridge_debug_meta(
    mut result: UpstreamResponseBridgeResult,
    upstream_request_id: &Option<String>,
    upstream_cf_ray: &Option<String>,
    upstream_auth_error: &Option<String>,
    upstream_identity_error_code: &Option<String>,
    upstream_content_type: &Option<String>,
    last_sse_event_type: Option<String>,
) -> UpstreamResponseBridgeResult {
    result.upstream_request_id = upstream_request_id.clone();
    result.upstream_cf_ray = upstream_cf_ray.clone();
    result.upstream_auth_error = upstream_auth_error.clone();
    result.upstream_identity_error_code = upstream_identity_error_code.clone();
    result.upstream_content_type = upstream_content_type.clone();
    result.last_sse_event_type = last_sse_event_type;
    result
}

pub(super) fn terminal_bridge_result_with_debug_meta(
    usage: UpstreamResponseUsage,
    delivery_error: Option<String>,
    upstream_error_hint: Option<String>,
    upstream_request_id: &Option<String>,
    upstream_cf_ray: &Option<String>,
    upstream_auth_error: &Option<String>,
    upstream_identity_error_code: &Option<String>,
    upstream_content_type: &Option<String>,
) -> UpstreamResponseBridgeResult {
    with_bridge_debug_meta(
        UpstreamResponseBridgeResult {
            usage,
            stream_terminal_seen: true,
            delivery_error,
            upstream_error_hint,
            ..UpstreamResponseBridgeResult::default()
        },
        upstream_request_id,
        upstream_cf_ray,
        upstream_auth_error,
        upstream_identity_error_code,
        upstream_content_type,
        None,
    )
}

pub(super) fn log_bridge_stream_diagnostics(
    response_adapter: ResponseAdapter,
    request_path: &str,
    result: &UpstreamResponseBridgeResult,
) {
    if result.delivery_error.is_none()
        && result.stream_terminal_seen
        && result.stream_terminal_error.is_none()
    {
        return;
    }

    log::warn!(
        "event=gateway_bridge_stream_diagnostics adapter={:?} path={} stream_terminal_seen={} stream_terminal_error={} delivery_error={} upstream_error_hint={} last_sse_event_type={} upstream_request_id={} upstream_cf_ray={} upstream_content_type={}",
        response_adapter,
        request_path,
        if result.stream_terminal_seen { "true" } else { "false" },
        result.stream_terminal_error.as_deref().unwrap_or("-"),
        result.delivery_error.as_deref().unwrap_or("-"),
        result.upstream_error_hint.as_deref().unwrap_or("-"),
        result.last_sse_event_type.as_deref().unwrap_or("-"),
        result.upstream_request_id.as_deref().unwrap_or("-"),
        result.upstream_cf_ray.as_deref().unwrap_or("-"),
        result.upstream_content_type.as_deref().unwrap_or("-"),
    );
}

#[cfg(test)]
mod tests {
    use super::{copy_upstream_response_headers, upstream_response_metadata};
    use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};

    #[test]
    fn upstream_response_metadata_extracts_debug_headers_and_content_flags() {
        let mut upstream = HeaderMap::new();
        upstream.insert("x-oai-request-id", HeaderValue::from_static(" req-123 "));
        upstream.insert("cf-ray", HeaderValue::from_static("cf-123"));
        upstream.insert(
            "x-openai-authorization-error",
            HeaderValue::from_static("quota exceeded"),
        );
        upstream.insert(
            "x-error-json",
            HeaderValue::from_static("{\"identity_error_code\":\"revoked\"}"),
        );
        upstream.insert(
            CONTENT_TYPE,
            HeaderValue::from_static("text/event-stream; charset=utf-8"),
        );

        let metadata = upstream_response_metadata(&upstream);

        assert_eq!(metadata.request_id.as_deref(), Some("req-123"));
        assert_eq!(metadata.cf_ray.as_deref(), Some("cf-123"));
        assert_eq!(metadata.auth_error.as_deref(), Some("quota exceeded"));
        assert_eq!(metadata.identity_error_code.as_deref(), Some("revoked"));
        assert_eq!(
            metadata.content_type.as_deref(),
            Some("text/event-stream; charset=utf-8")
        );
        assert!(metadata.is_sse);
        assert!(!metadata.is_json);
    }

    #[test]
    fn copy_upstream_response_headers_filters_hop_by_hop_and_adds_trace_id() {
        let mut upstream = HeaderMap::new();
        upstream.insert("content-type", HeaderValue::from_static("application/json"));
        upstream.insert("transfer-encoding", HeaderValue::from_static("chunked"));
        upstream.insert("content-length", HeaderValue::from_static("12"));
        upstream.insert("connection", HeaderValue::from_static("keep-alive"));

        let headers = copy_upstream_response_headers(&upstream, Some(" trace-123 "));
        let names = headers
            .iter()
            .map(|header| header.field.as_str().as_str().to_ascii_lowercase())
            .collect::<Vec<_>>();

        assert!(names.contains(&"content-type".to_string()));
        assert!(names.contains(&crate::error_codes::TRACE_ID_HEADER_NAME.to_ascii_lowercase()));
        assert!(!names.contains(&"transfer-encoding".to_string()));
        assert!(!names.contains(&"content-length".to_string()));
        assert!(!names.contains(&"connection".to_string()));
    }
}
