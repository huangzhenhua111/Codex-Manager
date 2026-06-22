use serde::Deserialize;
use serde_json::Value;

use super::{extract_error_hint_from_body, extract_error_message_from_json};

pub(super) fn compact_debug_suffix(
    kind: Option<&str>,
    request_id: Option<&str>,
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> String {
    let mut details = Vec::new();
    if let Some(kind) = kind.map(str::trim).filter(|value| !value.is_empty()) {
        details.push(format!("kind={kind}"));
    }
    if let Some(request_id) = request_id.map(str::trim).filter(|value| !value.is_empty()) {
        details.push(format!("request_id={request_id}"));
    }
    if let Some(cf_ray) = cf_ray.map(str::trim).filter(|value| !value.is_empty()) {
        details.push(format!("cf_ray={cf_ray}"));
    }
    if let Some(auth_error) = auth_error.map(str::trim).filter(|value| !value.is_empty()) {
        details.push(format!("auth_error={auth_error}"));
    }
    if let Some(identity_error_code) = identity_error_code
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        details.push(format!("identity_error_code={identity_error_code}"));
    }
    if details.is_empty() {
        String::new()
    } else {
        format!(" [{}]", details.join(", "))
    }
}

pub(super) fn with_upstream_debug_suffix(
    message: Option<String>,
    kind: Option<&str>,
    request_id: Option<&str>,
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> Option<String> {
    let message = message?;
    let suffix = compact_debug_suffix(kind, request_id, cf_ray, auth_error, identity_error_code);
    if suffix.is_empty() {
        Some(message)
    } else {
        Some(format!("{message}{suffix}"))
    }
}

fn looks_like_blocked_marker(value: &str) -> bool {
    let normalized = value.trim().to_ascii_lowercase();
    normalized.contains("blocked")
        || normalized.contains("unsupported_country_region_territory")
        || normalized.contains("unsupported_country")
        || normalized.contains("region_restricted")
}

fn body_as_trimmed_text(body: &[u8]) -> Option<&str> {
    std::str::from_utf8(body)
        .ok()
        .map(str::trim)
        .filter(|value| !value.is_empty())
}

fn text_looks_like_html(text: &str) -> bool {
    let normalized = text.trim().to_ascii_lowercase();
    normalized.contains("<html")
        || normalized.contains("<!doctype html")
        || normalized.contains("<body")
        || normalized.contains("</html>")
}

fn body_looks_like_html(body: &[u8]) -> bool {
    body_as_trimmed_text(body).is_some_and(text_looks_like_html)
}

fn body_looks_like_cloudflare_challenge(status_code: u16, body: &[u8]) -> bool {
    body_as_trimmed_text(body).is_some_and(|text| {
        let normalized = text.to_ascii_lowercase();
        let looks_like_challenge = normalized.contains("cloudflare")
            || normalized.contains("cf-chl")
            || normalized.contains("just a moment")
            || normalized.contains("attention required")
            || normalized.contains("captcha")
            || normalized.contains("security check")
            || normalized.contains("access denied")
            || normalized.contains("waf");
        looks_like_challenge || (text_looks_like_html(text) && matches!(status_code, 401 | 403))
    })
}

fn classify_compact_invalid_success_kind(
    body: &[u8],
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> &'static str {
    if auth_error.is_some_and(looks_like_blocked_marker)
        || identity_error_code.is_some_and(looks_like_blocked_marker)
    {
        return "cloudflare_blocked";
    }
    if body_looks_like_cloudflare_challenge(502, body) {
        return "cloudflare_challenge";
    }
    if body_looks_like_html(body) {
        return "html";
    }
    if identity_error_code.is_some() {
        return "identity_error";
    }
    if auth_error.is_some() {
        return "auth_error";
    }
    if cf_ray.is_some() {
        return "cloudflare_edge";
    }
    if serde_json::from_slice::<Value>(body).is_ok() {
        "invalid_success_body"
    } else if body.is_empty() {
        "empty"
    } else {
        "non_json"
    }
}

pub(super) fn classify_compact_non_success_kind(
    status_code: u16,
    content_type: Option<&str>,
    body: &[u8],
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> &'static str {
    if auth_error.is_some_and(looks_like_blocked_marker)
        || identity_error_code.is_some_and(looks_like_blocked_marker)
    {
        return "cloudflare_blocked";
    }
    if body_looks_like_cloudflare_challenge(status_code, body) {
        return "cloudflare_challenge";
    }
    if body_looks_like_html(body) {
        return "html";
    }
    if content_type
        .map(crate::gateway::is_html_content_type)
        .unwrap_or(false)
    {
        return "html";
    }
    if identity_error_code.is_some() {
        return "identity_error";
    }
    if auth_error.is_some() {
        return "auth_error";
    }
    if cf_ray.is_some() {
        return "cloudflare_edge";
    }
    if serde_json::from_slice::<Value>(body).is_ok() {
        "json_error"
    } else if body.is_empty() {
        "empty"
    } else {
        "non_json"
    }
}

pub(super) fn compact_success_body_is_valid(body: &[u8]) -> bool {
    serde_json::from_slice::<CompactHistoryResponse>(body).is_ok()
}

#[derive(Debug, Deserialize)]
struct CompactHistoryResponse {
    #[allow(dead_code)]
    output: Vec<ResponseItem>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum ResponseItem {
    Message {
        #[allow(dead_code)]
        role: String,
        #[allow(dead_code)]
        content: Vec<Value>,
    },
    Reasoning {
        #[allow(dead_code)]
        summary: Vec<Value>,
        #[allow(dead_code)]
        encrypted_content: Option<String>,
        #[allow(dead_code)]
        content: Option<Vec<Value>>,
    },
    LocalShellCall {
        #[allow(dead_code)]
        call_id: Option<String>,
        #[allow(dead_code)]
        status: Value,
        #[allow(dead_code)]
        action: Value,
    },
    FunctionCall {
        #[allow(dead_code)]
        name: String,
        #[allow(dead_code)]
        namespace: Option<String>,
        #[allow(dead_code)]
        arguments: String,
        #[allow(dead_code)]
        call_id: String,
    },
    ToolSearchCall {
        #[allow(dead_code)]
        call_id: Option<String>,
        #[allow(dead_code)]
        status: Option<String>,
        #[allow(dead_code)]
        execution: String,
        #[allow(dead_code)]
        arguments: Value,
    },
    FunctionCallOutput {
        #[allow(dead_code)]
        call_id: String,
        #[allow(dead_code)]
        output: Value,
    },
    CustomToolCall {
        #[allow(dead_code)]
        status: Option<String>,
        #[allow(dead_code)]
        call_id: String,
        #[allow(dead_code)]
        name: String,
        #[allow(dead_code)]
        input: String,
    },
    CustomToolCallOutput {
        #[allow(dead_code)]
        call_id: String,
        #[allow(dead_code)]
        name: Option<String>,
        #[allow(dead_code)]
        output: Value,
    },
    ToolSearchOutput {
        #[allow(dead_code)]
        call_id: Option<String>,
        #[allow(dead_code)]
        status: String,
        #[allow(dead_code)]
        execution: String,
        #[allow(dead_code)]
        tools: Vec<Value>,
    },
    WebSearchCall {
        #[allow(dead_code)]
        status: Option<String>,
        #[allow(dead_code)]
        action: Option<Value>,
    },
    ImageGenerationCall {
        #[allow(dead_code)]
        id: String,
        #[allow(dead_code)]
        status: String,
        #[allow(dead_code)]
        revised_prompt: Option<String>,
        #[allow(dead_code)]
        result: String,
    },
    #[serde(alias = "compaction_summary")]
    Compaction {
        #[allow(dead_code)]
        encrypted_content: String,
    },
    CompactionTrigger,
    ContextCompaction {
        #[allow(dead_code)]
        encrypted_content: Option<String>,
    },
    #[serde(other)]
    Other,
}

pub(super) fn build_invalid_compact_success_message(
    body: &[u8],
    request_id: Option<&str>,
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> String {
    let kind = classify_compact_invalid_success_kind(body, cf_ray, auth_error, identity_error_code);
    if let Ok(value) = serde_json::from_slice::<Value>(body) {
        if let Some(message) = extract_error_message_from_json(&value) {
            return format!(
                "invalid upstream compact response: {message}{}",
                compact_debug_suffix(
                    Some(kind),
                    request_id,
                    cf_ray,
                    auth_error,
                    identity_error_code
                )
            );
        }
    }
    if let Some(hint) = extract_error_hint_from_body(502, body) {
        return format!(
            "invalid upstream compact response: {hint}{}",
            compact_debug_suffix(
                Some(kind),
                request_id,
                cf_ray,
                auth_error,
                identity_error_code
            )
        );
    }
    format!(
        "invalid upstream compact response: cannot parse compact output{}",
        compact_debug_suffix(
            Some(kind),
            request_id,
            cf_ray,
            auth_error,
            identity_error_code
        )
    )
}

pub(super) fn non_success_body_should_be_normalized(
    status_code: u16,
    content_type: Option<&str>,
    body: &[u8],
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> bool {
    if status_code < 400 {
        return false;
    }
    if auth_error
        .map(str::trim)
        .is_some_and(|value| !value.is_empty())
        || identity_error_code
            .map(str::trim)
            .is_some_and(|value| !value.is_empty())
    {
        return true;
    }
    if content_type
        .map(crate::gateway::is_html_content_type)
        .unwrap_or(false)
    {
        return true;
    }
    body_looks_like_cloudflare_challenge(status_code, body) || body_looks_like_html(body)
}

pub(super) fn compact_non_success_body_should_be_normalized(
    status_code: u16,
    content_type: Option<&str>,
    body: &[u8],
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> bool {
    non_success_body_should_be_normalized(
        status_code,
        content_type,
        body,
        auth_error,
        identity_error_code,
    )
}

pub(super) fn build_passthrough_non_success_message(
    status_code: u16,
    content_type: Option<&str>,
    body: &[u8],
    request_id: Option<&str>,
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> String {
    let kind = classify_compact_non_success_kind(
        status_code,
        content_type,
        body,
        cf_ray,
        auth_error,
        identity_error_code,
    );
    if let Some(hint) = extract_error_hint_from_body(status_code, body).or_else(|| {
        header_only_cloudflare_challenge_hint(
            status_code,
            content_type,
            body,
            cf_ray,
            auth_error,
            identity_error_code,
        )
    }) {
        return format!(
            "upstream server error: {hint}{}",
            compact_debug_suffix(
                Some(kind),
                request_id,
                cf_ray,
                auth_error,
                identity_error_code
            )
        );
    }
    format!(
        "upstream server error: status={status_code}{}",
        compact_debug_suffix(
            Some(kind),
            request_id,
            cf_ray,
            auth_error,
            identity_error_code
        )
    )
}

fn header_only_cloudflare_challenge_hint(
    status_code: u16,
    content_type: Option<&str>,
    body: &[u8],
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> Option<String> {
    if status_code < 400 || !body.is_empty() {
        return None;
    }
    if auth_error.is_some_and(looks_like_blocked_marker)
        || identity_error_code.is_some_and(looks_like_blocked_marker)
    {
        return Some("Cloudflare 安全验证页".to_string());
    }
    let is_html = content_type
        .map(crate::gateway::is_html_content_type)
        .unwrap_or(false);
    if cf_ray.is_some() || (is_html && matches!(status_code, 401 | 403)) {
        return Some("Cloudflare 安全验证页".to_string());
    }
    None
}

pub(super) fn extract_error_hint_from_body_or_headers(
    status_code: u16,
    content_type: Option<&str>,
    body: &[u8],
    cf_ray: Option<&str>,
    auth_error: Option<&str>,
    identity_error_code: Option<&str>,
) -> Option<String> {
    extract_error_hint_from_body(status_code, body).or_else(|| {
        header_only_cloudflare_challenge_hint(
            status_code,
            content_type,
            body,
            cf_ray,
            auth_error,
            identity_error_code,
        )
    })
}
