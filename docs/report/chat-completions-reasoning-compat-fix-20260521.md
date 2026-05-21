# Chat Completions Reasoning 兼容修复（2026-05-21）

## 背景

Hermes 通过 OpenAI-compatible `/v1/chat/completions` 接入 CodeXManager 时，CodeXManager 会把请求改写到 `/v1/responses`。当上游只返回 reasoning summary 而没有普通 `output_text` 时，旧转换逻辑不会把 reasoning 暴露回 Chat Completions 响应，客户端会把这类成功响应识别成 empty。

## 修复

- Chat Completions 兼容请求改写到 Responses 时，默认补：
  - `reasoning.effort = "medium"`
  - `reasoning.summary = "auto"`
- 客户端传 `reasoning_effort` 时保留 effort，并补 `summary = "auto"`。
- 客户端传 `reasoning` 对象时保留已有字段，仅在缺失 `summary` 时补默认值。
- Responses 转 Chat Completions 时，将 reasoning summary 映射到兼容扩展字段：
  - 流式：`choices[].delta.reasoning` 和 `choices[].delta.reasoning_content`
  - 非流式：`choices[].message.reasoning` 和 `choices[].message.reasoning_content`
- 普通答案仍只进入 `content`，reasoning 不写入普通可见答案文本。

## 涉及文件

- `crates/service/src/gateway/local_validation/request.rs`
- `crates/service/src/gateway/observability/http_bridge/aggregate/output_text.rs`
- `crates/service/src/gateway/observability/http_bridge/delivery.rs`
- `crates/service/src/gateway/observability/http_bridge/stream_readers/chat_completions.rs`
- `crates/service/src/gateway/local_validation/tests/request_tests.rs`
- `crates/service/src/gateway/observability/tests/http_bridge_tests.rs`

## 验证

- `cargo test -p codexmanager-service gateway::observability::tests::http_bridge_tests`
  - 当前仓库真实测试路径不匹配该过滤条件，Cargo 显示 0 个测试执行。
- `cargo test -p codexmanager-service gateway::local_validation::tests::request_tests`
  - 当前仓库真实测试路径不匹配该过滤条件，Cargo 显示 0 个测试执行。
- `cargo test -p codexmanager-service gateway::http_bridge::tests`
  - 70 passed。
- `cargo test -p codexmanager-service gateway::local_validation::request::tests`
  - 36 passed。
- `cargo test -p codexmanager-service`
  - 951 passed。
- `cargo build -p codexmanager-service`
  - 通过。

## 本机运行状态

检查本机没有正在监听 `48761` 的 CodeXManager service 进程，因此本次没有执行服务重启。当前 Hermes 配置的远端端点需要部署包含本修复的 CodeXManager 版本后，才能用 Hermes 端到端验收该问题。
