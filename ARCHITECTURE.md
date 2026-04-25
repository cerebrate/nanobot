# nanobot: Comprehensive Architecture & Design Description

> **Working document.** This is the baseline description of the nanobot architecture,
> saved as a reference for designing a related but architecturally distinct system.
> Sections will be progressively updated to reflect the target design.

---

## Overview

**nanobot** is an ultra-lightweight, Python 3.11+ personal AI agent framework. Its design
philosophy is intentional minimalism: a small, readable core loop with well-defined extension
points, rather than a large orchestration framework. The system is async-first (built on
`asyncio`), ships as a single PyPI package (`nanobot-ai`), and is designed to run as a
long-lived personal assistant that can be reached over chat applications, a CLI, an HTTP API,
or a WebSocket interface.

---

## 1. Top-Level Layout

```
nanobot/                 # Python package root
  agent/                 # Core agent loop, memory, tools, subagents
  api/                   # OpenAI-compatible HTTP API server
  bus/                   # Async message bus (events + queue)
  channels/              # Chat platform integrations
  cli/                   # Typer CLI commands + interactive REPL
  command/               # Slash command routing
  config/                # Pydantic schema + JSON loader
  cron/                  # Scheduled task service
  heartbeat/             # Periodic agent wake-up service
  providers/             # LLM backends (Anthropic, OpenAI-compat, Azure, etc.)
  security/              # Network/SSRF protection
  session/               # Per-conversation session persistence
  skills/                # Built-in skill definitions (Markdown)
  templates/             # Jinja2 prompt templates (Markdown)
  utils/                 # Shared helpers
  nanobot.py             # High-level programmatic facade (Nanobot, RunResult)
webui/                   # React/Bun web UI (dev server, served over WebSocket channel)
bridge/                  # Platform bridge helpers (bundled into wheel)
docs/                    # Markdown documentation
```

Entry points: the `nanobot` CLI command maps to `nanobot.cli.commands:app`, and
`python -m nanobot` also lands there. The public Python API is `from nanobot import Nanobot`.

---

## 2. Configuration System

**File**: `~/.nanobot/config.json` (camelCase keys, merged with sane defaults).

The config is a single Pydantic `Config` model (`nanobot.config.schema`) with sections:

- **`agents.defaults`** — model, provider, workspace path, context window sizes, temperature,
  max tokens, tool iterations, retry mode, reasoning effort, timezone, session TTL, dream
  scheduling, disabled skills.
- **`providers`** — per-provider `ProviderConfig` (apiKey, apiBase, extraHeaders). One entry
  per supported provider.
- **`channels`** — per-channel config dict (`enabled`, `allowFrom`, etc.) plus shared settings
  like `sendProgress`, `sendToolHints`, `transcriptionProvider`.
- **`tools`** — web tool config (proxy, search engine, API key), exec tool config (timeout,
  sandbox backend, env passthrough), MCP server definitions, SSRF whitelist.
- **`gateway`** / **`api`** — host/port/heartbeat for the gateway server and OpenAI-compatible
  API server.

Secrets can be referenced as `${VAR_NAME}` in the JSON file; a `resolve_config_env_vars` pass
expands them at startup. The schema also accepts `NANOBOT_` prefixed environment variables
(nested with `__` delimiters).

Provider auto-detection: `Config._match_provider()` walks the `PROVIDERS` registry in priority
order, matching by: explicit config `provider` field, model-name prefix, model-name keyword,
API key prefix, API base URL substring, local-server detection, and finally any configured
gateway as fallback.

---

## 3. LLM Provider System

**Location**: `nanobot/providers/`

### 3.1 Registry (`registry.py`)

A static ordered tuple `PROVIDERS` of `ProviderSpec` frozen dataclasses. Each entry declares:

- `name` — config key and registry identifier.
- `keywords` — model-name fragments that auto-select this provider.
- `env_key` — environment variable for the API key.
- `backend` — which provider class to instantiate: `"openai_compat"`, `"anthropic"`,
  `"azure_openai"`, `"openai_codex"`, or `"github_copilot"`.
- Flags: `is_gateway`, `is_local`, `is_oauth`, `is_direct`, `strip_model_prefix`,
  `supports_prompt_caching`, `thinking_style`, `model_overrides`, `detect_by_key_prefix`,
  `detect_by_base_keyword`, `default_api_base`.

Current registered providers include: OpenRouter, AiHubMix, SiliconFlow, VolcEngine, BytePlus,
Anthropic, OpenAI, OpenAI Codex (OAuth), GitHub Copilot (OAuth), DeepSeek, Gemini, Zhipu,
DashScope/Qwen, Moonshot/Kimi, MiniMax, Mistral, StepFun, Xiaomi MIMO, vLLM, Ollama, LM
Studio, OpenVINO, Groq, Qianfan. Adding a new provider requires adding one `ProviderSpec` and
one `ProviderConfig` field in the schema.

### 3.2 Base (`base.py`)

`LLMProvider` is an abstract base class. Subclasses implement:

- `get_default_model() -> str`
- `chat_with_retry(messages, tools, model, ...) -> LLMResponse` — handles retries with
  exponential backoff; distinguishes transient (rate-limit, server errors, timeouts) from
  non-retryable (quota exhaustion, billing) errors using structured error metadata and string
  marker lists. Supports "standard" and "persistent" retry modes, the latter continuing
  indefinitely with a 60 s ceiling.
- `chat_stream(messages, tools, model, hook, ...) -> AsyncGenerator[str, None]` — streaming
  variant.

`LLMResponse` carries: content, tool_calls (`list[ToolCallRequest]`), finish_reason, usage,
retry_after, reasoning_content, thinking_blocks, and structured error metadata.

`GenerationSettings` holds temperature, max_tokens, and reasoning_effort as a frozen dataclass
attached to each provider.

### 3.3 Implementations

- **`OpenAICompatProvider`** — wraps the official `openai` Python SDK. Handles OpenAI-style
  tool calls, streaming, prompt caching annotations for supported gateways, thinking-mode
  injection (multiple per-provider `thinking_style` values: `"thinking_type"`,
  `"enable_thinking"`, `"reasoning_split"`), bedrock routing, and model-prefix stripping for
  gateways. Provider-specific quirks (e.g., Moonshot temperature enforcement,
  max_completion_tokens vs max_tokens naming) are handled here.
- **`AnthropicProvider`** — wraps the official `anthropic` Python SDK. Converts tool call
  format, handles Anthropic extended thinking blocks, prompt caching via `cache_control`
  content annotations.
- **`AzureOpenAIProvider`** — Azure-specific configuration (API version, deployment name as
  model).
- **`OpenAICodexProvider`** — OAuth-based ChatGPT Codex flow, storing tokens via
  `oauth-cli-kit`.
- **`GitHubCopilotProvider`** — GitHub OAuth flow.

### 3.4 Transcription

A separate `TranscriptionProvider` hierarchy wraps Whisper-compatible audio transcription for
voice messages, with `GroqTranscriptionProvider` and `OpenAITranscriptionProvider`
implementations.

---

## 4. Message Bus

**Location**: `nanobot/bus/`

The bus is a thin asyncio wrapper around two `asyncio.Queue` instances:

- **Inbound queue** (`InboundMessage`): chat platform → agent.
- **Outbound queue** (`OutboundMessage`): agent → chat platform.

`InboundMessage` fields: channel name, sender_id, chat_id, content (text), timestamp, media
(list of local file paths), metadata dict, optional session_key_override. The `session_key`
property defaults to `"{channel}:{chat_id}"`.

`OutboundMessage` fields: channel, chat_id, content, optional reply_to, media, metadata,
buttons. Special metadata keys control delivery behavior: `_stream_delta`, `_stream_end`
(streaming chunks), `_progress`, `_tool_hint` (optional UI hints), `_retry_wait`, `_streamed`.

The bus provides `publish_inbound`, `consume_inbound`, `publish_outbound`, `consume_outbound`
coroutines.

---

## 5. Channel System

**Location**: `nanobot/channels/`

### 5.1 BaseChannel

Abstract base class defining the interface all channels must implement:

- `start()` / `stop()` — long-running async tasks.
- `send(OutboundMessage)` — synchronous message delivery.
- `send_delta(chat_id, delta, metadata)` — streaming chunk delivery (optional, opt-in via
  `supports_streaming`).
- `login(force)` — interactive OAuth/QR login (optional).
- `is_allowed(sender_id)` — checks the `allowFrom` list; empty list blocks all, `"*"` allows
  all.
- `_handle_message(...)` — validates permission and publishes an `InboundMessage` to the bus.
- `transcribe_audio(file_path)` — delegates to the configured Whisper provider.

The `supports_streaming` property returns True only when both config enables it AND the
subclass overrides `send_delta`.

### 5.2 Built-in Channels

| Channel    | Technology                                          |
|------------|-----------------------------------------------------|
| `telegram` | `python-telegram-bot` (async polling/webhook)       |
| `discord`  | `discord.py`                                        |
| `slack`    | `slack-sdk` WebSocket mode                          |
| `whatsapp` | HTTP webhook (via bridge)                           |
| `email`    | IMAP polling + SMTP                                 |
| `qq`       | `qq-botpy`                                          |
| `weixin`   | WeChat personal account bridge (QR login, HTTP gateway) |
| `wecom`    | WeCom/Enterprise WeChat SDK                         |
| `feishu`   | Lark/Feishu Open SDK                                |
| `dingtalk` | DingTalk Stream SDK                                 |
| `matrix`   | `matrix-nio`                                        |
| `msteams`  | Microsoft Teams (JWT-validated webhooks)            |
| `mochat`   | MoChat (generic HTTP webhook)                       |
| `websocket`| Pure-Python WebSocket server via `websockets`       |

The WebSocket channel also serves the bundled web UI static files (if the `nanobot.web` package
is present) and implements a token-issuance endpoint for short-lived auth.

### 5.3 Channel Discovery

`nanobot.channels.registry.discover_all()` uses `pkgutil` to scan built-in channels **and**
Python entry points registered under the `nanobot.channels` group for third-party plugins. Any
channel class with a matching config section where `enabled: true` is instantiated.

### 5.4 ChannelManager

Initializes and coordinates all enabled channels:

- Populates transcription credentials on each channel.
- Runs all channels as concurrent asyncio tasks via `start_all()`.
- Runs an `_dispatch_outbound()` coroutine that consumes the outbound queue and routes
  messages to the correct channel, applying exponential-backoff retry (1 s, 2 s, 4 s;
  configurable via `sendMaxRetries`).
- Coalesces consecutive `_stream_delta` messages for the same target to reduce API call volume.
- Validates `allowFrom` is non-empty on startup.

---

## 6. Agent Loop

**Location**: `nanobot/agent/`

The agent loop is the central dispatch and execution engine. It sits between the message bus,
the LLM provider, tools, sessions, memory, and channels.

### 6.1 AgentLoop (`loop.py`)

Top-level class. Constructed with all runtime dependencies (bus, provider, workspace, sessions,
tools, MCP configs, etc.). Key responsibilities:

1. **Message consumption**: `run()` pulls `InboundMessage` from the bus in a tight `while`
   loop. Priority commands (`/stop`, `/restart`) are dispatched synchronously; all other
   messages become async tasks.

2. **Session routing**: For a given `session_key` (or unified key if `unified_session: true`),
   if a task is already active for that session, the new message is placed in a per-session
   `_pending_queue` for mid-turn injection (up to `_MAX_INJECTIONS_PER_TURN=3` per turn,
   `_MAX_INJECTION_CYCLES=5` total). This allows the agent to incorporate follow-up user
   messages without losing the current LLM turn context.

3. **Task management**: Each session maintains a list of `asyncio.Task` objects. Concurrent
   request throughput is bounded by `NANOBOT_MAX_CONCURRENT_REQUESTS` (default 3) via an
   `asyncio.Semaphore`. Tasks are tracked for cancellation (`/stop`) and for preventing
   auto-compact during active processing.

4. **MCP connection**: At first message, connects to all configured MCP servers lazily. Retries
   on failure at the next message.

5. **Context assembly**: Before calling the LLM, invokes `ContextBuilder` to build the system
   prompt and message list.

6. **Response routing**: After the agent run completes, the final content is published as an
   `OutboundMessage` back to the bus.

7. **Process dispatch**: `process_direct()` is the direct-call entry point (used by CLI and
   SDK), bypassing the bus entirely.

### 6.2 AgentRunner (`runner.py`)

A stateless class that executes the LLM tool-use loop given an `AgentRunSpec`. The spec
contains the full initial message list, tool registry, model name, iteration budget, callbacks,
etc.

The loop structure per iteration:

1. **Context governance**: Before sending to the LLM, the messages-for-model array goes
   through several transforms:
   - `_drop_orphan_tool_results()` — removes tool result messages without a matching tool call.
   - `_backfill_missing_tool_results()` — inserts placeholder text for tool calls with no result.
   - `_microcompact()` — replaces tool result content in old messages (beyond a recency window)
     with a short summary token.
   - `_apply_tool_result_budget()` — hard-truncates individual tool results exceeding
     `max_tool_result_chars`.
   - `_snip_history()` — drops oldest message pairs when the estimated token count exceeds the
     context window limit.

2. **LLM call**: Invokes `provider.chat_with_retry()` (or streaming variant if hooks request
   it). The hook lifecycle fires: `before_iteration`, `on_stream` deltas, `on_stream_end`,
   `before_execute_tools`, `after_iteration`.

3. **Tool execution**: If `response.should_execute_tools` is true (tool calls AND non-error
   finish_reason), tools run concurrently (when `concurrent_tools=True`). Sequential execution
   is used otherwise. Results are normalized and appended as tool-role messages.

4. **Mid-turn injection**: After tool execution (and after errors), `_try_drain_injections()`
   checks for pending user messages from the injection_callback and appends them to continue
   the turn rather than creating a new one.

5. **Finalization**: When the model returns a final response (no tool calls), the content is
   stored as `final_content` and the loop exits.

6. **Checkpointing**: After each tool-execution phase, the session checkpoint is updated
   (tool-call state, pending results) for crash recovery.

**Error handling**: Empty responses retry up to `_MAX_EMPTY_RETRIES=2` times. Length-truncated
responses (finish_reason `length`) recover up to `_MAX_LENGTH_RECOVERIES=3` times. Repeated
identical external lookup errors short-circuit with a user-visible message.

`AgentRunResult` carries: final_content, messages (the appended new turn), tools_used, usage,
stop_reason (`completed`, `max_iterations`, `tool_error`, `error`), had_injections.

### 6.3 AgentHook (`hook.py`)

The `AgentHook` base class is a lifecycle extension point with async methods:

- `before_iteration(context)` — called at the start of each LLM iteration.
- `on_stream(context, delta)` — called for each streaming content chunk.
- `on_stream_end(context, *, resuming)` — called when a streaming segment ends (resuming=True
  means tool calls follow).
- `before_execute_tools(context)` — called before tool execution begins.
- `after_iteration(context)` — called after tool results are appended.
- `finalize_content(context, content)` — pipeline transform on the final content string.
- `wants_streaming()` — returns True to request streaming delivery.

`CompositeHook` fans out to a list of hooks with per-hook exception isolation (only hooks with
`reraise=True` propagate exceptions).

The `_LoopHook` internal implementation bridges from hook callbacks to the `on_progress`,
`on_stream`, and `on_stream_end` callables passed from the channel layer.

### 6.4 ContextBuilder (`context.py`)

Assembles the complete message list for an LLM call:

**System prompt construction**:
1. Identity block (rendered from `agent/identity.md` Jinja2 template — runtime info, workspace
   path, platform policy, channel-specific format hints).
2. Bootstrap files — reads `AGENTS.md`, `SOUL.md`, `USER.md`, `TOOLS.md` from the workspace
   if present.
3. Long-term memory — includes `MEMORY.md` content (skipped if it still matches the default
   template).
4. Always-loaded skills — skills marked `always: true` in frontmatter are embedded in full.
5. Skills summary — a compact table of all other available skills with their descriptions and
   file paths (for progressive loading by the agent).
6. Recent history — the last 50 entries from `history.jsonl` (since the last Dream cursor),
   capped at 32,000 chars.

**Per-message context**: A runtime context block is prepended to each user message: current
time, channel name, chat ID, and optionally a session summary (injected when the session was
auto-compacted from an idle state).

**Multimodal**: Images attached to user messages are base64-encoded and included as `image_url`
content blocks.

---

## 7. Tool System

**Location**: `nanobot/agent/tools/`

### 7.1 Tool Base (`base.py`)

Every tool subclasses `Tool` (abstract) and declares:

- `name` (property) — unique identifier used in LLM function calls.
- `description` (property) — natural-language description for the LLM.
- `parameters` (property) — JSON Schema object describing the tool's parameters.
- `execute(**kwargs)` (abstract async) — runs the tool and returns a string or content block
  list.
- `read_only`, `concurrency_safe`, `exclusive` — concurrency hints.

The `@tool_parameters(schema_dict)` class decorator is the standard way to declare the JSON
Schema; it injects a `parameters` property and stores a frozen copy on the class.

Parameter validation and safe type coercion (string→int, string→bool, etc.) is handled by
`cast_params()` and `validate_params()` before `execute()` is called.

`to_schema()` returns the OpenAI function-calling format
(`{"type": "function", "function": {...}}`).

### 7.2 ToolRegistry (`registry.py`)

A dict-based registry. `register(tool)` / `unregister(name)`. `get_definitions()` returns a
cached, stably-ordered list of all schemas (built-ins alphabetically, then MCP tools
alphabetically). The ordering is stable across runs to maximize LLM prompt caching.

`execute(name, params)` performs: `prepare_call()` (resolve → cast → validate), then
`tool.execute()`. Errors are returned as strings with a
`[Analyze the error above and try a different approach.]` hint appended.

### 7.3 Built-in Tools

| Tool            | Purpose                                                                     |
|-----------------|-----------------------------------------------------------------------------|
| `read_file`     | Read file contents (line range, encoding detection, PDF/DOCX/XLSX/PPTX)    |
| `write_file`    | Overwrite a file                                                            |
| `edit_file`     | String-replacement edits (old_str → new_str)                               |
| `list_dir`      | List directory contents                                                     |
| `glob`          | Find files by glob pattern                                                  |
| `grep`          | Search file contents (content/files_with_matches/count modes)              |
| `notebook_edit` | Edit Jupyter notebook cells                                                 |
| `exec`          | Execute shell commands with timeout, deny patterns, optional bwrap sandbox |
| `web_search`    | Web search (DuckDuckGo, Brave, Tavily, SearXNG, Jina, Kagi)               |
| `web_fetch`     | Fetch and extract URL content (with SSRF protection)                       |
| `message`       | Send text/media to the originating chat channel                            |
| `spawn`         | Launch a background subagent task                                          |
| `cron`          | Create/list/delete scheduled tasks                                         |
| `my`            | Inspect (and optionally modify) live agent state                           |
| `mcp_*`         | Wrapped MCP server tools (prefixed `mcp_{server}_`)                       |

#### ExecTool Security

Shell execution uses a configurable deny list of regex patterns (blocking `rm -rf`, disk
operations, fork bomb, writes to `history.jsonl`/`.dream_cursor`, etc.) and an optional
`bwrap` (bubblewrap) sandbox. When `restrict_to_workspace=true`, file and exec tools are
constrained to the workspace directory.

#### SSRF Protection

`WebFetchTool` and MCP HTTP tools validate URLs against a blocklist of private/internal IP
ranges (RFC 1918, loopback, link-local, carrier-grade NAT). A configurable CIDR whitelist
(`ssrf_whitelist`) exempts specific ranges (e.g., Tailscale).

### 7.4 MCP Integration (`tools/mcp.py`)

MCP server connections are established lazily at first use. Both stdio (subprocess) and
HTTP/SSE servers are supported (`type: stdio | sse | streamableHttp`, auto-detected if
omitted). Windows shell launchers (`npx`, `npm`, etc.) are automatically wrapped with
`cmd.exe /d /c` on Windows. Each connected server's tools are registered in the `ToolRegistry`
with a `mcp_{server_name}_{tool_name}` prefix. An `enabled_tools` filter per server can
restrict which MCP tools are exposed. Transient connection errors trigger a single retry.

---

## 8. Memory System

**Location**: `nanobot/agent/memory.py`, `nanobot/utils/gitstore.py`

Memory operates in three layers with increasing permanence.

### 8.1 Session Layer (`session/manager.py`)

`Session` holds the live conversation history as a list of message dicts (role, content,
tool_calls, tool_call_id, timestamp, media). `last_consolidated` marks the boundary between
messages already summarized to disk and the live tail.

`SessionManager` persists each session as an atomic JSON write to
`workspace/sessions/{safe_key}.json`. Atomic writes use a temp file + rename to prevent
corruption. `get_or_create(key)` loads or creates; `save(session)` persists; `invalidate(key)`
drops from the in-memory cache. Legacy session files at old paths are migrated automatically.

### 8.2 History Layer (`MemoryStore`, `memory/history.jsonl`)

`MemoryStore` provides pure file I/O over:

- `MEMORY.md` — curated long-term facts.
- `history.jsonl` — append-only JSONL log of compressed past turns. Each entry:
  `{"cursor": N, "timestamp": "YYYY-MM-DD HH:MM", "content": "..."}`.
- `SOUL.md` — agent personality/communication style.
- `USER.md` — stable user profile.
- `.cursor` — last written cursor (for fast `_next_cursor()` lookup).
- `.dream_cursor` — last cursor consumed by Dream (for incremental processing).

`append_history(entry)` strips `<think>` blocks, truncates to a hard cap (64,000 chars),
appends the JSONL record, and atomically updates `.cursor`.

Legacy `HISTORY.md` files are automatically migrated to `history.jsonl` on first startup.

### 8.3 Consolidator

Triggered when the estimated token count for the current session exceeds the context window
budget. Uses `pick_consolidation_boundary()` to find the oldest safe user-turn boundary that
frees enough tokens, then asks the LLM to summarize those messages, and appends the summary to
`history.jsonl`. Falls back to a raw format dump if the LLM call fails.

### 8.4 Dream

A two-phase background memory processor triggered on a configurable schedule (default every
2 hours, configurable via `agents.defaults.dream.intervalH`):

**Phase 1 (Analysis)**: Presents the current `MEMORY.md`, `SOUL.md`, `USER.md`,
age-annotated (via `GitStore.blame()`) and a batch of new `history.jsonl` entries (capped by
`maxBatchSize`, default 20) to the LLM, asking it to identify what needs to change in the
long-term files.

**Phase 2 (Editing)**: Runs a full `AgentRunner` loop (budget: `maxIterations` tool calls,
default 15) with `read_file` and `write_file` tools to make the identified edits to `SOUL.md`,
`USER.md`, and `MEMORY.md`. Each skill discovered during Phase 2 can also be learned into
memory.

After Phase 2, advances the `.dream_cursor` to mark the processed entries. Changes are
committed to `GitStore`.

### 8.5 AutoCompact

When `sessionTtlMinutes` is set (default 0 = disabled), idle sessions beyond the TTL are
automatically archived: their unconsolidated messages are summarized by the `Consolidator`, a
compact summary is stored in session metadata, and the session is trimmed to a small recent
suffix (8 messages). The summary is injected as context on the next session start via the
runtime context block.

### 8.6 GitStore

Manages a git repository in the workspace for `SOUL.md`, `USER.md`, and `memory/MEMORY.md`.
Uses `dulwich` (pure-Python git). After Dream edits these files, it auto-commits with a message
describing the change. `blame()` annotates each line with its commit age (days) for the Dream
Phase 1 prompt. `/dream-restore` and `/dream-log` slash commands expose this history to users.

---

## 9. Skills System

**Location**: `nanobot/agent/skills.py`, `nanobot/skills/`, workspace `skills/`

Skills are Markdown files (`SKILL.md`) inside named subdirectories. They carry YAML
frontmatter:

```yaml
---
description: Short description shown in the skills summary
metadata:
  nanobot:
    always: false    # if true, embedded in every system prompt
    requires:
      bins: [gh]     # required CLI binaries
      env: [GH_TOKEN] # required env vars
---
```

`SkillsLoader` discovers skills from two roots: the workspace's `skills/` directory
(user-defined, takes precedence) and the package's built-in `skills/` directory. Skills with
unmet requirements are listed as unavailable in the summary but not included.

Inclusion in the system prompt is progressive:

- `always: true` skills are embedded verbatim in every prompt.
- Other skills appear only as a summary table (name, description, file path). The agent can
  `read_file` the SKILL.md when it needs the full instructions.

Built-in skills: `github` (gh CLI), `weather` (wttr.in/Open-Meteo), `summarize`
(URL/file/YouTube), `tmux`, `clawhub` (ClawHub registry search), `skill-creator`, `memory`,
`cron`.

---

## 10. Subagent System

**Location**: `nanobot/agent/subagent.py`

`SubagentManager` runs independent background `AgentRunner` loops as `asyncio.Task` objects.
The `spawn` tool creates a new subagent with a task description and label. Each subagent:

- Has its own `ToolRegistry` (same tools as the main agent).
- Runs the full `AgentRunner` loop with its own `AgentRunSpec`.
- Is keyed by a UUID task ID; tracked by session key for per-session cancellation.
- Posts results back into the parent session's `_pending_queue` as `InboundMessage` objects for
  mid-turn injection.

`SubagentStatus` tracks phase, iteration, tool events, usage, and errors for the `my` tool's
status inspection. A `_SubagentHook` logs tool calls and updates the status object.

---

## 11. Scheduled Tasks (Cron)

**Location**: `nanobot/cron/`

`CronService` manages a persistent JSON store (`cron.json`) of `CronJob` objects. Each job
has: id, label, payload, schedule (`CronSchedule`), state (`active`/`paused`/`done`),
`next_run_ms`, and a run history of up to 20 records.

`CronSchedule` supports three kinds:

- `at` — one-shot run at a specific epoch-ms timestamp.
- `every` — periodic with a fixed interval in ms.
- `cron` — standard cron expression with timezone (`croniter` library).

The service loop (`run_loop()`) checks for due jobs on each tick (configurable `max_sleep_ms`,
default 5 minutes), calls the `on_job` callback (which feeds a message through the agent loop),
and updates `next_run_ms`.

An `action.jsonl` file provides an out-of-process control channel for add/remove/pause/resume
operations (used by the `CronTool` from within the agent loop). A `filelock` prevents
concurrent writes. The service reconciles the action file on each tick.

---

## 12. Heartbeat Service

**Location**: `nanobot/heartbeat/`

`HeartbeatService` wakes the agent on a timer (default 30 minutes). On each tick:

1. **Phase 1 (Decision)**: Reads `HEARTBEAT.md` from the workspace. Makes a one-shot LLM call
   with a `heartbeat` virtual tool (action: skip/run, tasks summary). If `action == "skip"`,
   the tick is silent.

2. **Phase 2 (Execution)**: If `action == "run"`, calls the `on_execute` callback (runs the
   full agent loop with the tasks summary as input). After execution, a post-run evaluator
   decides whether the response warrants notification to the user (suppresses heartbeat noise).

`HEARTBEAT.md` is a workspace file the agent can edit (e.g., via the cron skill) to configure
what it checks on each wake-up.

---

## 13. Command System

**Location**: `nanobot/command/`

`CommandRouter` implements a three-tier dispatch table:

1. **Priority** — checked before the session lock (e.g., `/stop`, `/restart`).
2. **Exact** — matched inside the lock.
3. **Prefix** — longest-prefix-first match (e.g., `/team `).
4. **Interceptors** — arbitrary fallback predicates.

`CommandContext` carries the inbound message, current session, session key, raw text, args, and
a reference to the `AgentLoop`.

Built-in commands include: `/help`, `/status`, `/stop` (cancel active task), `/restart`
(restarts the nanobot process), `/clear` (reset session), `/dream` (trigger Dream now),
`/dream-log`, `/dream-restore`, `/memory`, `/history`, and more.

---

## 14. CLI

**Location**: `nanobot/cli/`

Built with `typer`. Subcommands:

- `nanobot onboard` — interactive setup wizard (provider selection, model autocomplete, config
  generation, workspace template sync).
- `nanobot agent` — start the interactive CLI REPL. Uses `prompt_toolkit` for line editing,
  persistent file history, multi-line input support, and ANSI/rich rendering of responses.
- `nanobot gateway` — start the full gateway (channels + agent loop + cron + heartbeat) as a
  daemon.
- `nanobot api` — start only the OpenAI-compatible HTTP API server.
- `nanobot status` — display configured providers and channels.
- `nanobot provider login <name>` — interactive OAuth login for Codex/Copilot.

The CLI REPL uses `StreamRenderer` for live streaming output, a `ThinkingSpinner` to indicate
model reasoning, and `prompt_toolkit`'s `patch_stdout` to prevent interleaving.

---

## 15. OpenAI-Compatible API Server

**Location**: `nanobot/api/server.py`

An `aiohttp` web application exposing:

- `POST /v1/chat/completions` — accepts standard OpenAI chat completion requests. Routes to
  the agent loop via `process_direct()`. Supports SSE streaming (`stream: true`).
- `GET /v1/models` — returns a static model list.
- `POST /v1/files` — accepts base64 data URL uploads and saves them to the workspace media
  directory.

All requests are routed to a single persistent session (`api:default`). The server is started
by `nanobot api` or alongside the gateway.

---

## 16. WebSocket Channel

**Location**: `nanobot/channels/websocket.py`

A `websockets`-based server. Features:

- Each connected client gets its own session (keyed by a per-connection `chat_id`).
- Optional HMAC-signed short-lived tokens issued via a separate HTTP GET endpoint for secure
  browser clients.
- Static file serving for the bundled web UI dist directory.
- Streaming output via `send_delta` with `_stream_delta`/`_stream_end` metadata markers.
- File/media upload support (base64 encoded in WebSocket messages).
- `allow_from` controls which `client_id` values are permitted.

The web UI (`webui/`) is a Vite/Bun React application that connects to this channel and
provides a browser-based chat interface.

---

## 17. Prompt Templates

**Location**: `nanobot/templates/`

Jinja2 templates for system prompt sections. Key files:

- `agent/identity.md` — runtime context, workspace path, format hints by channel.
- `agent/platform_policy.md` — OS-specific guidance.
- `agent/skills_section.md` — skills listing template.
- `agent/subagent_system.md` / `agent/subagent_announce.md` — subagent prompts.
- `agent/evaluator.md` — heartbeat post-run evaluation prompt.
- `agent/dream_phase1.md` / `agent/dream_phase2.md` — Dream memory prompts.
- `agent/max_iterations_message.md` — message injected when the iteration budget is exhausted.
- `SOUL.md`, `USER.md`, `TOOLS.md`, `AGENTS.md`, `HEARTBEAT.md`, `memory/MEMORY.md` —
  workspace template defaults, copied to new workspaces on first run.

Templates are rendered via `render_template(path, **kwargs)` which loads from the
`nanobot.templates` package namespace.

---

## 18. Data Flow Summary

```
User input (any channel)
  │
  ▼
Channel.start() → _handle_message() → bus.publish_inbound(InboundMessage)
  │
  ▼
AgentLoop.run() ← bus.consume_inbound()
  │
  ├─ Priority command? → CommandRouter.dispatch_priority() → bus.publish_outbound()
  ├─ Active session? → pending_queue (mid-turn injection)
  └─ New task → asyncio.Task(_dispatch())
       │
       ▼
       Session load + AutoCompact check
       │
       ▼
       ContextBuilder.build_messages(history, user_message, skills, media)
         ├─ System prompt: identity + bootstrap + memory + skills summary + recent history
         └─ Message list: history + runtime_ctx + user_content
       │
       ▼
       AgentRunner.run(AgentRunSpec)
         ├─ Context governance (orphan cleanup, microcompact, budget trim, snip)
         ├─ LLMProvider.chat_with_retry() → LLMResponse
         ├─ [if tool calls] ToolRegistry.execute() for each tool
         ├─ [if mid-turn injection] drain pending_queue
         └─ Repeat until final response or max_iterations
       │
       ▼
       Session save + Consolidator (if over token budget)
       │
       ▼
       bus.publish_outbound(OutboundMessage)
         │
         ▼
         ChannelManager._dispatch_outbound() → channel.send() / channel.send_delta()
           │
           ▼
           User receives response
```

Background services running concurrently:

- `HeartbeatService` — periodic agent wake-up.
- `CronService` — scheduled job execution.
- `Dream` — memory consolidation on schedule.
- `AutoCompact` — idle session compression.

---

## 19. Key Design Decisions

1. **Bus decoupling**: Channels and the agent core communicate only through the async queue.
   Neither knows about the other's internals. This makes adding new channels (or replacing the
   agent core) independent.

2. **Stateless provider backends**: The `AgentRunner` holds no state between calls. All state
   lives in the `Session` (message history) and `MemoryStore` (files). This makes
   serialization, crash recovery, and testing straightforward.

3. **Progressive skill loading**: Skills are not blindly embedded in every prompt. The system
   prompt contains only a summary table and always-loaded skills; the agent retrieves full skill
   content with `read_file` when needed. This keeps prompts compact.

4. **Two-cursor memory**: The distinction between `.cursor` (Consolidator write position) and
   `.dream_cursor` (Dream read position) allows the two memory processes to run independently
   and at different speeds without coordination.

5. **Mid-turn injection**: Rather than creating a new agent task when a follow-up message
   arrives during processing, it is injected into the ongoing turn via `_pending_queues`. This
   preserves tool-call context and allows sub-agent results to be consumed in-order.

6. **Command priority tiers**: The three-tier command router separates commands that must
   interrupt active tasks (priority tier) from commands that run within the normal session lock,
   avoiding deadlocks and race conditions.

7. **Git-versioned long-term memory**: Using `dulwich` for git operations on
   `SOUL.md`/`USER.md`/`MEMORY.md` gives memory full version history without requiring system
   git and allows the agent to expose restore/diff commands to users.

8. **No framework lock-in**: Providers, channels, and skills are all pluggable. The provider
   system uses a registry pattern (no class hierarchy beyond the abstract base), and channels
   are discoverable via Python entry points. A new LLM provider requires two file edits; a new
   channel is a single subclass installable as a package.
