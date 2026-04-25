"""Tests for the Mattermost channel implementation."""

from __future__ import annotations

import json
import time
from typing import Any

import pytest

from nanobot.bus.events import OutboundMessage
from nanobot.channels.mattermost import (
    MattermostChannel,
    MattermostConfig,
    _StreamBuf,
)

# ---------------------------------------------------------------------------
# Fakes
# ---------------------------------------------------------------------------


class _FakeBus:
    """Minimal message bus that records inbound messages."""

    def __init__(self) -> None:
        self.inbound: list[Any] = []

    async def publish_inbound(self, msg: Any) -> None:
        self.inbound.append(msg)


class _FakeResponse:
    """Fake httpx response."""

    def __init__(
        self,
        payload: dict[str, Any] | None = None,
        *,
        status_code: int = 200,
        should_raise: bool = False,
    ) -> None:
        self._payload = payload or {}
        self.status_code = status_code
        self._should_raise = should_raise

    def raise_for_status(self) -> None:
        if self._should_raise:
            raise RuntimeError("http error")

    def json(self) -> dict[str, Any]:
        return self._payload


class _FakeHttpClient:
    """Fake httpx async client that records calls."""

    def __init__(
        self,
        *,
        me_payload: dict[str, Any] | None = None,
        post_payload: dict[str, Any] | None = None,
        file_payload: dict[str, Any] | None = None,
        should_raise: bool = False,
    ) -> None:
        self.me_payload = me_payload or {"id": "bot-user-id", "username": "mybot"}
        self.post_payload = post_payload or {"id": "new-post-id"}
        self.file_payload = file_payload or {"file_infos": [{"id": "file-abc"}]}
        self.should_raise = should_raise

        self.get_calls: list[tuple[str, dict[str, Any]]] = []
        self.post_calls: list[tuple[str, dict[str, Any]]] = []
        self.put_calls: list[tuple[str, dict[str, Any]]] = []
        self.delete_calls: list[tuple[str, dict[str, Any]]] = []

    async def get(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.get_calls.append((url, kwargs))
        if "/users/me" in url:
            return _FakeResponse(self.me_payload, should_raise=self.should_raise)
        return _FakeResponse({}, should_raise=self.should_raise)

    async def post(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.post_calls.append((url, kwargs))
        if "/files" in url:
            return _FakeResponse(self.file_payload, should_raise=self.should_raise)
        if "/reactions" in url:
            return _FakeResponse({}, should_raise=self.should_raise)
        return _FakeResponse(self.post_payload, should_raise=self.should_raise)

    async def put(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.put_calls.append((url, kwargs))
        return _FakeResponse(self.post_payload, should_raise=self.should_raise)

    async def delete(self, url: str, **kwargs: Any) -> _FakeResponse:
        self.delete_calls.append((url, kwargs))
        return _FakeResponse({}, should_raise=self.should_raise)

    async def aclose(self) -> None:
        pass


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_channel(**overrides: Any) -> MattermostChannel:
    """Create a MattermostChannel with sensible test defaults."""
    config: dict[str, Any] = {
        "enabled": True,
        "url": "https://mm.example.com",
        "token": "test-token",
        "allowFrom": ["*"],
    }
    config.update(overrides)
    return MattermostChannel(config, _FakeBus())


def _posted_event(
    *,
    user_id: str = "user-1",
    channel_id: str = "channel-abc",
    channel_type: str = "O",
    post_id: str = "post-1",
    root_id: str = "",
    message: str = "hello",
    post_type: str = "",
) -> dict[str, Any]:
    """Build a minimal Mattermost WebSocket `posted` event dict."""
    post = {
        "id": post_id,
        "user_id": user_id,
        "channel_id": channel_id,
        "root_id": root_id,
        "message": message,
        "type": post_type,
    }
    return {
        "event": "posted",
        "data": {
            "post": json.dumps(post),
            "channel_type": channel_type,
        },
    }


# ---------------------------------------------------------------------------
# Config tests
# ---------------------------------------------------------------------------


def test_config_defaults() -> None:
    cfg = MattermostConfig()
    assert cfg.enabled is False
    assert cfg.url == ""
    assert cfg.token == ""
    assert cfg.scheme == "https"
    assert cfg.port == 443
    assert cfg.allow_from == []
    assert cfg.group_policy == "mention"
    assert cfg.reply_in_thread is True
    assert cfg.react_emoji == "eyes"
    assert cfg.done_emoji == "white_check_mark"
    assert cfg.streaming is False


def test_config_camelcase_keys_accepted() -> None:
    cfg = MattermostConfig.model_validate({
        "url": "https://mm.example.com",
        "token": "tok",
        "allowFrom": ["user-1"],
        "groupPolicy": "open",
        "replyInThread": False,
        "reactEmoji": "thumbsup",
        "doneEmoji": "tada",
        "streaming": True,
    })
    assert cfg.allow_from == ["user-1"]
    assert cfg.group_policy == "open"
    assert cfg.reply_in_thread is False
    assert cfg.react_emoji == "thumbsup"
    assert cfg.done_emoji == "tada"
    assert cfg.streaming is True


def test_default_config_uses_camelcase_keys() -> None:
    cfg = MattermostChannel.default_config()
    assert "allowFrom" in cfg
    assert "groupPolicy" in cfg
    assert "replyInThread" in cfg
    assert "reactEmoji" in cfg
    assert "doneEmoji" in cfg
    assert cfg["enabled"] is False


# ---------------------------------------------------------------------------
# URL helper tests
# ---------------------------------------------------------------------------


def test_api_url_built_from_https_url() -> None:
    ch = _make_channel(url="https://mm.example.com")
    assert ch._api("/posts") == "https://mm.example.com/api/v4/posts"


def test_ws_url_from_https_base() -> None:
    ch = _make_channel(url="https://mm.example.com")
    assert ch._ws_url() == "wss://mm.example.com/api/v4/websocket"


def test_ws_url_from_http_base() -> None:
    ch = _make_channel(url="http://mm.example.com")
    assert ch._ws_url() == "ws://mm.example.com/api/v4/websocket"


def test_api_url_strips_trailing_slash() -> None:
    ch = _make_channel(url="https://mm.example.com/")
    assert ch._api("/posts") == "https://mm.example.com/api/v4/posts"


# ---------------------------------------------------------------------------
# send() tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_plain_text() -> None:
    ch = _make_channel()
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-user-id"

    await ch.send(OutboundMessage(
        channel="mattermost",
        chat_id="channel-abc",
        content="Hello, world!",
        metadata={"mattermost": {"post_id": "p1", "root_id": ""}},
    ))

    post_calls = [c for c in ch._http.post_calls if "/posts" in c[0]]
    assert len(post_calls) == 1
    _, kwargs = post_calls[0]
    body = kwargs["json"]
    assert body["channel_id"] == "channel-abc"
    assert body["message"] == "Hello, world!"
    assert "root_id" not in body or body["root_id"] == ""


@pytest.mark.asyncio
async def test_send_uses_root_id_for_thread_reply() -> None:
    ch = _make_channel()
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-user-id"

    await ch.send(OutboundMessage(
        channel="mattermost",
        chat_id="channel-abc",
        content="Threaded reply",
        metadata={"mattermost": {"post_id": "p1", "root_id": "root-post-id"}},
    ))

    post_calls = [c for c in ch._http.post_calls if "/posts" in c[0]]
    assert len(post_calls) == 1
    body = post_calls[0][1]["json"]
    assert body["root_id"] == "root-post-id"


@pytest.mark.asyncio
async def test_send_splits_long_message() -> None:
    ch = _make_channel()
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-user-id"

    from nanobot.channels.mattermost import MM_MAX_POST_LEN

    long_text = "A" * (MM_MAX_POST_LEN + 4_000)  # Exceeds the per-post limit → should split
    await ch.send(OutboundMessage(
        channel="mattermost",
        chat_id="channel-abc",
        content=long_text,
        metadata={"mattermost": {"post_id": "p1", "root_id": ""}},
    ))

    post_calls = [c for c in ch._http.post_calls if "/posts" in c[0]]
    assert len(post_calls) == 2
    total_text = "".join(c[1]["json"]["message"] for c in post_calls)
    assert len(total_text) == len(long_text)


@pytest.mark.asyncio
async def test_send_removes_react_emoji_and_adds_done_emoji_on_final_response() -> None:
    ch = _make_channel(reactEmoji="eyes", doneEmoji="white_check_mark")
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-user-id"

    await ch.send(OutboundMessage(
        channel="mattermost",
        chat_id="channel-abc",
        content="Done!",
        metadata={"mattermost": {"post_id": "p1", "root_id": ""}},
    ))

    reaction_posts = [c for c in ch._http.post_calls if "/reactions" in c[0]]
    assert len(reaction_posts) == 1
    assert reaction_posts[0][1]["json"]["emoji_name"] == "white_check_mark"

    reaction_deletes = [c for c in ch._http.delete_calls if "/reactions/" in c[0]]
    assert len(reaction_deletes) == 1
    assert "eyes" in reaction_deletes[0][0]


@pytest.mark.asyncio
async def test_send_skips_reactions_for_progress_message() -> None:
    ch = _make_channel()
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-user-id"

    await ch.send(OutboundMessage(
        channel="mattermost",
        chat_id="channel-abc",
        content="Working...",
        metadata={
            "_progress": True,
            "mattermost": {"post_id": "p1", "root_id": ""},
        },
    ))

    reaction_posts = [c for c in ch._http.post_calls if "/reactions" in c[0]]
    assert reaction_posts == []


@pytest.mark.asyncio
async def test_send_uploads_file_and_attaches_to_post(tmp_path: Any) -> None:
    ch = _make_channel()
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-user-id"

    media_file = tmp_path / "test.txt"
    media_file.write_text("file content")

    await ch.send(OutboundMessage(
        channel="mattermost",
        chat_id="channel-abc",
        content="See attached",
        media=[str(media_file)],
        metadata={"mattermost": {"post_id": "p1", "root_id": ""}},
    ))

    file_upload_calls = [c for c in ch._http.post_calls if "/files" in c[0]]
    assert len(file_upload_calls) == 1

    post_calls = [c for c in ch._http.post_calls if "/posts" in c[0]]
    assert len(post_calls) == 1
    body = post_calls[0][1]["json"]
    assert body["file_ids"] == ["file-abc"]


@pytest.mark.asyncio
async def test_send_raises_on_delivery_failure() -> None:
    ch = _make_channel()
    ch._http = _FakeHttpClient(should_raise=True)
    ch._bot_user_id = "bot-user-id"

    with pytest.raises(RuntimeError, match="http error"):
        await ch.send(OutboundMessage(
            channel="mattermost",
            chat_id="channel-abc",
            content="Hello",
            metadata={"mattermost": {"post_id": "p1", "root_id": ""}},
        ))


@pytest.mark.asyncio
async def test_send_raises_when_http_not_initialized() -> None:
    ch = _make_channel()
    ch._http = None

    with pytest.raises(RuntimeError, match="not initialized"):
        await ch.send(OutboundMessage(
            channel="mattermost",
            chat_id="channel-abc",
            content="Hello",
        ))


# ---------------------------------------------------------------------------
# _handle_event() tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_handle_event_dm_publishes_to_bus() -> None:
    ch = _make_channel(allowFrom=["*"])
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"
    ch._bot_username = "mybot"

    await ch._handle_event(_posted_event(
        user_id="user-1",
        channel_id="dm-channel",
        channel_type="D",
        post_id="p1",
        message="Hello bot",
    ))

    assert len(ch.bus.inbound) == 1
    msg = ch.bus.inbound[0]
    assert msg.channel == "mattermost"
    assert msg.sender_id == "user-1"
    assert msg.chat_id == "dm-channel"
    assert msg.content == "Hello bot"
    assert msg.metadata["mattermost"]["post_id"] == "p1"


@pytest.mark.asyncio
async def test_handle_event_dm_adds_react_emoji() -> None:
    ch = _make_channel(allowFrom=["*"], reactEmoji="eyes")
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"
    ch._bot_username = "mybot"

    await ch._handle_event(_posted_event(
        user_id="user-1",
        channel_id="dm-channel",
        channel_type="D",
        post_id="p1",
        message="Hello bot",
    ))

    reaction_posts = [c for c in ch._http.post_calls if "/reactions" in c[0]]
    assert len(reaction_posts) == 1
    assert reaction_posts[0][1]["json"]["emoji_name"] == "eyes"
    assert reaction_posts[0][1]["json"]["post_id"] == "p1"


@pytest.mark.asyncio
async def test_handle_event_channel_mention_mode_fires_when_mentioned() -> None:
    ch = _make_channel(allowFrom=["*"], groupPolicy="mention")
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"
    ch._bot_username = "mybot"

    await ch._handle_event(_posted_event(
        user_id="user-1",
        channel_id="channel-abc",
        channel_type="O",
        message="@mybot please help",
    ))

    assert len(ch.bus.inbound) == 1
    # Mention should be stripped from the forwarded content
    assert "@mybot" not in ch.bus.inbound[0].content


@pytest.mark.asyncio
async def test_handle_event_channel_mention_mode_ignores_when_not_mentioned() -> None:
    ch = _make_channel(allowFrom=["*"], groupPolicy="mention")
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"
    ch._bot_username = "mybot"

    await ch._handle_event(_posted_event(
        user_id="user-1",
        channel_id="channel-abc",
        channel_type="O",
        message="just a regular message",
    ))

    assert ch.bus.inbound == []


@pytest.mark.asyncio
async def test_handle_event_channel_open_mode_fires_without_mention() -> None:
    ch = _make_channel(allowFrom=["*"], groupPolicy="open")
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"
    ch._bot_username = "mybot"

    await ch._handle_event(_posted_event(
        user_id="user-1",
        channel_id="channel-abc",
        channel_type="O",
        message="just a regular message",
    ))

    assert len(ch.bus.inbound) == 1


@pytest.mark.asyncio
async def test_handle_event_skips_own_message() -> None:
    ch = _make_channel(allowFrom=["*"])
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"
    ch._bot_username = "mybot"

    await ch._handle_event(_posted_event(
        user_id="bot-id",  # Same as bot
        channel_type="D",
        message="I sent this",
    ))

    assert ch.bus.inbound == []


@pytest.mark.asyncio
async def test_handle_event_skips_system_messages() -> None:
    ch = _make_channel(allowFrom=["*"])
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"
    ch._bot_username = "mybot"

    await ch._handle_event(_posted_event(
        user_id="user-1",
        channel_type="D",
        post_type="system_add_to_channel",  # Non-empty type = system
        message="User joined the channel",
    ))

    assert ch.bus.inbound == []


@pytest.mark.asyncio
async def test_handle_event_denied_sender_does_not_publish() -> None:
    ch = _make_channel(allowFrom=["allowed-user"])
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"
    ch._bot_username = "mybot"

    await ch._handle_event(_posted_event(
        user_id="denied-user",
        channel_type="D",
        message="hello",
    ))

    assert ch.bus.inbound == []


@pytest.mark.asyncio
async def test_handle_event_ignores_non_posted_events() -> None:
    ch = _make_channel(allowFrom=["*"])
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"

    # A status_change event should be silently ignored
    await ch._handle_event({"event": "status_change", "data": {"user_id": "user-1"}})

    assert ch.bus.inbound == []


@pytest.mark.asyncio
async def test_handle_event_thread_session_key_for_channel_messages() -> None:
    ch = _make_channel(allowFrom=["*"], groupPolicy="open", replyInThread=True)
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"
    ch._bot_username = "mybot"

    await ch._handle_event(_posted_event(
        user_id="user-1",
        channel_id="channel-abc",
        channel_type="O",
        post_id="p1",
        root_id="",  # Will be set to post_id since reply_in_thread=True
        message="hello",
    ))

    assert len(ch.bus.inbound) == 1
    msg = ch.bus.inbound[0]
    # With reply_in_thread=True and no root_id, root_id becomes the post_id
    assert msg.session_key == "mattermost:channel-abc:p1"


@pytest.mark.asyncio
async def test_handle_event_existing_root_id_used_for_thread_session() -> None:
    ch = _make_channel(allowFrom=["*"], groupPolicy="open", replyInThread=True)
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"
    ch._bot_username = "mybot"

    await ch._handle_event(_posted_event(
        user_id="user-1",
        channel_id="channel-abc",
        channel_type="O",
        post_id="p2",
        root_id="thread-root-id",
        message="continuing a thread",
    ))

    assert len(ch.bus.inbound) == 1
    msg = ch.bus.inbound[0]
    assert msg.session_key == "mattermost:channel-abc:thread-root-id"


@pytest.mark.asyncio
async def test_handle_event_dm_has_no_session_key() -> None:
    ch = _make_channel(allowFrom=["*"])
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"
    ch._bot_username = "mybot"

    await ch._handle_event(_posted_event(
        user_id="user-1",
        channel_id="dm-channel",
        channel_type="D",
        post_id="p1",
        root_id="",
        message="hello",
    ))

    assert len(ch.bus.inbound) == 1
    msg = ch.bus.inbound[0]
    assert msg.session_key_override is None


@pytest.mark.asyncio
async def test_handle_event_group_dm_has_no_session_key() -> None:
    ch = _make_channel(allowFrom=["*"])
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"
    ch._bot_username = "mybot"

    await ch._handle_event(_posted_event(
        user_id="user-1",
        channel_id="gm-channel",
        channel_type="G",
        post_id="p1",
        root_id="",
        message="hello",
    ))

    assert len(ch.bus.inbound) == 1
    msg = ch.bus.inbound[0]
    assert msg.session_key_override is None


@pytest.mark.asyncio
async def test_handle_event_metadata_includes_channel_type() -> None:
    ch = _make_channel(allowFrom=["*"], groupPolicy="open")
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"
    ch._bot_username = "mybot"

    await ch._handle_event(_posted_event(
        user_id="user-1",
        channel_type="P",
        message="private channel",
    ))

    assert len(ch.bus.inbound) == 1
    assert ch.bus.inbound[0].metadata["mattermost"]["channel_type"] == "P"


# ---------------------------------------------------------------------------
# send_delta() / streaming tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_send_delta_creates_initial_post() -> None:
    ch = _make_channel()
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"

    await ch.send_delta("channel-abc", "Hello ", {"_stream_delta": True})

    post_calls = [c for c in ch._http.post_calls if "/posts" in c[0]]
    assert len(post_calls) == 1
    assert post_calls[0][1]["json"]["message"] == "Hello "
    assert "channel-abc" in ch._stream_bufs
    assert ch._stream_bufs["channel-abc"].post_id == "new-post-id"


@pytest.mark.asyncio
async def test_send_delta_throttles_edits() -> None:
    ch = _make_channel()
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"

    # First delta creates the post
    await ch.send_delta("channel-abc", "Hello", {"_stream_delta": True})

    # Second delta immediately after should be throttled (no edit call)
    await ch.send_delta("channel-abc", " world", {"_stream_delta": True})

    put_calls = ch._http.put_calls
    assert put_calls == []
    # Text should be accumulated
    assert ch._stream_bufs["channel-abc"].text == "Hello world"


@pytest.mark.asyncio
async def test_send_delta_edits_after_interval() -> None:
    ch = _make_channel()
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"

    # Seed a buf with an old last_edit timestamp
    ch._stream_bufs["channel-abc"] = _StreamBuf(
        text="Hello",
        post_id="existing-post",
        last_edit=time.monotonic() - 10.0,  # 10s ago → past throttle window
    )

    await ch.send_delta("channel-abc", " world", {"_stream_delta": True})

    put_calls = ch._http.put_calls
    assert len(put_calls) == 1
    assert put_calls[0][1]["json"]["message"] == "Hello world"


@pytest.mark.asyncio
async def test_send_delta_stream_end_does_final_edit() -> None:
    ch = _make_channel()
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"

    # Seed a streaming buffer
    ch._stream_bufs["channel-abc"] = _StreamBuf(
        text="Hello world",
        post_id="existing-post",
        last_edit=time.monotonic(),
    )

    await ch.send_delta("channel-abc", "", {"_stream_end": True})

    put_calls = ch._http.put_calls
    assert len(put_calls) == 1
    assert put_calls[0][1]["json"]["message"] == "Hello world"
    assert "channel-abc" not in ch._stream_bufs


@pytest.mark.asyncio
async def test_send_delta_stream_end_noop_when_no_buffer() -> None:
    ch = _make_channel()
    ch._http = _FakeHttpClient()

    # No buffer for this chat_id — should be silent no-op
    await ch.send_delta("channel-abc", "", {"_stream_end": True})

    assert ch._http.put_calls == []


@pytest.mark.asyncio
async def test_send_delta_uses_root_id_from_metadata() -> None:
    ch = _make_channel()
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"

    await ch.send_delta(
        "channel-abc",
        "Reply in thread",
        {"_stream_delta": True, "mattermost": {"root_id": "thread-root"}},
    )

    post_calls = [c for c in ch._http.post_calls if "/posts" in c[0]]
    assert len(post_calls) == 1
    body = post_calls[0][1]["json"]
    assert body["root_id"] == "thread-root"


@pytest.mark.asyncio
async def test_send_delta_skips_empty_whitespace_text() -> None:
    ch = _make_channel()
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"

    await ch.send_delta("channel-abc", "   ", {"_stream_delta": True})

    # No post created since text is just whitespace
    post_calls = [c for c in ch._http.post_calls if "/posts" in c[0]]
    assert post_calls == []


# ---------------------------------------------------------------------------
# Reaction helpers
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_add_reaction_calls_reactions_api() -> None:
    ch = _make_channel()
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"

    await ch._add_reaction("post-1", "eyes")

    calls = [c for c in ch._http.post_calls if "/reactions" in c[0]]
    assert len(calls) == 1
    body = calls[0][1]["json"]
    assert body["post_id"] == "post-1"
    assert body["emoji_name"] == "eyes"
    assert body["user_id"] == "bot-id"


@pytest.mark.asyncio
async def test_remove_reaction_calls_delete_endpoint() -> None:
    ch = _make_channel()
    ch._http = _FakeHttpClient()
    ch._bot_user_id = "bot-id"

    await ch._remove_reaction("post-1", "eyes")

    calls = ch._http.delete_calls
    assert len(calls) == 1
    assert "/users/bot-id/posts/post-1/reactions/eyes" in calls[0][0]


@pytest.mark.asyncio
async def test_add_reaction_is_noop_when_bot_user_id_missing() -> None:
    ch = _make_channel()
    ch._http = _FakeHttpClient()
    ch._bot_user_id = None  # Not yet resolved

    await ch._add_reaction("post-1", "eyes")

    calls = [c for c in ch._http.post_calls if "/reactions" in c[0]]
    assert calls == []


# ---------------------------------------------------------------------------
# Mention helpers
# ---------------------------------------------------------------------------


def test_is_mentioned_returns_true_when_at_username_present() -> None:
    ch = _make_channel()
    ch._bot_username = "mybot"
    assert ch._is_mentioned("@mybot please help") is True


def test_is_mentioned_case_insensitive() -> None:
    ch = _make_channel()
    ch._bot_username = "mybot"
    assert ch._is_mentioned("@MyBot help me") is True


def test_is_mentioned_returns_false_when_absent() -> None:
    ch = _make_channel()
    ch._bot_username = "mybot"
    assert ch._is_mentioned("no mention here") is False


def test_is_mentioned_returns_false_when_bot_username_not_set() -> None:
    ch = _make_channel()
    ch._bot_username = None
    assert ch._is_mentioned("@somebot help") is False


def test_strip_mention_removes_at_username() -> None:
    ch = _make_channel()
    ch._bot_username = "mybot"
    assert ch._strip_mention("@mybot help me please") == "help me please"


def test_strip_mention_handles_middle_of_text() -> None:
    ch = _make_channel()
    ch._bot_username = "mybot"
    result = ch._strip_mention("hey @mybot can you help?")
    assert "@mybot" not in result


def test_strip_mention_returns_text_unchanged_when_no_mention() -> None:
    ch = _make_channel()
    ch._bot_username = "mybot"
    assert ch._strip_mention("no mention") == "no mention"
