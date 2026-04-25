"""Mattermost channel implementation using the Mattermost v4 REST API and WebSocket."""

from __future__ import annotations

import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal

import httpx
from loguru import logger
from pydantic import Field
from websockets.asyncio.client import connect as ws_connect
from websockets.exceptions import ConnectionClosed

from nanobot.bus.events import OutboundMessage
from nanobot.bus.queue import MessageBus
from nanobot.channels.base import BaseChannel
from nanobot.config.schema import Base
from nanobot.utils.helpers import split_message

# Mattermost hard limit for post text (16 383 characters)
MM_MAX_POST_LEN = 16_383
# Minimum interval between streaming edits to stay within Mattermost rate limits
MM_STREAM_EDIT_INTERVAL = 0.5
# Reconnect back-off delays (seconds) after a WebSocket disconnect
_WS_BACKOFF = (1, 2, 5, 10, 30)


class MattermostConfig(Base):
    """Mattermost channel configuration."""

    enabled: bool = False
    url: str = ""
    token: str = ""
    scheme: str = "https"
    port: int = 443
    allow_from: list[str] = Field(default_factory=list)
    group_policy: Literal["mention", "open"] = "mention"
    reply_in_thread: bool = True
    react_emoji: str = "eyes"
    done_emoji: str = "white_check_mark"
    streaming: bool = False


@dataclass
class _StreamBuf:
    """Per-chat streaming accumulator for progressive Mattermost message edits."""

    text: str = ""
    post_id: str | None = None
    last_edit: float = field(default_factory=time.monotonic)
    stream_id: str | None = None


class MattermostChannel(BaseChannel):
    """Mattermost channel using the Mattermost v4 REST API and WebSocket."""

    name = "mattermost"
    display_name = "Mattermost"

    @classmethod
    def default_config(cls) -> dict[str, Any]:
        return MattermostConfig().model_dump(by_alias=True)

    def __init__(self, config: Any, bus: MessageBus):
        if isinstance(config, dict):
            config = MattermostConfig.model_validate(config)
        super().__init__(config, bus)
        self.config: MattermostConfig = config
        self._http: httpx.AsyncClient | None = None
        self._bot_user_id: str | None = None
        self._bot_username: str | None = None
        self._ws: Any | None = None
        self._stream_bufs: dict[str, _StreamBuf] = {}

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Start the Mattermost channel — resolves bot identity then runs a WebSocket loop."""
        if not self.config.token:
            logger.error("Mattermost token not configured")
            return
        if not self.config.url:
            logger.error("Mattermost url not configured")
            return

        self._running = True
        self._http = httpx.AsyncClient(
            headers={"Authorization": f"Bearer {self.config.token}"},
            timeout=30.0,
        )

        # Resolve bot identity
        try:
            resp = await self._http.get(self._api("/users/me"))
            resp.raise_for_status()
            data = resp.json()
            self._bot_user_id = data.get("id")
            self._bot_username = data.get("username")
            logger.info(
                "Mattermost bot connected as @{} ({})",
                self._bot_username,
                self._bot_user_id,
            )
        except Exception as e:
            logger.warning("Mattermost /users/me failed: {}", e)

        # WebSocket reconnect loop
        backoff_idx = 0
        while self._running:
            try:
                ws_url = self._ws_url()
                logger.info("Mattermost connecting to WebSocket: {}", ws_url)
                async with ws_connect(
                    ws_url,
                    additional_headers={"Authorization": f"Bearer {self.config.token}"},
                    ping_interval=30,
                    ping_timeout=10,
                ) as ws:
                    self._ws = ws
                    backoff_idx = 0
                    # Authenticate via challenge (some MM deployments require it)
                    await ws.send(json.dumps({
                        "seq": 1,
                        "action": "authentication_challenge",
                        "data": {"token": self.config.token},
                    }))
                    async for raw in ws:
                        if not self._running:
                            break
                        try:
                            event = json.loads(raw)
                            await self._handle_event(event)
                        except Exception as e:
                            logger.warning("Mattermost event handling error: {}", e)
            except ConnectionClosed as e:
                if not self._running:
                    break
                logger.warning("Mattermost WebSocket closed: {}", e)
            except Exception as e:
                if not self._running:
                    break
                logger.warning("Mattermost WebSocket error: {}", e)

            if not self._running:
                break

            delay = _WS_BACKOFF[min(backoff_idx, len(_WS_BACKOFF) - 1)]
            backoff_idx += 1
            logger.info("Mattermost reconnecting in {}s...", delay)
            await asyncio.sleep(delay)

        self._ws = None

    async def stop(self) -> None:
        """Stop the channel and clean up resources."""
        self._running = False
        if self._ws:
            try:
                await self._ws.close()
            except Exception as e:
                logger.debug("Mattermost WS close failed: {}", e)
            self._ws = None
        if self._http:
            try:
                await self._http.aclose()
            except Exception as e:
                logger.debug("Mattermost HTTP close failed: {}", e)
            self._http = None

    # ------------------------------------------------------------------
    # Outbound
    # ------------------------------------------------------------------

    async def send(self, msg: OutboundMessage) -> None:
        """Send a message through Mattermost."""
        if not self._http:
            raise RuntimeError("Mattermost HTTP client not initialized")

        channel_id = str(msg.chat_id)
        mm_meta = (msg.metadata or {}).get("mattermost", {})
        root_id = mm_meta.get("root_id") or ""

        # Upload files first so file_ids can be attached to posts
        file_ids: list[str] = []
        for media_path in msg.media or []:
            try:
                fid = await self._upload_file(channel_id, media_path)
                if fid:
                    file_ids.append(fid)
            except Exception as e:
                logger.error("Mattermost file upload failed for {}: {}", media_path, e)

        # Split long messages
        chunks = split_message(msg.content or "", MM_MAX_POST_LEN)
        if not chunks:
            # No text but may have files
            if file_ids:
                chunks = [""]
            else:
                return

        for i, chunk in enumerate(chunks):
            post_payload: dict[str, Any] = {
                "channel_id": channel_id,
                "message": chunk,
            }
            if root_id:
                post_payload["root_id"] = root_id
            if i == len(chunks) - 1 and file_ids:
                post_payload["file_ids"] = file_ids

            try:
                resp = await self._http.post(self._api("/posts"), json=post_payload)
                resp.raise_for_status()
            except Exception as e:
                logger.error("Mattermost send failed: {}", e)
                raise

        # Update reactions when this is a final (non-progress) response
        if not (msg.metadata or {}).get("_progress"):
            post_id = mm_meta.get("post_id")
            if post_id:
                await self._remove_reaction(post_id, self.config.react_emoji)
                if self.config.done_emoji:
                    await self._add_reaction(post_id, self.config.done_emoji)

    async def send_delta(
        self, chat_id: str, delta: str, metadata: dict[str, Any] | None = None
    ) -> None:
        """Progressive Mattermost delivery: send once, then edit until the stream ends."""
        if not self._http:
            return

        meta = metadata or {}
        stream_id = meta.get("_stream_id")

        if meta.get("_stream_end"):
            buf = self._stream_bufs.get(chat_id)
            if not buf or not buf.text:
                self._stream_bufs.pop(chat_id, None)
                return
            if stream_id is not None and buf.stream_id is not None and buf.stream_id != stream_id:
                return
            await self._stream_commit(chat_id, buf)
            return

        buf = self._stream_bufs.get(chat_id)
        if buf is None or (
            stream_id is not None and buf.stream_id is not None and buf.stream_id != stream_id
        ):
            buf = _StreamBuf(stream_id=stream_id)
            self._stream_bufs[chat_id] = buf
        elif buf.stream_id is None and stream_id is not None:
            buf.stream_id = stream_id

        buf.text += delta
        if not buf.text.strip():
            return

        mm_meta = meta.get("mattermost", {})
        root_id = mm_meta.get("root_id") or ""

        if buf.post_id is None:
            # Create the initial post
            post_payload: dict[str, Any] = {
                "channel_id": chat_id,
                "message": buf.text,
            }
            if root_id:
                post_payload["root_id"] = root_id
            try:
                resp = await self._http.post(self._api("/posts"), json=post_payload)
                resp.raise_for_status()
                buf.post_id = resp.json().get("id")
                buf.last_edit = time.monotonic()
            except Exception as e:
                logger.warning("Mattermost stream initial post failed: {}", e)
                raise
            return

        # Throttle edits
        now = time.monotonic()
        if (now - buf.last_edit) < MM_STREAM_EDIT_INTERVAL:
            return

        try:
            resp = await self._http.put(
                self._api(f"/posts/{buf.post_id}/patch"),
                json={"message": buf.text},
            )
            resp.raise_for_status()
            buf.last_edit = now
        except Exception as e:
            logger.warning("Mattermost stream edit failed: {}", e)
            raise

    async def _stream_commit(self, chat_id: str, buf: _StreamBuf) -> None:
        """Finalize a stream by editing the post to its full content."""
        if buf.post_id and buf.text:
            try:
                resp = await self._http.put(
                    self._api(f"/posts/{buf.post_id}/patch"),
                    json={"message": buf.text},
                )
                resp.raise_for_status()
            except Exception as e:
                logger.warning("Mattermost stream final edit failed: {}", e)
                raise
        self._stream_bufs.pop(chat_id, None)

    # ------------------------------------------------------------------
    # Inbound
    # ------------------------------------------------------------------

    async def _handle_event(self, event: dict[str, Any]) -> None:
        """Handle an incoming Mattermost WebSocket event."""
        event_type = event.get("event")
        if event_type != "posted":
            return

        data = event.get("data") or {}
        post_raw = data.get("post")
        if not post_raw:
            return

        try:
            post = json.loads(post_raw) if isinstance(post_raw, str) else post_raw
        except Exception:
            return

        user_id = post.get("user_id") or ""
        post_id = post.get("id") or ""
        channel_id = post.get("channel_id") or ""
        root_id = post.get("root_id") or ""
        message = str(post.get("message") or "")
        post_type = post.get("type") or ""

        # Skip own messages
        if self._bot_user_id and user_id == self._bot_user_id:
            return

        # Skip system/webhook posts (non-empty type field)
        if post_type:
            return

        if not user_id or not channel_id:
            return

        channel_type = data.get("channel_type") or ""
        # D = DM, G = group DM, O = public channel, P = private channel
        is_dm = channel_type in ("D", "G")

        if is_dm:
            # DMs: apply global allow_from check (done in _handle_message)
            pass
        else:
            # Channel: enforce group_policy
            if self.config.group_policy == "mention":
                if not self._is_mentioned(message):
                    return
            # Strip mention before passing to agent
            message = self._strip_mention(message)

        # Add in-progress reaction (best-effort)
        if self.config.react_emoji and post_id:
            await self._add_reaction(post_id, self.config.react_emoji)

        # Determine root_id for thread replies
        if self.config.reply_in_thread and not root_id:
            root_id = post_id

        # Thread-scoped session key for non-DM messages
        session_key: str | None = None
        if not is_dm and root_id:
            session_key = f"mattermost:{channel_id}:{root_id}"

        logger.debug(
            "Mattermost posted: user={} channel={} channel_type={} post_id={} root_id={} text={}",
            user_id,
            channel_id,
            channel_type,
            post_id,
            root_id,
            message[:80],
        )

        try:
            await self._handle_message(
                sender_id=user_id,
                chat_id=channel_id,
                content=message,
                metadata={
                    "mattermost": {
                        "post_id": post_id,
                        "root_id": root_id,
                        "channel_type": channel_type,
                    },
                },
                session_key=session_key,
            )
        except Exception:
            logger.exception("Error handling Mattermost message from {}", user_id)

    # ------------------------------------------------------------------
    # Reactions
    # ------------------------------------------------------------------

    async def _add_reaction(self, post_id: str, emoji_name: str) -> None:
        """Add an emoji reaction to a post (best-effort)."""
        if not self._http or not self._bot_user_id or not emoji_name or not post_id:
            return
        try:
            resp = await self._http.post(
                self._api("/reactions"),
                json={
                    "user_id": self._bot_user_id,
                    "post_id": post_id,
                    "emoji_name": emoji_name,
                    "create_at": 0,  # Required by MM API; server overrides with actual timestamp
                },
            )
            resp.raise_for_status()
        except Exception as e:
            logger.debug("Mattermost add_reaction failed: {}", e)

    async def _remove_reaction(self, post_id: str, emoji_name: str) -> None:
        """Remove an emoji reaction from a post (best-effort)."""
        if not self._http or not self._bot_user_id or not emoji_name or not post_id:
            return
        try:
            resp = await self._http.delete(
                self._api(f"/users/{self._bot_user_id}/posts/{post_id}/reactions/{emoji_name}"),
            )
            resp.raise_for_status()
        except Exception as e:
            logger.debug("Mattermost remove_reaction failed: {}", e)

    # ------------------------------------------------------------------
    # File upload
    # ------------------------------------------------------------------

    async def _upload_file(self, channel_id: str, file_path: str) -> str | None:
        """Upload a file to a Mattermost channel, returning the file_id."""
        if not self._http:
            return None
        path = Path(file_path)
        if not path.is_file():
            logger.warning("Mattermost file not found, skipping: {}", file_path)
            return None
        try:
            with path.open("rb") as fh:
                resp = await self._http.post(
                    self._api("/files"),
                    data={"channel_id": channel_id},
                    files={"files": (path.name, fh)},
                )
            resp.raise_for_status()
            file_infos = resp.json().get("file_infos") or []
            if file_infos:
                return str(file_infos[0].get("id") or "")
        except Exception as e:
            logger.error("Mattermost upload_file failed: {}", e)
        return None

    # ------------------------------------------------------------------
    # Mention helpers
    # ------------------------------------------------------------------

    def _is_mentioned(self, text: str) -> bool:
        """Return True when the bot's @username appears in *text*."""
        if not self._bot_username:
            return False
        return bool(re.search(rf"@{re.escape(self._bot_username)}\b", text, re.IGNORECASE))

    def _strip_mention(self, text: str) -> str:
        """Remove the bot's @mention from the beginning or anywhere in *text*."""
        if not self._bot_username or not text:
            return text
        return re.sub(
            rf"@{re.escape(self._bot_username)}\b\s*", "", text, flags=re.IGNORECASE
        ).strip()

    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------

    def _base_url(self) -> str:
        """Build the base HTTP URL from config."""
        url = self.config.url.rstrip("/")
        if url:
            return url
        # Fall back to constructing from scheme/port if url is empty
        scheme = self.config.scheme
        port = self.config.port
        return f"{scheme}://localhost:{port}"

    def _api(self, path: str) -> str:
        """Build a full Mattermost API v4 URL."""
        return f"{self._base_url()}/api/v4{path}"

    def _ws_url(self) -> str:
        """Build the WebSocket URL for the Mattermost v4 API."""
        base = self._base_url()
        # Replace http(s) scheme with ws(s)
        if base.startswith("https://"):
            return base.replace("https://", "wss://", 1) + "/api/v4/websocket"
        if base.startswith("http://"):
            return base.replace("http://", "ws://", 1) + "/api/v4/websocket"
        return f"wss://{base}/api/v4/websocket"
