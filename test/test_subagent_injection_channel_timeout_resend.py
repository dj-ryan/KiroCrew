"""A channel completion injection that times out is not sent to the parent twice.

The channel route makes up to two attempts and only a timeout starts the second. A
timeout can land after the prompt reached the backend, and the timeout's reset keeps
the parent's conversation, so re-sending the completion would put it in that
conversation twice. The second attempt sends the completion again only when the
first never wrote its prompt; otherwise it asks the parent to continue.

Drives the real ``SessionManager`` lease. The first attempt hangs on an event that is
never set, so the patched injection timeout is what ends it. The route's own pause
between attempts runs as written.
"""

from __future__ import annotations

import asyncio
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from kiro_crew.config import KiroCrewConfig
from kiro_crew.session import SessionManager
from kiro_crew.subagent import SubagentInfo

#: Ceiling for a wait that only fails on a hang, so a regression fails by name.
_HANG_CEILING_S = 30
_ANNOUNCE = "child-1 finished: the answer is 42"


def _provider_factory(session_ids: list[str]) -> Any:
    """Each provider the manager builds takes the next backend session id."""

    def factory(session_key: Any = None, agent: Any = None, channel_id: Any = None, **kwargs: Any):
        m = AsyncMock()
        m.start = AsyncMock()
        m.memory_mode = kwargs.get("memory_mode", "persistent")
        m.shutdown = AsyncMock()
        m.is_process_alive = lambda: True
        m.disown_work_dir = MagicMock()
        m.context_usage_pct = lambda: 0.0
        m.context_window_tokens = lambda: 0
        m.has_active_turn = lambda: False
        m.runtime_abort_target = lambda: None
        m.spawn_identity = ""
        m._runtime = None
        m.cancel = AsyncMock(return_value="acked")
        m.prompts_written = 0
        m.session_id = session_ids.pop(0)
        return m

    return factory


def _gateway(sessions: SessionManager) -> Any:
    from kiro_crew.slack.gateway import GatewayOrchestrator

    gateway = GatewayOrchestrator.__new__(GatewayOrchestrator)
    gateway.sessions = sessions
    gateway.ctx_builder = None
    gateway.conv_log = None
    gateway.dashboard_state = None
    gateway.slack = None
    gateway._owner_id = ""
    gateway.subagent_mgr = MagicMock()
    gateway._channel_reply_link = MagicMock(return_value=None)
    gateway._deliver_channel_reply = AsyncMock()
    return gateway


async def _run(
    *,
    first_attempt_writes: bool,
    retry_resumes: bool = True,
    retry_flags: tuple[bool, bool] = (True, True),
) -> tuple[list[str], Any]:
    key = "discord:dm:42"
    # The timeout's reset makes the retry's claim a new provider. A resume loads
    # the same backend session id; a fresh conversation gets another one.
    sessions = SessionManager(
        KiroCrewConfig(),
        provider_factory=_provider_factory(["sess-a", "sess-a" if retry_resumes else "sess-b"]),
    )
    gateway = _gateway(sessions)
    real_get_or_create = sessions.get_or_create
    claims = 0

    async def get_or_create(session_key: str, **kwargs: Any) -> tuple[Any, bool, bool]:
        # The retry's own (is_new, resumed) flags say nothing reliable about the
        # conversation: another claim can have used up its first turn first.
        nonlocal claims
        claims += 1
        client, is_new, resumed = await real_get_or_create(session_key, **kwargs)
        if claims > 1:
            return client, *retry_flags
        return client, is_new, resumed

    sessions.get_or_create = get_or_create  # type: ignore[method-assign]
    sent: list[str] = []
    never = asyncio.Event()

    async def inject(info, client, msg, parent_key, label):
        sent.append(msg)
        if len(sent) == 1:
            if first_attempt_writes:
                client.prompts_written += 1
            await never.wait()
        return "synthesized reply"

    gateway._inject_with_retry = inject
    info = SubagentInfo(id="child-1", task="work", parent_session_key=key)
    info.result = "done"
    info.done = True
    try:
        with (
            patch("kiro_crew.slack.gateway._MAX_INJECT_ATTEMPTS", 2),
            patch("kiro_crew.slack.gateway.INJECTION_TIMEOUT", 0.05),
        ):
            await asyncio.wait_for(
                gateway._inject_subagent_into_channel_parent(
                    info,
                    announce=_ANNOUNCE,
                    body="b",
                    title="t",
                    parent_key=key,
                    carried=(info.id,),
                ),
                _HANG_CEILING_S,
            )
    finally:
        await sessions.close_all()
    return sent, gateway


@pytest.mark.asyncio
async def test_timeout_after_the_prompt_went_out_does_not_resend_the_completion() -> None:
    sent, gateway = await _run(first_attempt_writes=True)

    assert len(sent) == 2
    assert sent[0] == _ANNOUNCE
    assert _ANNOUNCE not in sent[1], "the completion was sent to the parent a second time"
    assert "child-1" in sent[1] and "Continue" in sent[1]
    gateway._deliver_channel_reply.assert_awaited_once()
    gateway.subagent_mgr.notify_injection_failed.assert_not_called()


@pytest.mark.asyncio
async def test_timeout_before_the_prompt_went_out_resends_the_completion() -> None:
    sent, gateway = await _run(first_attempt_writes=False)

    assert sent == [_ANNOUNCE, _ANNOUNCE]
    gateway._deliver_channel_reply.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "retry_flags",
    [(True, False), (False, False)],
    ids=["reported_new", "first_turn_used_by_a_queued_message"],
)
async def test_a_retry_on_a_conversation_that_did_not_come_back_resends_the_completion(
    retry_flags: tuple[bool, bool],
) -> None:
    sent, gateway = await _run(
        first_attempt_writes=True, retry_resumes=False, retry_flags=retry_flags
    )

    assert sent == [_ANNOUNCE, _ANNOUNCE], "the new conversation never saw the completion"
    gateway._deliver_channel_reply.assert_awaited_once()
