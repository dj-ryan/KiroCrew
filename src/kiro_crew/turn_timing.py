"""Per-turn first-byte timing and the start-queue wait histogram.

Two measurements for reading where a turn's time goes before its first model
byte, under load.

**The ``turn_timing`` log line** (only when ``KIROCREW_TURN_TIMING=1``). One
record per top-level turn rides a context variable from the ``/api/chat``
handler into the turn task (``create_task`` copies the context, and the record
is a shared mutable object), so the gate waits recorded deep inside session
allocation land on the turn that paid them. The record opens at the handler
and is adopted at runner entry, not at the session claim, because the memory
preparation wait, the session dispatch lock and the worker-thread hops of turn
admission all sit before the claim. It is written to ``gateway.log`` as one
``turn_timing outcome=<outcome>`` line when the turn's first visible model
output arrives (``visible``), or when the turn ends without one
(``no_visible``, ``ended_at_open``, ``ended_at_slash_gate``). Every field is
on every line; a span the turn never reached reads ``-``. An SSE request also
logs one ``turn_timing stage=sse_first_frame`` line at its first content frame.

Gate waits come from ``start_priority.notify_start_queue``, which each of the
three start gates (cold-start, spawn admission, session/new) already calls at
acquire with ``(wait_ms, queue)``; provider phases come from the ``phases``
map of the provider start. Nothing is timed twice. Waits and phases are each
summed over the turn, which pays them twice when it claims a replacement
session. ``spawn_init_ms`` includes the spawn-admission wait and
``session_new_ms`` includes the session/new wait, so pure spawn time is
``spawn_init_ms - spawn_admission_wait_ms`` and pure session/new time is
``session_new_ms - session_new_wait_ms``.
``exec_q0`` and ``exec_q_claim`` are the default executor's (``mc-default``)
work-queue depth at turn start and at claim start.

Count starts from ``turn_timing`` lines or ``acp_cold_start stage=spawn``
lines, never from the outer gate's ``session_cold_start stage=queue_wait``
lines: that gate logs only waits of ``START_QUEUE_LOG_MIN_MS`` or more.

The ``kiro_crew.turn_timing`` logger alone is set to INFO when enabled, so the
line appears at the default ``agent.log_level`` of WARNING without turning on
every other INFO line in the gateway, which would change the load being
measured. With the variable unset every record function returns after one
boolean test and the logger level is left alone.

**The start-queue wait histogram** (through the metrics recorder). Every
acquire at one of the three gates records its wait on
``kirocrew.session.start_queue.wait`` (ms) with a ``gate`` attribute, so it
counts every start (subagents, crons, eager spawns), not only turns. Like every
recorder metric it is collected only while telemetry is on
(``telemetry.enabled`` or ``KIROCREW_TELEMETRY=1``, no restart needed) and is a
no-op otherwise; it does not need ``KIROCREW_TURN_TIMING``.

Neither measurement reaches a WebSocket or SSE frame: gate depth and wait
reveal other users' activity.
"""

from __future__ import annotations

import contextvars
import logging
import os
import time
from typing import Any

from kiro_crew.metrics.events import START_QUEUE_WAIT, emit_histogram

ENABLED = os.environ.get("KIROCREW_TURN_TIMING", "") == "1"

logger = logging.getLogger("kiro_crew.turn_timing")
if ENABLED:
    logger.setLevel(logging.INFO)

#: Start-queue name (``kiro_crew.start_priority``) -> record field.
_WAIT_FIELDS = {
    "cold-start": "cold_start_wait_ms",
    "spawn admission": "spawn_admission_wait_ms",
    "session/new": "session_new_wait_ms",
}

#: Provider start phase -> record field. ``session_load`` is a resume's
#: ``session/load``, logged beside ``session_new`` because a resume that falls
#: back to a new session pays both.
_PHASE_FIELDS = {
    "spawn_init": "spawn_init_ms",
    "session_load": "session_load_ms",
    "session_new": "session_new_ms",
    "set_model": "set_model_ms",
}

#: Start-queue name -> the histogram's closed ``gate`` attribute.
_GATE_ATTRS = {
    "cold-start": "cold_start",
    "spawn admission": "spawn_admission",
    "session/new": "session_new",
}

_current: contextvars.ContextVar[dict[str, Any] | None] = contextvars.ContextVar(
    "kirocrew_turn_timing", default=None
)


def _default_executor_queue() -> int:
    """Items queued on the loop's default executor (``mc-default``), or -1."""
    try:
        import asyncio

        pool = getattr(asyncio.get_running_loop(), "_default_executor", None)
        queue = getattr(pool, "_work_queue", None)
        return int(queue.qsize()) if queue is not None else -1
    except Exception:
        return -1


def now() -> float:
    """``time.monotonic()``, for a caller that opens the record later."""
    return time.monotonic()


def begin(slot_key: str, origin: str, t0: float | None = None) -> None:
    """Open a record for the turn about to start in this context."""
    if not ENABLED:
        return
    _current.set(
        {
            "t0": time.monotonic() if t0 is None else t0,
            "slot": slot_key,
            "origin": origin,
            "marks": {},
            "waits": {},
            "phases": {},
            "exec_q0": _default_executor_queue(),
            "emitted": False,
        }
    )


def ensure(slot_key: str, origin: str) -> None:
    """Adopt the handler's record, or open one (queued, cron and drained turns).

    A record a runner already adopted belongs to an earlier turn whose task this
    one was started from, so it is never reused.
    """
    if not ENABLED:
        return
    rec = _current.get()
    if rec is None or rec["emitted"] or rec["slot"] != slot_key or "runner" in rec["marks"]:
        begin(slot_key, origin)
    mark("runner")


def mark(name: str) -> None:
    """Record ms since the record opened, first occurrence wins."""
    if not ENABLED:
        return
    rec = _current.get()
    if rec is None or rec["emitted"]:
        return
    rec["marks"].setdefault(name, round((time.monotonic() - rec["t0"]) * 1000.0, 1))
    if name == "claim_start":
        rec.setdefault("exec_q_claim", _default_executor_queue())


def gate_acquired(queue: object, wait_ms: object) -> None:
    """Record one start-queue wait (called from ``notify_start_queue``)."""
    if not isinstance(queue, str) or queue not in _WAIT_FIELDS:
        return
    if isinstance(wait_ms, bool) or not isinstance(wait_ms, (int, float)):
        return
    wait = float(wait_ms)
    emit_histogram(START_QUEUE_WAIT, wait, {"gate": _GATE_ATTRS[queue]}, unit="ms")
    if not ENABLED:
        return
    rec = _current.get()
    if rec is None or rec["emitted"]:
        return
    field = _WAIT_FIELDS[queue]
    rec["waits"][field] = round(rec["waits"].get(field, 0.0) + wait, 1)


def phase(name: str, ms: float) -> None:
    """Add a provider start phase duration (it includes that phase's gate wait)."""
    if not ENABLED:
        return
    rec = _current.get()
    if rec is None or rec["emitted"]:
        return
    rec["phases"][name] = round(rec["phases"].get(name, 0.0) + float(ms), 1)


def note(**fields: object) -> None:
    """Attach small scalar facts (is_new, resumed, priority)."""
    if not ENABLED:
        return
    rec = _current.get()
    if rec is None or rec["emitted"]:
        return
    rec.setdefault("notes", {}).update(fields)


def emit(outcome: str) -> None:
    """Write the record once, then close it."""
    if not ENABLED:
        return
    rec = _current.get()
    if rec is None or rec["emitted"]:
        return
    rec["emitted"] = True
    marks = rec["marks"]
    waits = rec["waits"]
    phases = rec["phases"]
    notes = rec.get("notes", {})

    def span(a: str, b: str) -> str:
        if a in marks and b in marks:
            return f"{marks[b] - marks[a]:.1f}"
        return "-"

    def at(name: str) -> str:
        return f"{marks[name]:.1f}" if name in marks else "-"

    parts = [
        "turn_timing",
        f"outcome={outcome}",
        f"slot={rec['slot']}",
        f"origin={rec['origin']}",
        f"total_ms={(time.monotonic() - rec['t0']) * 1000.0:.1f}",
        f"exec_q0={rec['exec_q0']}",
        f"exec_q_claim={rec.get('exec_q_claim', -1)}",
        f"handler_ms={at('dispatched')}",
        f"task_start_ms={span('dispatched', 'runner')}",
        f"pre_admit_ms={span('runner', 'admit_start')}",
        f"memory_wait_ms={span('admit_start', 'memory_ready')}",
        f"admit_prelude_ms={span('memory_ready', 'dispatch_lock')}",
        f"pre_claim_ms={span('dispatch_lock', 'claim_start')}",
        f"claim_ms={span('claim_start', 'claim_end')}",
        f"build_prompt_ms={span('claim_end', 'prompt_built')}",
        f"dispatch_gates_ms={span('prompt_built', 'stream_open')}",
        f"first_event_ms={span('stream_open', 'first_event')}",
        f"first_visible_ms={at('first_visible')}",
    ]
    for field in _WAIT_FIELDS.values():
        parts.append(f"{field}={waits.get(field, 0.0):.1f}")
    for name, field in _PHASE_FIELDS.items():
        parts.append(f"{field}={phases[name]:.1f}" if name in phases else f"{field}=-")
    for key in ("is_new", "resumed", "priority"):
        parts.append(f"{key}={notes.get(key, '-')}")
    for key, value in sorted(notes.items()):
        if key not in ("is_new", "resumed", "priority"):
            parts.append(f"{key}={value}")
    logger.info(" ".join(parts))


def sse_first_frame(slot_key: str) -> None:
    """The handler wrote this turn's first SSE content frame."""
    if not ENABLED:
        return
    rec = _current.get()
    if rec is None or rec.get("sse_logged"):
        return
    rec["sse_logged"] = True
    logger.info(
        "turn_timing stage=sse_first_frame slot=%s first_frame_ms=%.1f",
        slot_key,
        (time.monotonic() - rec["t0"]) * 1000.0,
    )
