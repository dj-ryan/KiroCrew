---
title: Provider throttle queue - queue and retry turns while the model provider throttles, instead of failing them
status: accepted
author: pearcekieser
created: 2026-11-03
last-audited: 2026-11-03
audited-at: b44d1d0e2f
doc-pr: null
implementation-prs: []
tracking-issues: []
supersedes: []
superseded-by: []
---

# RFC: Provider throttle queue

- Status: accepted. This is the product owner's decision, given 2026-11-03 by
  Pearce Kieser (@Pearcekieser). Nothing of it is on main. This document lands
  on its own first, because the implementation changes what a chat turn does
  when the provider throttles, and the First Principles lane reads that decision
  off the base.
- Extends [rfc-overload-resilience.md](rfc-overload-resilience.md) §14.4 (the
  dependency coordinator) to chat turns, cron turns and the other prompt starts.
  It does not edit that document.
- Claims about the code were checked at `b44d1d0e2f` (main).

## 1. Problem

When the model provider throttles, a chat turn ends on an error card
approximately 70 s after the first throttle. A throttle window of several minutes
outlasts that. A search of one host's dashboard chat transcripts for the `error`
rows with the two card texts below, over the 14 days to 2026-11-03, found 107
such failures:

- 99 turns ended on the throttle card ("The model provider is throttling
  requests" or "Bedrock is throttling requests"). 41 of them were on 2026-11-03.
- 8 turns ended on "The selected model is unavailable ... (capacity throttle or
  region rollout)".

Between 20:56Z and 21:09Z on 2026-11-03, 25 failures hit 18 sessions. Most of
those turns started together. A fleet orchestrator session sent them with
`session_send`, 5 to 8 in one minute.

A tool that counts these failures today must match the card text, because the
error row carries no structured kind for a throttle.

Today's behaviour, at `b44d1d0e2f`:

1. KAS retries a throttled request for approximately 6 s inside one attempt.
2. The chat ladder then re-sends the turn `_TRANSIENT_RETRIES = 3` times
   (`llm_helpers_parts/backend_errors.py`) on `throttle_retry_delay`. That curve
   starts at `THROTTLE_RETRY_BASE_DELAY = 6.0` s and doubles, with up to 25 %
   jitter. The waits add up to approximately 45 s. With the four KAS attempts,
   the turn ends approximately 70 s after the first throttle, on the throttle
   card with Resume.
3. Each session retries on its own clock. `_shared_dependency_delay` in
   `dashboard/chat_turn/recovery.py` floors the delay by the error's own stated
   reset and by the dependency coordinator's `retry_at` for the scope. A chat turn
   has no task row, so it never joins that schedule. A schedule exists only when
   a sub-agent, a TaskRunner step or a workflow agent opened one.
4. A cron turn's prompt goes through `stream_and_collect`
   (`llm_helpers_parts/turn_stream.py`). That loop re-sends a throttled prompt
   `_TRANSIENT_RETRIES = 3` times on `transient_retry_delay`, the 2 s curve, then
   tries the `agent.fallback_model` chain, then fails. The cron's own
   `_CRON_TRANSIENT_RETRIES = 2` (`slack/gateway_runtime/cron_dispatch.py`)
   covers only the steps before the prompt is sent.
5. A sub-agent with a task row parks on the `DependencyCoordinator` in
   `taskq/dependency.py`. It has one schedule per scope, one probe, then a ramp,
   a 3600 s deadline and 20 attempts. TaskRunner steps (`task_executor.py`) and
   workflow agents (`workflows/agent_pool.py`) park on it too.
6. Every caller classifies a throttle with no scope hint, and a throttle error
   names no model. So `scope_for` in `taskq/adapters/acp_provider.py` puts every
   throttle under `provider:acp`, and `record_provider_throttle` gets the same
   key.

So 18 sessions that the same provider throttles keep sending at the same time,
and each one fails on its own.

## 2. Goals

1. A throttle that any prompt start sees opens a cool-off for its scope. The
   cool-off applies to every prompt start on that scope: chat turns, cron turns,
   sub-agents and the other callers in §4.4.
2. A chat turn, cron turn or other `stream_and_collect` turn whose normal
   re-sends are spent on a throttle waits in a queue. It does not fail. A
   sub-agent keeps its durable park. While a cool-off is open, a new turn on that
   scope also waits. No turn is dropped, and no message runs twice.
3. The scope backs off exponentially with full jitter, up to a cap. A
   provider-stated Retry-After or reset time wins. A turn that waits past a
   give-up window ends on today's card with Resume.
4. When the provider recovers, the queue releases turns gradually by AIMD
   (additive increase, multiplicative decrease), in arrival order.
5. The chat shows that the turn is queued and when it retries next, with
   "Retry now" and "Cancel".
6. One config key turns all of this off and gives today's behaviour exactly.
7. A throttle card row carries a structured error kind, the provider and the
   model, so a tool can read it without matching the card text.
8. While a scope recovers from a throttle, turns that no person typed, such as
   `session_send` deliveries, start no faster than a set rate on that scope.

## 3. Non-goals

- Changing provider credentials or regions.
- Choosing a fallback model on its own. The opt-in `agent.fallback_model` chain
  keeps its place (§5.3). An automatic fallback is a possible later step.
- The "selected model is unavailable" error. It keeps today's path. Adding it to
  the queue is open question 2.
- A usage limit or a spent allowance (#21679). It stays terminal and never
  queues.
- The open sub-agent bugs #25212 and #17439 in the coordinator's wake and its
  one-continue rule. The gate does not depend on those paths.
- Surviving a gateway restart. The gate is in memory (§5.5).

## 4. Design

### 4.1 One gate, one schedule per scope

A new in-memory `ProviderThrottleGate` in `src/kiro_crew/adaptive/` holds one
record per scope. A scope is one ACP backend and one model, written
`provider:<backend>:<model>`. If throttles are account-wide, every model on the
account opens its own scope, which costs one extra probe per model. If they are
per model, a wider key would hold back models that are not throttled. So the
narrow key is the safe default.

The scope comes from the ACP client at admission, before any error exists. The
backend is the client's ACP backend. The model is the model that the client
requests, so a session on `auto` uses `provider:<backend>:auto` for every start.
A fallback swap changes the requested model, and with it the scope.

With the gate on, every caller passes that same scope as the hint to
`classify_exception`. So the coordinator schedule, `record_provider_throttle` and
the gate all use one key. With the gate off, no hint is passed, and every
throttle stays under `provider:acp` as today.

Admission sits where a turn's `session/prompt` request goes out, once for each
turn. There are two such places, one for each transport:
`AcpSessionHandle.prompt` (`acp/session_handle_turn.py`) for the shared runtime,
and `AcpClient._send_prompt` (`acp/client_turn.py`) for a direct client. Every
turn reaches one of them. That covers the chat runner and the slash commands it
sends as prompt text (`stream_command`), `stream_and_collect`, `TurnDriver`, sub-agents, TaskRunner
steps, workflow agents, compaction, `run_bg_oneliner` callers such as titles and
summaries, and direct callers such as the auto-improvement runner and the
code-review pool. A steer that the client sends as a second `session/prompt`
into a turn in flight (`_steer_via_concurrent_prompt`) is part of that turn and
does not ask again. Neither does a steer on `session/steering`.

Two paths send no `session/prompt` and keep today's behaviour. They are a
kiro-cli command sent with `_kiro.dev/commands/execute`, a typed `/compact` on a
direct kiro client included, and a model switch or other session-config call.

A caller sets an admission mode for its call (§4.4), and the default mode holds.
Time that a start waits for admission does not count toward its prompt timeout
or toward any stall watchdog.

A start counts as accepted at the first model-output event of its own session:
text, a thought, a tool call or the end of the turn. A co-tenant event that the
shared runtime forwards (`runtime_global=True`) does not count, and neither does
an MCP-server or steer event. Acceptance is what grows the window (§4.2). The
in-flight unit is separate. An admitted holding start (§4.4) holds its unit until
its turn ends, is throttled, fails or is cancelled, so a long turn that streams
output still counts against the window. A park start holds no unit. A throttle is an error that `acp_error_is_throttle`
accepts. The ACP adapter maps it to `rate_limited` or `concurrency_exceeded`.

The 429 rule from [dynamic-subagent-sizing](../../src/kiro_crew/docs/dynamic-subagent-sizing.md)
and `docs/system-specs/modules/adaptive-concurrency.md` stays. A throttle is
still reported to `record_provider_throttle(scope)`, and no host cap moves for
it. The gate is a per-provider channel only.

### 4.2 State machine

| State | Admits holding starts | Moves on |
|---|---|---|
| open (no record) | every one | a throttle: cooling, `k = 1` |
| cooling | none | `retry_at` passes, or Retry now on a waiter: probing |
| probing | one probe: the queue head, or the next holding start when the queue is empty | the first start accepted, the probe or a park start: ramping, window 2, so one more start can go while the probe's turn runs. A throttle: cooling, `k + 1`, or `k` unchanged when a Retry now started the probe. The probe ends with no output and no throttle (Stop, Cancel, a caller timeout or another error): the next queue head is the probe, `k` unchanged |
| ramping | while holding starts in flight < window | each accepted start, hold or park: window + 1. A throttle: window halved, rounded down. Window 0: cooling, `k + 1`. Window >= ramp ceiling and queue empty: open |

The window counts holding starts only. A park start (§4.4) is not admitted or
refused by the window, but its outcome moves the scope like any other start's.

- A cool-off is full jitter. Its length is uniform in
  `[0, min(max_secs, base_secs * 2^(k-1))]`. A stated Retry-After or reset time
  replaces it when it is later.
- No event moves `retry_at` earlier while a scope cools. A later stated reset
  moves it later. The coordinator's `retry_at` for the same scope is one more
  floor.
- `k` goes up only when a start that the gate released is throttled. It never
  goes up for each reporter, so 18 sessions that report one burst spend one step.
  This is the coordinator's rule (`DependencyCoordinator.report`).
- A throttle that a start reports while the scope is cooling joins the cool-off.
- Waiters are released in arrival order. Each waiter has a `(priority, seq)` sort
  key with priority 0. When the per-chat priority of #15543 lands, it can fill in
  the priority without a rewrite.
- The give-up window is per waiter. It is measured from that turn's first
  throttle or from its first wait, whichever came first.

### 4.3 Chat turns

A chat turn has one wait point. That point is admission at dispatch, before the
prompt goes out.

1. A throttle on a chat turn is reported to the gate. The ladder counts the
   re-send as it does today. The rewind of the throttled KAS attempt (#21876)
   runs, and then the turn puts its replay at the queue head with no sleep. The
   turn ends.
2. The drain starts the replay, and its prompt waits for admission before it is
   sent. This is the same wait that a new turn on a cooling scope gets. So a
   re-send and a new send wait in the same queue. A slash-command turn takes the
   same path, because its prompt goes through the same send.
3. The slot shows busy while the turn waits. A message sent to that session goes
   to the slot queue as it does today and runs once, after the waiting turn. A
   steer that the waiting turn does not consume becomes today's head card when
   that turn ends.
4. Stop and Cancel cancel the turn, as they do today. That cancel ends the
   admission wait at once, and the waiter leaves the gate. Nothing reads Stop
   only after a timer.
5. After `TRANSIENT_RETRIES` re-sends, the turn keeps its place in the queue and
   does not end. It ends only at the give-up window, or at
   `agent.chat_turn_timeout_secs`, whichever comes first. Then it shows today's
   throttle card with Resume.

A turn that waits in the gate holds no attended-turn permit. The open RFC PR
[#28633](https://github.com/kirodotdev/KiroCrew/pull/28633), at status `draft`,
proposes such a permit. Whichever of the two lands second follows that rule.

### 4.4 The other prompt starts

Every turn asks for admission before its prompt is sent (§4.1), so no caller
sends into a cooling scope. A caller's own timeout or cancel ends its wait and
removes its waiter. Each throttle outcome goes to the gate and, where the caller
parks, to the coordinator. A caller passes its mode down through
`stream_and_collect` or its stream call to the send. There are two admission
modes.

`hold` is the default. The start waits in the queue until the gate admits it.
These callers hold:

| Prompt start | What a throttle does |
|---|---|
| the chat runner | §4.3 |
| `stream_and_collect` with its re-sends on (`retry_transient=True`, the default): cron turns, history consolidation, heartbeat turns, app and side callers | the 3 re-sends and the fallback chain run as today, and each re-send holds. After them, a throttle re-sends again through the queue until the give-up window or the caller's own timeout, whichever comes first |
| `stream_and_collect` with `retry_transient=False`: `nudge_channel_fire.py`, `runtime_charge.py`, and the cron continuation in `cron_dispatch.py` after its one resume | as today: no re-send, and the caller's own loop retries. Each of its starts holds |
| `TurnDriver.run` for Slack, Telegram and the other channel turns | as today: the turn fails. Queueing its re-send is open question 3 |
| a direct caller, such as the auto-improvement runner, the code-review pool, `run_bg_oneliner` or compaction | as today |
| a sub-agent, TaskRunner step or workflow agent with no coordinator park, because it has no task row or no durable queue | as today, and each re-send holds |

`park` is for a start that the coordinator will park: a sub-agent, TaskRunner
step or workflow agent that has a coordinator and a task row. It never waits in
the gate:

- While the scope cools, the start fails at once with a throttle whose stated
  reset is the gate's `retry_at`, and nothing is sent. The coordinator parks it
  until then, as for a provider throttle. A cool-off comes only from a provider
  throttle on the same scope. Without the gate, the same start would go into
  that throttle. So a refused wave costs one coordinator attempt, which is what
  the throttle would cost it today.
- While the scope probes or ramps, the start is admitted at once. The gate does
  not refuse it there. The coordinator's own
  staged wake paces these starts, as it does today: one probe, then
  `dependency_wake_per_tick` for each `dependency_wake_spacing_secs`. So park
  starts reach the provider at today's pace, and the gate adds no starts of its
  own to them.
- A park start holds no unit, so a sub-agent fan-out cannot use up the window
  that chat turns wait for. Its outcome still moves the scope (§4.2). An accepted
  park start ends probing and grows the window, and a throttled one halves the
  window or starts a cool-off.
- The gate never refuses a park start outside a cool-off. So it never spends a
  coordinator attempt that no provider answer caused, and it needs no second wake
  path.
- The cost is that during a ramp the provider gets the holding window plus the
  coordinator's pace, not one combined limit. Each half is no faster than it
  is alone, and the first throttle from either half shrinks the window.

In `stream_and_collect`, a `park` start makes no re-send of its own after a
refusal. The refusal goes straight to the caller, which parks.

`park` exists so that no caller timeout, such as the `asyncio.wait_for` of a
workflow agent, runs inside a gate wait while a durable park is on offer. A
caller with no park would re-send on its own short curve against a cooling
scope and fail without a send, so it holds instead.

### 4.5 Injected turns

A turn is typed when the chat runner gets `_directive_user_origin=True`, which
`chat_send` sets for the owner's own send, or when it is a Resume or a Retry now.
Every other turn is injected. That covers `session_send` and every other
`session_deliver.deliver` origin, the dashboard nudge, workflow and TaskRunner
results, sub-agent completions, app-token sends to `/api/chat`, and every turn
outside the chat runner. A script cron that posts to `/api/chat` with no app
claim counts as typed, because `chat_send` treats it as the owner's send. The
window still paces it. Because the default is injected, a new turn path needs no
entry in a list.

The gate's typed bit is its own value and grants nothing. For a merged batch of
queued entries it is typed when any entry is typed. `_directive_user_origin`
keeps its rule in the drain, which is that every entry must have it. So a
peer's text merged with a typed message gains none of the owner's authority.

The chat runner passes this bit down beside the admission mode. The replay at the
queue head (§4.3), each re-send and each fallback attempt keep the bit of the turn
they repeat.

An injected turn waits in the same queue as every other holding start. While its
scope cools, probes, or ramps with a window below the ramp ceiling, the gate also
admits at most `agent.throttle_queue_injected_starts_per_min` injected starts per
scope in any 60 s. The count is a sliding 60 s of admission times. A re-send of
an injected turn counts against the limit too.

- The limit check, the record of the admission time and the unit for that start
  are one step under the scope's lock, so two starts cannot take the same place.
- An injected waiter that only the limit holds back does not block the waiters
  behind it. The next waiter that the window and the limit allow goes first, so a
  typed turn is not held behind a wave of injected ones.
- The gate sets one timer per scope for the earliest moment that frees a place:
  the oldest admission time leaving the 60 s, or the earliest give-up deadline.
  So a held-back waiter is never left without a wake.
- The limit ends when a ramping window reaches the ramp ceiling, and it applies
  again whenever a throttle halves the window below the ceiling. The scope's
  record stays, and every waiter, a held-back one included, is then released
  through the window in arrival order. The window keeps its +1 for each accepted
  start, so a backlog leaves no faster than the window allows, and it cannot keep
  a healthy scope from opening. The scope opens, and its record is dropped, when
  the window is at or above the ceiling and the queue is empty (§4.2).
- The limit does not apply to an open scope, so a healthy provider sees no
  change. The first burst on an open scope goes out unpaced, as today. The first
  throttle from it opens a cool-off, and the limit then spreads the injected
  starts in that scope until its window reaches the ramp ceiling again.

When the limit holds an injected waiter back, the queued notice in that chat
names the limit as the reason.

## 5. Interaction with existing behaviour

1. `_shared_dependency_delay` stays for the gate-off path. With the gate on, the
   gate decides every wait, and the coordinator's `retry_at` is a floor (§4.2).
2. A throttle after the turn streamed output keeps today's single continue. The
   continue is a turn of its own, so it waits for admission, and a throttle
   before its own output takes §4.3.
3. `agent.fallback_model` keeps its place. When it is set, the swap runs after
   the normal re-sends, as it does today. That is the chat ladder, and
   `stream_and_collect` for the callers that pass the chain (cron turns and
   heartbeat turns). The fallback model has its own scope. The queue applies when
   no swap applies or the chain is spent.
4. When a waiter ends (Stop, Cancel, caller timeout or give-up), it leaves the
   queue and gives up any unit.
5. On restart the gate is empty. A chat turn that waited in it ends with today's
   restart card and Resume, like every other in-flight turn. Sub-agents keep their
   rows, and the coordinator rebuilds them.

## 6. What the user sees

- From a turn's first wait, its transcript row is the queued notice. It says that
  the model provider throttles requests, that the turn is queued, and the local
  time of the next try. A countdown counts down to that time. When the gate moves
  `retry_at`, the row updates in place.
- The row is a new `throttle_queued` token with a family of its own in the
  gateway-notice table (`website/src/pages/chat/gatewayNotice.ts`), in the
  `notice` role. The existing NoticeCard draws it, with two additions: the
  countdown and an action row. There is no new error card.
- A crewmate chat does not draw the queued row. The maintainer decision
  `docs/decisions/2026-10-09-crewmate-chat-draws-messages-only.md` keeps runtime
  notices out of that chat, and `isCrewmateChatRow` drops every `notice` row.
  The crewmate's Sessions tab shows the row, and the give-up card still draws in
  the crewmate chat.
- The row has two buttons. "Retry now" moves this turn to the head of the queue.
  While the scope cools, it also ends the cool-off for one probe, and this turn
  is that probe. While the scope probes or ramps, the turn goes when the next
  unit is free. If a Retry now probe is accepted, the scope starts to ramp for
  every waiter. If it is throttled, the turn stays at the head and `k` does not
  change. `retry_at` moves only for a later stated reset, as for any throttle
  (§4.2). So a click adds no backoff step for other sessions. "Cancel" is the
  existing Stop for this turn.
- At the give-up window the row becomes today's throttle card with Resume.
- The buttons are in the dashboard only. A linked channel gets the notice text.

The rows below carry the same structured fields in their `meta`: the queued row,
today's throttle card, and the "selected model is unavailable" card. The fields
are on each of these rows, whether a give-up wrote it or the gate-off ladder did.

| Key | Value |
|---|---|
| `error_kind` | `classify_provider_error(...).kind` for the error, so `throttle` or `model_unavailable` |
| `provider` | the ACP backend, for example `kas` |
| `model` | the requested model id |
| `scope` | the gate scope, `provider:<backend>:<model>` |
| `held_by` | on a give-up only: `injected_limit` when the limit of §4.5 held the turn last, else absent |

- `error_kind` is a new key. It does not replace `meta.kind`, which still selects
  the card and its fix. So no card changes because of it.
- The `error_kind` values are the provider error classifier's own names. The
  `model_unavailable` gateway notice keeps its own `notice` token, which is a
  different key on a different row.
- `error_kind` joins `RESERVED_ROW_META_KEYS` in `dashboard/chat_handlers.py`, so a
  caller of `/api/chat` cannot write it on a user row. A reader still checks that
  the row role is `error` or `notice`.

## 7. Config

All keys are read on each decision, so no restart is needed. Each key is
documented in `src/kiro_crew/docs/configuration.md`.

| Key | Default | Meaning |
|---|---|---|
| `agent.throttle_queue_enabled` | `true` | Turns the gate on. `false` gives today's behaviour exactly (§8). |
| `agent.throttle_queue_base_secs` | `30` | Cool-off ceiling for the first step. It doubles for each failed release. |
| `agent.throttle_queue_max_secs` | `600` | Highest cool-off ceiling. A stated Retry-After can be longer. |
| `agent.throttle_queue_give_up_secs` | `2700` | Longest wait for one turn. Then it ends on today's card with Resume. |
| `agent.throttle_queue_ramp_ceiling` | `8` | Window at which the injected limit ends and a ramping scope with an empty queue opens again. |
| `agent.throttle_queue_injected_starts_per_min` | `2` | Most injected starts per scope in any 60 s while the scope cools, probes or ramps below the ramp ceiling (§4.5). `0` turns the limit off. |

The AIMD steps are +1 for each accepted start and x0.5 for each throttle. They
are part of the algorithm and are not config keys (open question 1).

## 8. When it is off

With `agent.throttle_queue_enabled = false`, nothing calls the gate, it keeps no
records, and no caller passes a scope hint. Every path is the one at
`b44d1d0e2f`:

- A chat turn re-sends 3 times on the 6 s throttle curve, floored by
  `_shared_dependency_delay`, and then ends on the throttle card with Resume.
- `stream_and_collect` re-sends 3 times on the 2 s curve, then tries the
  fallback chain, then fails. A caller that passes `retry_transient=False` gets
  no re-send from it. A cron turn's 2 retries before the prompt stay.
- Sub-agents, TaskRunner steps and workflow agents park on the coordinator only,
  under `provider:acp`.
- No prompt send asks for admission, so a channel turn on `TurnDriver` and every
  direct caller keep their own paths.

- No injected-start limit applies (§4.5).

If the key turns off while turns wait, the gate admits every waiter at once and
drops its records. Each waiter then runs on today's path. A parity test pins each
path above with the key off, and one more test pins the switch while turns wait.

The structured `meta` fields of §6 are written with the key on or off. They are
data on the row, and they change no card and no retry path. With the key off,
`scope` names the scope that the gate would use.

## 9. Verification

A Dev Fleet pod runs a stub ACP backend that answers `ThrottlingException` for a
set time and then recovers. With N concurrent sessions, the evidence must show
these results:

1. The turns queue and do not fail.
2. The retries spread out.
3. The release is gradual.
4. No turn or message is lost or duplicated.
5. The key off gives today's failures.
6. A burst of `session_send` deliveries into N idle sessions during a cool-off
   starts no more than the injected limit allows in each minute, and a typed
   turn in another session is not held behind them. When the window reaches the
   ramp ceiling, the held-back turns leave through the window, not all at once.
7. Each throttle row carries `error_kind`, `provider`, `model` and `scope`.

The evidence goes on the implementation PR.

## 10. Alternatives considered

1. Make the chat ladder longer only, for example 10 re-sends. Each session would
   still retry on its own clock, so the burst stays a burst. Rejected.
2. Put chat turns on the `DependencyCoordinator`. The coordinator writes task-row
   events for each wait, and a chat turn has no row. A row for each chat turn is a
   much larger change to the task store. The coordinator also releases in
   capacity-sized batches, not by AIMD. Rejected for now. The gate shares the
   coordinator's scope key and reads its `retry_at` (§4.1, §4.2), so the two do
   not disagree.
3. Sleep inside the throttle arm, as today, for the whole wait. A timer that
   reads Stop only when it returns holds a stopped turn for up to the give-up
   window, and the arm would be a second wait point beside dispatch. Rejected.
4. End the turn and park the message in the slot queue with a hold, as the
   sign-in hold does. An idle slot with held entries needs its own drain timer,
   and a new send to an idle slot would pass the held entry. Rejected.

## 11. Open questions

1. Must the AIMD steps (+1, x0.5) be config keys too? This document keeps them
   fixed.
2. Must "the selected model is unavailable (capacity throttle or region
   rollout)" queue too? It was 8 of the 107 failures in §1. It keeps today's path
   until a decision.
3. Must a channel turn on `TurnDriver` re-send through the queue after a
   throttle, as a dashboard chat turn does? This document gives it admission
   only, so it no longer starts into a cooling scope, but a throttle still ends
   it.
4. Must park starts also count against the window, so that one limit covers the
   whole scope during a ramp (§4.4)? This document keeps the coordinator's
   pacing for them, as today, so a sub-agent fan-out cannot hold back chat turns.
