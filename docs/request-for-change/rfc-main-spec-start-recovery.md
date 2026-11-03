---
title: Recover a refused main-agent start inside the start gate once the kept spec reads again
status: draft
author: kirocrew agent session, directed by jeeshofone
created: 2026-11-02
last-audited: 2026-11-02
audited-at: d6d40e23a2
doc-pr: 28664
implementation-prs: []
tracking-issues: [18860]
supersedes: []
superseded-by: []
---

# RFC: Recover a refused main-agent start inside the start gate once the kept spec reads again

Status: draft. Acceptance requested from a maintainer of agent spec projection and fork governance. Part of [#18860](https://github.com/kirodotdev/KiroCrew/issues/18860). Main-side references were read at `d6d40e23a2`. References to the unreadable-spec mechanism were read on [#27535](https://github.com/kirodotdev/KiroCrew/pull/27535) at head `d0a920bab1`, which is open and not on main. No code in this document can land before #27535 does.

## Summary

[#27535](https://github.com/kirodotdev/KiroCrew/pull/27535) (moved from #15546) stops a rebuild from overwriting an unreadable `kirocrew.json` with defaults. While the kept file has not been written again by a rebuild, it refuses every start that could load it: the main agent, and the heartbeat and worker specs mirrored from it. The refusal clears only when an existing rebuild path writes the spec. Those paths are the hourly `retry_held_conductor_specs` (`agent_parts/spec_rebuild.py`), a config change the watcher applies, a restart, and a stopgap early-retry timer #27535 adds (`_schedule_main_spec_early_retry` in `agent_parts/spec_unreadable.py`, at 5, 15 and 60 seconds, then every 60 seconds).

The issue asks for two things that were cut from #15546: fast recovery on the start path, and admitting a start while the file is still unreadable.

This document recommends building fast recovery and **not** building admit-while-unreadable. When a refused start finds the kept file readable again, it joins one shared rebuild. The episode closes only if that rebuild wrote the spec and the governance generation the rebuild projected equals one polled after the write, and the start is admitted only once the episode is closed. Otherwise the start runs one more shared rebuild and is refused if the episode is still open. Once this is in, the early-retry timer and its gateway hooks are deleted, not adapted.

## Motivation

### The gap

After a user restores read access to `kirocrew.json`, every main-agent start stays refused until the next rebuild writes the spec. With #27535 alone, that means waiting for the early-retry timer, the hourly retry, a watched config change or a restart. The timer narrows the wait to about a minute, but it is a second mechanism that runs rebuilds on a schedule whether or not anyone is waiting for one. It also needs its own start and shutdown hooks (`resume_main_spec_early_retry` in `slack/gateway_runtime/service_boot.py`, `cancel_main_spec_early_retry` in `slack/gateway_runtime/exit_path.py`).

The start that is being refused is the one caller that knows a rebuild is needed now. Doing the recovery on that path removes the wait and the timer at the same time.

### Why it was cut from #15546

The #15546 author set a rule that a fourth review round in one design family moves that layer into its own PR. The admit-and-recover code took four rounds of security findings from the GPT review lane:

1. Admission compared only the ceiling counter, so a profile-only tightening was missed. Fixed by comparing `governance_answer_generation()` (ceiling plus profile layers, `platform/governance_profiles.py`), with `poll_profiles_fresh()` called first.
2. The generation was read after the rebuild had read the layers, so a layer that moved mid-rebuild was recorded as the newer value over grants built from the older one. Fixed by reading it before the rebuild reads either layer.
3. The gate was keyed on agent names, so a main agent renamed in `kirocrew.json` skipped it. Fixed by keying on the resolved spec file (`_governed_by_main_spec`). That fix stayed in #15546 and is in #27535.
4. The recovery return admitted on the rebuild's `wrote` flag alone, so a profile revoked during the recovery write was never compared. This round was never fixed in code.

Round 4 is the reason this document exists: its fix changes the admission rule, so it needs a design sign-off, not another patch.

## Goals

- A start that finds the kept file readable again is admitted as soon as one rebuild has written it and the projection is shown current, with no wait for a timer.
- Overlapping starts share one rebuild. No start creates a thread.
- The early-retry timer and everything #27535 adds only for the wait between "the file reads again" and "a rebuild wrote it" is removed, with the one cost named in section 3.6.
- The security invariant in section 3 holds.

## Non-goals

- Admitting a start while the file still cannot be read (option C below). Rejected in section 2.
- Changing which starts #27535 refuses, or the refusal's SEL denial row.
- Fork gating at `set_mode` and binding every start in any process to the verdict ([#18646](https://github.com/kirodotdev/KiroCrew/issues/18646)).
- A process launched on a view from before an episode ([#14112](https://github.com/kirodotdev/KiroCrew/issues/14112)).
- MCP handlers reporting success after a skipped rebuild ([#18679](https://github.com/kirodotdev/KiroCrew/issues/18679)).

## 1. Background: what #27535 provides

These are the pieces this design builds on, all at #27535 head `d0a920bab1`:

- `_AgentSpecUnreadable` is raised when any rebuild read of a present `kirocrew.json` fails with an `OSError` other than `FileNotFoundError`. `_skip_unreadable_main_spec` keeps the file and its mirrors and opens an episode.
- `require_main_spec_projected(agent, project_dir)` in `agent_parts/spec_unreadable.py` is the start gate. It refuses any start `_governed_by_main_spec` resolves to the kept file while an episode is open, through `refuse_unprojected_main_spec_start`, with one SEL denial each (`_audit_main_spec_denial`).
- `_note_main_spec_readable(path, declared_name, episode_revision)` ends the episode when a rebuild writes the spec.
- `main_spec_episode()` returns the episode revision and whether it is open, and the send-time checks (`require_main_spec_launch_bound`, `check_mode_frame`) refuse a session open or mode switch whose spec was read before the episode ended.

## 2. Options

### A. Do nothing

Keep #27535's early-retry timer as the recovery path. Recovery takes up to about a minute after access is restored, and the timer, its delay table, its thread and its two gateway hooks stay. Safe, because the timer goes through the same rebuild and the same `_note_main_spec_readable`, but it keeps a stopgap that #27535 itself labels as one, and it rebuilds on a schedule with nobody waiting.

### B. Recover inside the gate (recommended)

Section 3. One shared rebuild on the start path, admission on a generation check after the write, the timer deleted.

### C. Admit while the file stays unreadable

Admit a start while the file still cannot be read, as long as neither governance layer has moved since the last write in this process was projected. Rejected:

- kiro-cli loads the spec file itself. If the file cannot be read, the start does not load what the last projection wrote. It falls back to `kiro_default` or fails, which is the silent change #15515 reported.
- A generation that has not moved proves the layers did not change. It does not prove the file on disk is the file this process wrote. A link or retarget during the episode can put a different file under the same name, which is why #27535 refuses every start in an episode with no exemption.
- It would make a start allowed that #27535 refuses, which breaks the invariant in section 3.

## 3. Recommended design

### 3.1 Invariant

**No start that #27535 refuses while the spec is unreadable becomes allowed by this change.** A start is only ever admitted on a path that proves both of these:

1. A rebuild in this process wrote the spec after the file read again.
2. The governance generation the rebuild projected equals `governance_answer_generation()` polled after the write, with `poll_profiles_fresh()` called first.

Both are checked in one place, `_note_main_spec_readable`, which ends the episode only when both hold (section 3.2). A start is admitted only when `main_spec_episode()` shows the episode closed.

Any path that cannot prove both refuses with the existing refusal and SEL row.

### 3.2 The generation check closes the episode

The check lives where the episode ends, not in the caller that asked for the rebuild. If only the recovering start compared generations, every other start arriving after the write would pass the gate's "no episode open" fast path on a stale projection.

`rebuild_agent_config` (`agent_parts/spec_rebuild.py`) reads the generation once, before it reads the ceiling or a profile: `poll_profiles_fresh()` then `governance_answer_generation()`. On a write it passes that value to `_note_main_spec_readable` as a new `projected_generation` parameter. `_note_main_spec_readable` calls `poll_profiles_fresh()` and reads `governance_answer_generation()` before it takes `_main_spec_episode_lock`, because that lock is held only for bookkeeping and never across I/O. Under the lock, it closes the episode only when the polled generation equals the value the rebuild read. A layer that moves after the lock is released is the same race a host with a readable spec already has, and the next rebuild projects it. On a mismatch, or when the episode revision moved (the existing early return when a newer skip raced the write), the episode stays open and the function returns `closed=False`. A rebuild reports `closed`, not `wrote`, and nothing admits on `wrote`.

### 3.3 Shared rebuild

`_MainSpecRebuild` holds one in-flight rebuild, in `_main_spec_rebuild` under a lock. It is one per process, matching the episode, which is also one per process. `_shared_main_spec_rebuild()` starts the rebuild on the calling thread if none is in flight, or waits on the in-flight one's event with a bounded timeout. Each `_MainSpecRebuild` carries its own `closed` result, and the entry is cleared when its rebuild ends, so a later caller never reads an earlier rebuild's result. No thread is created: the first caller runs it and the others wait. A waiter that times out is refused. `rebuild_agent_config` sets a per-thread "in rebuild" marker for its whole run, whoever called it (the shared slot, the hourly retry, the watcher or boot). A refusal raised on a thread that carries the marker (a fork refresh or sibling install inside a rebuild) refuses without recovering, so no rebuild ever runs inside another or waits on itself. After a recovery that did not close the episode in both of its rebuilds, the next recovery cannot start a rebuild for 5 seconds, so repeated starts against a file that still fails cannot run rebuilds back to back. The second rebuild inside one recovery (section 3.4, step 4) is not subject to this backoff. The hourly retry and the config watcher keep calling `rebuild_agent_config` directly. Their writes go through the same check in section 3.2, so they are safe alongside the gate.

### 3.4 Where recovery runs

Recovery is a new function, `recover_main_spec_episode() -> bool`, in `agent_parts/spec_unreadable.py`. It returns True only when it saw an open episode and a rebuild closed it. It never admits anything by itself. Each refusal path in #27535 calls it before it refuses, and on True runs its own check again from the start. The start goes ahead only if that check passes:

| Caller (#27535) | Today | With this change |
|---|---|---|
| `require_main_spec_projected` (the start gate) | raises `ForkGovernanceUnresolved(refuse_unprojected_main_spec_start(...))` while an episode is open | calls `recover_main_spec_episode()` first; on True, re-reads `main_spec_episode()` and returns if no episode is open, otherwise raises as today |
| `require_main_spec_launch_bound`, bare-name and `view_source` branches | raises while an episode is open | same: recover, then re-run the branch's own check |
| `require_main_spec_launch_bound`, stale `before` reading (KAS payload, a reading that overlapped an episode) | raises when the reading predates the current episode | unchanged: no recovery. A closed episode does not make a reading taken before it current |
| the native skill projection's send-time refusal | raises from the event loop | unchanged: no recovery on the event loop |

`recover_main_spec_episode()` runs these steps:

1. Return False if no episode is open. A refusal for a spec read before the episode ended (the `kind == "read"` branch of `refuse_unprojected_main_spec_start`) therefore never recovers, so recovery cannot admit a start #27535 refuses for a stale reading.
2. Return False if called on the event loop (detected with `asyncio.get_running_loop()` succeeding on this thread) or on a thread that carries the "in rebuild" marker. Recovery does file I/O and may wait, which must not block the gateway or nest a rebuild.
3. Probe the file with one `read_bytes_with_retry`. If it still fails, return False. The probe only decides whether a rebuild is worth running. Admission rests on section 3.2, because the file the probe reads through a link need not be the file the rebuild reads.
4. Join `_shared_main_spec_rebuild()`. If it closed the episode, return True. Otherwise join one more shared rebuild and return whether that one closed it.

`refuse_unprojected_main_spec_start` stays the one place that writes the refusal and its SEL row. Its text names which check failed, so a user can tell a still-unreadable file from a governance change.

### 3.5 What is deleted

When this lands, the change deletes, rather than adapts, everything #27535 adds only for the wait between "the file reads again" and "a rebuild wrote it":

- Code: `_MAIN_SPEC_EARLY_RETRY_DELAYS`, `_start_main_spec_retry_timer`, `_schedule_main_spec_early_retry`, `_run_main_spec_early_retry`, `cancel_main_spec_early_retry` and `resume_main_spec_early_retry`, with their calls in `exit_path.py` and `service_boot.py` and their test resets.
- State: `_main_spec_retry_timer`, `_main_spec_retry_attempts`, `_main_spec_retry_stopped` and `_main_spec_retry_timer_factory`.
- Edited, not deleted: the schedule call in `refuse_unprojected_main_spec_start` (removed; callers recover first, section 3.4), the timer cancel in `_note_main_spec_readable`, the refusal text that promises "an early retry within about a minute", and every docstring or comment that names the timer (the module docstring, the `_main_spec_episode_lock` and `_main_spec_warned` comments, and the `require_main_spec_projected`, `_audit_main_spec_denial`, `require_main_spec_launch_bound` and `_note_main_spec_readable` docstrings).
- Imports left unused once the timer goes (`retry_held_conductor_specs`, `contextlib`, `Callable`), and `_EarlyRetryRunning` where it is defined in `kiro_crew.agent`.
- Any ratchet that pins those paths, deleted or re-keyed to the recovery in section 3.4.

The send-time checks (`require_main_spec_launch_bound`, `check_mode_frame`) and the KAS episode check stay. `docs/system-specs/modules/governance.md` is updated to match.

### 3.6 Cost: holds with no start waiting

The timer also clears holds that `_spec_hold_open` drives and that no start triggers: the boot baseline, the pending ceiling memo, the default-model applier's "not landed" result, and held conductor and fork specs. With the timer gone and nobody starting the main agent, these clear at the next start, the next watched config change, the hourly `retry_held_conductor_specs` or a restart. Today, with #27535 alone, they clear within about a minute. This document accepts that regression: a hold with nobody waiting on it costs nothing until someone starts an agent, and that start now recovers at once. Open question 3 asks whether a maintainer wants it covered.

## 4. Building on #27535

The implementation PR is based on main after #27535 merges and touches `agent_parts/spec_unreadable.py`, `agent_parts/spec_rebuild.py`, the two gateway hooks and their tests. If #27535 changes the gate's shape in its remaining review, this document is revised against the merged head before code starts. The implementation does not stack on an unmerged #27535.

## 5. Test plan

In `test/test_agent_install.py` (`TestInstallAgent`) and the #27535 spec-unreadable tests:

- **Readable again, unchanged governance:** a start after access is restored is admitted after one rebuild, with no timer.
- **Still unreadable:** the start is refused with the "cannot be read" text and one SEL denial.
- **Ceiling moved / profile moved during the rebuild:** parameterised `[unchanged|ceiling|profile]`; a layer that moves between the rebuild's generation read and the post-write poll causes one more rebuild, then admission if it settles or refusal if it moves again.
- **Profile revoked during the recovery write:** refused. This test must go red if the gate admits on `wrote` alone (the round-4 regression), and the PR shows that mutation failing.
- **Generation read order:** `poll_profiles_fresh` is called before `governance_answer_generation`, and the generation is read before the rebuild reads either layer.
- **Peers after a mismatch:** while a recovering start sees a generation mismatch, a second start arriving after the write is refused too. The episode is still open, because the check is in `_note_main_spec_readable`.
- **Raced skip:** a write whose episode revision moved does not close the episode, and the start is refused.
- **Launch-bound branches recover:** a refusal from the bare-name and `view_source` branches of `require_main_spec_launch_bound` recovers the same way once the file reads, and the branch's own check runs again before the start goes ahead.
- **Event loop:** a refusal raised on the event loop does not run a rebuild.
- **Stale reading:** a KAS payload or a reading taken before the episode ended is still refused after the file reads and a rebuild closes the episode.
- **Nested rebuild:** a refusal raised inside a rebuild started by the hourly retry or the watcher does not start a second rebuild.
- **Lock:** `poll_profiles_fresh` is not called while `_main_spec_episode_lock` is held.
- **Overlapping starts:** 8 concurrent starts share 1 recovery rebuild, and the thread count does not grow.
- **Re-entry and timeout:** a thread that reaches the gate inside its own shared rebuild is refused at once, and a waiter that times out is refused.
- **Backoff:** a second start within 5 seconds of a rebuild that did not close the episode does not run another rebuild.
- **Timer removed:** a ratchet asserts no early-retry symbol or gateway hook remains.
- **Invariant:** for each start #27535's tests refuse during an open episode, the same start is still refused when the file stays unreadable.

The repo's contract-test family and both AI review lanes run before the implementation is pushed.

## Open questions

1. Is the waiter's bounded timeout better set from the rebuild's measured duration or as a constant? This document proposes a constant (30 seconds) and refusal on timeout. Expected latency for a refused start once the file reads again: one rebuild when the episode closes on the first pass, two when the generation check asks for the second, and never more than the 30-second waiter cap for a start that joins a rebuild already in flight.
2. Should a refused start's SEL row record which gate step refused it? This document proposes adding the cause to the existing row, with no new event type.
3. Is the slower clearing of holds with no start waiting (section 3.6) acceptable, or should a hold-side trigger replace the timer for them? The surfaces that would show "not landed" longer are the default-model applier's result, the pending ceiling memo, and the held state of conductor and fork specs, each until one of the triggers in section 3.6 runs.
