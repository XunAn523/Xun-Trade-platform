# TradeCore QMT Windows Edge — manual deployment and P0 probe

For the next **DISARMED dual mTLS transport probe**, use
[DUAL_MTLS_PROBE.md](DUAL_MTLS_PROBE.md). It binds the dedicated Windows
`192.168.251.2:19443` interface, accepts Linux `192.168.251.1`, and uses two TLS
connections on that port. This separate probe does not run a QMT model or load
native rings; it does not replace the native P0 steps below.

This directory is the Windows-only half of the `QmtBrokerAdapter`.  It is
deliberately separate from the QMT installation and from Linux.  The current
delivery is deployable for **P0 local, disarmed verification**.  It creates the
scope-local SQLite/WAL state store and two bounded SPSC ring files, verifies
the audited atomic helper, and lets the Big QMT model validate its own ABI.

It neither includes credentials nor starts a listener nor submits an order.
Leaving `mode` as `PROBE` and `allow_mutation` as `false` is mandatory until
the P0 evidence and signed capability manifest have been reviewed.

## Files and ownership

```text
Windows Edge CPython 3.10 process        Big QMT embedded Python 3.6 model
----------------------------------       -----------------------------------
qmt_edge_probe.py                        qmt_native_engine.py
QmtEdgeStore (one SQLite writer)         only official run_time/callback code
command.ring producer                     command.ring consumer
event.ring consumer                       event.ring producer
```

`tradecore_qmt_atomic.dll` is the sole native dependency introduced for the
ring.  It provides 64-bit Interlocked load/store; it does not contact QMT,
open a socket, or handle order content.

## Prerequisites

1. Use a dedicated, 64-bit CPython **3.10** environment for the Edge process;
   do not use Big QMT's embedded Python for this process.
2. Install the checked-out TradeCore revision into that environment.
3. Use an official SQLite runtime carrying the WAL-reset correction: `3.44.6`
   through `3.44.x`, `3.50.7` through `3.50.x`, or `>=3.51.3`.  The normal
   CPython 3.10 bundle may be older and the probe intentionally rejects it.
   These are SQLite's documented fixed branches; neither `synchronous=OFF`
   nor disabling WAL is an acceptable substitute.
4. Open an **x64 Visual Studio Developer PowerShell**.  The `cl.exe` compiler
   must be available.
5. Give the Edge service identity and the QMT desktop/model identity access
   only to the Edge runtime directory.  Do not grant Users/Everyone access to
   the ring files, database, config, certificate/private-key locations, or the
   atomic DLL.  Record the ACL result in P0 evidence.

## Build and verify the atomic helper

From the repository root in an x64 Developer PowerShell:

```powershell
Set-Location <TradeCore-repository-root>
.\deploy\windows-qmt-edge\scripts\build-native-atomics.ps1 `
  -SourceRoot (Get-Location).Path `
  -OutputDirectory C:\TradeCore\qmt-edge\bin
```

Keep the printed SHA-256 as P0 evidence.  The Edge config must reference the
exact DLL at that path.  Rebuilds require a new digest/evidence record; do not
replace the DLL under a running Edge or QMT model.

## Create the disarmed scope configuration

Copy `edge_config.example.json` outside the repository, for example to
`C:\TradeCore\qmt-edge\config\edge.json`.  Fill only values established by
the deployment and P0 scope plan.  Keep all QMT callback attribute placeholders
until the P0 callback probe records the official fields.

The configuration contains paths and non-secret identifiers only.  It must not
contain an account password, QMT login token, mTLS private key, raw route token
or order payload.  Use a restrictive ACL for it nonetheless.

The required `scope_digest_hex` is a SHA-256 digest of the approved scope—not
the account ID.  `generation_hex` must be freshly rotated whenever rings are
recreated.  Do not reuse old files with a new generation.

## Upgrade an existing P0 deployment to ring ABI 2

ABI 1 was published with both unaligned and aligned headers. ABI 2 rejects
**both** variants, including the aligned rings created by `872c9ad`. The
128-byte header and 512-byte slot sizes remain unchanged. Do not patch an ABI
byte or truncate/reuse a legacy ring. Previously signed ABI manifests require
new evidence and signatures; this upgrade does not activate trading.

1. Stop the QMT model first, then all Edge/monitor processes. Preserve old
   rings, SQLite/WAL, logs and the active config. Record the actual tested Git
   HEAD; `5a6ad4b8` was the deployment's starting base, not its final revision.
2. Fast-forward `dev`, update the installed Edge package in its independent
   CPython 3.10 environment, and verify `git status`. The engine now has an
   explicit `eol=lf` rule and a matching byte-exact pin. Do not use an editor
   to convert its line endings or bypass the loader's hash check.
3. From `D:\projects\TradeCore\qmt-edge\src`, prepare a new config:

   ```powershell
   python .\deploy\windows-qmt-edge\prepare_probe_ring_upgrade.py `
     --config D:\projects\TradeCore\qmt-edge\config\edge.json `
     --output-config D:\projects\TradeCore\qmt-edge\config\edge-abi2.json
   ```

   This tool requires `PROBE`, mutation disabled, no native policy, matching
   ABI-1 ring identities and a database with **no command history**. It creates
   only the new config with a fresh generation and new ring paths, retains the
   database path, and refuses to overwrite any existing output. It does not
   start processes, delete files, reset SQLite or replace the active config.
   If command history exists, stop here and preserve it for recovery review.
4. Review the generated paths and apply the deployment's restricted ACL to
   the new config/ring directory. Keep a separately named copy of `edge.json`,
   then install the prepared content at that active path while both owners
   remain stopped. Never change only one process's generation/config.
5. Run `qmt_edge_probe.py --config ...\config\edge.json`. Require
   `PROBE_READY_DISARMED`, `ring_abi=2`, mutation disabled, the expected new
   generation and a recorded `native_abi_digest`. Keep the old files untouched.

For a fresh deployment, create a fresh config/generation directly; the upgrade
tool intentionally requires existing ABI-1 evidence. Raw native event IDs now
include the ring generation, so the retained SQLite outbox can store a new
generation's sequence 1 without colliding with an earlier initialization.

## Optional P1 mTLS service deployment

After P0 local prerequisites pass, copy `edge_service_config.example.json`
outside the repository and fill the Windows listener address, CA, Windows Edge
leaf certificate/private key and the SHA-256 of the one permitted Linux client
leaf certificate. Certificates are pinned in addition to normal mutual TLS CA
validation. Never put the private key in Git or in `edge.json`.

Run it from the dedicated CPython 3.10 environment:

```powershell
python .\deploy\windows-qmt-edge\qmt_edge_service.py `
  --service-config C:\TradeCore\qmt-edge\config\edge-service.json
```

In `PROBE` it permits only mTLS HELLO/health testing and always reports
`DISARMED`; the Linux adapter therefore refuses to start it as a tradable
profile. A listening port is never authority to place an order.

## Run the Edge P0 local probe

In the dedicated CPython 3.10 Edge environment:

```powershell
python .\deploy\windows-qmt-edge\qmt_edge_probe.py `
  --config C:\TradeCore\qmt-edge\config\edge.json
```

Only a `PROBE_READY_DISARMED` result with an accepted SQLite version is valid.
This example uses `3.51.3`; accepted official backport versions are described
in the prerequisites above:

```json
{"command_ring_capacity":1024,"event_ring_capacity":1024,"mode":"PROBE","mutation_allowed":false,"sqlite_version":"3.51.3","status":"PROBE_READY_DISARMED"}
```

`PROBE_BLOCKED` is an expected safe failure for a missing DLL, an invalid ACL
deployment, an old SQLite runtime, placeholder fields, or a bad ring identity.
Fix the stated prerequisite; do not bypass the check with `synchronous=OFF`,
an unsafe Python lock fallback, or a different ring ABI.

## Attach the Big QMT model in P0 only

1. Ensure QMT and the selected account are in the simulation environment.
2. Create a **new, dedicated** QMT strategy and select (or paste the complete
   contents of) [`bigqmt_strategy/qmt_strategy_loader.py`](./bigqmt_strategy/qmt_strategy_loader.py).
   This stable entry reads `tradecore_qmt_p0_probe_strategy.py` from disk on
   each model load. Verify `HELPER_PATH`, and the inner loader's `CONFIG_PATH`
   and `ENGINE_PATH`, match the deployment. The inner loader verifies the
   byte-exact engine SHA-256 and rejects non-`PROBE` configuration before `init`.
   An already pasted inner loader must be replaced with this disk loader;
   pulling Git alone cannot update Python text stored in the QMT editor.
3. Do not run `qmt_native_engine.py` directly as a strategy and do not add an
   environment-variable bootstrap by hand: the pinned P0 loader does both.
4. Start the dedicated model in `PROBE` mode.  It registers its configured
   `ContextInfo.run_time` function, opens the pre-created rings, and remains
   disarmed.  A command frame in this mode is a fault, not an order.  It emits
   one `PROBE_INIT` health frame after its timer is registered, then a bounded
   ring `PROBE_TIMER` health record (tick counter + QPC entry time) on each
   successful timer callback. These contain no account/order payloads. A full
   event ring halts the model; always keep its sole Edge consumer running.
5. `stop()` emits `MODEL_STOP` when healthy, halts drains and closes both
   mappings. Capture timer, header, alignment, wrap/full/CRC and stop/restart
   evidence as below. A failed initialization must be stopped before retry.

Do **not** change `mode` to `ARMED` or set `allow_mutation=true` during P0.
The follow-on activation additionally requires a signed QMT capability
manifest, approved native enum/price/route mappings, mTLS peer identity, and
the QMT callback/query completion evidence described in
[`17-QMT-Adapter未完成能力开发实施与验收计划.md`](../../docs/01-架构与定档/17-QMT-Adapter未完成能力开发实施与验收计划.md).

## Safe rollback

Stop the QMT model first, then the Edge process.  Keep the SQLite database and
ring files for forensic/recovery review; do not delete them after a fault.  A
subsequent process must retain `DISARMED` and classify pre-call uncertainty as
`UNKNOWN_EFFECT` until an authoritative QMT query/replay flow is implemented
and accepted.

## Deployment test handoff: init, sustained timer, stop and restart

Use the independent CPython 3.10 environment. Stop the model and ensure no
other Edge/monitor owns the rings or database. Start the monitor **before**
the model; it requires empty rings and a fresh `PROBE_INIT`. Never discard
pending frames to make this check pass: preserve and review them first.

```powershell
python .\deploy\windows-qmt-edge\qmt_edge_event_monitor.py `
  --config D:\projects\TradeCore\qmt-edge\config\edge.json `
  --seconds 60 --min-timer-samples 20 --max-timer-gap-seconds 5 --expect-stop
```

Wait for `MONITOR_READY`, then run the disk loader in the dedicated QMT model.
Collect at least 20 `PROBE_TIMER` records, keep it running long enough to
observe continuity, then stop the model **before the 60-second deadline**,
leaving at least one second for the quiet observation window. For a proven
period longer than five seconds, choose and record an appropriate gap bound.
Repeat this sequence with a fresh monitor/model window on the same ABI-2
rings and database to verify restart. Do not leave the model running after
the monitor exits. Evidence is bounded to 100,000 timer samples per run.

Require exit code 0 and `MONITOR_COMPLETE` with
`acceptance=PROBE_INIT_TIMER_STOP`. The monitor now returns exit code 2 with
`MONITOR_BLOCKED` for no events, init-only, missing/late timer, fault/non-health
frames, sequence/tick gaps, CRC/read/store failures, premature/missing stop,
or frames after stop. Only an empty ring is retried. Frame persistence occurs
before the consumer cursor is released; retries after a SQLite commit reuse
the same outbox delivery. `NORMAL` retains its existing power-loss limits;
these tests do not certify machine-power-loss durability.

Archive the full tested HEAD, engine/DLL SHA-256, ABI digest, redacted config
identity, Python/SQLite/QMT builds, both complete JSONL monitor outputs and
their SHA-256 digests. Each sample preserves native QPC and monitor receive
time separately; interval P50/P95/P99/P100 are **observed callback spacing**.
Official scheduled time is unavailable (`scheduled_ns=null`); do not claim
scheduler jitter, minimum supported period, order/query callback capability,
full P0 acceptance, or Linux–Windows mTLS from these results.

Also verify that pre-upgrade rings are rejected as `PROBE_BLOCKED` without
modification. The offline regression suite covers both legacy layouts,
cross-copy aligned wrap, atomic alignment rejection, byte pin, crash/retry,
corrupt/no-event failures and the embedded model lifecycle; it does not
replace the Windows/QMT run above. Report remaining P0 capabilities and
§1.5 dual-channel production assembly as unaccepted. Keep `PROBE`, mutation
disabled, and `native_policy=null`; no order/cancel/query or network listener
is needed for this handoff.
