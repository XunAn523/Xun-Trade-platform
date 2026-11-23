# Consumer V2 server-local UDS deployment

> Production release note (2026-10-29): the installed wheel is TradeCore
> Python 1.0.0 from Git tag `tradecore-python-v1.0.0`, not the server's
> `/opt/xuanling-tradecore` `dev` checkout. Do not rebuild or reinstall the
> checkout as a routine restart: doing so can replace 1.0.0 with a different
> version. Verify the release tag, wheel hash, rollback material, and explicit
> target revision before any future installation. The detailed current
> deployment and acceptance boundaries are recorded in
> `docs/04-测试验收与性能/04-OMS性能与容量/27-生产版本与性能基线-2026-10-28.md`.

This unit is the server-side target for `11-真实UDS-Consumer-V2压测与验收标准.md`.
It is intentionally separate from `xuanling-tradecore.service`:

```text
same-host generator (Unix uid 1000 / xuanling)
  -> /run/tradecore-consumer-uds/consumer-v2.sock
  -> SO_PEERCRED authorization
  -> Consumer V2 / XTC2 validation
  -> file-backed mmap OMS fact journal
  -> IntentReceipt
```

It imports no Adapter and starts neither EMT nor QMT.  A successful receipt is
therefore only `journal_first=true, gateway_dispatched=false`; it is not an
order, venue, or callback assertion.

The root-owned profile may authorize up to 16 additional exact
`SO_PEERCRED` identities with `additional_peer_credentials`. The default
socket mode remains decimal `384` (`0600`). A reviewed shared test deployment
may set decimal `432` (`0660`) only when the runtime directory is owned by the
service uid and its effective gid, has no group-write or other permissions,
and every additional `(uid, gid)` pair is explicitly listed. Supplementary
group membership alone never authorizes a request.

## Installation boundary

The target configuration is installed by the TradeCore operator, not by the
generator author:

1. Update `/opt/xuanling-tradecore` by `git pull --ff-only origin dev`, then
   install that checkout into the dedicated environment with
   `/opt/xuanling-tradecore/.venv/bin/python -m pip install --no-deps --no-build-isolation /opt/xuanling-tradecore`.
   A Git pull alone does **not** update a previously installed wheel. Verify
   the installed server config model exposes `listen_backlog`,
   `pending_capacity` and `preframe_capacity` before restarting this service.
2. Copy `consumer-uds.server.example.json` to
   `/etc/xuanling-tradecore/consumer-uds.json` as `root:xuanling`, mode `0640`.
3. Install `tradecore.slice`, `xuanling-tradecore-cpu-partition.service` and
   `xuanling-tradecore-consumer-uds.service` below `/etc/systemd/system`.
   Stop the old Consumer service before establishing the new partition: its
   previous cgroup must not retain a CPU pool wholly inside the reserved set.
   Install `tradecore-production-slice.conf` as `30-production-slice.conf` in
   both `xuanling-tradecore.service.d` and
   `xuanling-tradecore-fault-supervisor.service.d`. All three formal TradeCore
   services share this partition; keep their existing DISARMED/SDK boundaries
   and per-unit memory limits (Consumer 6 GiB, runtime 2 GiB, supervisor 256 MiB).
   Install the reviewed Git file `deploy/consumer-uds/cpu_partition.py` as
   `/usr/local/libexec/tradecore-cpu-partition.py`, owner `root:root`, mode `0755`,
   under a root-owned non-writable-by-group/others parent. Privileged helpers
   execute that protected copy in Python isolated mode, not user-writable Git
   source. Use `umask 022` for wheel installation; retain private backup
   directories at `0700` without leaking that mask into package installation.
   Before restart, verify installed imports as both actual service users from
   `/tmp`, rather than accidentally importing the working tree as root.
4. Run `systemctl daemon-reload`, `enable --now`, then verify socket owner,
   configured mode, UID/GID binding, `ss -xlpn` backlog and no Adapter process.

The formal service reserves CPUs `64-79,160-175` (sixteen complete SMT core
pairs on the reviewed 96-core/192-thread host), `MemoryHigh=4.5G`,
`MemoryMax=6G` and
`TasksMax=256`. This is a **host-specific allocation**, not portable discovery.
The required root-owned partition helper checks topology and conflicting CPU
pools, then creates a valid cgroup v2 `root` partition at `tradecore.slice`.
It verifies the effective exclusive set and non-overlap of every other
top-level cgroup. `AllowedCPUs` alone, `CPUWeight` and `Nice` are not evidence
of exclusivity. Other ordinary host tasks retain the remaining eighty physical
cores; no foreign service is stopped, restarted or given a new explicit mask.
The kernel still adjusts their effective CPU availability. IRQs, bound kernel
workers and shared memory/IO bandwidth are **not** claimed isolated.

The separately created performance candidate uses the same `MemoryHigh=4.5G`
and `MemoryMax=6G` pair. The soft boundary lets cgroup v2 reclaim clean file
cache before the hard limit; it is not admission headroom and must not be used
to ignore `memory.events max`, OOM, or OOM-kill evidence. Promotion to the
formal Consumer unit requires a successful same-profile mixed-load qualification.

After reboot the Consumer dependency reruns the partition helper before opening
the socket; a failed partition check prevents Consumer startup. After any CPU
hotplug or administrator cgroup change, rerun
`sudo /usr/bin/python3 -I /usr/local/libexec/tradecore-cpu-partition.py check`.
Each of the three services also reruns the read-only check in `ExecStartPre`
on every start/restart, even when the partition helper is already active.
There is no continuous topology watchdog in this deployment. A formal generator
must run outside the reserved pool (for example `88-91,184-187`) with its own
memory/task budget. A 6 GiB limit is not an exclusive physical-memory reservation.

From 2026-10-20, requested deployments use the formal Git checkout
`/opt/xuanling-tradecore`; the old home-directory candidate is not the release
target. Keep the installed wheel synchronized with the fetched/fast-forwarded
revision. Preserve root-owned configuration and tester ACL overrides. Source
deployment does not promote the `LONG_RUNNING` profile, prove 12-hour gates or
enable any Adapter/SDK/READY. The existing no-Adapter BENCHMARK configuration
remains the selected formal UDS runtime until those separate gates pass.

Before changing the unit, retain the previous revision, installed wheel,
root-owned unit/drop-ins and configuration in a private operator backup. Rollback
requires stopping Consumer first, restoring the saved wheel/unit/configuration,
then returning the **empty, child-partition-free** `tradecore.slice` to `member`
and clearing its exclusive set before starting the old service. Do not delete
Journal evidence or release an occupied partition. No reboot is required.

The unit's `ExecStartPost` waits for the pathname socket to exist with the
expected ownership and mode (default `xuanling:xuanling:600`; a reviewed
shared-test override passes its expected identity as the script's second
argument). Therefore a successful
`systemctl start/restart` is also the reset readiness barrier; never start a
seed or formal release concurrently with, or before the command returns.
`Type=simple` process activation by itself is not socket readiness.

The supplied profile authorizes only Unix UID/GID `1000/1000` (`xuanling`). A
shared-test deployment may preserve that primary identity and explicitly add
reviewed tester UID/GID pairs. It must also use a dedicated group-owned socket,
not world-readable/writable access. Unlisted peers remain fail-closed.

## Formal-location long-runtime qualification entry

`xuanling-tradecore-longrun-qualification.service` installs the existing bounded
persistent no-Adapter composition from the formal checkout/installed wheel.
It is an **explicit manual qualification entry**, not registration of a
qualified `LONG_RUNNING` production builder and not a trading release. Do not
enable it at boot. Its 300-second RuntimeMax is an emergency ceiling, never a
successful test terminator. The operator stops it normally after each diagnostic.

Prepare a new private run below `/var/lib/tradecore-longrun-qualification` with
the Git tool `tools/performance/prepare_consumer_candidate.py`, then install its
configuration as root:xuanling/0640 at
`/etc/xuanling-tradecore/consumer-longrun-qualification.json`. Install the supplied
unit under `/etc/systemd/system` and reload systemd. The config explicitly uses
the `NO_ADAPTER_BENCHMARK/disabled-no-dispatch` binding and `LONG_RUNNING` resource
policy. The socket is the prepared run's private `s`, **not** the ordinary
Consumer benchmark socket. No broker password or SDK is accepted.

`--initialize-if-new` initializes only an absent or empty private persistent
directory. A populated directory goes through original identity/Journal/coverage
verification and recovery; a partial directory without its source manifest is
rejected, not reset. Normal restart preserves Journal and signed identity and
creates a new disposable projection generation under the existing bounded
generation budget. Do not delete source data to make restart pass.

The unit uses the same verified production partition, 6 GiB total cgroup memory,
fixed role CPUs 64/65/66/67 and optional projection readers 68/69. It conflicts
with the ordinary benchmark Consumer so two OMS instances cannot accidentally
compete during measurement. Starting qualification stops that benchmark; after
normal shutdown, the operator must explicitly restore
`xuanling-tradecore-consumer-uds.service`. The DISARMED runtime and supervisor
remain in their existing partition with unchanged budgets.

Record configuration/revision/Journal ID, source cut and generation before and
after one actual recovery restart. Only then use fresh namespaces for independent
30-second 2K Submit + 2K snapshot-page short diagnostics with a separate public
rolling-event/FULL-checkpoint consumer. Keep scheduled deadlines, all planned
requests, generator queue, source/checkpoint join and failures. Post-window drain
may prove closure but must not improve measured TPS or event-latency SLO. This
entry does not remove capacity/600-second/MIX/two-hour/twelve-hour gates.

Each service start creates a new mmap journal namespace below the private
`StateDirectory`.  This deliberately prevents a restart from being presented
as a recovered OMS projection.  Restart between independent benchmark rounds
to satisfy clean-state isolation, and keep the old mmap file as evidence.
The authorized benchmark principal `xuanling` may use only the scoped
passwordless command
`sudo -n /usr/bin/systemctl restart xuanling-tradecore-consumer-uds.service`
between rounds. Do not restart during a measurement window. The fixed
server-local profile uses an explicit backlog of 8192 and a single nonblocking
request reactor. At most 2048 complete requests wait for dispatch, 8192
incomplete peers wait for frames, and 10240 connections are owned in total.
These values are root-owned deployment parameters; each is capped at 65536 and
the two user-space capacities may not sum above 65536. This provides bounded
headroom for the formal 5000-client common release without turning overload
into an unbounded queue. A complete request rejected before
dispatch receives `RATE_LIMITED` / `INGRESS_OVERLOADED_PRE_DISPATCH` through the
bounded reply lane when it has capacity. Descriptor exhaustion, an incomplete
frame, or a saturated reply lane can still cause a transport close; classify
an unacknowledged request as unknown until reconciled.
The generator needs a soft `RLIMIT_NOFILE` above its target concurrency
(at least count + 64) or client-side `EMFILE` will be misreported as server
backpressure. Neither connection success nor the backlog size proves 2000
accepted TPS.

Performance generators are consumers, not service administrators. The
`consumer_uds_hot_probe.py`, `consumer_uds_burst_probe.py`, steady, replay,
snapshot and mixed probes only issue versioned Consumer V2 requests over the
authorized UDS socket and write local evidence. They cannot restart/reset the
unit, rotate/delete a Journal, seed it by direct file access, inspect private
server state, or invoke an Adapter/SDK. An operator performs each independent
reset and any synthetic-data preparation outside the measured generator; the
formal 5,000 common-release burst uses `--count 5000`, while a width above the
10,240 user-space connection envelope is reported only as bounded backpressure,
never as accepted trading capacity.

The server-local snapshot projection preserves the bounded legacy
`GetAccountOrderSnapshot` path and exposes `CreateOrderSnapshot` plus
`ReadOrderSnapshotPage` for large accounts. A single isolated read process
builds immutable versions; cold creation returns retryable `SNAPSHOT_NOT_READY`
instead of blocking the reactor. SubmitSignal, PollEvents and snapshot work use
separate bounded fair-scheduling lanes. Other operations retain their existing
serialization. The ingress service
logs cumulative accepted/overloaded/served counters and queue high-water mark
about once per second and at shutdown. These counters identify service-side
backpressure; a client-side connect success alone does not prove admission.
When `TRADECORE_UDS_TRACE=1`, the same counters, capacity limits, overload
reply/close counts and socket-accept total are also published asynchronously
under the `ingress` member of the trace `.status.json` sidecar. Collect the
sidecar before and after every formal window and use counter deltas; this
publication runs on the trace writer thread and adds no request-path file I/O.

The reactor performs nonblocking accept/read/write and dispatches at most 24
complete requests per turn. Idle persistent connections and partial response
writes do not pin a business worker. Only one response per connection is in
flight; pipelined requests retain their original order and request limit.
The formal `fresh-per-request` profile sets `max_requests_per_connection=1` in
the root-owned server configuration. After the complete response is sent, the
reactor closes that connection immediately instead of spending another
selector turn waiting for peer EOF. Set a reviewed value from 2 through 1024
only for an explicitly persistent profile; the default remains 1024 when the
field is absent so existing deployments are not silently changed.
The shared wire decoder, schema validation, current session authorization,
OMS admission and mmap commit-before-receipt path are unchanged. The mode is
opt-in for this local no-Adapter composition; transports with live subscriptions
continue to use their existing worker path. Incomplete first frames have a
5-second absolute deadline. Established peers have a 5-second inactivity timeout.
The ingress log retains frame-wait and `worker_queue_wait_ns_*` names for
compatibility; in reactor mode the latter measures ready-frame to dispatch wait.
There are no 16 connection workers in this profile. `ready_on_accept` counts
frames ready during the first accept/read. Use counter deltas for each run.
These aggregate counters do not constitute per-request server trace pairs or
a 5,000 QPS acceptance result.

The fixed Consumer V2 schema permits an exact fast validation path for valid
`GetAccountOrderSnapshot` and common `SubmitSignal` requests. It is enabled only
when the loaded request schema has the reviewed digest; malformed requests and other shapes
still use full JSON Schema validation. A schema revision disables the shortcut
until it is reviewed again. This reduces per-request Python validation work.
The benchmark's immutable single-generation cursor registry is also built once
at service start rather than revalidated for each snapshot/cursor request.

When WRITE and SNAPSHOT are both continuously ready, the cost-calibrated fair
queue serves them in a 2:5 request ratio. REPLAY and OTHER retain explicit
service slots and unused slots are immediately borrowed by non-empty lanes.
This ratio is based on mixed-window trace evidence: a durable write handler is
materially more expensive than a cached immutable page read, while the former
still meets its 2,000 TPS latency gate at the smaller request share. It is not
an unbounded snapshot priority override.

For S2 replay, call `CreateReplayPartitions(consumer_account_id, partition_count)`
once after seeding and assign each returned signed cursor to one worker. Tokens
carry disjoint `(after, high]` fences; ordinary `PollEvents` continuation keeps
the same end fence. Partition counts are 1..64. Reset invalidates old plans.
The local projection builds a cached immutable event index at preparation time;
fixed-watermark pages use binary search plus the requested slice, not a full
history reconstruction for every poll. S2 counts durably checkpointed unique
events, not RPCs or repeated reads. See the versioned S2 partition handoff document.

The deployment profile reserves 1,000,000 fact slots (approximately 3.82 GiB
maximum mmap file) and caps the UDS parent plus isolated read process at 6 GiB.
The server must pass the deployment preflight for both limits before a large
snapshot run. New SubmitSignal admissions write two facts each. The S5 plan of
200,000 seed orders plus 60,000 accepted window writes consumes 520,000 fact
slots and leaves 480,000 slots (92.3% of the planned consumption), exceeding
the greater-of-10%-or-10,000 safety margin. All figures exclude extra
cancel/conflict facts; preserve and account for those separately. These bounds
do not imply a 5,000 QPS result; rerun the three-round server-local benchmark
after each deployment.
