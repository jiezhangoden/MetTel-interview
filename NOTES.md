# Implementation Notes

## Task 1 — Publish enriched network state

### How you call three hosts that take 1, 2, and 3 minutes from a job due every 3 minutes

All three hosts are fetched in parallel using `concurrent.futures.ThreadPoolExecutor` with one thread per host. All three requests fire at the same time, so total wall time is bounded by the slowest host (~3 minutes for host_3), not the sum (1+2+3=6 minutes). This fits within the 3-minute schedule window.

The host list is discovered dynamically from `GET /vendor/hosts`, which returns each host's `devicesPath` and `delayMinutes`. We use `devicesPath` directly as the request URL (rather than constructing it ourselves) and set a per-host timeout of `(delayMinutes + 1) * 60` seconds — giving one minute of headroom over the expected delay.

See: `dagster/sdwan_orchestration/vendor_client.py` → `fetch_all_hosts_parallel()`

### What happens when a run is still in progress and the next tick fires

The job is tagged with `dagster/concurrency_key: network_poll` and `dagster/max_retries: 0`. The `dagster.yaml` file sets a `run_queue` tag concurrency limit of 1 for this key — this is what actually enforces the constraint at the daemon level. When the schedule fires a new tick while the previous run is still active, the queue coordinator sees the concurrency key is in use and holds the new run in the queue rather than launching it in parallel. The queued run starts immediately after the current one finishes. At most one run is ever active at a time.

### What you publish for a serial with no company row, and for a company row with no vendor edge

- **Vendor edge with no company row** (e.g. serials at index 19 on each host): still published to `network-events` with `inventory_match: false` and `company: null`. The device state is real and Task 2 still needs to monitor it for outages.
- **Company row with no vendor edge** (e.g. `COMPANY-ONLY-*` serials): not published. We only emit what the vendor reports. Those serials have no observable state during this poll.

### Whether a tick publishes a full snapshot or only a change

Full snapshot on every poll. Every edge from every host is published on every run, regardless of whether its state changed since the last poll.

Reason: Task 2 (Flink) manages timers that are cleared by "still healthy" readings. If we only published state changes, a device that stays `CONNECTED` would never send a message, and Flink would have no signal to cancel a pending outage timer. The full snapshot acts as a heartbeat.

### What you do with NEVER_ACTIVATED

Published as-is with `edge_state: NEVER_ACTIVATED` and `links: []` (the vendor sends no links for these edges). We do not suppress it — that is Task 2's decision.

Assumption: Task 2 ignores `NEVER_ACTIVATED` — it does not start a device outage timer (since it is not `OFFLINE`) and has no links to trigger a link failure timer.

### An offline edge reports its links as DISCONNECTED — device fact, link fact, or both

Both. The edge-level `OFFLINE` and each link's `DISCONNECTED` state are published together in the same record. Task 2 keys device timers on `serial_number` and link timers on `(serial_number, interface)` independently. Publishing both allows Task 2 to manage device and link timers separately without having to infer one from the other.

---

## Task 2 — Hold the outage before opening an incident

### Whether the timer uses event time or processing time, and what a late reading does

Processing time. Events arrive near real-time from a 3-minute poll cycle, so the difference between event time and processing time is small and bounded. Using event time would require watermarks tuned to the poll interval and adds complexity without meaningful benefit — a "late" reading in this context is just the next poll, which arrives within 3 minutes regardless.

A late reading (one that arrives after its logical time has passed) simply becomes the current reading and is processed normally. If its state is healthy it cancels a pending timer; if it is unhealthy and no timer is running it starts a new one. There is no concept of dropping or ignoring late records.

### What a second OFFLINE or DISCONNECTED reading does to a timer that is already running

Nothing — the existing timer keeps its original expiry. Resetting the timer on every new unhealthy reading would delay the incident indefinitely as long as polls keep arriving, which would defeat the purpose of the 5/10-minute threshold. The timer fires at a fixed point after the *first* unhealthy reading, regardless of how many subsequent unhealthy readings arrive.

### The key for a device timer and the key for a link timer

- Device timer key: `serial_number` (e.g. `SIM-1-00001`)
- Link timer key: `serial_number + "|" + interface` (e.g. `SIM-1-00001|GE1`)

Each key gets its own independent timer and state, held in Flink keyed state. This allows a device and its individual links to be tracked separately.

### What NEVER_ACTIVATED does

Ignored entirely. `NEVER_ACTIVATED` is not `OFFLINE`, so no device timer is started. The vendor sends no links for these edges, so no link timers are started either. The record passes through the Flink job without producing any timer or incident.

### What happens to link timers when the whole edge is offline

Link timers keep running independently. When an edge goes `OFFLINE` the vendor already reports each of its links as `DISCONNECTED`, so both a device timer and link timers are started (or are already running). They fire independently at their respective thresholds (5 min for device, 10 min for links).

The rationale: the README says "a healthy reading for that device clears a pending device timer" and "a healthy reading for that link clears a pending link timer" — these are separate conditions. When the edge recovers to `CONNECTED`, the device timer is cleared. When a link recovers to `STABLE`, that link's timer is cleared. Neither automatically cancels the other, which matches the independent recovery semantics required by Task 2.
