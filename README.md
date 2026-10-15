# SD-WAN Devices Monitoring Pipeline Exercise

Two pieces of this stack are unfinished. Those two pieces are the exercise. Everything else already runs.

The domain is SD-WAN edges and their WAN links.

## Start the stack

From this directory:

```bash
docker compose up --build
```

The first build pulls Kafka, Kafka UI, Flink, Dagster, and the .NET SDK. Give it several minutes.

| What | Where |
| --- | --- |
| Vendor and company API | http://localhost:8080 |
| Swagger | http://localhost:8080/swagger |
| Dagster | http://localhost:3000 |
| Flink | http://localhost:8081 |
| Kafka UI | http://localhost:8082 |
| Kafka from your machine | `localhost:9092` |
| Kafka from another container | `kafka:9093` |

Dagster loads one placeholder asset, `pipeline_scaffold`. Materialize it to confirm the code location can see the vendor URL and the Kafka broker. That asset is not the exercise. Replace it.

Python under `dagster/sdwan_orchestration` is mounted into the Dagster containers. You should not need to restart `dagster-webserver` and `dagster-daemon` after a code change. Rebuild the Dagster image only when you add a Python dependency. `requests` and `confluent-kafka` are already installed. Runs execute inside the daemon container, so use the container hostnames above, not `localhost`.

The Flink source to edit is `flink-job/src/main/java/exercise/NetworkEventsPassThroughJob.java`. After you change it:

```bash
docker compose up -d --build flink-job
```

That rebuilds the jar, cancels the running job, and submits the new one.

To read a topic from your machine, you can use the Kafka UI or:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic network-events --from-beginning
```

Use the same command with `network-incidents`.

## The APIs

Swagger for these routes is at http://localhost:8080/swagger. A try-it-out call to a vendor device route blocks for the full wait. `/health`, `/vendor/hosts`, and `/company/inventory` return immediately.

`GET /health` answers immediately.

`GET /vendor/hosts` lists the three hosts.

`GET /vendor/host_1/devices` waits 1 minute, then returns 100 edges.

`GET /vendor/host_2/devices` waits 2 minutes, then returns 200 edges.

`GET /vendor/host_3/devices` waits 3 minutes, then returns 1000 edges.

`GET /company/inventory` answers immediately. It is the company, site, and circuit data for those edges. It does not change during the exercise. Vendor state does. NOTE: structure your dagster asset as though this api is expensive to call, and data doesn't change often.

`VENDOR_DELAY_SCALE` in `docker-compose.yml` multiplies the three waits. It is `1`, so the waits above are real. You may lower it while iterating. Design the job for scale `1`, and say in your notes if you changed it.

The data returned from the vendor APIs are simulated to mirror the real-world behavior. The exact groups and clocks are in `vendor-api/src/VendorApi/Simulator.cs`.

The groups break down as follows:

- A few edges stay `OFFLINE`. Their links stay `DISCONNECTED`.
- A few edges are `OFFLINE` for 3 minutes and `CONNECTED` for 9.
- A few edges are `OFFLINE` for 8 minutes and `CONNECTED` for 8.
- A few edges stay `CONNECTED` while `GE1` is `DISCONNECTED` for 6 minutes and `STABLE` for 10. `GE2` stays `STABLE`.
- A few edges stay `CONNECTED` while `GE1` is `DISCONNECTED` for 14 minutes and `STABLE` for 10.
- A few edges stay `CONNECTED` with `GE1` disconnected the whole time.
- One edge on each host is `NEVER_ACTIVATED` and has no links.
- Serials at index 19 on each host are missing from company inventory.
- Company inventory has five `COMPANY-ONLY-*` serials that are on no vendor host.

`edgeState` is `CONNECTED`, `OFFLINE`, or `NEVER_ACTIVATED`. Link `state` is `STABLE` or `DISCONNECTED`. Company rows add the company, the site, a whole-hour UTC offset, a management status, and a service tier. Join vendor edges to company rows on `serialNumber`.

## What already runs

Kafka has `network-events` and `network-incidents`. Kafka UI at http://localhost:8082 is a browser for those topics. The console consumer below still works.

The Flink job reads `network-events` and writes every record, unchanged, to `network-incidents`. Task 2 replaces that behavior.

Dagster has a webserver and a daemon. It has no schedule yet.

## Task 1. Publish enriched network state

Add a Dagster asset that loads company inventory from `GET /company/inventory`.

Add a job that runs every 3 minutes. On each run, pull every vendor host, enrich each edge from the company inventory, and publish to `network-events`.

The record shape is yours. Task 2 has to be able to tell a device outage, a link failure, and a healthy device or link apart.

Write down the choices you make:

- How you call three hosts that take 1, 2, and 3 minutes to answer, from a job that is due every 3 minutes.
- What happens when a run is still in progress and the next tick fires.
- What you publish for a serial with no company row, and for a company row with no vendor edge.
- Whether a tick publishes a full snapshot or only a change.
- What you do with `NEVER_ACTIVATED`.
- An offline edge reports its links as `DISCONNECTED`. Whether that is a device fact, a fact per link, or both.

## Task 2. Hold the outage before opening an incident

Change the Flink job so `network-incidents` receives incidents, not a copy of every poll.

A device outage waits 5 minutes before an incident is written. A link failure waits 10 minutes. A healthy reading for that device clears a pending device timer. A healthy reading for that link clears a pending link timer. A healthy reading is not itself an incident, unless it is the first reading after a device outage or link failure incident was already published.

Use `edgeState` `OFFLINE` as the device outage and `CONNECTED` as device healthy. Use link `state` `DISCONNECTED` as the link failure and `STABLE` as link healthy.

Write down:

- Whether the timer uses event time or processing time, and what a late reading does.
- What a second offline or disconnected reading does to a timer that is already running.
- The key for a device timer and the key for a link timer.
- What `NEVER_ACTIVATED` does.
- What happens to link timers when the whole edge is offline.

## What to send back

The repository, in a state where `docker compose up --build` runs your job and your Flink change. The notes for the choices above. The commands you used to see a record on `network-events` and, after the wait, an incident on `network-incidents`. A short list of what you left unfinished.

Ask questions while you work. If you have to assume an answer, write the assumption down and keep going.
