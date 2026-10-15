"""
Dagster definitions for the SD-WAN network monitoring pipeline.

Assets
------
company_inventory   : Loads GET /company/inventory once and caches the result.
                      Modelled as a separate asset because the data changes infrequently
                      and the endpoint is expensive (treat it like a slow reference table).
                      Re-materialize manually whenever a full refresh is needed.

network_events_poll : Depends on company_inventory.  Fetches all three vendor hosts in
                      parallel, enriches each edge, and publishes to network-events.
                      This is the asset that runs every 3 minutes.

Job / Schedule
--------------
network_poll_job    : Selects only network_events_poll (company_inventory is upstream and
                      is included automatically when needed).
network_poll_schedule: Runs network_poll_job every 3 minutes.

Concurrency / overlap
---------------------
The schedule runs every 3 minutes. The job is tagged with a concurrency key so that
at most one run is active at a time — new ticks queue rather than pile up.
"""

from __future__ import annotations

import datetime
from typing import Any

import dagster as dg

from sdwan_orchestration.enrichment import build_inventory_index, enrich_edges
from sdwan_orchestration.kafka_publisher import publish_events
from sdwan_orchestration.vendor_client import (
    fetch_all_hosts_parallel,
    fetch_company_inventory,
    fetch_hosts,
)


# ---------------------------------------------------------------------------
# Asset 1: company inventory (reference data, infrequently changing)
# ---------------------------------------------------------------------------

@dg.asset(
    description=(
        "Loads the company inventory from GET /company/inventory. "
        "Treat as expensive and rarely-changing — materialize manually when needed."
    ),
    group_name="sdwan",
)
def company_inventory(context) -> dict[str, Any]:
    context.log.info("Fetching company inventory …")
    data = fetch_company_inventory()
    device_count = len(data.get("devices", []))
    context.log.info(f"Fetched {device_count} company inventory rows.")
    context.add_output_metadata({"device_count": device_count, "observed_at": data.get("observedAt")})
    return data


# ---------------------------------------------------------------------------
# Asset 2: network events poll (runs every 3 minutes)
# ---------------------------------------------------------------------------

@dg.asset(
    description=(
        "Polls all vendor hosts in parallel, enriches edges from company inventory, "
        "and publishes enriched records to network-events."
    ),
    group_name="sdwan",
)
def network_events_poll(context, company_inventory: dict[str, Any]) -> None:
    polled_at = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    context.log.info(f"Poll started at {polled_at}")

    # Build the O(1) serial → company row index once for this run
    inventory_index = build_inventory_index(company_inventory)
    context.log.info(f"Inventory index has {len(inventory_index)} entries.")

    # Discover hosts (allows the vendor to add/remove hosts without code changes)
    hosts = fetch_hosts()
    context.log.info(f"Vendor hosts: {[h['host'] for h in hosts]}")

    # Fetch all hosts in parallel — blocks until the slowest host (host_3) responds
    context.log.info("Fetching all vendor hosts in parallel (this may take ~3 min at scale=1) …")
    vendor_responses = fetch_all_hosts_parallel(hosts)

    # Enrich and collect all records
    all_events: list[dict[str, Any]] = []
    for vendor_response in vendor_responses:
        host = vendor_response["host"]
        edges = vendor_response.get("edges", [])
        context.log.info(f"Host {host}: {len(edges)} edges received.")
        enriched = enrich_edges(vendor_response, inventory_index, polled_at)
        all_events.extend(enriched)

    context.log.info(f"Total enriched events to publish: {len(all_events)}")

    # Publish to Kafka
    published = publish_events(all_events, logger=context.log)

    context.add_output_metadata({
        "polled_at": polled_at,
        "total_events": published,
        "hosts_polled": len(vendor_responses),
    })
    context.log.info(f"Poll complete. Published {published} events.")


# ---------------------------------------------------------------------------
# Job
# ---------------------------------------------------------------------------

network_poll_job = dg.define_asset_job(
    name="network_poll_job",
    selection=dg.AssetSelection.assets(network_events_poll),
    tags={
        "dagster/concurrency_key": "network_poll",
        "dagster/max_retries": "0",
    },
)


# ---------------------------------------------------------------------------
# Schedule: every 3 minutes
# ---------------------------------------------------------------------------

@dg.schedule(
    job=network_poll_job,
    cron_schedule="*/3 * * * *",
    default_status=dg.DefaultScheduleStatus.RUNNING,
    description="Polls vendor hosts and publishes to network-events every 3 minutes.",
)
def network_poll_schedule(context):
    return dg.RunRequest(run_key=None)


# ---------------------------------------------------------------------------
# Definitions
# ---------------------------------------------------------------------------

defs = dg.Definitions(
    assets=[company_inventory, network_events_poll],
    jobs=[network_poll_job],
    schedules=[network_poll_schedule],
)
