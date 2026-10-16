"""
HTTP client for the vendor and company APIs.
All three vendor hosts are fetched in parallel using a thread pool so the
total wall-clock time is bounded by the slowest host (~3 min at scale=1).
"""

from __future__ import annotations

import concurrent.futures
from typing import Any

import requests

from sdwan_orchestration.settings import SETTINGS


def fetch_hosts() -> list[dict]:
    """Return the list of vendor hosts from GET /vendor/hosts."""
    resp = requests.get(f"{SETTINGS.vendor_api_base}/vendor/hosts", timeout=30)
    resp.raise_for_status()
    return resp.json()


def fetch_host_devices(host: dict) -> dict[str, Any]:
    """
    Fetch all devices for a single vendor host.
    Uses devicesPath and delayMinutes from the host descriptor returned by
    GET /vendor/hosts, so we don't hardcode URL structure or timeout values.
    Timeout is set to (delayMinutes + 1) * 60s to give a tight per-host ceiling.
    """
    delay_minutes = host.get("delayMinutes", 5)
    timeout = (delay_minutes + 1) * 60  # e.g. host_3: (3+1)*60 = 240s
    url = f"{SETTINGS.vendor_api_base}{host['devicesPath']}"
    resp = requests.get(url, timeout=timeout)
    resp.raise_for_status()
    return resp.json()


def fetch_all_hosts_parallel(hosts: list[dict]) -> list[dict[str, Any]]:
    """
    Fire one thread per host and collect results.
    If any host fails, its exception propagates and the Dagster run fails.
    Returns results in the same order as `hosts`.
    """
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(hosts)) as pool:
        futures = {
            pool.submit(fetch_host_devices, h): h["host"]
            for h in hosts
        }
        results_by_host: dict[str, dict] = {}
        for future in concurrent.futures.as_completed(futures):
            host_name = futures[future]
            results_by_host[host_name] = future.result()  # re-raises on error

    # Preserve original ordering
    return [results_by_host[h["host"]] for h in hosts]


def fetch_company_inventory() -> dict[str, Any]:
    """Return the company inventory from GET /company/inventory."""
    resp = requests.get(f"{SETTINGS.vendor_api_base}/company/inventory", timeout=30)
    resp.raise_for_status()
    return resp.json()
