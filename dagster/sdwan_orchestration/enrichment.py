"""
Enrichment logic: joins vendor edge data with company inventory rows.

Record shape published to network-events
-----------------------------------------
{
    "event_type":        "NETWORK_EVENT",        # constant — Task 2 can filter on this
    "polled_at":         "<ISO-8601 UTC>",        # when this poll run started
    "host":              "host_1",
    "serial_number":     "SIM-1-00001",
    "edge_id":           1000001,
    "edge_name":         "edge-SIM-1-00001",
    "model_number":      "edge610",
    "edge_state":        "CONNECTED" | "OFFLINE" | "NEVER_ACTIVATED",
    "edge_state_time":   "<ISO-8601 UTC>",
    "last_contact":      "<ISO-8601 UTC>",
    "service_state":     "IN_SERVICE" | "OUT_OF_SERVICE",
    "inventory_match":   true | false,           # false if serial not in company inventory
    "company": {                                  # null when inventory_match is false
        "company_id":         "C-NORTH",
        "company_name":       "Northwind Clinics",
        "site_id":            "SITE-1001",
        "site_name":          "Chicago site 1001",
        "city":               "Chicago",
        "region":             "Illinois",
        "utc_offset_hours":   -5,
        "management_status":  "Managed",
        "service_tier":       "Platinum"
    },
    "links": [                                    # empty list for NEVER_ACTIVATED
        {
            "link_id":        1000011,
            "interface":      "GE1",
            "display_name":   "SIM-1-00001 GE1",
            "ip_address":     "10.1.1.1",
            "state":          "STABLE" | "DISCONNECTED",
            "vpn_state":      "STABLE",
            "network_side":   "WAN",
            "network_type":   "ETHERNET",
            "last_active":    "<ISO-8601 UTC>",
            "circuit_id":     "CKT-SIM-1-00001-GE1",  # null when inventory_match is false
            "carrier":        "Metro Fiber"             # null when inventory_match is false
        }
    ]
}

Design notes
------------
- A vendor edge with no company row  → published with inventory_match=false, company=null,
  link circuit/carrier fields=null.  Task 2 can still monitor edge/link state.
- A company row with no vendor edge  → NOT published. We only emit what the vendor reports.
  (COMPANY-ONLY-* serials and serials at index 19 on each host fall in this bucket.)
- NEVER_ACTIVATED edges              → published as-is; links list is empty (vendor sends none).
- OFFLINE edges                      → vendor already reports each link as DISCONNECTED;
  both the edge-level OFFLINE and per-link DISCONNECTED are included so Task 2 can
  key timers on either dimension independently.
- Full snapshot on every poll        → every edge is published each run, not just changes.
  Task 2 needs "still healthy" heartbeats to clear / prevent spurious timers.
"""

from __future__ import annotations

from typing import Any


def build_inventory_index(inventory: dict[str, Any]) -> dict[str, dict]:
    """Build a serial → company-device mapping for O(1) lookups."""
    return {d["serialNumber"]: d for d in inventory["devices"]}


def enrich_edges(
    vendor_response: dict[str, Any],
    inventory_index: dict[str, dict],
    polled_at: str,
) -> list[dict[str, Any]]:
    """
    Merge one vendor host's edge list with company inventory.
    Returns a list of enriched event dicts ready for Kafka.
    """
    host = vendor_response["host"]
    records: list[dict[str, Any]] = []

    for edge in vendor_response["edges"]:
        serial = edge["serialNumber"]
        company_row = inventory_index.get(serial)
        inventory_match = company_row is not None

        # Build per-link company lookup (interface → CompanyLinkDto)
        company_links: dict[str, dict] = {}
        if company_row:
            for cl in company_row.get("links", []):
                company_links[cl["interface"]] = cl

        enriched_links = []
        for link in edge.get("links", []):
            iface = link["interface"]
            cl = company_links.get(iface)
            enriched_links.append({
                "link_id":      link["id"],
                "interface":    iface,
                "display_name": link["displayName"],
                "ip_address":   link["ipAddress"],
                "state":        link["state"],
                "vpn_state":    link["vpnState"],
                "network_side": link["networkSide"],
                "network_type": link["networkType"],
                "last_active":  link["lastActive"],
                "circuit_id":   cl["circuitId"] if cl else None,
                "carrier":      cl["carrier"] if cl else None,
            })

        company_block = None
        if company_row:
            company_block = {
                "company_id":        company_row["companyId"],
                "company_name":      company_row["companyName"],
                "site_id":           company_row["siteId"],
                "site_name":         company_row["siteName"],
                "city":              company_row["city"],
                "region":            company_row["region"],
                "utc_offset_hours":  company_row["utcOffsetHours"],
                "management_status": company_row["managementStatus"],
                "service_tier":      company_row["serviceTier"],
            }

        records.append({
            "event_type":       "NETWORK_EVENT",
            "polled_at":        polled_at,
            "host":             host,
            "serial_number":    serial,
            "edge_id":          edge["id"],
            "edge_name":        edge["name"],
            "model_number":     edge["modelNumber"],
            "edge_state":       edge["edgeState"],
            "edge_state_time":  edge["edgeStateTime"],
            "last_contact":     edge["lastContact"],
            "service_state":    edge["serviceState"],
            "inventory_match":  inventory_match,
            "company":          company_block,
            "links":            enriched_links,
        })

    return records
