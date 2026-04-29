#!/usr/bin/env python3
"""
Update dealer leaderboard 2026 YTD data from HubSpot.

Behaviour:
  - Reads data/historical_dealers.json (static 2022 to 2025 baseline, never modified here).
  - Fetches deals from HubSpot in the CONFIRMED SALES ORDERS pipeline with
    closedate from 2026-01-01 onwards, excluding any 'refunded' or 'credited' stages.
  - Only counts deals associated with companies whose hs_persona is one of:
    Rosie: Retailer / Dealer Dan: Installer/Shade Specialist / Distributor Dylan.
  - Aggregates total deal amount per company (y2026_ytd) and product line item
    counts per company (p_*_2026_ytd, plus updates the all-time p_* totals
    using the 2026 increment on top of the historical baseline).
  - Writes data/dealer_data.json = historical + 2026 YTD overlay.

Required environment variables:
  HUBSPOT_TOKEN     HubSpot Private App access token

Required Private App scopes:
  crm.objects.companies.read
  crm.objects.deals.read
  crm.objects.line_items.read
  crm.schemas.companies.read
"""
from __future__ import annotations

import json
import os
import sys
import time
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

PIPELINE_ID = "c87e5411-b793-4337-a1d6-b662e6df6e83"  # CONFIRMED SALES ORDERS
EXCLUDED_STAGE_KEYWORDS = ("refunded", "credited")
ALLOWED_PERSONAS = {
    "Rosie: Retailer",
    "Dealer Dan: Installer/Shade Specialist",
    "Distributor Dylan",
}
YEAR_START_ISO = "2026-01-01T00:00:00.000Z"

# Map raw line item names (case-insensitive substring) to product key
PRODUCT_PATTERNS = {
    "retreat":  ["retreat"],
    "serenity": ["serenity"],
    "unity":    ["unity"],
    "su2":      ["su2", "su 2"],
    "su4":      ["su4", "su 4"],
    "su10":     ["su10", "su 10"],
    "oasis":    ["oasis"],
}

REPO_ROOT = Path(__file__).resolve().parent.parent
HISTORICAL_PATH = REPO_ROOT / "data" / "historical_dealers.json"
OUTPUT_PATH = REPO_ROOT / "data" / "dealer_data.json"

DEBUG = os.environ.get("DEBUG") == "1"

# -----------------------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------------------

def log(msg: str) -> None:
    print(msg, flush=True)

def dlog(msg: str) -> None:
    if DEBUG:
        print("  [debug] " + msg, flush=True)

def hs_session() -> requests.Session:
    token = os.environ.get("HUBSPOT_TOKEN")
    if not token:
        sys.exit("ERROR: HUBSPOT_TOKEN environment variable not set.")
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json",
    })
    return s

def post_with_retry(s: requests.Session, url: str, payload: dict, max_retries: int = 4) -> dict:
    for attempt in range(max_retries):
        r = s.post(url, json=payload, timeout=60)
        if r.status_code == 429:
            wait = 2 ** attempt
            log(f"  rate limited, sleeping {wait}s...")
            time.sleep(wait)
            continue
        if r.status_code >= 500:
            wait = 2 ** attempt
            log(f"  server error {r.status_code}, retry in {wait}s...")
            time.sleep(wait)
            continue
        r.raise_for_status()
        return r.json()
    r.raise_for_status()
    return {}

# -----------------------------------------------------------------------------
# Step 1: pipeline stage filtering
# -----------------------------------------------------------------------------

def fetch_allowed_stages(s: requests.Session) -> set[str]:
    """Return stage IDs in the CONFIRMED SALES ORDERS pipeline excluding refunded/credited."""
    r = s.get(f"https://api.hubapi.com/crm/v3/pipelines/deals/{PIPELINE_ID}", timeout=30)
    r.raise_for_status()
    stages = r.json().get("stages", [])
    allowed = set()
    for st in stages:
        label = (st.get("label") or "").lower()
        if any(kw in label for kw in EXCLUDED_STAGE_KEYWORDS):
            log(f"  excluding stage: {st.get('label')} ({st.get('id')})")
            continue
        allowed.add(st.get("id"))
    log(f"  allowed stage IDs: {len(allowed)} of {len(stages)} total")
    return allowed

# -----------------------------------------------------------------------------
# Step 2: fetch 2026 deals via CRM search
# -----------------------------------------------------------------------------

def fetch_2026_deals(s: requests.Session, allowed_stage_ids: set[str]) -> list[dict]:
    """Search deals in the target pipeline with closedate >= 2026-01-01."""
    url = "https://api.hubapi.com/crm/v3/objects/deals/search"
    out: list[dict] = []
    after = None
    while True:
        body = {
            "filterGroups": [{
                "filters": [
                    {"propertyName": "pipeline", "operator": "EQ", "value": PIPELINE_ID},
                    {"propertyName": "closedate", "operator": "GTE", "value": YEAR_START_ISO},
                ]
            }],
            "properties": ["dealname", "amount", "closedate", "dealstage", "pipeline"],
            "limit": 100,
            "sorts": [{"propertyName": "closedate", "direction": "ASCENDING"}],
        }
        if after:
            body["after"] = after
        data = post_with_retry(s, url, body)
        results = data.get("results", [])
        for d in results:
            stage = d.get("properties", {}).get("dealstage")
            if stage in allowed_stage_ids:
                out.append(d)
        paging = data.get("paging", {}).get("next", {}).get("after")
        if not paging:
            break
        after = paging
        log(f"  paged through {len(out)} deals so far...")
    log(f"  fetched {len(out)} qualifying 2026 deals")
    return out

# -----------------------------------------------------------------------------
# Step 3: deal -> company associations (v4 batch)
# -----------------------------------------------------------------------------

def fetch_deal_company_map(s: requests.Session, deal_ids: list[str]) -> dict[str, str]:
    """Return {deal_id: company_id} via v4 batch associations."""
    if not deal_ids:
        return {}
    out: dict[str, str] = {}
    url = "https://api.hubapi.com/crm/v4/associations/deals/companies/batch/read"
    for i in range(0, len(deal_ids), 100):
        batch = deal_ids[i:i+100]
        payload = {"inputs": [{"id": did} for did in batch]}
        data = post_with_retry(s, url, payload)
        for row in data.get("results", []):
            from_id = row.get("from", {}).get("id")
            tos = row.get("to", [])
            if from_id and tos:
                out[str(from_id)] = str(tos[0].get("toObjectId"))
    return out

# -----------------------------------------------------------------------------
# Step 4: filter companies by persona
# -----------------------------------------------------------------------------

def fetch_company_personas(s: requests.Session, company_ids: list[str]) -> dict[str, str]:
    """Return {company_id: hs_persona} via batch read."""
    if not company_ids:
        return {}
    out: dict[str, str] = {}
    url = "https://api.hubapi.com/crm/v3/objects/companies/batch/read"
    unique_ids = list(set(company_ids))
    for i in range(0, len(unique_ids), 100):
        batch = unique_ids[i:i+100]
        payload = {
            "inputs": [{"id": cid} for cid in batch],
            "properties": ["hs_persona", "name"],
        }
        data = post_with_retry(s, url, payload)
        for row in data.get("results", []):
            cid = str(row.get("id"))
            persona = row.get("properties", {}).get("hs_persona") or ""
            out[cid] = persona
    return out

# -----------------------------------------------------------------------------
# Step 5: line items per deal
# -----------------------------------------------------------------------------

def fetch_deal_lineitem_map(s: requests.Session, deal_ids: list[str]) -> dict[str, list[str]]:
    if not deal_ids:
        return {}
    out: dict[str, list[str]] = defaultdict(list)
    url = "https://api.hubapi.com/crm/v4/associations/deals/line_items/batch/read"
    for i in range(0, len(deal_ids), 100):
        batch = deal_ids[i:i+100]
        payload = {"inputs": [{"id": did} for did in batch]}
        data = post_with_retry(s, url, payload)
        for row in data.get("results", []):
            from_id = str(row.get("from", {}).get("id"))
            for to in row.get("to", []):
                out[from_id].append(str(to.get("toObjectId")))
    return out

def fetch_lineitem_names(s: requests.Session, li_ids: list[str]) -> dict[str, tuple[str, float]]:
    """Return {line_item_id: (name, quantity)}."""
    if not li_ids:
        return {}
    out: dict[str, tuple[str, float]] = {}
    url = "https://api.hubapi.com/crm/v3/objects/line_items/batch/read"
    unique_ids = list(set(li_ids))
    for i in range(0, len(unique_ids), 100):
        batch = unique_ids[i:i+100]
        payload = {
            "inputs": [{"id": x} for x in batch],
            "properties": ["name", "quantity"],
        }
        data = post_with_retry(s, url, payload)
        for row in data.get("results", []):
            lid = str(row.get("id"))
            name = (row.get("properties", {}).get("name") or "").lower()
            qty_raw = row.get("properties", {}).get("quantity")
            try:
                qty = float(qty_raw) if qty_raw is not None else 1.0
            except (TypeError, ValueError):
                qty = 1.0
            out[lid] = (name, qty)
    return out

def classify_product(name: str) -> str | None:
    n = name.lower()
    for key, patterns in PRODUCT_PATTERNS.items():
        for pat in patterns:
            if pat in n:
                return key
    return None

# -----------------------------------------------------------------------------
# Aggregate
# -----------------------------------------------------------------------------

def aggregate_2026(s: requests.Session) -> dict[str, dict]:
    """Return {company_id: {amount: float, products: {key: count}}}."""
    log("\n[1/5] Fetching pipeline stages...")
    allowed_stages = fetch_allowed_stages(s)

    log("\n[2/5] Fetching 2026 deals...")
    deals = fetch_2026_deals(s, allowed_stages)
    if not deals:
        log("  no deals found, returning empty result")
        return {}

    deal_ids = [str(d["id"]) for d in deals]

    log("\n[3/5] Resolving deal -> company associations...")
    deal_to_co = fetch_deal_company_map(s, deal_ids)
    log(f"  resolved {len(deal_to_co)} of {len(deal_ids)} deal-company links")

    log("\n[4/5] Filtering companies by dealer persona...")
    co_personas = fetch_company_personas(s, list(deal_to_co.values()))
    valid_companies = {cid for cid, p in co_personas.items() if p in ALLOWED_PERSONAS}
    log(f"  {len(valid_companies)} of {len(co_personas)} companies match dealer personas")

    log("\n[5/5] Fetching line items for product breakdown...")
    deal_to_lis = fetch_deal_lineitem_map(s, deal_ids)
    all_li_ids = [li for lis in deal_to_lis.values() for li in lis]
    log(f"  fetching {len(set(all_li_ids))} unique line items...")
    li_data = fetch_lineitem_names(s, all_li_ids)

    # Aggregate
    out: dict[str, dict] = defaultdict(lambda: {"amount": 0.0, "products": defaultdict(int)})
    for d in deals:
        did = str(d["id"])
        cid = deal_to_co.get(did)
        if not cid or cid not in valid_companies:
            continue
        amt_raw = d.get("properties", {}).get("amount")
        try:
            amt = float(amt_raw) if amt_raw else 0.0
        except (TypeError, ValueError):
            amt = 0.0
        out[cid]["amount"] += amt

        for li_id in deal_to_lis.get(did, []):
            name, qty = li_data.get(li_id, ("", 0))
            pk = classify_product(name)
            if pk:
                out[cid]["products"][pk] += int(qty)

    log(f"\n  aggregated: {len(out)} companies with 2026 sales")
    total = sum(v["amount"] for v in out.values())
    log(f"  total 2026 amount: {total:,.2f}")
    return dict(out)

# -----------------------------------------------------------------------------
# Merge
# -----------------------------------------------------------------------------

PRODUCT_KEYS = ["retreat", "serenity", "unity", "su2", "su4", "su10", "oasis"]

def merge_dealers(historical: list[dict], live_2026: dict[str, dict]) -> list[dict]:
    out = []
    matched_ids = set()
    for h in historical:
        d = dict(h)
        live = live_2026.get(str(d.get("id"))) if d.get("id") else None
        if live:
            d["y2026_ytd"] = round(live["amount"], 2)
            for pk in PRODUCT_KEYS:
                d[f"p_{pk}_2026_ytd"] = live["products"].get(pk, 0)
            matched_ids.add(str(d["id"]))
        else:
            d["y2026_ytd"] = None
            for pk in PRODUCT_KEYS:
                d[f"p_{pk}_2026_ytd"] = None

        if d.get("y2025") and d.get("y2026_ytd"):
            d["pct_25_26"] = d["y2026_ytd"] / d["y2025"]
        else:
            d["pct_25_26"] = None
        out.append(d)

    # New dealers in 2026 that aren't in historical (logged for awareness)
    new_ids = set(live_2026.keys()) - matched_ids
    if new_ids:
        log(f"  note: {len(new_ids)} dealers have 2026 sales but no historical record. "
            "Add them to the source Excel and re-export historical_dealers.json to include them.")
        for cid in list(new_ids)[:10]:
            log(f"    company id {cid}: ${live_2026[cid]['amount']:,.2f}")
    return out

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> int:
    log(f"Dealer leaderboard refresh started at {datetime.now(timezone.utc).isoformat()}")
    if not HISTORICAL_PATH.exists():
        sys.exit(f"ERROR: {HISTORICAL_PATH} not found. Generate it from the Excel exports first.")
    historical = json.loads(HISTORICAL_PATH.read_text())
    log(f"Loaded historical: {len(historical['nzau_dealers'])} NZAU + {len(historical['usa_dealers'])} USA dealers")

    s = hs_session()
    live_2026 = aggregate_2026(s)

    log("\n[merge] Combining historical + 2026 YTD...")
    nzau_merged = merge_dealers(historical["nzau_dealers"], live_2026)
    usa_merged = merge_dealers(historical["usa_dealers"], live_2026)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "historical_generated_at": historical.get("generated_at"),
        "nzau_dealers": nzau_merged,
        "usa_dealers": usa_merged,
    }
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, separators=(",", ":")))
    size_kb = OUTPUT_PATH.stat().st_size / 1024
    log(f"\nWrote {OUTPUT_PATH} ({size_kb:.1f} KB)")
    log("Done.")
    return 0

if __name__ == "__main__":
    sys.exit(main())
