#!/usr/bin/env python3
"""
Update dealer leaderboard data from HubSpot.

Runs on a schedule via GitHub Actions. Pulls:
- Dealer companies (those with a dealer grade set, persona = Rosie/Dealer Dan/Distributor Dylan)
- Deals in the "Confirmed Sales Orders" pipeline, excluding Refunded/Credited stages
- Deal totals per company per calendar year
- Product line items per dealer per year, classified by product family

Writes data/dealer_data.json with the schema the dashboard expects.

Required environment variables:
- HUBSPOT_TOKEN   HubSpot Private App access token

Optional environment variables:
- DEBUG=1         Verbose logging
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
# Config: ADJUST THESE TO MATCH YOUR HUBSPOT INSTANCE
# -----------------------------------------------------------------------------

# Property name on Companies that holds the dealer grade. Run discover_properties.py
# if you are not sure of the internal name.
COMPANY_GRADE_PROP = "dealer_grade"
COMPANY_DISCOUNT_PROP = "discount_rate"
COMPANY_REGION_PROP = "state"
COMPANY_COUNTRY_PROP = "country"
COMPANY_TARGET_PROP = "target_2026"
COMPANY_REVENUE_TARGET_PROP = "revenue_target"
COMPANY_PERSONA_PROP = "hs_persona"     # the "Persona Company" filter

# Persona values that count as a dealer/distributor for filtering deals.
# These match the Persona Company filter from your HubSpot report:
# Rosie: Retailer, Dealer Dan: Installer/Shade Specialist, Distributor Dylan.
# We match labels permissively; the script will keep companies whose persona
# is empty too (better to over-include than drop a real dealer).
DEALER_PERSONAS = {
    "Rosie: Retailer",
    "Dealer Dan: Installer/Shade Specialist",
    "Distributor Dylan",
    "Distributor Dylan: Distributor",
    # short-name fallbacks:
    "Rosie",
    "Dealer Dan",
}

# Pipeline + stages, looked up by label so no manual IDs needed.
DEAL_PIPELINE_LABEL = "Confirmed Sales Orders"
EXCLUDED_STAGE_LABELS = {"Refunded", "Credited"}  # case-insensitive contains match

# Country values that map to each "world".
NZAU_COUNTRIES = {"new zealand", "nz", "australia", "au"}
USA_COUNTRIES = {"united states", "usa", "us", "united states of america"}

# Mapping from internal product key -> list of name prefixes / substrings that identify it.
PRODUCT_MATCHERS: dict[str, list[str]] = {
    "retreat":  ["retreat"],
    "serenity": ["serenity"],
    "unity":    ["unity"],
    "su2":      ["su2", "su 2"],
    "su4":      ["su4", "su 4"],
    "su10":     ["su10", "su 10"],
    "oasis":    ["oasis"],
}

# Years to include per world. Current year always added automatically below.
YEARS_NZAU_BASE = [2022, 2023, 2024, 2025]
YEARS_USA_BASE = [2023, 2024, 2025]

# -----------------------------------------------------------------------------
# Fixed setup
# -----------------------------------------------------------------------------

CURRENT_YEAR = datetime.now().year
YEARS_NZAU = sorted(set(YEARS_NZAU_BASE + [CURRENT_YEAR]))
YEARS_USA = sorted(set(YEARS_USA_BASE + [CURRENT_YEAR]))

HUBSPOT_TOKEN = os.environ.get("HUBSPOT_TOKEN")
if not HUBSPOT_TOKEN:
    sys.exit("ERROR: HUBSPOT_TOKEN environment variable is required.")

DEBUG = os.environ.get("DEBUG") == "1"
BASE = "https://api.hubapi.com"
HEADERS = {
    "Authorization": f"Bearer {HUBSPOT_TOKEN}",
    "Content-Type": "application/json",
}

OUTPUT_PATH = Path(__file__).resolve().parent.parent / "data" / "dealer_data.json"


# -----------------------------------------------------------------------------
# HubSpot helpers
# -----------------------------------------------------------------------------

def log(msg: str) -> None:
    print(msg, flush=True)


def debug(msg: str) -> None:
    if DEBUG:
        print(f"  [debug] {msg}", flush=True)


def hs_search(object_type: str, body: dict[str, Any]) -> list[dict]:
    """POST /crm/v3/objects/{type}/search with cursor pagination and basic retry."""
    url = f"{BASE}/crm/v3/objects/{object_type}/search"
    out: list[dict] = []
    after = None
    while True:
        if after:
            body["after"] = after
        for attempt in range(4):
            try:
                r = requests.post(url, headers=HEADERS, json=body, timeout=60)
                if r.status_code == 429:
                    wait = 2 ** attempt
                    debug(f"rate limited, sleeping {wait}s")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                break
            except requests.HTTPError as e:
                if attempt == 3:
                    raise
                debug(f"retrying after error: {e}")
                time.sleep(2 ** attempt)
        data = r.json()
        out.extend(data.get("results", []))
        nxt = data.get("paging", {}).get("next")
        if not nxt:
            break
        after = nxt["after"]
    return out


def year_window(year: int) -> tuple[str, str]:
    """Return ISO start and end timestamps for a given calendar year (UTC)."""
    start = datetime(year, 1, 1, tzinfo=timezone.utc).isoformat()
    if year == CURRENT_YEAR:
        end = datetime.now(timezone.utc).isoformat()
    else:
        end = datetime(year, 12, 31, 23, 59, 59, tzinfo=timezone.utc).isoformat()
    return start, end


# -----------------------------------------------------------------------------
# Pipeline lookup (by label, no manual IDs needed)
# -----------------------------------------------------------------------------

def fetch_pipeline_config() -> tuple[str, list[str]]:
    """Look up the 'Confirmed Sales Orders' pipeline ID and the list of stage IDs
    we WANT (i.e. all stages except those whose label contains 'refunded' or 'credited').
    Returns (pipeline_id, allowed_stage_ids)."""
    r = requests.get(f"{BASE}/crm/v3/pipelines/deals", headers=HEADERS, timeout=30)
    r.raise_for_status()
    pipelines = r.json().get("results", [])

    target = None
    for p in pipelines:
        if (p.get("label") or "").strip().lower() == DEAL_PIPELINE_LABEL.lower():
            target = p
            break
    if not target:
        names = [p.get("label") for p in pipelines]
        sys.exit(f"ERROR: could not find pipeline labelled '{DEAL_PIPELINE_LABEL}'. "
                 f"Available pipelines: {names}")

    pipeline_id = target["id"]
    allowed_stage_ids = []
    excluded = []
    for stage in target.get("stages", []):
        label = (stage.get("label") or "").strip().lower()
        if any(ex.lower() in label for ex in EXCLUDED_STAGE_LABELS):
            excluded.append(stage.get("label"))
        else:
            allowed_stage_ids.append(stage["id"])

    log(f"  Pipeline '{DEAL_PIPELINE_LABEL}' (id {pipeline_id})")
    log(f"  Allowed stages: {len(allowed_stage_ids)}, excluded: {excluded}")
    return pipeline_id, allowed_stage_ids


# -----------------------------------------------------------------------------
# Fetch dealer companies
# -----------------------------------------------------------------------------

def fetch_dealers() -> list[dict]:
    """Fetch all companies with a dealer grade set AND a dealer-type persona."""
    body = {
        "filterGroups": [{"filters": [
            {"propertyName": COMPANY_GRADE_PROP, "operator": "HAS_PROPERTY"},
        ]}],
        "properties": [
            "name",
            COMPANY_GRADE_PROP,
            COMPANY_DISCOUNT_PROP,
            COMPANY_REGION_PROP,
            COMPANY_COUNTRY_PROP,
            COMPANY_TARGET_PROP,
            COMPANY_REVENUE_TARGET_PROP,
            COMPANY_PERSONA_PROP,
        ],
        "limit": 100,
    }
    log(f"Fetching companies with property '{COMPANY_GRADE_PROP}' set...")
    results = hs_search("companies", body)

    # Persona filter: keep only companies whose persona looks like a dealer.
    # Permissive: if persona property isn't set we still include (better to include
    # than to silently drop a real dealer).
    kept = []
    persona_skipped = 0
    persona_seen: dict[str, int] = defaultdict(int)
    for c in results:
        persona = (c.get("properties", {}).get(COMPANY_PERSONA_PROP) or "").strip()
        if persona:
            persona_seen[persona] += 1
        if persona and DEALER_PERSONAS and persona not in DEALER_PERSONAS:
            persona_skipped += 1
            continue
        kept.append(c)

    log(f"  Found {len(results)} companies with dealer grade.")
    log(f"  Persona filter: kept {len(kept)}, skipped {persona_skipped}.")
    if DEBUG and persona_seen:
        log(f"  Personas observed: {dict(persona_seen)}")
    return kept


def classify_world(country_value: str | None) -> str | None:
    if not country_value:
        return None
    c = str(country_value).strip().lower()
    if c in NZAU_COUNTRIES:
        return "nzau"
    if c in USA_COUNTRIES:
        return "usa"
    return None


# -----------------------------------------------------------------------------
# Fetch deals (sales totals per dealer per year)
# -----------------------------------------------------------------------------

def fetch_deals_for_year(year: int, pipeline_id: str, allowed_stage_ids: list[str]) -> list[dict]:
    """Fetch deals in the Confirmed Sales Orders pipeline that:
    - closed within the given year
    - are NOT in a Refunded/Credited stage."""
    start, end = year_window(year)
    body = {
        "filterGroups": [{"filters": [
            {"propertyName": "pipeline", "operator": "EQ", "value": pipeline_id},
            {"propertyName": "dealstage", "operator": "IN", "values": allowed_stage_ids},
            {"propertyName": "closedate", "operator": "BETWEEN", "value": start, "highValue": end},
        ]}],
        "properties": ["dealname", "amount", "closedate", "dealstage", "pipeline"],
        "limit": 100,
    }
    deals = hs_search("deals", body)
    debug(f"  Year {year}: {len(deals)} qualifying deals fetched")
    return deals


def fetch_deal_company_associations(deal_ids: list[str]) -> dict[str, list[str]]:
    """Batch-fetch associations from deals to companies. Returns {deal_id: [company_id, ...]}."""
    out: dict[str, list[str]] = defaultdict(list)
    if not deal_ids:
        return out
    url = f"{BASE}/crm/v4/associations/deals/companies/batch/read"
    BATCH = 100
    for i in range(0, len(deal_ids), BATCH):
        chunk = deal_ids[i:i + BATCH]
        body = {"inputs": [{"id": did} for did in chunk]}
        r = requests.post(url, headers=HEADERS, json=body, timeout=60)
        r.raise_for_status()
        for row in r.json().get("results", []):
            did = row["from"]["id"]
            for assoc in row.get("to", []):
                out[did].append(assoc["toObjectId"])
    return out


# -----------------------------------------------------------------------------
# Fetch line items (product counts per dealer per year)
# -----------------------------------------------------------------------------

def fetch_line_items_for_deals(deal_ids: list[str]) -> dict[str, list[dict]]:
    """Fetch line items associated to the given deals. Returns {deal_id: [line_item, ...]}."""
    out: dict[str, list[dict]] = defaultdict(list)
    if not deal_ids:
        return out
    url = f"{BASE}/crm/v4/associations/deals/line_items/batch/read"
    deal_to_li: dict[str, list[str]] = defaultdict(list)
    BATCH = 100
    for i in range(0, len(deal_ids), BATCH):
        chunk = deal_ids[i:i + BATCH]
        body = {"inputs": [{"id": did} for did in chunk]}
        r = requests.post(url, headers=HEADERS, json=body, timeout=60)
        r.raise_for_status()
        for row in r.json().get("results", []):
            did = row["from"]["id"]
            for assoc in row.get("to", []):
                deal_to_li[did].append(assoc["toObjectId"])

    all_li_ids = list({lid for lids in deal_to_li.values() for lid in lids})
    li_props_url = f"{BASE}/crm/v3/objects/line_items/batch/read"
    li_by_id: dict[str, dict] = {}
    for i in range(0, len(all_li_ids), BATCH):
        chunk = all_li_ids[i:i + BATCH]
        body = {
            "inputs": [{"id": lid} for lid in chunk],
            "properties": ["name", "quantity", "hs_product_id"],
        }
        r = requests.post(li_props_url, headers=HEADERS, json=body, timeout=60)
        r.raise_for_status()
        for li in r.json().get("results", []):
            li_by_id[li["id"]] = li

    for did, lids in deal_to_li.items():
        for lid in lids:
            if lid in li_by_id:
                out[did].append(li_by_id[lid])
    return out


def classify_product(name: str | None) -> str | None:
    if not name:
        return None
    n = str(name).strip().lower()
    for product_key, prefixes in PRODUCT_MATCHERS.items():
        for p in prefixes:
            if n.startswith(p):
                return product_key
    return None


# -----------------------------------------------------------------------------
# Aggregation
# -----------------------------------------------------------------------------

def num_or_none(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def build_dealer_record(company: dict, world: str) -> dict:
    p = company.get("properties", {})
    return {
        "id": company["id"],
        "company": (p.get("name") or "").strip().lower(),
        "grade": p.get(COMPANY_GRADE_PROP),
        "discount_rate": p.get(COMPANY_DISCOUNT_PROP),
        "region": (p.get(COMPANY_REGION_PROP) or "").strip().lower() or None,
        "target_2026": num_or_none(p.get(COMPANY_TARGET_PROP)),
        "revenue_target": num_or_none(p.get(COMPANY_REVENUE_TARGET_PROP)),
    }


def aggregate(world: str, dealers: list[dict], years: list[int],
              pipeline_id: str, allowed_stage_ids: list[str]) -> list[dict]:
    """Build the full per-dealer records for one world."""
    log(f"\n=== Building {world.upper()} world ({len(dealers)} dealers, years {years}) ===")
    records = {d["id"]: build_dealer_record(d, world) for d in dealers}
    dealer_ids = set(records.keys())

    product_keys = list(PRODUCT_MATCHERS.keys())
    for rec in records.values():
        for y in years:
            suffix = f"{y}_ytd" if y == CURRENT_YEAR else str(y)
            rec[f"y{suffix}"] = 0.0
            for pk in product_keys:
                rec[f"p_{pk}_{suffix}"] = 0
        for pk in product_keys:
            rec[f"p_{pk}"] = 0

    for year in years:
        suffix = f"{year}_ytd" if year == CURRENT_YEAR else str(year)
        log(f"  Year {year}: fetching deals...")
        deals = fetch_deals_for_year(year, pipeline_id, allowed_stage_ids)
        if not deals:
            continue

        deal_ids = [d["id"] for d in deals]
        log(f"    {len(deal_ids)} deals, fetching company associations...")
        deal_to_company = fetch_deal_company_associations(deal_ids)
        log(f"    Fetching line items...")
        deal_to_lis = fetch_line_items_for_deals(deal_ids)

        attributed = 0
        for d in deals:
            company_ids = deal_to_company.get(d["id"], [])
            amount = num_or_none(d.get("properties", {}).get("amount")) or 0.0
            for cid in company_ids:
                if cid in dealer_ids:
                    records[cid][f"y{suffix}"] += amount
                    attributed += 1

        for d in deals:
            company_ids = [c for c in deal_to_company.get(d["id"], []) if c in dealer_ids]
            if not company_ids:
                continue
            for li in deal_to_lis.get(d["id"], []):
                pname = li.get("properties", {}).get("name")
                qty = num_or_none(li.get("properties", {}).get("quantity")) or 1
                pkey = classify_product(pname)
                if not pkey:
                    continue
                for cid in company_ids:
                    records[cid][f"p_{pkey}_{suffix}"] += int(qty)
                    records[cid][f"p_{pkey}"] += int(qty)

        log(f"    Attributed to {attributed} dealer-deal pairs")

    for rec in records.values():
        if rec.get("y2024") and rec.get("y2025"):
            rec["pct_24_25"] = rec["y2025"] / rec["y2024"]
        if rec.get("y2025") and rec.get(f"y{CURRENT_YEAR}_ytd"):
            rec["pct_25_26"] = rec[f"y{CURRENT_YEAR}_ytd"] / rec["y2025"]

    return list(records.values())


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

def main() -> int:
    log(f"Dealer leaderboard refresh started at {datetime.now(timezone.utc).isoformat()}")

    log("\nLooking up pipeline config...")
    pipeline_id, allowed_stage_ids = fetch_pipeline_config()

    all_dealers = fetch_dealers()

    nzau_dealers, usa_dealers, skipped = [], [], 0
    for c in all_dealers:
        country = c.get("properties", {}).get(COMPANY_COUNTRY_PROP)
        world = classify_world(country)
        if world == "nzau":
            nzau_dealers.append(c)
        elif world == "usa":
            usa_dealers.append(c)
        else:
            skipped += 1
    log(f"\nSplit: {len(nzau_dealers)} NZAU, {len(usa_dealers)} USA, {skipped} skipped (no country match)")

    nzau_records = aggregate("nzau", nzau_dealers, YEARS_NZAU, pipeline_id, allowed_stage_ids)
    usa_records = aggregate("usa", usa_dealers, YEARS_USA, pipeline_id, allowed_stage_ids)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "nzau_dealers": nzau_records,
        "usa_dealers": usa_records,
    }

    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUTPUT_PATH.write_text(json.dumps(payload, separators=(",", ":")))
    log(f"\nWrote {OUTPUT_PATH} ({OUTPUT_PATH.stat().st_size / 1024:.1f} KB)")
    log("Done.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
