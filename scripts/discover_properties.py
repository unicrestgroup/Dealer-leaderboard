#!/usr/bin/env python3
"""
Discover HubSpot property names. Run this ONCE to find the internal names
of the company properties used by the dealer leaderboard, then plug them
into update_dealer_data.py.

Usage:
  HUBSPOT_TOKEN=xxx python3 scripts/discover_properties.py
"""
import os
import sys
import requests

token = os.environ.get("HUBSPOT_TOKEN")
if not token:
    sys.exit("Set HUBSPOT_TOKEN first.")

headers = {"Authorization": f"Bearer {token}"}

print("\n=== COMPANY PROPERTIES (filter for likely dealer fields) ===")
r = requests.get(
    "https://api.hubapi.com/crm/v3/properties/companies",
    headers=headers,
    timeout=30,
)
r.raise_for_status()
props = r.json().get("results", [])

keywords = ["grade", "discount", "region", "country", "state", "target", "revenue", "dealer"]
matched = [p for p in props if any(k in p["name"].lower() or k in (p.get("label") or "").lower() for k in keywords)]
for p in sorted(matched, key=lambda x: x["name"]):
    print(f"  {p['name']:40s}  ({p.get('type', '')})  label: {p.get('label', '')}")

print(f"\n  ({len(matched)} matched of {len(props)} total company properties)")

print("\n=== DEAL PIPELINE STAGES ===")
r = requests.get(
    "https://api.hubapi.com/crm/v3/pipelines/deals",
    headers=headers,
    timeout=30,
)
r.raise_for_status()
for pipeline in r.json().get("results", []):
    print(f"\n  Pipeline: {pipeline.get('label')}  ({pipeline.get('id')})")
    for stage in pipeline.get("stages", []):
        won = " ← WON" if stage.get("metadata", {}).get("isClosed") == "true" and stage.get("metadata", {}).get("probability") == "1.0" else ""
        print(f"    {stage.get('id'):30s}  {stage.get('label')}{won}")

print("\nNext step: edit scripts/update_dealer_data.py to set COMPANY_GRADE_PROP, COMPANY_DISCOUNT_PROP, etc. to the names shown above.")
