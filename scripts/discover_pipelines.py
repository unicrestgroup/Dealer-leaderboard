#!/usr/bin/env python3
"""
List all HubSpot deal pipelines and their stages, so you can confirm the
script will find 'Confirmed Sales Orders' and correctly identify Refunded/Credited.

Usage:
  HUBSPOT_TOKEN=xxx python3 scripts/discover_pipelines.py
"""
import os
import sys
import requests

token = os.environ.get("HUBSPOT_TOKEN")
if not token:
    sys.exit("Set HUBSPOT_TOKEN first.")

headers = {"Authorization": f"Bearer {token}"}

r = requests.get("https://api.hubapi.com/crm/v3/pipelines/deals", headers=headers, timeout=30)
r.raise_for_status()
pipelines = r.json().get("results", [])

print(f"\nFound {len(pipelines)} deal pipeline(s):\n")
for p in pipelines:
    label = p.get("label")
    pid = p.get("id")
    print(f"  Pipeline: {label!r}  (id: {pid})")
    for stage in p.get("stages", []):
        sid = stage.get("id")
        slabel = stage.get("label", "")
        flag = ""
        if "refunded" in slabel.lower() or "credited" in slabel.lower():
            flag = "  ← would be EXCLUDED"
        print(f"    {sid:30s}  {slabel}{flag}")
    print()
