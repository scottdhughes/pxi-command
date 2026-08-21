#!/usr/bin/env python3
"""Download the authenticated immutable D1 research snapshot."""

from __future__ import annotations

import argparse
import json
import os
import urllib.request
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--url", default="https://api.pxicommand.com/api/export/research-snapshot?limit=2000")
    parser.add_argument("--out", default="research/out/d1_snapshot.json")
    args = parser.parse_args()

    api_key = os.environ.get("WRITE_API_KEY")
    if not api_key:
        raise SystemExit("WRITE_API_KEY is required")

    request = urllib.request.Request(args.url, headers={"Authorization": f"Bearer {api_key}"})
    with urllib.request.urlopen(request, timeout=60) as response:
        payload = json.load(response)

    if payload.get("point_in_time_guarantee") is not True:
        raise SystemExit("export did not assert its point-in-time contract")

    output_path = Path(args.out)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote {len(payload.get('rows', []))} immutable rows to {output_path}")


if __name__ == "__main__":
    main()
