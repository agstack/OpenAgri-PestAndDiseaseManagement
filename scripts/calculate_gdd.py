#!/usr/bin/env python3
"""
gdd_pipeline.py

Create a parcel (WKT) and a disease model, then calculate GDD for a date range
and (optionally) plot daily + accumulated GDD.

Requirements:
  pip install requests
  # only if you want plots:
  pip install matplotlib

Examples:
  python gdd_pipeline.py \
    --base-url https://pdm.test.horizon-openagri.eu \
    --token YOUR_BEARER_TOKEN \
    --parcel-name "Parcel 1" \
    --wkt "POLYGON((23.90788293506056 37.98810424577469,23.907381957300185 37.988277198315174,23.90688901661618 37.988336255186866,23.906776497547003 37.98810002741493,23.907880256035103 37.987969258142684,23.90788293506056 37.98810424577469))" \
    --disease-json ./colorado_potato_beetle.json \
    --from-date 2024-03-01 \
    --to-date 2024-09-30 \
    --plot
"""

import argparse
import json
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests


# -----------------------------
# HTTP helpers
# -----------------------------
def _headers(token: str) -> Dict[str, str]:
    """Build common headers for authenticated JSON requests."""
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


def _pretty(obj: Any, lim: int = 2000) -> str:
    """Pretty-print JSON with a safe length limit."""
    try:
        txt = json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        txt = str(obj)
    return (txt[:lim] + " ...") if len(txt) > lim else txt


# -----------------------------
# API calls
# -----------------------------
def create_parcel(base_url: str, token: str, name: str, wkt_polygon: str) -> Dict[str, Any]:
    """
    POST /api/v1/parcel/
    Body: { "name": "...", "wkt_polygon": "POLYGON((...))" }
    Returns the created parcel object (expects an 'id' field).
    """
    url = f"{base_url.rstrip('/')}/api/v1/parcel/wkt-format/"
    body = {"name": name, "wkt_polygon": wkt_polygon}
    resp = requests.post(url, headers=_headers(token), data=json.dumps(body), timeout=60)
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}
    if resp.status_code >= 400:
        raise RuntimeError(f"Parcel creation failed ({resp.status_code}): {_pretty(data)}")
    return data


def create_disease(base_url: str, token: str, disease_model: Dict[str, Any]) -> Dict[str, Any]:
    """
    POST /api/v1/disease/
    Body: disease_model dict from file.
    Returns the created disease model (expects an 'id' field).
    """
    url = f"{base_url.rstrip('/')}/api/v1/disease/"
    resp = requests.post(url, headers=_headers(token), data=json.dumps(disease_model), timeout=60)
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}
    if resp.status_code >= 400:
        raise RuntimeError(f"Disease creation failed ({resp.status_code}): {_pretty(data)}")
    return data


def calculate_gdd(
    base_url: str,
    token: str,
    parcel_id: str,
    model_ids: str,
    from_date: str,
    to_date: str,
) -> Dict[str, Any]:
    """
    GET /api/v1/tool/calculate-gdd/parcel/{parcel_id}/model/{model_ids}/verbose/{from_date}/from/{to_date}/to/
    Returns JSON with daily GDD/accumulated GDD series.
    """
    path = f"/api/v1/tool/calculate-gdd/parcel/{parcel_id}/model/{model_ids}/verbose/{from_date}/from/{to_date}/to/"
    url = f"{base_url.rstrip('/')}{path}"
    resp = requests.get(url, headers=_headers(token), timeout=120)
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}
    if resp.status_code >= 400:
        raise RuntimeError(f"GDD calculation failed ({resp.status_code}): {_pretty(data)}")
    return data


# -----------------------------
# Data extraction & plotting
# -----------------------------
def _parse_iso_date(s: str) -> datetime:
    """Parse a date string (YYYY-MM-DD or ISO 8601) to datetime (naive)."""
    # Accept 'YYYY-MM-DD' or more detailed ISO strings; keep it simple.
    try:
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            return datetime.strptime(s, "%Y-%m-%d")
        return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
    except Exception:
        # last resort: try basic YYYY-MM-DD
        return datetime.strptime(s[:10], "%Y-%m-%d")


def extract_gdd_series(gdd_json: dict) -> list[dict]:
    """
    Parse JSON-LD response like:

    {
      "@context": [...],
      "@graph": [[
        {
          "@type": "ObservationCollection",
          "hasMember": [
            {
              "@type": "Observation",
              "phenomenonTime": "YYYY-MM-DD",
              "hasResult": { "@type": "QuantityValue", "hasValue": "179", "unit": "..." },
              "descriptor": "Most effective time ..."
            },
            ...
          ]
        }
      ]]
    }

    Returns a list sorted by date with dicts:
      { "date": datetime, "gdd": float|None, "accumulated_gdd": float, "descriptor": str|None }

    Notes:
      - Daily GDD is derived as the day-to-day difference of accumulated GDD.
      - If a day is the first entry, daily GDD == its accumulated value.
      - Tolerant to nested @graph arrays and minor schema variations.
    """
    def _parse_iso_date(s: str) -> datetime:
        try:
            if len(s) == 10 and s[4] == "-" and s[7] == "-":
                return datetime.strptime(s, "%Y-%m-%d")
            return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
        except Exception:
            # Fallback to first 10 chars as YYYY-MM-DD if possible
            return datetime.strptime(s[:10], "%Y-%m-%d")

    # 1) Find the ObservationCollection node(s)
    graph = gdd_json.get("@graph", [])
    # JSON-LD toolchains sometimes nest arrays; flatten one level if needed
    # so @graph can be: [ {...} ] or [ [ {...} ] ].
    flat_graph = []
    for item in graph:
        if isinstance(item, list):
            flat_graph.extend(item)
        else:
            flat_graph.append(item)

    collections = []
    for node in flat_graph:
        if isinstance(node, dict):
            t = node.get("@type")
            # @type may be string or list
            if (isinstance(t, str) and t == "ObservationCollection") or \
               (isinstance(t, list) and "ObservationCollection" in t):
                collections.append(node)

    if not collections:
        return []

    # 2) Collect all Observation members
    observations = []
    for coll in collections:
        members = coll.get("hasMember", [])
        if isinstance(members, dict):
            members = [members]
        for obs in members:
            if not isinstance(obs, dict):
                continue
            # basic shape guard
            pt = obs.get("phenomenonTime")
            res = obs.get("hasResult", {})
            if isinstance(res, dict):
                val = res.get("hasValue")
            else:
                val = None
            descriptor = obs.get("descriptor")
            if pt is None or val is None:
                continue
            try:
                acc = float(val)
            except Exception:
                # skip non-numeric values
                continue
            observations.append({
                "date": _parse_iso_date(str(pt)),
                "accumulated_gdd": acc,
                "descriptor": descriptor
            })

    if not observations:
        return []

    # 3) Sort by date and derive daily GDD as differences
    observations.sort(key=lambda x: x["date"])
    series = []
    prev_acc = None
    for o in observations:
        acc = o["accumulated_gdd"]
        if prev_acc is None:
            daily = acc  # first entry: daily == accumulated
        else:
            daily = acc - prev_acc
        prev_acc = acc
        series.append({
            "date": o["date"],
            "gdd": daily,
            "accumulated_gdd": acc,
            "descriptor": o.get("descriptor")
        })

    print(series)
    return series



def plot_gdd(series: List[Dict[str, Any]], outfile: str = "gdd_plot.png") -> None:
    """Plot daily GDD and accumulated GDD on one chart (dates on X)."""
    if not series:
        print("No data to plot.")
        return

    import matplotlib.pyplot as plt  # imported lazily so script runs without matplotlib if --plot not used

    dates = [p["date"] for p in series]
    daily = [p["gdd"] for p in series]
    accum = [p["accumulated_gdd"] for p in series]

    plt.figure(figsize=(12, 6))
    if any(v is not None for v in daily):
        plt.plot(dates, daily, label="Daily GDD")
        plt.scatter(dates, daily, s=20)
        for d, y in zip(dates, daily):
            if y is None:
                continue
            plt.annotate(
                f"({d:%Y-%m-%d}, {y:.1f})",
                xy=(d, y),
                xytext=(4, 6),
                textcoords="offset points",
                fontsize=8,
                rotation=30,
            )
    if any(v is not None for v in accum):
        plt.plot(dates, accum, label="Accumulated GDD")
        plt.scatter(dates, accum, s=20)
        for d, y in zip(dates, accum):
            if y is None:
                continue
            plt.annotate(
                f"({d:%Y-%m-%d}, {y:.1f})",
                xy=(d, y),
                xytext=(4, -10),  # slight offset to reduce overlap with daily labels
                textcoords="offset points",
                fontsize=8,
                rotation=30,
            )
    plt.title("GDD & Accumulated GDD")
    plt.xlabel("Date")
    plt.ylabel("GDD")
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig(outfile, dpi=150)
    print(f"[saved] {outfile}")
    plt.close()


# -----------------------------
# Main
# -----------------------------
def main():
    parser = argparse.ArgumentParser(description="Upload parcel + disease model, then calculate and (optionally) plot GDD.")
    parser.add_argument("--base-url", required=True, help="Base URL, e.g. https://pdm.test.horizon-openagri.eu")
    parser.add_argument("--token", required=True, help="Bearer token for authentication")
    parser.add_argument("--parcel-name", required=True, help="Parcel name to create")
    parser.add_argument("--wkt", required=True, help="WKT POLYGON string for the parcel")
    parser.add_argument("--disease-json", required=True, help="Path to a JSON file with the disease model")
    parser.add_argument("--from-date", required=True, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--to-date", required=True, help="End date (YYYY-MM-DD)")
    parser.add_argument("--plot", action="store_true", help="Generate a plot of daily and accumulated GDD")
    args = parser.parse_args()

    # 1) Create parcel
    print("Creating parcel ...")
    parcel = create_parcel(args.base_url, args.token, args.parcel_name, args.wkt)
    print("Parcel response:", _pretty(parcel))
    parcel_id = str(parcel.get("id") or parcel.get("parcel_id") or parcel.get("uuid") or "")
    if not parcel_id:
        # raise RuntimeError("Could not determine parcel id from parcel response.")
        parcel_id = 2

    # 2) Create disease model from file
    print("\nCreating disease model ...")
    with open(args.disease_json, "r", encoding="utf-8") as f:
        disease_model = json.load(f)
    disease = create_disease(args.base_url, args.token, disease_model)
    print("Disease response:", _pretty(disease))
    disease_id = str(disease.get("id") or disease.get("model_id") or disease.get("uuid") or "")
    if not disease_id:
        raise RuntimeError("Could not determine disease model id from disease response.")

    # 3) Calculate GDD
    print("\nCalculating GDD ...")
    gdd_json = calculate_gdd(
        base_url=args.base_url,
        token=args.token,
        parcel_id=parcel_id,
        model_ids=disease_id,  # if multiple, provide comma-separated ids
        from_date=args.from_date,
        to_date=args.to_date,
    )
    print("GDD response (truncated):", _pretty(gdd_json))

    # 4) Extract and plot (optional)
    series = extract_gdd_series(gdd_json)
    if not series:
        print("No series extracted from GDD response.")
    elif args.plot:
        plot_gdd(series, outfile="gdd_plot.png")
    else:
        print("Plotting disabled (use --plot to enable).")

    print("\nDone.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
