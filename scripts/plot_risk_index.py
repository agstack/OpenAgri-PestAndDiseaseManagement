#!/usr/bin/env python3
"""
risk_plot.py
Parse risk-index JSON-LD and plot (date, risk) with point labels.
"""

import json
from datetime import datetime
from typing import List, Dict, Any

# Map qualitative risk to numeric values for plotting.
# Adjust as needed.
RISK_MAP = {
    "low": 1,
    "medium": 2,
    "med": 2,
    "high": 3
}

def parse_iso(s: str) -> datetime:
    """Parse ISO-like date strings; fall back to YYYY-MM-DD."""
    try:
        if len(s) >= 19 and s[4] == "-" and s[7] == "-" and s[10] == "T":
            return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except Exception:
        return datetime.strptime(s[:10], "%Y-%m-%d")

def extract_risk_series(jsonld: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    From JSON-LD:
      @graph -> ObservationCollection -> hasMember[] of Observation
        - phenomenonTime: string date
        - hasSimpleResult: string risk label
    Returns a sorted list of {date: datetime, risk_label: str, risk_value: float}
    """
    graph = jsonld.get("@graph", [])
    # Some JSON-LD serializers may wrap/flatten differently; normalize to list of dicts.
    flat = []
    for item in graph:
        if isinstance(item, list):
            flat.extend(item)
        else:
            flat.append(item)

    series: List[Dict[str, Any]] = []
    for node in flat:
        if not isinstance(node, dict):
            continue
        t = node.get("@type")
        if (isinstance(t, str) and t == "ObservationCollection") or (isinstance(t, list) and "ObservationCollection" in t):
            members = node.get("hasMember", [])
            if isinstance(members, dict):
                members = [members]
            for obs in members:
                if not isinstance(obs, dict):
                    continue
                dt_s = obs.get("phenomenonTime")
                label = obs.get("hasSimpleResult")
                if not dt_s or label is None:
                    continue
                dt = parse_iso(str(dt_s))
                label_norm = str(label).strip().lower()
                value = RISK_MAP.get(label_norm)
                # Skip unknown labels; or set a default (e.g., 0)
                if value is None:
                    continue
                series.append({"date": dt, "risk_label": label_norm, "risk_value": float(value)})

    series.sort(key=lambda x: x["date"])
    return series

def plot_series(series: List[Dict[str, Any]], outfile: str = "risk_index.png"):
    import matplotlib.pyplot as plt  # lazy import to keep script light if plotting is not needed
    if not series:
        print("No data to plot.")
        return

    dates = [p["date"] for p in series]
    vals  = [p["risk_value"] for p in series]

    plt.figure(figsize=(12, 6))
    plt.plot(dates, vals, label="Risk index (numeric)", marker="o")
    plt.yticks([1, 2, 3], ["Low", "Medium", "High"])
    plt.title("Pest Infection Risk Index")
    plt.xlabel("Date/time")
    plt.ylabel("Risk (Low=1, Medium=2, High=3)")
    plt.legend(loc="best")
    plt.tight_layout()
    plt.savefig(outfile, dpi=150)
    print(f"[saved] {outfile}")
    plt.close()

if __name__ == "__main__":
    # Example usage:
    #   python risk_plot.py < response.json
    # or read from a file:
    #   python risk_plot.py response.json
    import sys, os
    if len(sys.argv) > 1 and os.path.exists(sys.argv[1]):
        with open(sys.argv[1], "r", encoding="utf-8") as f:
            data = json.load(f)
    else:
        data = json.load(sys.stdin)
    series = extract_risk_series(data)
    print(f"Parsed {len(series)} points.")
    plot_series(series, outfile="risk_index.png")
