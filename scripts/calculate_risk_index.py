#!/usr/bin/env python3
"""
risk_index_pipeline.py

End-to-end script that:
- (Optional) uploads a pest-model Excel to /api/v1/pest-model/upload-excel/
- GETs all pest models and selects the FIRST id
- GETs all parcels and selects the FIRST id (or creates one from WKT if none exist)
- Calls risk index:
    GET /api/v1/tool/calculate-risk-index/weather/{parcel_id}/model/{model_ids}/verbose/{from_date}/from/{to_date}/to/
- Parses JSON-LD response to a time series (date, risk_label, numeric risk_value)
- Plots risk vs date (x-axis MM-DD, all ticks shown, point annotations)

Requirements:
  pip install requests matplotlib
"""

import argparse
import json
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# ---------- Config / mappings ----------
# Map qualitative risk levels to numeric values for plotting.
RISK_MAP = {"low": 1, "medium": 2, "med": 2, "high": 3}


# ---------- Utilities ----------
def headers(token: str) -> Dict[str, str]:
    """Common JSON headers with Bearer token."""
    return {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
    }


def pretty(obj: Any, limit: int = 1200) -> str:
    """Safe pretty-printer for debugging/logging."""
    try:
        s = json.dumps(obj, indent=2, ensure_ascii=False)
    except Exception:
        s = str(obj)
    return (s[:limit] + " ...") if len(s) > limit else s


def parse_iso(s: str) -> datetime:
    """Parse ISO-like strings; fallback to YYYY-MM-DD."""
    try:
        if len(s) >= 19 and s[4] == "-" and s[7] == "-" and s[10] == "T":
            return datetime.fromisoformat(s.replace("Z", "+00:00")).replace(tzinfo=None)
        return datetime.strptime(s[:10], "%Y-%m-%d")
    except Exception:
        return datetime.strptime(s[:10], "%Y-%m-%d")


# ---------- API wrappers ----------
def upload_pest_models_excel(base_url: str, token: str, excel_path: str) -> Dict[str, Any]:
    """
    Upload pest-model Excel via multipart/form-data.
    POST /api/v1/pest-model/upload-excel/
    """
    url = f"{base_url.rstrip('/')}/api/v1/pest-model/upload-excel/"
    with open(excel_path, "rb") as f:
        files = {"file": (excel_path, f, "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")}
        resp = requests.post(url, headers={"Authorization": f"Bearer {token}", "Accept": "application/json"}, files=files, timeout=120)
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}
    if resp.status_code >= 400:
        raise RuntimeError(f"Upload Excel failed ({resp.status_code}): {pretty(data)}")
    return data


def list_pest_models(base_url: str, token: str) -> List[Dict[str, Any]]:
    """
    GET /api/v1/pest-model/
    Returns list of models.
    """
    url = f"{base_url.rstrip('/')}/api/v1/pest-model/"
    resp = requests.get(url, headers=headers(token), timeout=60)
    try:
        data = resp.json()
    except Exception:
        data = []
    if resp.status_code >= 400:
        raise RuntimeError(f"List pest models failed ({resp.status_code}): {pretty(data)}")
    if isinstance(data, dict):
        # some APIs wrap results
        data = data.get("pests", data.get("data", []))
        if not isinstance(data, list):
            data = []
    return data


def list_parcels(base_url: str, token: str) -> List[Dict[str, Any]]:
    """
    GET /api/v1/parcel/
    Returns list of parcels.
    """
    url = f"{base_url.rstrip('/')}/api/v1/parcel/"
    resp = requests.get(url, headers=headers(token), timeout=60)
    try:
        data = resp.json()
    except Exception:
        data = []
    if resp.status_code >= 400:
        raise RuntimeError(f"List parcels failed ({resp.status_code}): {pretty(data)}")
    if isinstance(data, dict):
        data = data.get("elements", data.get("data", []))
        if not isinstance(data, list):
            data = []
    return data


def create_parcel_from_wkt(base_url: str, token: str, name: str, wkt_polygon: str) -> Dict[str, Any]:
    """
    POST /api/v1/parcel/wkt-format/
    Body: {"name": "...", "wkt_polygon": "POLYGON((...))"}
    """
    url = f"{base_url.rstrip('/')}/api/v1/parcel/wkt-format/"
    body = {"name": name, "wkt_polygon": wkt_polygon}
    resp = requests.post(url, headers={**headers(token), "Content-Type": "application/json"}, data=json.dumps(body), timeout=60)
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}
    if resp.status_code >= 400:
        raise RuntimeError(f"Create parcel failed ({resp.status_code}): {pretty(data)}")
    return data


def calculate_risk_index(
    base_url: str,
    token: str,
    parcel_id: str,
    model_id: str,
    from_date: str,
    to_date: str,
) -> Dict[str, Any]:
    """
    GET /api/v1/tool/calculate-risk-index/weather/{parcel_id}/model/{model_ids}/verbose/{from_date}/from/{to_date}/to/
    """
    path = f"/api/v1/tool/calculate-risk-index/weather/{parcel_id}/model/{model_id}/verbose/{from_date}/from/{to_date}/to/"
    url = f"{base_url.rstrip('/')}{path}"
    resp = requests.get(url, headers={**headers(token), "Accept": "application/ld+json"}, timeout=120)
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}
    if resp.status_code >= 400:
        raise RuntimeError(f"Risk index calculation failed ({resp.status_code}): {pretty(data)}")
    return data


# ---------- JSON-LD parsing ----------
def extract_risk_series(jsonld: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    Extract a sorted list of {date: datetime, risk_label: str, risk_value: float}
    from JSON-LD ObservationCollection -> hasMember[] (phenomenonTime, hasSimpleResult).
    """
    graph = jsonld.get("@graph", [])

    # Normalize @graph (could be list of dicts or nested lists)
    flat_nodes: List[Dict[str, Any]] = []
    for item in graph:
        if isinstance(item, list):
            flat_nodes.extend([x for x in item if isinstance(x, dict)])
        elif isinstance(item, dict):
            flat_nodes.append(item)

    series: List[Dict[str, Any]] = []
    for node in flat_nodes:
        t = node.get("@type")
        if (isinstance(t, str) and t == "ObservationCollection") or (isinstance(t, list) and "ObservationCollection" in t):
            members = node.get("hasMember", [])
            if isinstance(members, dict):
                members = [members]
            for obs in members:
                if not isinstance(obs, dict):
                    continue
                ts = obs.get("phenomenonTime")
                label = obs.get("hasSimpleResult")
                if not ts or label is None:
                    continue
                dt = parse_iso(str(ts))
                label_norm = str(label).strip().lower()
                value = RISK_MAP.get(label_norm)
                if value is None:
                    # Unknown label; skip or map to 0 if you prefer
                    continue
                series.append({"date": dt, "risk_label": label_norm, "risk_value": float(value)})

    series.sort(key=lambda x: x["date"])
    return series


# ---------- Plotting ----------
def plot_risk_series(series: List[Dict[str, Any]], outfile: str = "risk_index.png") -> None:
    """
    Plot risk series:
      - x-axis: dates formatted as MM-DD
      - y-axis: numeric risk (1=Low, 2=Medium, 3=High)
      - show ALL date ticks and annotate every point "(MM-DD, value)"
    """
    if not series:
        print("No data to plot.")
        return

    dates = [p["date"] for p in series]
    vals = [p["risk_value"] for p in series]

    fig, ax = plt.subplots(figsize=(12, 6))
    ax.plot(dates, vals, label="Risk index (numeric)")
    ax.scatter(dates, vals, s=25)

    # Format X axis as MM-DD without year
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%m-%d'))
    # Show ALL ticks (can be dense for hourly data)
    # ax.set_xticks(dates)

    # Map y-ticks back to labels
    ax.set_yticks([1, 2, 3])
    ax.set_yticklabels(["Low", "Medium", "High"])

    ax.set_title("Pest Infection Risk Index")
    ax.set_xlabel("Date")
    ax.set_ylabel("Risk (Low=1, Medium=2, High=3)")
    ax.legend(loc="best")
    plt.xticks(rotation=45, ha='right', fontsize=8)
    fig.tight_layout()
    fig.savefig(outfile, dpi=150)
    print(f"[saved] {outfile}")
    plt.close(fig)


# ---------- Main flow ----------
def main():
    parser = argparse.ArgumentParser(description="Upload (optional) pest models, pick first model and parcel, calculate risk index, plot results.")
    parser.add_argument("--base-url", required=True, help="Service base URL, e.g., https://pdm.test.horizon-openagri.eu")
    parser.add_argument("--token", required=True, help="JWT Bearer token")
    parser.add_argument("--from-date", required=True, help="From date (YYYY-MM-DD or ISO-8601)")
    parser.add_argument("--to-date", required=True, help="To date (YYYY-MM-DD or ISO-8601)")
    parser.add_argument("--excel", help="Path to pest-model Excel to upload (optional)")
    parser.add_argument("--wkt", help="WKT polygon to create parcel if none exists (optional)")
    parser.add_argument("--parcel-name", default="Parcel 1", help="Parcel name when creating from WKT (optional)")
    parser.add_argument("--no-plot", action="store_true", help="Skip plotting")
    args = parser.parse_args()

    # (Optional) Upload pest models from Excel
    if args.excel:
        print(f"Uploading pest-model Excel: {args.excel}")
        up = upload_pest_models_excel(args.base_url, args.token, args.excel)
        print("Upload response:", pretty(up))

    # Get ALL pest models and choose the FIRST id
    models = list_pest_models(args.base_url, args.token)
    if not models:
        raise RuntimeError("No pest models available. Upload an Excel or ensure models exist.")
    model_id = str(models[0].get("id") or models[0].get("uuid") or models[0].get("model_id") or "")
    if not model_id:
        raise RuntimeError(f"Could not find a model id in first entry: {pretty(models[0])}")
    print(f"Using model id (first): {model_id}")

    # Get ALL parcels and choose the FIRST id (or create if none)
    parcels = list_parcels(args.base_url, args.token)
    if not parcels:
        if not args.wkt:
            raise RuntimeError("No parcels found. Provide --wkt to create one.")
        print("No parcels found. Creating a new parcel from WKT...")
        created = create_parcel_from_wkt(args.base_url, args.token, args.parcel_name, args.wkt)
        print("Parcel created:", pretty(created))
        parcel_id = str(created.get("id") or created.get("parcel_id") or created.get("uuid") or "")
        if not parcel_id:
            raise RuntimeError(f"Could not determine parcel id from create response: {pretty(created)}")
    else:
        parcel_id = str(parcels[0].get("id") or parcels[0].get("uuid") or parcels[0].get("parcel_id") or "")
        if not parcel_id:
            raise RuntimeError(f"Could not find parcel id in first entry: {pretty(parcels[0])}")
        print(f"Using parcel id (first): {parcel_id}")

    # Calculate risk index over the requested dates
    print("Calculating risk index...")
    risk_jsonld = calculate_risk_index(args.base_url, args.token, parcel_id, model_id, args.from_date, args.to_date)
    print("Risk response (truncated):", pretty(risk_jsonld))

    # Parse series and plot
    series = extract_risk_series(risk_jsonld)
    print(f"Parsed {len(series)} observations.")
    if not args.no_plot:
        plot_risk_series(series, outfile="risk_index_2.png")
    else:
        print("Plotting skipped (--no-plot).")

    print("Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)
