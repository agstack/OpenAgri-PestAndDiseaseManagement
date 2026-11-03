"""Add crop and threat_model tables, seed from xlsx/json

Revision ID: a1b2c3d4e5f6
Revises: 9949dfb9da1e
Create Date: 2026-04-23 09:00:00.000000
"""

from __future__ import annotations

import json
import re
import uuid
from pathlib import Path
from typing import Sequence, Union

import pandas as pd
import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB

# revision identifiers
revision: str = "a1b2c3d4e5f6"
down_revision: Union[str, None] = "9949dfb9da1e"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None

# Seed data paths (relative to project root where alembic runs)
_XLSX = Path("data/db_crops_PDM.xlsx")
_JSON = Path("data/pest.json")

# ─── risk score mapping (mirrors fuzzy_risk._RISK_SCORE_MAP) ─────────────────
_RISK_SCORE_MAP = {
    "critical": 100, "high": 80, "moderate": 40, "medium": 40, "low": 10,
}


# ─── xlsx parsing helpers (ported from other/risk_model.py) ──────────────────

def _parse_humidity(raw: str) -> tuple[float, float]:
    h = str(raw).strip().replace("%", "").replace(" ", "")
    if not h or h.lower() == "whatever":
        return (0.0, 100.0)
    if h.startswith(">="):
        return (float(h[2:]), 100.0)
    if h.startswith(">"):
        return (float(h[1:]), 100.0)
    if h.startswith("<="):
        return (0.0, float(h[2:]))
    if h.startswith("<"):
        return (0.0, float(h[1:]))
    if "-" in h:
        parts = h.split("-")
        if len(parts) == 2:
            try:
                return (float(parts[0]), float(parts[1]))
            except ValueError:
                pass
    try:
        v = float(h)
        return (v, v)
    except ValueError:
        return (0.0, 100.0)


def _parse_temp(raw: str) -> tuple[float, float]:
    raw_clean = str(raw).strip().replace(" ", "")
    if not raw_clean or raw_clean.lower() == "whatever":
        return (-999.0, 999.0)
    t = raw_clean.upper()
    m = re.match(r"^([-\d.]+)\s*[<>]=?\s*T\s*[<>]=?\s*([-\d.]+)$", t, re.IGNORECASE)
    if m:
        return (float(m.group(1)), float(m.group(2)))
    t2 = t.replace("T", "").strip()
    if t2.startswith(">="):
        return (float(t2[2:]), 999.0)
    if t2.startswith(">"):
        return (float(t2[1:]), 999.0)
    if t2.startswith("<="):
        return (-999.0, float(t2[2:]))
    if t2.startswith("<"):
        return (-999.0, float(t2[1:]))
    try:
        v = float(t2)
        return (v, v)
    except ValueError:
        return (-999.0, 999.0)


def _parse_rainfall(raw: str) -> float:
    r = str(raw).strip().lower()
    if not r or r in ("whatever", "nan"):
        return 0.0
    r = r.replace("mm", "").replace("<", "").replace(">", "").replace("=", "").strip()
    try:
        return max(0.0, float(r))
    except ValueError:
        return 0.0


def _load_xlsx_rules() -> list[dict]:
    df = pd.read_excel(_XLSX)
    df.columns = ["crop", "pest", "humidity", "temp", "rainfall", "risk", "type"]
    rules = []
    for _, row in df.iterrows():
        if pd.isna(row["crop"]):
            continue
        pest_str = str(row["pest"]).strip()
        pest_key = pest_str.split(",")[0].strip()
        risk_str = str(row["risk"]).strip()
        hum_lo, hum_hi   = _parse_humidity(str(row["humidity"]))
        temp_lo, temp_hi = _parse_temp(str(row["temp"]))
        rain_min         = _parse_rainfall(str(row["rainfall"]))
        rules.append({
            "crop":       str(row["crop"]).strip(),
            "pest_key":   pest_key,
            "type":       str(row["type"]).strip() if not pd.isna(row["type"]) else None,
            "hum_lo":     hum_lo,
            "hum_hi":     hum_hi,
            "temp_lo":    temp_lo,
            "temp_hi":    temp_hi,
            "rain_min":   rain_min,
            "risk_level": risk_str.lower(),
        })
    return rules


def _load_pest_json() -> dict:
    """Load pest.json, stripping // line comments (JSONC format)."""
    raw = _JSON.read_text(encoding="utf-8")
    cleaned = re.sub(r"//[^\n]*", "", raw)
    data = json.loads(cleaned)
    result = {}
    for crop, pests in data.items():
        if not isinstance(pests, dict):
            continue
        for pest_key, params in pests.items():
            if not isinstance(params, dict):
                continue
            result[(crop, pest_key)] = {
                "label":           params.get("label"),
                "t_base":          params.get("t_base"),
                "t_lethal_min":    params.get("t_lethal_min"),
                "t_lethal_max":    params.get("t_lethal_max"),
                "t_optimal_min":   params.get("t_optimal_min"),
                "t_optimal_max":   params.get("t_optimal_max"),
                "min_streak":      params.get("min_streak"),
                "pheno_lo":        (params.get("phenology_window_gdd") or [None, None])[0],
                "pheno_hi":        (params.get("phenology_window_gdd") or [None, None])[1],
                "pheno_frac_lo":   (params.get("pheno_fraction") or [None, None])[0],
                "pheno_frac_hi":   (params.get("pheno_fraction") or [None, None])[1],
                "pheno_frac_ref_gdd5": params.get("pheno_fraction_ref_gdd5"),
                "min_wetness_hours_critical": params.get("min_wetness_hours_critical"),
                "min_wetness_hours_high":     params.get("min_wetness_hours_high"),
            }
    return result


def _build_seed_data() -> tuple[list[dict], list[dict]]:
    """Return (crop_rows, threat_model_rows) for seeding."""
    rules     = _load_xlsx_rules()
    pest_conf = _load_pest_json()

    # Group rules by (crop, pest_key)
    from collections import defaultdict
    cp_rules: dict[tuple, list] = defaultdict(list)
    for r in rules:
        cp_rules[(r["crop"], r["pest_key"])].append(r)

    # Collect unique crops
    crops_seen: dict[str, uuid.UUID] = {}
    crop_rows = []
    for crop_name in dict.fromkeys(r["crop"] for r in rules):
        cid = uuid.uuid4()
        crops_seen[crop_name] = cid
        crop_rows.append({"id": cid, "name": crop_name, "description": None})

    # Build threat_model rows
    threat_model_rows = []
    for (crop_name, pest_key), group in cp_rules.items():
        crop_id  = crops_seen[crop_name]
        bio      = pest_conf.get((crop_name, pest_key), {})
        label    = bio.get("label")
        common   = label or pest_key

        bio_params = {
            k: bio.get(k)
            for k in (
                "t_base", "t_lethal_min", "t_lethal_max",
                "t_optimal_min", "t_optimal_max", "min_streak",
                "pheno_lo", "pheno_hi",
                "pheno_frac_lo", "pheno_frac_hi", "pheno_frac_ref_gdd5",
                "min_wetness_hours_critical", "min_wetness_hours_high",
            )
        }
        # Drop None values so bio_params stays clean
        bio_params = {k: v for k, v in bio_params.items() if v is not None}

        fuzzy_rules = [
            {
                "hum_lo":     r["hum_lo"],
                "hum_hi":     r["hum_hi"],
                "temp_lo":    r["temp_lo"],
                "temp_hi":    r["temp_hi"],
                "rain_min":   r["rain_min"],
                "risk_level": r["risk_level"],
                "type":       r["type"],
            }
            for r in group
        ]

        definition = {"bio_params": bio_params, "fuzzy_rules": fuzzy_rules}

        threat_model_rows.append({
            "id":              uuid.uuid4(),
            "scientific_name": pest_key[:50],
            "common_name":     common[:50],
            "label":           (label or "")[:50] or None,
            "note":            None,
            "definition":      json.dumps(definition),
            "crop_id":         crop_id,
        })

    return crop_rows, threat_model_rows


# ─── migration ────────────────────────────────────────────────────────────────

def upgrade() -> None:
    op.create_table(
        "crop",
        sa.Column("id",          sa.UUID(),         nullable=False),
        sa.Column("name",        sa.String(100),    nullable=False),
        sa.Column("description", sa.String(500),    nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_table(
        "threat_model",
        sa.Column("id",              sa.UUID(),      nullable=False),
        sa.Column("scientific_name", sa.String(50),  nullable=False),
        sa.Column("common_name",     sa.String(50),  nullable=False),
        sa.Column("label",           sa.String(50),  nullable=True),
        sa.Column("note",            sa.String(300), nullable=True),
        sa.Column("definition",      JSONB(),        nullable=False),
        sa.Column("crop_id",         sa.UUID(),      nullable=False),
        sa.ForeignKeyConstraint(["crop_id"], ["crop.id"], ondelete="CASCADE"),
        sa.PrimaryKeyConstraint("id"),
    )

    # Seed from xlsx + json
    crop_rows, threat_model_rows = _build_seed_data()

    conn = op.get_bind()

    if crop_rows:
        conn.execute(
            sa.text(
                "INSERT INTO crop (id, name, description) "
                "VALUES (:id, :name, :description)"
            ),
            crop_rows,
        )

    if threat_model_rows:
        conn.execute(
            sa.text(
                "INSERT INTO threat_model "
                "(id, scientific_name, common_name, label, note, definition, crop_id) "
                "VALUES (:id, :scientific_name, :common_name, :label, :note, "
                "        CAST(:definition AS jsonb), :crop_id)"
            ),
            threat_model_rows,
        )


def downgrade() -> None:
    op.drop_table("threat_model")
    op.drop_table("crop")
