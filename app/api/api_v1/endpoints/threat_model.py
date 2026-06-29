import json
import re
from typing import List, Optional
from uuid import UUID

import pandas as pd
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from sqlalchemy.orm import Session

import crud
from api import deps
from schemas.threat_model import (
    ThreatModelCreate, ThreatModelUpdate, ThreatModelDB, ThreatModelDefinition,
    BioParams, FuzzyRule,
)

router = APIRouter()

_RISK_SCORE_MAP = {
    "critical": 100, "high": 80, "moderate": 40, "medium": 40, "low": 10,
}


# ─── helpers shared with import logic ────────────────────────────────────────

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
        return (float(h), float(h))
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
        return (float(t2), float(t2))
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


# ─── CRUD endpoints ───────────────────────────────────────────────────────────

@router.get("/", response_model=List[ThreatModelDB], dependencies=[Depends(deps.get_jwt)])
def list_threat_models(
    crop_id: Optional[UUID] = None,
    db: Session = Depends(deps.get_db),
):
    if crop_id:
        return crud.threat_model.get_by_crop(db=db, crop_id=crop_id)
    return crud.threat_model.get_multi(db=db)


@router.post("/", response_model=ThreatModelDB, dependencies=[Depends(deps.get_jwt)])
def create_threat_model(
    tm_in: ThreatModelCreate,
    db: Session = Depends(deps.get_db),
):
    crop = crud.crop.get(db=db, id=tm_in.crop_id)
    if not crop:
        raise HTTPException(status_code=404, detail="Crop not found")
    return crud.threat_model.create(db=db, obj_in=tm_in)


@router.patch("/{tm_id}/", response_model=ThreatModelDB, dependencies=[Depends(deps.get_jwt)])
def update_threat_model(
    tm_id: UUID,
    tm_in: ThreatModelUpdate,
    db: Session = Depends(deps.get_db),
):
    obj = crud.threat_model.get(db=db, id=tm_id)
    if not obj:
        raise HTTPException(status_code=404, detail="ThreatModel not found")
    return crud.threat_model.update(db=db, db_obj=obj, obj_in=tm_in)


@router.delete("/{tm_id}/", response_model=ThreatModelDB, dependencies=[Depends(deps.get_jwt)])
def delete_threat_model(
    tm_id: UUID,
    db: Session = Depends(deps.get_db),
):
    obj = crud.threat_model.get(db=db, id=tm_id)
    if not obj:
        raise HTTPException(status_code=404, detail="ThreatModel not found")
    return crud.threat_model.remove(db=db, id=tm_id)


# ─── bulk import from Excel ───────────────────────────────────────────────────

@router.post("/import-excel/", response_model=dict, dependencies=[Depends(deps.get_jwt)])
def import_from_excel(
    file: UploadFile = File(...),
    db: Session = Depends(deps.get_db),
):
    """Bulk import fuzzy rules from db_crops_PDM.xlsx format.

    Creates Crop records (if not present) and ThreatModel records grouped by
    (crop, pest). Existing crops are matched by name; existing threat models
    matched by (scientific_name, crop_id) are skipped.
    """
    try:
        df = pd.read_excel(file.file)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Cannot read Excel: {e}")

    df.columns = ["crop", "pest", "humidity", "temp", "rainfall", "risk", "type"]
    df = df.dropna(subset=["crop"])

    from collections import defaultdict
    cp_rules: dict[tuple, list] = defaultdict(list)
    for _, row in df.iterrows():
        pest_str = str(row["pest"]).strip()
        pest_key = pest_str.split(",")[0].strip()
        risk_str = str(row["risk"]).strip().lower()
        hum_lo, hum_hi   = _parse_humidity(str(row["humidity"]))
        temp_lo, temp_hi = _parse_temp(str(row["temp"]))
        rain_min         = _parse_rainfall(str(row["rainfall"]))
        cp_rules[(str(row["crop"]).strip(), pest_key)].append({
            "hum_lo": hum_lo, "hum_hi": hum_hi,
            "temp_lo": temp_lo, "temp_hi": temp_hi,
            "rain_min": rain_min,
            "risk_level": risk_str,
            "type": str(row["type"]).strip() if not pd.isna(row["type"]) else None,
        })

    created_crops = 0
    created_tms   = 0

    for (crop_name, pest_key), rules in cp_rules.items():
        # find or create crop
        all_crops = crud.crop.get_multi(db=db, limit=10000)
        crop_obj  = next((c for c in all_crops if c.name == crop_name), None)
        if not crop_obj:
            from schemas.crop import CropCreate
            crop_obj = crud.crop.create(db=db, obj_in=CropCreate(name=crop_name))
            created_crops += 1

        # skip if threat model already exists
        existing = [
            tm for tm in crud.threat_model.get_by_crop(db=db, crop_id=crop_obj.id)
            if tm.scientific_name == pest_key
        ]
        if existing:
            continue

        fuzzy_rules = [FuzzyRule(**r) for r in rules]
        definition  = ThreatModelDefinition(bio_params=BioParams(), fuzzy_rules=fuzzy_rules)
        tm_in       = ThreatModelCreate(
            scientific_name=pest_key[:50],
            common_name=pest_key[:50],
            definition=definition,
            crop_id=crop_obj.id,
        )
        crud.threat_model.create(db=db, obj_in=tm_in)
        created_tms += 1

    return {"created_crops": created_crops, "created_threat_models": created_tms}


# ─── merge bio params from pest.json ─────────────────────────────────────────

@router.post("/import-json/", response_model=dict, dependencies=[Depends(deps.get_jwt)])
def import_from_json(
    file: UploadFile = File(...),
    db: Session = Depends(deps.get_db),
):
    """Merge bio_params from pest.json format into existing ThreatModel records.

    Matches on ThreatModel.label (= pest.json pest_key) or scientific_name.
    Only updates the bio_params portion of the definition; fuzzy_rules unchanged.
    Supports JSONC (// line comments).
    """
    try:
        raw = file.file.read().decode("utf-8")
        cleaned = re.sub(r"//[^\n]*", "", raw)
        data = json.loads(cleaned)
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Cannot parse JSON: {e}")

    updated = 0
    all_tms = crud.threat_model.get_multi(db=db, limit=10000)

    for crop_name, pests in data.items():
        if not isinstance(pests, dict):
            continue
        for pest_key, params in pests.items():
            if not isinstance(params, dict):
                continue
            bio = {
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
            bio = {k: v for k, v in bio.items() if v is not None}
            label = params.get("label", "")

            # Match by label or scientific_name
            match = next(
                (tm for tm in all_tms
                 if tm.label == pest_key or tm.scientific_name == pest_key),
                None,
            )
            if not match:
                continue

            new_def = dict(match.definition)
            new_def["bio_params"] = {**(new_def.get("bio_params") or {}), **bio}
            if label and not match.label:
                crud.threat_model.update(
                    db=db, db_obj=match,
                    obj_in={"definition": new_def, "label": label[:50], "common_name": label[:50]},
                )
            else:
                crud.threat_model.update(db=db, db_obj=match, obj_in={"definition": new_def})
            updated += 1

    return {"updated_threat_models": updated}
