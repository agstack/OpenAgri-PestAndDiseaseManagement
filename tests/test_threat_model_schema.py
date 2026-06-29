from __future__ import annotations

from uuid import uuid4

import pytest
from pydantic import ValidationError

# schemas.threat_model -> utils.threat_model_warnings pulls utils/__init__, which
# (via risk_index -> crud -> crud_threat_model) imports schemas.threat_model again.
# Importing the package first lets that pre-existing load-order cycle resolve.
import utils  # noqa: F401,E402
from schemas.threat_model import ThreatModelCreate, ThreatModelDB, ThreatType


def _definition() -> dict:
    return {
        "bio_params": {"t_base": 5.0},
        "fuzzy_rules": [
            {"hum_lo": 80, "hum_hi": 100, "temp_lo": 10, "temp_hi": 30,
             "rain_min": 0.0, "risk_level": "high", "type": "fungal"}
        ],
    }


def test_create_accepts_valid_threat_type():
    tm = ThreatModelCreate(
        scientific_name="X", common_name="Y", threat_type="bacterium",
        definition=_definition(), crop_id=uuid4(),
    )
    assert tm.threat_type is ThreatType.bacterium


def test_create_rejects_unknown_threat_type():
    with pytest.raises(ValidationError):
        ThreatModelCreate(
            scientific_name="X", common_name="Y", threat_type="virus",
            definition=_definition(), crop_id=uuid4(),
        )


def test_create_threat_type_optional():
    tm = ThreatModelCreate(
        scientific_name="X", common_name="Y",
        definition=_definition(), crop_id=uuid4(),
    )
    assert tm.threat_type is None


def test_db_surfaces_threat_type_from_orm_attrs():
    class _Orm:
        id = uuid4()
        scientific_name = "X"
        common_name = "Y"
        label = None
        note = None
        threat_type = "bacterium"
        definition = _definition()
        crop_id = uuid4()

    db = ThreatModelDB.model_validate(_Orm())
    assert db.threat_type == "bacterium"
