from typing import List, Optional
from enum import Enum

from pydantic import BaseModel, ConfigDict, UUID4, Field, model_validator

from utils.threat_model_warnings import collect_warnings


class RiskLevel(str, Enum):
    low      = "low"
    moderate = "moderate"
    high     = "high"
    critical = "critical"


class FuzzyRule(BaseModel):
    hum_lo:     float = Field(0.0,   ge=0.0,   le=100.0)
    hum_hi:     float = Field(100.0, ge=0.0,   le=100.0)
    temp_lo:    float = Field(-999.0)
    temp_hi:    float = Field(999.0)
    rain_min:   float = Field(0.0,   ge=0.0)
    risk_level: RiskLevel
    type:       Optional[str] = None  # "fungal" | "insect" — informational

    @model_validator(mode="after")
    def hum_range_valid(self) -> "FuzzyRule":
        if self.hum_lo > self.hum_hi:
            raise ValueError("hum_lo must be <= hum_hi")
        return self

    @model_validator(mode="after")
    def temp_range_valid(self) -> "FuzzyRule":
        if self.temp_lo > self.temp_hi:
            raise ValueError("temp_lo must be <= temp_hi")
        return self


class BioParams(BaseModel):
    t_base:                     float           = 5.0
    t_lethal_min:               Optional[float] = None
    t_lethal_max:               Optional[float] = None
    t_optimal_min:              Optional[float] = None
    t_optimal_max:              Optional[float] = None
    min_streak:                 Optional[int]   = None
    pheno_frac_lo:              Optional[float] = None
    pheno_frac_hi:              Optional[float] = None
    pheno_frac_ref_gdd5:        Optional[float] = None
    pheno_lo:                   Optional[float] = None
    pheno_hi:                   Optional[float] = None
    min_wetness_hours_critical: Optional[float] = None
    min_wetness_hours_high:     Optional[float] = None


class ThreatModelDefinition(BaseModel):
    bio_params:  BioParams
    fuzzy_rules: List[FuzzyRule] = Field(..., min_length=1)


class ThreatModelCreate(BaseModel):
    scientific_name: str = Field(..., max_length=50)
    common_name:     str = Field(..., max_length=50)
    label:           Optional[str] = Field(None, max_length=50)
    note:            Optional[str] = Field(None, max_length=300)
    definition:      ThreatModelDefinition
    crop_id:         UUID4


class ThreatModelUpdate(BaseModel):
    scientific_name: Optional[str] = Field(None, max_length=50)
    common_name:     Optional[str] = Field(None, max_length=50)
    label:           Optional[str] = Field(None, max_length=50)
    note:            Optional[str] = Field(None, max_length=300)
    definition:      Optional[ThreatModelDefinition] = None


class ThreatModelDB(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id:              UUID4
    scientific_name: str
    common_name:     str
    label:           Optional[str] = None
    note:            Optional[str] = None
    definition:      dict
    crop_id:         UUID4
    warnings:        List[str] = []

    @model_validator(mode="after")
    def populate_warnings(self) -> "ThreatModelDB":
        if isinstance(self.definition, dict):
            self.warnings = collect_warnings(self.definition)
        return self
