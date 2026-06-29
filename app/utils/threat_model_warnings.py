from typing import List

_OPTIONAL_BIO_KEYS = [
    "t_lethal_min", "t_lethal_max",
    "t_optimal_min", "t_optimal_max",
    "min_streak",
    "pheno_frac_lo", "pheno_frac_hi", "pheno_frac_ref_gdd5",
    "pheno_lo", "pheno_hi",
    "min_wetness_hours_critical", "min_wetness_hours_high",
]


def collect_warnings(definition: dict) -> List[str]:
    warns: List[str] = []
    rules = definition.get("fuzzy_rules", []) if isinstance(definition, dict) else []
    bio   = (definition.get("bio_params") or {}) if isinstance(definition, dict) else {}

    def _trivial(r: dict) -> bool:
        return (
            r.get("hum_lo",  0.0)    == 0.0
            and r.get("hum_hi",  100.0) == 100.0
            and r.get("temp_lo", -999.0) == -999.0
            and r.get("temp_hi",  999.0) == 999.0
        )

    if rules and all(_trivial(r) for r in rules):
        warns.append(
            "All fuzzy rules match any weather condition — "
            "model returns identical risk regardless of temperature or humidity"
        )

    if all(bio.get(k) is None for k in _OPTIONAL_BIO_KEYS):
        warns.append(
            "No biological parameters set — "
            "lethal temperature, phenology, and wetness gating all disabled"
        )

    return warns
