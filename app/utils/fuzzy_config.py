"""
Fuzzy risk engine constants.
GDD_RESET_MONTH lives in core.config.Settings so it is env-configurable.
"""

# Fuzzy membership function edge softness
FUZZY_TRANSITION_FRACTION = 0.15
# Minimum activation to count a rule
FUZZY_MIN_MU = 0.01

# Risk classification thresholds (score 0–100)
#  0-29   Low       unfavourable, no action
# 30-64   Moderate  partially favourable, increase monitoring
# 65-87   High      strongly favourable, intervention recommended
# 88-100  Critical  all factors optimal, immediate action required
RISK_THRESHOLD_CRITICAL = 88
RISK_THRESHOLD_HIGH     = 65
RISK_THRESHOLD_MODERATE = 30

# Minimum score factor applied when streak requirement not met
STREAK_MIN_FACTOR = 0.30

# Global phenological proxy T_base (pest-specific overrides come from bio_params.t_base)
PHENOLOGY_T_BASE = 5.0

# Fraction of phenological window used as soft transition zone at each edge
PHENO_FUZZY_MARGIN_FRAC = 0.12

# Leaf wetness estimation — RH contribution
# Based on Pedro & Gillespie (1982); Sentelhas et al. (2008)
WETNESS_HOURS_BY_RH = [
    (95, 20),
    (85, 14),
    (75,  8),
    (65,  4),
    ( 0,  0),
]

# Leaf wetness estimation — rainfall contribution (additive, capped at 24 h)
WETNESS_HOURS_BY_RAIN = [
    (5.0, 6),
    (1.0, 4),
    (0.1, 2),
    (0.0, 0),
]
