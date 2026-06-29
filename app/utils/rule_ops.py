"""Safe operator dispatch for pest-rule conditions.

Replaces ``eval()`` on operator/value strings (a code-injection-shaped smell) with a
fixed operator map. Leaf module — imports only ``operator`` and ``pandas`` so it can
be imported from both ``risk_index`` and ``wdutils`` without an import cycle.
"""
from __future__ import annotations

import operator
from typing import Any, Iterable

import pandas as pd

OPERATORS = {
    ">": operator.gt,
    ">=": operator.ge,
    "<": operator.lt,
    "<=": operator.le,
    "==": operator.eq,
    "!=": operator.ne,
}


def condition_true(value: Any, symbol: str, threshold: Any) -> bool:
    """Evaluate a single ``value <symbol> threshold`` comparison safely.

    Unknown operator → KeyError (was a silently-eval'd string before)."""
    return bool(OPERATORS[symbol](value, threshold))


def rule_mask(df: pd.DataFrame, conditions: Iterable[Any]) -> pd.Series:
    """Boolean mask = AND of ``df[cond.unit.name] <op> cond.value`` over conditions.

    Mirrors the previous ``eval("lambda x: (x['unit'] op val) & ...")`` vectorised
    form. Empty conditions → all-True; unknown operator → KeyError.
    """
    mask = pd.Series(True, index=df.index)
    for cond in conditions:
        mask &= OPERATORS[cond.operator.symbol](df[cond.unit.name], cond.value)
    return mask
