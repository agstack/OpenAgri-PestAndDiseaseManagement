from __future__ import annotations

import inspect

import pandas as pd
import pytest

from utils.rule_ops import OPERATORS, condition_true, rule_mask

from tests.fakes import FakeCondition, FakeOperator, FakeUnit


def _cond(unit: str, symbol: str, value) -> FakeCondition:
    return FakeCondition(FakeUnit(unit), FakeOperator(symbol), value)


def test_operators_cover_expected_symbols():
    assert set(OPERATORS) == {">", ">=", "<", "<=", "==", "!="}


def test_condition_true_scalar():
    assert condition_true(22.0, ">", 20.0) is True
    assert condition_true(18.0, ">", 20.0) is False
    assert condition_true(20.0, ">=", 20.0) is True


def test_condition_true_unknown_operator_raises():
    with pytest.raises(KeyError):
        condition_true(1, "~", 2)


def test_rule_mask_reproduces_old_eval():
    # equivalent to the old eval("lambda x: (x['temp'] > 20) & (x['rh'] >= 80)")
    df = pd.DataFrame({
        "atmospheric_temperature": [10.0, 25.0, 30.0],
        "atmospheric_relative_humidity": [50.0, 85.0, 95.0],
    })
    conds = [
        _cond("atmospheric_temperature", ">", 20.0),
        _cond("atmospheric_relative_humidity", ">=", 80.0),
    ]
    mask = rule_mask(df, conds)
    expected = (df["atmospheric_temperature"] > 20.0) & (
        df["atmospheric_relative_humidity"] >= 80.0
    )
    assert list(mask) == list(expected) == [False, True, True]


def test_rule_mask_empty_conditions_all_true():
    df = pd.DataFrame({"x": [1, 2, 3]})
    assert list(rule_mask(df, [])) == [True, True, True]


def test_rule_mask_unknown_operator_raises():
    df = pd.DataFrame({"atmospheric_temperature": [10.0]})
    with pytest.raises(KeyError):
        rule_mask(df, [_cond("atmospheric_temperature", "~", 5)])


def test_no_eval_in_rule_index_and_wdutils():
    """The flag-independent security win: the inline rule code is eval-free."""
    import utils.risk_index as ri
    import utils.wdutils as wd

    assert "eval(" not in inspect.getsource(ri)
    assert "eval(" not in inspect.getsource(wd)
