from __future__ import annotations

import pandas as pd

from utils.custom_schemas import context as EXPECTED_CONTEXT
from utils.fuzzy_risk import _format_results

from tests.fakes import FakeParcel


def _results_df() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "date": pd.Timestamp("2026-05-01"),
                "scientific_name": "Plasmopara viticola",
                "common_name": "Downy mildew",
                "risk_score": 72.5,
                "risk_class": "High",
                "detail": "",
            },
            {
                "date": pd.Timestamp("2026-05-02"),
                "scientific_name": "Plasmopara viticola",
                "common_name": "Downy mildew",
                "risk_score": 12.0,
                "risk_class": "Low",
                "detail": "no active rules",
            },
        ]
    )


def _normalize(obj):
    if isinstance(obj, dict):
        return {
            k: ("<TS>" if k == "resultTime" else _normalize(v)) for k, v in obj.items()
        }
    if isinstance(obj, list):
        return [_normalize(x) for x in obj]
    if isinstance(obj, str) and obj.startswith("urn:openagri:fuzzyRisk:"):
        return obj.rsplit(":", 1)[0] + ":<ID>"
    return obj


def test_jsonld_envelope_snapshot():
    envelope = _format_results(
        _results_df(), FakeParcel(latitude=45.1, longitude=12.3), "json-ld"
    )

    assert _normalize(envelope) == {
        "@context": EXPECTED_CONTEXT,
        "@graph": [
            {
                "@id": "urn:openagri:fuzzyRisk:col:<ID>",
                "@type": ["ObservationCollection"],
                "description": "Fuzzy risk for Plasmopara viticola (Downy mildew)",
                "observedProperty": {
                    "@id": "urn:openagri:fuzzyRisk:op:<ID>",
                    "@type": ["ObservableProperty", "PestInfection"],
                    "name": "Plasmopara viticola",
                    "commonName": "Downy mildew",
                },
                "madeBySensor": {
                    "@id": "urn:openagri:fuzzyRisk:model:<ID>",
                    "@type": ["Sensor", "FuzzyRiskModel"],
                    "name": "Fuzzy Pest & Disease Risk Model v2.0",
                },
                "hasFeatureOfInterest": {
                    "@id": "urn:openagri:fuzzyRisk:foi:<ID>",
                    "@type": ["FeatureOfInterest", "Point"],
                    "long": "12.3",
                    "lat": "45.1",
                },
                "resultTime": "<TS>",
                "hasMember": [
                    {
                        "@id": "urn:openagri:fuzzyRisk:obs:<ID>",
                        "@type": ["Observation", "PestInfestationRisk"],
                        "phenomenonTime": "2026-05-01",
                        "hasSimpleResult": "72.5",
                        "riskClass": "High",
                    },
                    {
                        "@id": "urn:openagri:fuzzyRisk:obs:<ID>",
                        "@type": ["Observation", "PestInfestationRisk"],
                        "phenomenonTime": "2026-05-02",
                        "hasSimpleResult": "12.0",
                        "riskClass": "Low",
                        "meta": "no active rules",
                    },
                ],
            }
        ],
    }


def test_json_records_format():
    records = _format_results(_results_df(), FakeParcel(45.1, 12.3), "json")
    assert isinstance(records, list) and len(records) == 2
    assert records[0]["date"] == "2026-05-01"
    assert set(records[0]) == {
        "date",
        "scientific_name",
        "common_name",
        "risk_score",
        "risk_class",
        "detail",
    }
