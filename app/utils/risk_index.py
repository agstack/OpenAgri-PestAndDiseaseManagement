import datetime
from typing import List, Optional

from pandas import DataFrame
from sqlalchemy.orm import Session
import pandas as pd

import crud
import utils
import uuid

from models import PestModel, Parcel
from .wdutils import openmeteo_friendly_variables
from .rule_ops import condition_true, rule_mask
from core.config import settings

prob_values = {
    "low": 1,
    "moderate": 2,
    "high": 3
}

def calculate_risk_index_probability(db: Session, parcel: Parcel, pest_models: List[PestModel],
                                     from_date: datetime.date, to_date:datetime.date,
                                     parameter: Optional[str] = None):
    # SQL query for the data
    data_db = crud.data.get_data_query_by_parcel_id_and_date_interval(db=db, parcel_id=parcel.id,
                                                                      date_from=from_date, date_to=to_date)

    df = pd.read_sql(sql=data_db.statement, con=db.bind, parse_dates={"date": "%Y-%m-%d"})

    # Calculate the risks associated with each pest_model
    for pm in pest_models:
        risks_for_current_pm = ["Low"] * df.shape[0]

        for rule in pm.rules:
            mask = rule_mask(df, rule.conditions)

            # With this, only one rule should only ever fire for one singular date/time weather data point.
            # If multiple rules turn up as valid (both are true), then the last one from the db is going to have its
            # prob. value written as a final response for this pest model
            risks_for_current_pm = [rule.probability_value if x else y for x, y in zip(mask, risks_for_current_pm)]

        df["{}".format(pm.name)] = risks_for_current_pm

    context = utils.context

    graph = []

    for pm in pest_models:

        calculated_risks = []
        for date, time, risk in zip(df["date"], df["time"], df["{}".format(pm.name)]):

            if parameter and risk != parameter:
                continue

            calculated_risks.append(
                {
                    "@id": "urn:openagri:pestInfectationRisk:obs2:{}".format(uuid.uuid4()),
                    "@type": ["Observation", "PestInfestationRisk"],
                    "phenomenonTime": "{}".format(str(date).split(" ")[0] + "T" + str(time)),
                    "hasSimpleResult": "{}".format(risk)
                }
            )

        graph_element = {
            "@id": "urn:openagri:pestInfectationRisk:{}".format(uuid.uuid4()),
            "@type": ["ObservationCollection"],
            "description": "{} pest infectation risk forecast in x ".format(pm.name),
            "observedProperty": {
                "@id": "urn:openagri:pestInfectationRisk:op:{}".format(uuid.uuid4()),
                "@type": ["ObservableProperty", "PestInfection"],
                "name": "UNCINE pest infection",
                "hasAgriPest": {
                    "@id": "urn:openagri:pest:UNCINE",
                    "@type": "AgriPest",
                    "name": "UNCINE",
                    "description": "Uncinula necator (syn. Erysiphe necator) is a fungus that causes powdery mildew of grape. It is a common pathogen of Vitis species, including the wine grape, Vitis vinifera",
                    "eppoConcept": "https://gd.eppo.int/taxon/UNCINE"
                }
            },
            "madeBySensor": {
                "@id": "urn:openagri:pestInfectationRisk:model:{}".format(uuid.uuid4()),
                "@type": ["Sensor", "AIPestDetectionModel"],
                "name": "AI pest detaction model xyz"
            },
            "hasFeatureOfInterest": {
                "@id": "urn:openagri:pestInfectationRisk:foi:{}".format(uuid.uuid4()),
                "@type": ["FeatureOfInterest", "Point"],
                "long": "{}".format(parcel.longitude),
                "lat": "{}".format(parcel.latitude)
            },
            "basedOnWeatherDataset": {
                "@id": "urn:openagri:weatherDataset:{}".format(parcel.id),
                "@type": "WeatherDataset",
                "name": "parcel_name_tba"
            },
            "resultTime": "{}".format(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")),
            "hasMember": calculated_risks
        }

        graph.append(graph_element)

    doc = {
        "@context": context,
        "@graph": graph
    }

    return doc

def calculate_risk_index_probability_wd(
        parcel: dict,
        pest_models: List[PestModel],
        weather_data: dict,
        lat: float,
        lon: float,
        parameter: Optional[str] = None
):
    if settings.USE_AGSTACK_PND:
        return _calculate_risk_index_probability_wd_agstack(
            parcel, pest_models, weather_data, lat, lon, parameter
        )

    graph = []

    for pm in pest_models:
        calculated_risks = []

        for hour in weather_data["data"]:

            current_rule_risk_index = "low"
            for rule in pm.rules:

                # Filter, if parameter set to "high", skip low and medium ones
                if parameter and prob_values.get(rule.probability_value, 0) < prob_values.get(parameter, 0):
                    continue

                # Skip these since their calculation wouldn't meaningfully add to the current model limits
                if rule.probability_value and prob_values.get(rule.probability_value, 0) <= prob_values.get(current_rule_risk_index, 0):
                    continue

                current_rule_applies = True
                for cond in rule.conditions:
                    condition_applies = condition_true(hour["values"][openmeteo_friendly_variables[cond.unit.name]], cond.operator.symbol, cond.value)
                    if not condition_applies:
                        current_rule_applies = False
                        break

                if current_rule_applies:
                    current_rule_risk_index = rule.probability_value

            if not parameter:
                calculated_risks.append(
                    {
                        "@id": "urn:openagri:pestInfectationRisk:obs2:{}".format(uuid.uuid4()),
                        "@type": ["Observation", "PestInfestationRisk"],
                        "phenomenonTime": "{}".format(hour["timestamp"]),
                        "hasSimpleResult": "{}".format(current_rule_risk_index)
                    }
                )
                continue

            if parameter and prob_values.get(current_rule_risk_index, 0) >= prob_values.get(parameter, 0):
                calculated_risks.append(
                    {
                        "@id": "urn:openagri:pestInfectationRisk:obs2:{}".format(uuid.uuid4()),
                        "@type": ["Observation", "PestInfestationRisk"],
                        "phenomenonTime": "{}".format(hour["timestamp"]),
                        "hasSimpleResult": "{}".format(current_rule_risk_index)
                    }
                )
                continue

        graph.append(
            _risk_index_graph_element(pm, calculated_risks, lat, lon, parcel)
        )

    doc = {"@context": utils.context, "@graph": graph}

    return doc


def _risk_index_graph_element(pm, calculated_risks, lat, lon, parcel) -> dict:
    """The per-pest-model ObservationCollection envelope. Shared verbatim by the
    inline and agstack rule-index paths so the JSON-LD contract is identical."""
    return {
        "@id": "urn:openagri:pestInfectationRisk:{}".format(uuid.uuid4()),
        "@type": ["ObservationCollection"],
        "description": "{} pest infectation risk forecast in x ".format(pm.name),
        "observedProperty": {
            "@id": "urn:openagri:pestInfectationRisk:op:{}".format(uuid.uuid4()),
            "@type": ["ObservableProperty", "PestInfection"],
            "name": "UNCINE pest infection",
            "hasAgriPest": {
                "@id": "urn:openagri:pest:UNCINE",
                "@type": "AgriPest",
                "name": "UNCINE",
                "description": "Uncinula necator (syn. Erysiphe necator) is a fungus that causes powdery mildew of grape. It is a common pathogen of Vitis species, including the wine grape, Vitis vinifera",
                "eppoConcept": "https://gd.eppo.int/taxon/UNCINE"
            }
        },
        "madeBySensor": {
            "@id": "urn:openagri:pestInfectationRisk:model:{}".format(uuid.uuid4()),
            "@type": ["Sensor", "AIPestDetectionModel"],
            "name": "AI pest detaction model xyz"
        },
        "hasFeatureOfInterest": {
            "@id": "urn:openagri:pestInfectationRisk:foi:{}".format(uuid.uuid4()),
            "@type": ["FeatureOfInterest", "Point"],
            "long": "{}".format(lon),
            "lat": "{}".format(lat)
        },
        "basedOnWeatherDataset": {
            "@id": "urn:openagri:weatherDataset:{}".format(parcel["@id"]),
            "@type": "WeatherDataset",
            "name": "parcel_name_tba"
        },
        "resultTime": "{}".format(datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")),
        "hasMember": calculated_risks
    }


def _calculate_risk_index_probability_wd_agstack(
        parcel: dict,
        pest_models: List[PestModel],
        weather_data: dict,
        lat: float,
        lon: float,
        parameter: Optional[str] = None
):
    """agstack-pnd RuleBasedRisk path for calculate_risk_index_probability_wd.

    Replaces the inline per-hour eval call with the package's safe operator dispatch.
    The package reports the highest-risk level per calendar day; that daily verdict
    is broadcast back across the day's hours so the per-hour JSON-LD shape is
    unchanged (single-day inputs match the inline engine exactly; multi-day inputs
    may promote some hours to the day's peak — a recorded divergence).
    """
    from agstack_pnd.models.disease.rule_based import RuleBasedRisk
    from .agstack_adapter import _pestmodel_to_rules, risk_index_weather_to_wdf

    wdf = risk_index_weather_to_wdf(weather_data)
    model = RuleBasedRisk()
    graph = []

    for pm in pest_models:
        # Drop conditions whose field is absent from the weather (the package would
        # treat an all-absent rule as vacuously true); drop a rule left empty.
        rules = []
        for r in _pestmodel_to_rules(pm):
            conditions = [c for c in r["conditions"] if c["field"] in wdf]
            if conditions:
                rules.append({"conditions": conditions, "risk": r["risk"]})

        if rules:
            result = model.calculate(weather_data=wdf, parameters={"rules": rules})
            day_level = {s.date: s.risk_level.value.lower() for s in result.daily_scores}
        else:
            day_level = {}

        calculated_risks = []
        for hour in weather_data["data"]:
            level = day_level.get(pd.Timestamp(hour["timestamp"]).date(), "low")
            if parameter and prob_values.get(level, 0) < prob_values.get(parameter, 0):
                continue
            calculated_risks.append(
                {
                    "@id": "urn:openagri:pestInfectationRisk:obs2:{}".format(uuid.uuid4()),
                    "@type": ["Observation", "PestInfestationRisk"],
                    "phenomenonTime": "{}".format(hour["timestamp"]),
                    "hasSimpleResult": "{}".format(level)
                }
            )

        graph.append(
            _risk_index_graph_element(pm, calculated_risks, lat, lon, parcel)
        )

    return {"@context": utils.context, "@graph": graph}


def calculate_forecast_risk_index(
    parcel: Parcel, pest_models: list, weather_data: DataFrame
):
    reverse_dict = {v: k for k, v in openmeteo_friendly_variables.items()}
    weather_data.rename(columns=reverse_dict, inplace=True)

    for pm in pest_models:
        risks_for_current_pm = ["Low"] * weather_data.shape[0]

        for rule in pm.rules:
            if len(rule.conditions) == 0:
                continue
            mask = rule_mask(weather_data, rule.conditions)

            risks_for_current_pm = [
                rule.probability_value if x else y
                for x, y in zip(mask, risks_for_current_pm)
            ]

        weather_data["{}".format(pm.name)] = risks_for_current_pm

    context = utils.context

    graph = []

    for pm in pest_models:

        calculated_risks = []
        for date, risk in zip(weather_data["date"], weather_data["{}".format(pm.name)]):
            calculated_risks.append(
                {
                    "@id": "urn:openagri:pestInfectationRisk:obs2:{}".format(
                        uuid.uuid4()
                    ),
                    "@type": ["Observation", "PestInfestationRisk"],
                    "phenomenonTime": "{}".format(str(date).replace(" ", "T")),
                    "hasSimpleResult": "{}".format(risk),
                }
            )

        graph_element = {
            "@id": "urn:openagri:pestInfectationRisk:{}".format(uuid.uuid4()),
            "@type": ["ObservationCollection"],
            "description": "{} pest infectation risk forecast in x ".format(pm.name),
            "observedProperty": {
                "@id": "urn:openagri:pestInfectationRisk:op:{}".format(uuid.uuid4()),
                "@type": ["ObservableProperty", "PestInfection"],
                "name": "UNCINE pest infection",
                "hasAgriPest": {
                    "@id": "urn:openagri:pest:UNCINE",
                    "@type": "AgriPest",
                    "name": "UNCINE",
                    "description": "Uncinula necator (syn. Erysiphe necator) is a fungus that causes powdery mildew of grape. It is a common pathogen of Vitis species, including the wine grape, Vitis vinifera",
                    "eppoConcept": "https://gd.eppo.int/taxon/UNCINE",
                },
            },
            "madeBySensor": {
                "@id": "urn:openagri:pestInfectationRisk:model:{}".format(uuid.uuid4()),
                "@type": ["Sensor", "AIPestDetectionModel"],
                "name": "AI pest detaction model xyz",
            },
            "hasFeatureOfInterest": {
                "@id": "urn:openagri:pestInfectationRisk:foi:{}".format(uuid.uuid4()),
                "@type": ["FeatureOfInterest", "Point"],
                "long": "{}".format(parcel.longitude),
                "lat": "{}".format(parcel.latitude),
            },
            "basedOnWeatherDataset": {
                "@id": "urn:openagri:weatherDataset:{}".format(parcel.id),
                "@type": "WeatherDataset",
                "name": "parcel_name_tba",
            },
            "resultTime": "{}".format(
                datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            ),
            "hasMember": calculated_risks,
        }

        graph.append(graph_element)

    doc = {"@context": context, "@graph": graph}

    return doc
