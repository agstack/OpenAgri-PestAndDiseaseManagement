from fastapi import APIRouter, Depends, UploadFile, File
from sqlalchemy.orm import Session
from api import deps

import crud
from models import User
from schemas import PestModels, CreatePestModel, PestModelDB, Message, CreateCondition, CreateRule

import pandas as pd

router = APIRouter()


@router.get("/", response_model=PestModels, dependencies=[Depends(deps.get_jwt)])
def get_pest_models(
        db: Session = Depends(deps.get_db)
):
    """
    Returns all pest models
    """

    pest_models_db = crud.pest_model.get_all(db=db)

    return PestModels(pests=pest_models_db)

@router.post("/", response_model=PestModelDB, dependencies=[Depends(deps.get_jwt)])
def create_pest_model(
        pm: CreatePestModel,
        db: Session = Depends(deps.get_db)
):
    """
    Create a base pest model
    """

    return crud.pest_model.create(db=db, obj_in=pm)

@router.post("/upload-excel/", response_model=Message, dependencies=[Depends(deps.get_jwt)])
def upload_pest_model(
        excel_file: UploadFile = File(...),
        db: Session = Depends(deps.get_db)
) -> Message:
    """
    Upload pest models via a definition file (excel)
    """

    df = pd.read_excel(excel_file.file)

    disease_names = df["Pest/Pathogen"].unique().tolist()

    for disease in disease_names:
        loced = df.loc[df["Pest/Pathogen"] == disease]

        pest_schema = CreatePestModel(
            name = disease,
            description=None,
            geo_areas_of_application=None,
            cultivations=[]
        )

        pest_model_db = crud.pest_model.create(db=db, obj_in=pest_schema)

        for row in loced.itertuples(index=False):

            rule_schema = CreateRule(
                name = disease,
                pest_model_id=pest_model_db.id,
                probability_value=row[4].lower()
            )

            rule_db = crud.rule.create(db=db, obj_in=rule_schema)

            # parse humidity
            if row[1][0] == "<" or row[1][0] == ">":
                condition_value = float(row[1].strip("><%"))
                operator_symbol = row[1][0]

                condition_schema = CreateCondition(
                    rule_id=rule_db.id,
                    unit_id=crud.unit.get_by_name(db=db, name="atmospheric_relative_humidity").id,
                    operator_id=crud.operator.get_by_symbol(db=db, symbol=operator_symbol).id,
                    value=condition_value
                )

                crud.condition.create(db=db, obj_in=condition_schema)
            else:
                lower_value = float(str(row[1][0]) + str(row[1][1]))
                higher_value = float(str(row[1][3]) + str(row[1][4]))

                condition_schema_lower = CreateCondition(
                    rule_id=rule_db.id,
                    unit_id=crud.unit.get_by_name(db=db, name="atmospheric_relative_humidity").id,
                    operator_id=crud.operator.get_by_symbol(db=db, symbol=">").id,
                    value=lower_value
                )

                condition_schema_higher = CreateCondition(
                    rule_id=rule_db.id,
                    unit_id=crud.unit.get_by_name(db=db, name="atmospheric_relative_humidity").id,
                    operator_id=crud.operator.get_by_symbol(db=db, symbol="<").id,
                    value=higher_value
                )

                crud.condition.create(db=db, obj_in=condition_schema_lower)
                crud.condition.create(db=db, obj_in=condition_schema_higher)

            # parse temp
            if row[2].find("T") == -1:
                value=row[2].strip("><=")
                if len(row[2]) == 4:
                    symbol = str(row[2][:2])
                else:
                    symbol = str(row[2][0])

                condition_schema = CreateCondition(
                    rule_id=rule_db.id,
                    unit_id=crud.unit.get_by_name(db=db, name="atmospheric_temperature").id,
                    operator_id=crud.operator.get_by_symbol(db=db, symbol=symbol).id,
                    value=value
                )

                crud.condition.create(db=db, obj_in=condition_schema)
            else:
                values = [x.strip("><=") for x in row[2].split("T")]

                condition_schema_lower = CreateCondition(
                    rule_id=rule_db.id,
                    unit_id=crud.unit.get_by_name(db=db, name="atmospheric_temperature").id,
                    operator_id=crud.operator.get_by_symbol(db=db, symbol=">").id,
                    value=min(values)
                )

                condition_schema_higher = CreateCondition(
                    rule_id=rule_db.id,
                    unit_id=crud.unit.get_by_name(db=db, name="atmospheric_temperature").id,
                    operator_id=crud.operator.get_by_symbol(db=db, symbol="<").id,
                    value=max(values)
                )

                crud.condition.create(db=db, obj_in=condition_schema_lower)
                crud.condition.create(db=db, obj_in=condition_schema_higher)

            # parse precipitation
            if row[3].find("Continuous"):
                # skip this one since it's unusable for now
                pass
            else:
                if row[3].find("-"):
                    values = [float(x) for x in row[3].strip("m ").replace(",", ".").split("-")]

                    condition_schema_lower = CreateCondition(
                        rule_id=rule_db.id,
                        unit_id=crud.unit.get_by_name(db=db, name="precipitation").id,
                        operator_id=crud.operator.get_by_symbol(db=db, symbol=">").id,
                        value=min(values)
                    )

                    condition_schema_higher = CreateCondition(
                        rule_id=rule_db.id,
                        unit_id=crud.unit.get_by_name(db=db, name="precipitation").id,
                        operator_id=crud.operator.get_by_symbol(db=db, symbol="<").id,
                        value=max(values)
                    )

                    crud.condition.create(db=db, obj_in=condition_schema_lower)
                    crud.condition.create(db=db, obj_in=condition_schema_higher)
                else:
                    value = float(row[3].strip("m ").replace(",", "."))
                    operator = "="

                    condition_schema = CreateCondition(
                        rule_id=rule_db.id,
                        unit_id=crud.unit.get_by_name(db=db, name="precipitation").id,
                        operator_id=crud.operator.get_by_symbol(db=db, symbol=operator).id,
                        value=value
                    )

                    crud.condition.create(db=db, obj_in=condition_schema)

    return Message(message="Successfully created pest models from file!")