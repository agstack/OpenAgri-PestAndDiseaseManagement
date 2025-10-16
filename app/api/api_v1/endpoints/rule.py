from fastapi import APIRouter, HTTPException, Depends
from sqlalchemy.orm import Session

import crud
from api import deps
from models import User
from schemas import Message, RulesDB, RuleDB, CreateRuleWithConditions, CreateCondition, CreateRule

router = APIRouter()


@router.post("/", response_model=RuleDB, dependencies=[Depends(deps.get_jwt)])
def create_rule(
        rule: CreateRuleWithConditions,
        db: Session = Depends(deps.get_db)
):
    """
    Create a rule such as: temperature > 50 AND air_pressure < 20 AND humidity < 50 and assign it to a pest model
    """

    pest_model_db = crud.pest_model.get(db=db, id=rule.pest_model_id)

    if not pest_model_db:
        raise HTTPException(
            status_code=400,
            detail="Error, can't create rule for non-existent pest model"
        )

    if len(rule.conditions) == 0:
        raise HTTPException(
            status_code=400,
            detail="Can't create rule with no conditions."
        )

    # Check whether there are two conditions with the same unit
    cond_times = {}
    for cond in rule.conditions:
        # Count for same conditions present in file
        if not cond.unit_id in cond_times:

            # Check for unit and operator ids
            operator_db = crud.operator.get(db=db, id=cond.operator_id)
            if not operator_db:
                raise HTTPException(
                    status_code=400,
                    detail="Operator with ID: {} does not exist.".format(cond.operator_id)
                )
            unit_db = crud.unit.get(db=db, id=cond.unit_id)
            if not unit_db:
                raise HTTPException(
                    status_code=400,
                    detail="Unit with ID: {} does not exist.".format(cond.unit_id)
                )

            cond_times[cond.unit_id] = [cond.unit_id]
            continue
        cond_times[cond.unit_id].append(cond.unit_id)

        # Check for unit and operator ids
        operator_db = crud.operator.get(db=db, id=cond.operator_id)
        if not operator_db:
            raise HTTPException(
                status_code=400,
                detail="Operator with ID: {} does not exist.".format(cond.operator_id)
            )
        unit_db = crud.unit.get(db=db, id=cond.unit_id)
        if not unit_db:
            raise HTTPException(
                status_code=400,
                detail="Unit with ID: {} does not exist.".format(cond.unit_id)
            )

    for cond in cond_times.values():
        if len(cond) > 2:
            raise HTTPException(
                status_code=400,
                detail="Can't have more that two condition per unit."
            )

    cr = CreateRule(name=rule.name, description=rule.description, probability_value=rule.probability_value, pest_model_id=rule.pest_model_id)
    rule_db = crud.rule.create(db=db, obj_in=cr)

    for cond in rule.conditions:
        crud.condition.create(db=db, obj_in=CreateCondition(**cond.model_dump(), rule_id=rule_db.id))

    return rule_db



@router.get("/", response_model=RulesDB, dependencies=[Depends(deps.get_jwt)])
def get_all_rules(
        db: Session = Depends(deps.get_db)
) -> RulesDB:
    """
    Returns all stored rules.
    """

    rules_db = crud.rule.get_all(db=db)

    return RulesDB(rules=rules_db)


@router.delete("/{rule_id}/", response_model=Message, dependencies=[Depends(deps.get_jwt)])
def delete_rule(
        rule_id: int,
        db: Session = Depends(deps.get_db)
) -> Message:
    """
    Delete a rule
    """

    rule_db = crud.rule.get(db=db, id=rule_id)

    if not rule_db:
        raise HTTPException(
            status_code=400,
            detail="Can't delete rule that doesn't exist."
        )

    crud.rule.remove(db=db, id=rule_id)

    return Message(message="Successfully removed rule!")
