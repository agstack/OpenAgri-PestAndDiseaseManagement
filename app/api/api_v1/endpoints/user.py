import requests
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import Any

from api import deps
from models import User
from schemas import Message, UserCreate, UserMe
from crud import user
from core import settings


router = APIRouter()


@router.post("/register/", response_model=Message)
def register(
        user_information: UserCreate,
        db: Session = Depends(deps.get_db)
) -> Message:
    """
    Registration API for the service.
    """

    pwd_check = settings.PASSWORD_SCHEMA_OBJ.validate(pwd=user_information.password)
    if not pwd_check:
        raise HTTPException(
            status_code=400,
            detail="Password needs to be at least 8 characters long,"
                   "contain at least one uppercase and one lowercase letter, one digit and have no spaces."
        )

    if settings.USING_GATEKEEPER:
        try:
            response = requests.post(
                url=settings.GATEKEEPER_BASE_URL.unicode_string() + "api/register/",
                headers={"Content-Type": "application/json"},
                json={"username": user_information.email,
                      "email": user_information.email, "password": user_information.password}
            )
        except Exception as e:
            raise HTTPException(
                status_code=400,
                detail="Error when sending request to gk, original error: {}".format(e)
            )

        if response.status_code == 400:
            raise HTTPException(
                status_code=400,
                detail="Error, got 400 from gk"
            )

    else:
        user_db = user.get_by_email(db=db, email=user_information.email)
        if user_db:
            raise HTTPException(
                status_code=400,
                detail="User with email:{} already exists.".format(user_information.email)
            )

        user.create(db=db, obj_in=user_information)

    response = Message(
        message="You have successfully registered!"
    )

    return response


@router.get("/me/", response_model=UserMe)
def get_me(
        current_user: User = Depends(deps.get_current_user)
) -> Any:
    """
    Returns user email
    """

    return current_user
