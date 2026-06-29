from typing import Annotated

from fastapi import APIRouter, Depends
from fastapi.security import OAuth2PasswordRequestForm
from sqlalchemy.orm import Session

from core.security import *
from core.config import settings
from api import deps
from crud import user
from schemas import Token, Message
from utils import get_logger, gatekeeper_logout
from utils.gatekeeper_client import GatekeeperClient

logger = get_logger(api_path_name=__name__)

router = APIRouter()

@router.post("/access-token/", response_model=Token)
def login_access_token(
        form_data: Annotated[OAuth2PasswordRequestForm, Depends()],
        db: Session = Depends(deps.get_db)
) -> Token:
    """
    OAuth2 compatible token login, get an [access token, refresh token] pair for future requests
    """

    if not settings.USING_GATEKEEPER:

        user_db = user.authenticate(
            db=db,
            email=form_data.username,
            password=form_data.password
        )

        if not user_db:
            raise HTTPException(
                status_code=400,
                detail="Incorrect email or password"
            )

        response_token = Token(
            access_token=create_token(user_db.id, settings.ACCESS_TOKEN_EXPIRATION_TIME),
            refresh_token=create_token(user_db.id, settings.REFRESH_TOKEN_EXPIRATION_TIME),
            token_type="bearer"
        )
    else:
        response_json = GatekeeperClient(str(settings.GATEKEEPER_BASE_URL)).login(
            username=form_data.username,
            password=form_data.password,
        )

        if response_json.get("success"):
            response_token = Token(
                access_token=response_json["access"],
                refresh_token=response_json["refresh"],
                token_type="bearer"
            )
        else:
            raise HTTPException(
                status_code=400,
                detail="Error, unsuccessful login attempt, GateKeeper returned 200 with success==False"
            )

    return response_token


@router.post("/logout/", response_model=Message, dependencies=[Depends(deps.is_using_gatekeeper), Depends(deps.get_jwt)])
def logout(
        refresh_token: str = Depends(deps.get_refresh_token)
) -> Message:
    """
    Logout
    """

    gatekeeper_logout(refresh_token)

    response_message = Message(
        message="Successfully logged out!"
    )

    return response_message
