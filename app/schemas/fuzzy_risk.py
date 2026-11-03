from __future__ import annotations

import datetime
import uuid
from typing import List, Optional

from pydantic import BaseModel


class FuzzyRiskCalculateRequest(BaseModel):
    parcel_id:         int
    from_date:         datetime.date
    to_date:           datetime.date
    threat_model_ids:  Optional[List[uuid.UUID]] = None  # None = all


class FuzzyRiskForecastRequest(BaseModel):
    parcel_id:         int
    threat_model_ids:  Optional[List[uuid.UUID]] = None
    days_ahead:        Optional[int] = 7


class FuzzyRiskFetchRequest(BaseModel):
    parcel_id:         int
    from_date:         datetime.date
    to_date:           datetime.date
    threat_model_ids:  Optional[List[uuid.UUID]] = None


class FuzzyRiskForecastFcRequest(BaseModel):
    parcel_id:         str               # UUID string from Farm Calendar
    threat_model_ids:  Optional[List[uuid.UUID]] = None
    days_ahead:        Optional[int] = 7
