import datetime
from typing import Optional, List

from pydantic import BaseModel, ConfigDict, Field
from pydantic import UUID4


class CreateDisease(BaseModel):
    name: str
    eppo_code: str
    base_gdd: int
    description: Optional[str]


class GDDIntervalDB(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: int
    start: int
    end: int
    descriptor: str


class DiseaseDB(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID4
    name: str
    eppo_code: str
    base_gdd: int
    description: Optional[str]
    gdd_points: List[GDDIntervalDB]


class ListDisease(BaseModel):
    diseases: List[DiseaseDB]


class GDDIntervalInput(BaseModel):
    start: int
    end: int
    descriptor: str


class InputDisease(BaseModel):
    name: str
    eppo_code: str
    base_gdd: int
    description: Optional[str]
    gdd_points: List[GDDIntervalInput] = Field(min_length=1)


class GDDResponseChunk(BaseModel):
    date: datetime.date
    gdd_value: int
    accumulated_gdd: int
    descriptor: str


class DiseaseModel(BaseModel):
    name: str
    eppo_code: str
    base_gdd: int
    description: Optional[str]
    gdd_points: List[GDDIntervalDB]
    gdd_values: List[GDDResponseChunk]


class UpdateGDDInterval(BaseModel):
    start: int
    end: int
    descriptor: str


class UpdateDiseaseModel(BaseModel):
    name: str | None = None
    eppo_code: str | None = None
    base_gdd: int | None = None
    description: str | None = None
    gdd_points: List[UpdateGDDInterval] | None = None


class GDDResponse(BaseModel):
    models: List[DiseaseModel]
