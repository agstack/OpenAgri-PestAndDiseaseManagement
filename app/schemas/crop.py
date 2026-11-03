from typing import Optional

from pydantic import BaseModel, ConfigDict, UUID4


class CropCreate(BaseModel):
    name: str
    description: Optional[str] = None


class CropUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None


class CropDB(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID4
    name: str
    description: Optional[str] = None
