from pydantic import BaseModel


class CreateDataset(BaseModel):
    name: str

class UpdateDataset(BaseModel):
    name: str
