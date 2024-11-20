from pydantic import BaseModel


class BaseDataset(BaseModel):
    name: str
