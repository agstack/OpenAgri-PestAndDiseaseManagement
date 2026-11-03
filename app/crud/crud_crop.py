from crud.base import CRUDBase
from models.crop import Crop
from schemas.crop import CropCreate, CropUpdate


class CRUDCrop(CRUDBase[Crop, CropCreate, CropUpdate]):
    pass


crop = CRUDCrop(Crop)
