# backend/app/crud/crud_imaging_order.py

from typing import Optional
from sqlalchemy.orm import Session

from app.crud.base import CRUDBase
from app.db.models.imaging_order import ImagingOrder
from app.schemas.imaging_order import ImagingOrderCreate, ImagingOrderUpdate

class CRUDImagingOrder(CRUDBase[ImagingOrder, ImagingOrderCreate, ImagingOrderUpdate]):
    def get_by_accession_number(self, db: Session, *, accession_number: str) -> Optional[ImagingOrder]:
        """
        Retrieves an imaging order by its accession number.
        """
        return db.query(self.model).filter(self.model.accession_number == accession_number).first()

    # We can add more complex queries here later for the DMWL SCP,
    # e.g., get_worklist_for_modality(modality: str, scheduled_date: date)
    # But for now, this is all we need.

imaging_order = CRUDImagingOrder(ImagingOrder)