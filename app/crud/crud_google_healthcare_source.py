# backend/app/crud/crud_google_healthcare_source.py
from typing import Any, Dict, Optional, Union, List

from sqlalchemy.orm import Session

from app.crud.base import CRUDBase
from app.db.models.google_healthcare_source import GoogleHealthcareSource
from app.schemas.google_healthcare_source import GoogleHealthcareSourceCreate, GoogleHealthcareSourceUpdate


class CRUDGoogleHealthcareSource(CRUDBase[GoogleHealthcareSource, GoogleHealthcareSourceCreate, GoogleHealthcareSourceUpdate]):
    def get_by_name(self, db: Session, *, name: str) -> Optional[GoogleHealthcareSource]:
        return db.query(GoogleHealthcareSource).filter(GoogleHealthcareSource.name == name).first()

    def create(self, db: Session, *, obj_in: GoogleHealthcareSourceCreate) -> GoogleHealthcareSource:
        # Check for duplicate name before creating
        existing = self.get_by_name(db, name=obj_in.name)
        if existing:
            # Consider raising a more specific exception here to be caught in the API layer
            raise ValueError(f"Google Healthcare Source with name '{obj_in.name}' already exists.")

        # Create the object using the base class method
        return super().create(db, obj_in=obj_in)

    def update(
        self,
        db: Session,
        *,
        db_obj: GoogleHealthcareSource,
        obj_in: Union[GoogleHealthcareSourceUpdate, Dict[str, Any]]
    ) -> GoogleHealthcareSource:
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            update_data = obj_in.model_dump(exclude_unset=True)

        # Check for name change conflict
        if "name" in update_data and update_data["name"] != db_obj.name:
            existing = self.get_by_name(db, name=update_data["name"])
            if existing and existing.id != db_obj.id:
                raise ValueError(f"Another Google Healthcare Source with name '{update_data['name']}' already exists.")

        # Handle the is_active / is_enabled validation constraint
        # If is_active is being set to True, is_enabled must also be True (either currently or in the update)
        if update_data.get("is_active") is True:
            if db_obj.is_enabled is False and update_data.get("is_enabled") is not True:
                 raise ValueError("Cannot activate a source that is not enabled. Set 'is_enabled' to true as well.")
        # If is_enabled is being set to False, is_active must also be False (either currently or in the update)
        elif update_data.get("is_enabled") is False:
            if db_obj.is_active is True and update_data.get("is_active") is not False:
                 raise ValueError("Cannot disable a source that is still active. Set 'is_active' to false as well.")

        return super().update(db, db_obj=db_obj, obj_in=update_data)

    def get_multi_enabled(self, db: Session, *, skip: int = 0, limit: int = 100) -> List[GoogleHealthcareSource]:
        return (
            db.query(self.model)
            .filter(GoogleHealthcareSource.is_enabled == True)
            .offset(skip)
            .limit(limit)
            .all()
        )

    def get_multi_active(self, db: Session, *, skip: int = 0, limit: int = 100) -> List[GoogleHealthcareSource]:
         return (
             db.query(self.model)
             .filter(GoogleHealthcareSource.is_active == True, GoogleHealthcareSource.is_enabled == True)
             .offset(skip)
             .limit(limit)
             .all()
         )


google_healthcare_source = CRUDGoogleHealthcareSource(GoogleHealthcareSource)
