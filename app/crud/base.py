# app/crud/base.py
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union

from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError # Import for handling potential errors

from app.db.base import Base # Assuming your SQLAlchemy Base is here

ModelType = TypeVar("ModelType", bound=Base)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)


class CRUDBase(Generic[ModelType, CreateSchemaType, UpdateSchemaType]):
    def __init__(self, model: Type[ModelType]):
        """
        CRUD object with default methods to Create, Read, Update, Delete (CRUD).

        **Parameters**

        * `model`: A SQLAlchemy model class
        * `schema`: A Pydantic model (schema) class
        """
        self.model = model

    def get(self, db: Session, id: Any) -> Optional[ModelType]:
        return db.query(self.model).filter(self.model.id == id).first()

    def get_multi(
        self, db: Session, *, skip: int = 0, limit: int = 100
    ) -> List[ModelType]:
        return db.query(self.model).offset(skip).limit(limit).all()

    def create(self, db: Session, *, obj_in: CreateSchemaType) -> ModelType:
        obj_in_data = jsonable_encoder(obj_in)
        db_obj = self.model(**obj_in_data)  # type: ignore
        db.add(db_obj)
        try:
            db.commit()
            db.refresh(db_obj)
        except IntegrityError as e:
             db.rollback()
             # You might want to log the error here or raise a more specific custom exception
             # logger.error(f"Integrity error creating {self.model.__name__}: {e}", exc_info=True)
             raise ValueError(f"Database integrity error: {e.orig}") from e # Raise ValueError for API layer to catch
        return db_obj

    def update(
        self,
        db: Session,
        *,
        db_obj: ModelType,
        obj_in: Union[UpdateSchemaType, Dict[str, Any]]
    ) -> ModelType:
        obj_data = jsonable_encoder(db_obj)
        if isinstance(obj_in, dict):
            update_data = obj_in
        else:
            # Use exclude_unset=True to only update fields provided in the Pydantic model
            update_data = obj_in.model_dump(exclude_unset=True)
        for field in obj_data:
            if field in update_data:
                setattr(db_obj, field, update_data[field])
        db.add(db_obj)
        try:
            db.commit()
            db.refresh(db_obj)
        except IntegrityError as e:
             db.rollback()
             # logger.error(f"Integrity error updating {self.model.__name__} ID {db_obj.id}: {e}", exc_info=True)
             raise ValueError(f"Database integrity error during update: {e.orig}") from e
        return db_obj

    def remove(self, db: Session, *, id: int) -> Optional[ModelType]:
        obj = db.query(self.model).get(id)
        if obj:
            db.delete(obj)
            try:
                 db.commit()
            except IntegrityError as e: # Catch potential FK constraints etc.
                 db.rollback()
                 # logger.error(f"Integrity error removing {self.model.__name__} ID {id}: {e}", exc_info=True)
                 raise ValueError(f"Cannot remove item due to database constraint: {e.orig}") from e
        return obj # Return the object that was deleted (or None)
