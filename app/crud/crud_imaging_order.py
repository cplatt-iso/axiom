# backend/app/crud/crud_imaging_order.py

from typing import List, Optional, Tuple
from datetime import date, datetime, time

from sqlalchemy import or_
from sqlalchemy.orm import Session

from app.crud.base import CRUDBase
from app.db.models.imaging_order import ImagingOrder
from app.schemas.imaging_order import ImagingOrderCreate, ImagingOrderUpdate
from app.schemas.enums import OrderStatus


class CRUDImagingOrder(CRUDBase[ImagingOrder, ImagingOrderCreate, ImagingOrderUpdate]):
    
    def get_by_accession_number(self, db: Session, *, accession_number: str) -> Optional[ImagingOrder]:
        """Retrieves an imaging order by its accession number."""
        return db.query(self.model).filter(self.model.accession_number == accession_number).first()

    # --- THIS IS THE NEW WEAPON IN OUR ARSENAL ---
    def get_by_placer_order_number(self, db: Session, *, placer_order_number: str) -> Optional[ImagingOrder]:
        """
        Retrieves an imaging order by its Placer Order Number (from ORC-2).
        This is the correct way to find an existing order for updates/cancellations.
        """
        if not placer_order_number:
            return None
        return db.query(self.model).filter(self.model.placer_order_number == placer_order_number).first()

    def get_worklist(
        self,
        db: Session,
        *,
        modality: Optional[str] = None,
        scheduled_station_ae_title: Optional[str] = None,
        patient_name: Optional[str] = None,
        patient_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> List[ImagingOrder]:
        """
        (Legacy) Queries for Modality Worklist items with DICOM-style matching logic.
        Supports the DIMSE C-FIND service.
        """
        query = db.query(self.model).filter(self.model.order_status == OrderStatus.SCHEDULED)

        if modality:
            if '*' in modality or '?' in modality:
                sql_wildcard = modality.replace('*', '%').replace('?', '_')
                query = query.filter(self.model.modality.ilike(sql_wildcard))
            else:
                query = query.filter(self.model.modality == modality)

        if scheduled_station_ae_title:
            query = query.filter(self.model.scheduled_station_ae_title == scheduled_station_ae_title)

        if patient_id:
            query = query.filter(self.model.patient_id == patient_id)
        
        if patient_name:
            sql_wildcard_name = patient_name.replace('*', '%').replace('?', '_')
            query = query.filter(self.model.patient_name.ilike(sql_wildcard_name))

        if start_date and end_date:
            start_datetime = datetime.combine(start_date, time.min)
            end_datetime = datetime.combine(end_date, time.max)
            query = query.filter(self.model.scheduled_procedure_step_start_datetime.between(start_datetime, end_datetime))
        elif start_date:
            start_datetime = datetime.combine(start_date, time.min)
            query = query.filter(self.model.scheduled_procedure_step_start_datetime >= start_datetime)

        return query.order_by(self.model.scheduled_procedure_step_start_datetime.asc()).all()

    def get_orders_paginated(
        self,
        db: Session,
        *,
        skip: int = 0,
        limit: int = 100,
        search: Optional[str] = None,
        modalities: Optional[List[str]] = None,
        statuses: Optional[List[str]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
    ) -> Tuple[List[ImagingOrder], int]:
        """
        (New) Queries for Imaging Orders with modern UI pagination, filtering, and searching.
        This is the workhorse for the Orders page UI.
        """
        query = db.query(self.model)

        # Apply filters
        if search:
            search_term = f"%{search}%"
            query = query.filter(
                or_(
                    self.model.patient_name.ilike(search_term),
                    self.model.patient_id.ilike(search_term),
                    self.model.accession_number.ilike(search_term),
                )
            )


        if modalities:
            query = query.filter(self.model.modality.in_(modalities))

        if statuses:
            query = query.filter(self.model.order_status.in_(statuses))

        if start_date:
            start_datetime = datetime.combine(start_date, time.min)
            query = query.filter(self.model.scheduled_procedure_step_start_datetime >= start_datetime)
        
        if end_date:
            end_datetime = datetime.combine(end_date, time.max)
            query = query.filter(self.model.scheduled_procedure_step_start_datetime <= end_datetime)

        # Get total count before pagination
        total_count = query.count()

        # Apply sorting and pagination
        items = query.order_by(self.model.updated_at.desc()).offset(skip).limit(limit).all()

        return items, total_count


imaging_order = CRUDImagingOrder(ImagingOrder)