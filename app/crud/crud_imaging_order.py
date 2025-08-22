# backend/app/crud/crud_imaging_order.py

from typing import List, Optional, Tuple
from datetime import date, datetime, time

import structlog
from sqlalchemy import or_
from sqlalchemy.orm import Session
from sqlalchemy.sql.functions import coalesce

from app.crud.base import CRUDBase
from app.db.models.imaging_order import ImagingOrder
from app.schemas.imaging_order import ImagingOrderCreate, ImagingOrderUpdate, ImagingOrderRead
from app.schemas.enums import OrderStatus

logger = structlog.get_logger(__name__)

class CRUDImagingOrder(CRUDBase[ImagingOrder, ImagingOrderCreate, ImagingOrderUpdate]):
    
    def get_by_accession_number(self, db: Session, *, accession_number: str) -> Optional[ImagingOrder]:
        """Retrieves an imaging order by its accession number."""
        return db.query(self.model).filter(self.model.accession_number == accession_number).first()

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
        accession_number: Optional[str] = None,
        modality: Optional[str] = None,
        scheduled_station_ae_title: Optional[str] = None,
        patient_name: Optional[str] = None,
        patient_id: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        status: Optional[str] = None,
    ) -> List[ImagingOrder]:
        """
        (Legacy) Queries for Modality Worklist items with DICOM-style matching logic.
        Supports the DIMSE C-FIND service.
        """
        query = db.query(self.model)

        if accession_number:
            # AccessionNumber should be an exact match (DICOM standard)
            query = query.filter(self.model.accession_number == accession_number)

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
            sql_wildcard_name = str(patient_name).replace('*', '%').replace('?', '_')
            query = query.filter(self.model.patient_name.ilike(sql_wildcard_name))

        if start_date and end_date:
            start_datetime = datetime.combine(start_date, time.min)
            end_datetime = datetime.combine(end_date, time.max)
            query = query.filter(self.model.scheduled_procedure_step_start_datetime.between(start_datetime, end_datetime))
        elif start_date:
            start_datetime = datetime.combine(start_date, time.min)
            query = query.filter(self.model.scheduled_procedure_step_start_datetime >= start_datetime)

        # --- THIS IS THE CORRECTED STATUS FILTER LOGIC ---
        if status:
            # FIX #2: Handle both string and list-like MultiValue types from pynetdicom
            status_str_list = []
            if isinstance(status, str):
                status_str_list = [s.strip().upper() for s in status.split('\\') if s.strip()]
            elif hasattr(status, '__iter__'): # Covers lists and pydicom.multival.MultiValue
                status_str_list = [str(s).strip().upper() for s in status if str(s).strip()]
            
            enum_status_list = []
            for s in status_str_list:
                try:
                    enum_status_list.append(OrderStatus(s))
                except ValueError:
                    logger.warn("DMWL_INVALID_STATUS_QUERY", invalid_status=s)
            
            if enum_status_list:
                query = query.filter(self.model.order_status.in_(enum_status_list))
        else:
            # Default behavior: only show SCHEDULED if no status filter is provided.
            query = query.filter(self.model.order_status == OrderStatus.SCHEDULED)

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
                    coalesce(self.model.patient_name, '').ilike(search_term),
                    coalesce(self.model.patient_id, '').ilike(search_term),
                    coalesce(self.model.accession_number, '').ilike(search_term),
                    coalesce(self.model.source_sending_facility, '').ilike(search_term),
                    coalesce(self.model.source_receiving_facility, '').ilike(search_term),
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

    def get_by_study_instance_uid(self, db: Session, *, study_uid: str) -> Optional[ImagingOrder]:
        return db.query(ImagingOrder).filter(ImagingOrder.study_instance_uid == study_uid).first()

    def update_status(self, db: Session, *, order_id: int, new_status: OrderStatus) -> Optional[ImagingOrder]:
        db_obj = db.query(ImagingOrder).filter(ImagingOrder.id == order_id).first()
        if db_obj:
            db_obj.order_status = new_status
            db.commit()
            db.refresh(db_obj)
        return db_obj

    async def update_status_with_event(self, db: Session, *, order_id: int, new_status: OrderStatus, connection) -> Optional[ImagingOrder]:
        """Update order status and publish SSE event"""
        from app.events import publish_order_event
        from app.schemas import imaging_order as schemas
        
        db_obj = self.update_status(db, order_id=order_id, new_status=new_status)
        
        if db_obj:
            try:
                await publish_order_event(
                    event_type="order_updated",
                    payload=schemas.ImagingOrderRead.model_validate(db_obj).model_dump(mode='json'),
                    connection=connection
                )
                logger.info("Order status updated with SSE event published", order_id=order_id, new_status=new_status.value)
            except Exception as e:
                logger.error("Failed to publish SSE event for order status update", order_id=order_id, error=str(e))
        
        return db_obj


imaging_order = CRUDImagingOrder(ImagingOrder)