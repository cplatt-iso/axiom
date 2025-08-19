# app/crud/crud_order_dicom_evidence.py
import json
from typing import Optional, List, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import func, distinct

from app.crud.base import CRUDBase
from app.db.models.order_dicom_evidence import OrderDicomEvidence
from app.schemas.order_dicom_evidence import (
    OrderDicomEvidenceCreate, 
    OrderDicomEvidenceUpdate,
    OrderDicomEvidenceSummary
)


class CRUDOrderDicomEvidence(CRUDBase[OrderDicomEvidence, OrderDicomEvidenceCreate, OrderDicomEvidenceUpdate]):
    
    def get_by_order_id(self, db: Session, *, imaging_order_id: int) -> List[OrderDicomEvidence]:
        """Get all DICOM evidence for a specific imaging order."""
        return db.query(self.model).filter(
            self.model.imaging_order_id == imaging_order_id
        ).order_by(self.model.processed_at.desc()).all()
    
    def get_by_sop_instance_uid(self, db: Session, *, sop_instance_uid: str) -> Optional[OrderDicomEvidence]:
        """Get evidence by SOP Instance UID (should be unique per order)."""
        return db.query(self.model).filter(
            self.model.sop_instance_uid == sop_instance_uid
        ).first()
    
    def get_by_order_and_sop(
        self, 
        db: Session, 
        *, 
        imaging_order_id: int, 
        sop_instance_uid: str
    ) -> Optional[OrderDicomEvidence]:
        """Get specific evidence entry for an order and SOP Instance UID."""
        return db.query(self.model).filter(
            self.model.imaging_order_id == imaging_order_id,
            self.model.sop_instance_uid == sop_instance_uid
        ).first()
    
    def create_or_update_evidence(
        self,
        db: Session,
        *,
        imaging_order_id: int,
        sop_instance_uid: str,
        study_instance_uid: Optional[str],
        series_instance_uid: Optional[str],
        accession_number: Optional[str],
        match_rule: str,
        applied_rule_names: Optional[str] = None,
        applied_rule_ids: Optional[str] = None,
        destination_results: Optional[str] = None,
        processing_successful: bool = True,
        source_identifier: Optional[str] = None
    ) -> OrderDicomEvidence:
        """
        Create new evidence or update existing evidence for an order/SOP combination.
        This is the main method used by the DICOM processing pipeline.
        """
        # Check if evidence already exists
        existing = self.get_by_order_and_sop(
            db, 
            imaging_order_id=imaging_order_id, 
            sop_instance_uid=sop_instance_uid
        )
        
        if existing:
            # Update existing evidence
            update_data = OrderDicomEvidenceUpdate(
                applied_rule_names=applied_rule_names,
                applied_rule_ids=applied_rule_ids,
                destination_results=destination_results,
                processing_successful=processing_successful
            )
            return self.update(db, db_obj=existing, obj_in=update_data)
        else:
            # Create new evidence
            create_data = OrderDicomEvidenceCreate(
                imaging_order_id=imaging_order_id,
                sop_instance_uid=sop_instance_uid,
                study_instance_uid=study_instance_uid,
                series_instance_uid=series_instance_uid,
                accession_number=accession_number,
                match_rule=match_rule,
                applied_rule_names=applied_rule_names,
                applied_rule_ids=applied_rule_ids,
                destination_results=destination_results,
                processing_successful=processing_successful,
                source_identifier=source_identifier
            )
            return self.create(db, obj_in=create_data)

    def create_or_update_evidence_with_events(
        self,
        db: Session,
        *,
        imaging_order_id: int,
        sop_instance_uid: str,
        study_instance_uid: str,
        series_instance_uid: Optional[str] = None,
        accession_number: Optional[str] = None,
        match_rule: str,
        applied_rule_names: Optional[str] = None,
        applied_rule_ids: Optional[str] = None,
        destination_results: Optional[str] = None,
        processing_successful: bool = True,
        source_identifier: Optional[str] = None
    ) -> OrderDicomEvidence:
        """
        Create or update DICOM evidence and publish SSE event.
        Same as create_or_update_evidence but with SSE event publishing.
        Uses synchronous event publishing to work with DICOM processing contexts.
        """
        # First, call the regular method to create/update the evidence
        evidence = self.create_or_update_evidence(
            db=db,
            imaging_order_id=imaging_order_id,
            sop_instance_uid=sop_instance_uid,
            study_instance_uid=study_instance_uid,
            series_instance_uid=series_instance_uid,
            accession_number=accession_number,
            match_rule=match_rule,
            applied_rule_names=applied_rule_names,
            applied_rule_ids=applied_rule_ids,
            destination_results=destination_results,
            processing_successful=processing_successful,
            source_identifier=source_identifier
        )
        
        # Publish SSE event (using sync version since this can be called from worker contexts)
        try:
            from app.events import publish_order_event_sync
            from app.schemas import imaging_order as order_schemas
            
            # Get the order to include in the event payload
            from app.crud import imaging_order
            order = imaging_order.get(db, id=imaging_order_id)
            
            if order:
                # Create event payload with order data and evidence info
                payload = {
                    "order": order_schemas.ImagingOrderRead.model_validate(order).model_dump(mode='json'),
                    "evidence": {
                        "id": evidence.id,
                        "sop_instance_uid": evidence.sop_instance_uid,
                        "study_instance_uid": evidence.study_instance_uid,
                        "match_rule": evidence.match_rule,
                        "processing_successful": evidence.processing_successful,
                        "created_at": evidence.created_at.isoformat() if evidence.created_at else None
                    }
                }
                
                publish_order_event_sync(
                    event_type="order_evidence_created",
                    payload=payload
                )
        except Exception as e:
            # Don't fail the evidence creation if event publishing fails
            import structlog
            log = structlog.get_logger(__name__)
            log.warning("Failed to publish evidence SSE event", 
                      error=str(e), imaging_order_id=imaging_order_id, evidence_id=evidence.id)
        
        return evidence
    
    def get_summary_for_order(self, db: Session, *, imaging_order_id: int) -> OrderDicomEvidenceSummary:
        """Get a summary of DICOM evidence for an imaging order."""
        
        # Get basic counts
        evidence_query = db.query(self.model).filter(self.model.imaging_order_id == imaging_order_id)
        
        total_count = evidence_query.count()
        successful_count = evidence_query.filter(self.model.processing_successful == True).count()
        failed_count = total_count - successful_count
        
        # Get unique study count
        unique_studies = db.query(func.count(distinct(self.model.study_instance_uid))).filter(
            self.model.imaging_order_id == imaging_order_id,
            self.model.study_instance_uid.isnot(None)
        ).scalar() or 0
        
        # Get match rules used
        match_rules = db.query(distinct(self.model.match_rule)).filter(
            self.model.imaging_order_id == imaging_order_id
        ).all()
        match_rules_list = [rule[0] for rule in match_rules if rule[0]]
        
        # Get processing timestamps
        time_query = db.query(
            func.min(self.model.processed_at).label('first_processed'),
            func.max(self.model.processed_at).label('last_processed')
        ).filter(self.model.imaging_order_id == imaging_order_id)
        
        time_result = time_query.first()
        first_processed = time_result.first_processed if time_result else None
        last_processed = time_result.last_processed if time_result else None
        
        # Parse destination results to get unique destination names
        # This is a bit tricky since destination_results is JSON text
        destinations_attempted = []
        evidence_with_destinations = evidence_query.filter(
            self.model.destination_results.isnot(None)
        ).all()
        
        dest_set = set()
        for evidence in evidence_with_destinations:
            if evidence.destination_results:
                try:
                    results = json.loads(evidence.destination_results)
                    if isinstance(results, dict):
                        dest_set.update(results.keys())
                except (json.JSONDecodeError, TypeError):
                    continue
        
        destinations_attempted = list(dest_set)
        
        return OrderDicomEvidenceSummary(
            imaging_order_id=imaging_order_id,
            total_dicom_objects=total_count,
            successful_objects=successful_count,
            failed_objects=failed_count,
            unique_studies=unique_studies,
            match_rules_used=match_rules_list,
            destinations_attempted=destinations_attempted,
            first_processed_at=first_processed,
            last_processed_at=last_processed
        )
    
    def get_evidence_for_study(
        self, 
        db: Session, 
        *, 
        study_instance_uid: str
    ) -> List[OrderDicomEvidence]:
        """Get all evidence entries for a specific study."""
        return db.query(self.model).filter(
            self.model.study_instance_uid == study_instance_uid
        ).order_by(self.model.processed_at.desc()).all()
    
    def get_evidence_count_for_order(self, db: Session, *, imaging_order_id: int) -> int:
        """Get the count of DICOM evidence objects for an order (most efficient method)."""
        return db.query(self.model).filter(
            self.model.imaging_order_id == imaging_order_id
        ).count()

    def get_recent_evidence(
        self, 
        db: Session, 
        *, 
        limit: int = 100,
        successful_only: Optional[bool] = None,
        accession_number: Optional[str] = None
    ) -> List[OrderDicomEvidence]:
        """Get recently processed DICOM evidence entries."""
        query = db.query(self.model)
        
        # Add join with imaging_order if we need to filter by accession_number
        if accession_number is not None:
            from app.db.models.imaging_order import ImagingOrder
            query = query.join(ImagingOrder, self.model.imaging_order_id == ImagingOrder.id)
            query = query.filter(ImagingOrder.accession_number == accession_number)
        
        if successful_only is not None:
            query = query.filter(self.model.processing_successful == successful_only)
            
        return query.order_by(self.model.processed_at.desc()).limit(limit).all()


# Create the instance to export
crud_order_dicom_evidence = CRUDOrderDicomEvidence(OrderDicomEvidence)
