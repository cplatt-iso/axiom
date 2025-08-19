# app/services/order_matching_service.py
"""
Service for matching DICOM objects to imaging orders and recording evidence.
This is the core logic that links orders to the DICOM objects that fulfill them.
"""

import json
import structlog
from typing import Optional, Dict, Any, List, Tuple
from sqlalchemy.orm import Session
import pydicom

from app.crud.crud_imaging_order import imaging_order
from app.crud.crud_order_dicom_evidence import crud_order_dicom_evidence
from app.db.models.imaging_order import ImagingOrder

logger = structlog.get_logger(__name__)


class OrderMatchingService:
    """
    Service responsible for matching DICOM objects to orders and recording evidence.
    
    The matching logic tries multiple strategies:
    1. Accession Number match (most common)
    2. Study Instance UID match (for orders that have it pre-populated)
    3. Future: Could add patient ID + modality + date matching
    """
    
    def __init__(self, db_session: Session):
        self.db = db_session
        self.logger = logger
    
    def find_matching_orders(
        self, 
        dataset: pydicom.Dataset
    ) -> List[Tuple[ImagingOrder, str]]:
        """
        Find all orders that match this DICOM dataset.
        
        Returns:
            List of tuples: (ImagingOrder, match_rule_used)
            
        The match_rule_used values are:
        - "ACCESSION_NUMBER": Matched on AccessionNumber tag
        - "STUDY_INSTANCE_UID": Matched on StudyInstanceUID tag  
        - "PATIENT_COMPOSITE": Matched on patient ID + other criteria (future)
        """
        matches = []
        log = self.logger.bind(
            sop_instance_uid=getattr(dataset, 'SOPInstanceUID', 'Unknown'),
            study_uid=getattr(dataset, 'StudyInstanceUID', 'Unknown')
        )
        
        # Extract key identifiers from DICOM
        accession_number = getattr(dataset, 'AccessionNumber', None)
        study_instance_uid = getattr(dataset, 'StudyInstanceUID', None)
        patient_id = getattr(dataset, 'PatientID', None)
        
        log = log.bind(
            accession_number=accession_number,
            study_instance_uid=study_instance_uid,
            patient_id=patient_id
        )
        
        # Strategy 1: Match on Accession Number (primary method)
        if accession_number and accession_number.strip():
            log.debug("Attempting accession number match")
            order = imaging_order.get_by_accession_number(
                self.db, accession_number=accession_number.strip()
            )
            if order:
                matches.append((order, "ACCESSION_NUMBER"))
                log.info("Found order by accession number", order_id=order.id)
        
        # Strategy 2: Match on Study Instance UID (for pre-scheduled orders)
        if study_instance_uid and study_instance_uid.strip():
            log.debug("Attempting study instance UID match")
            # Need to add this method to CRUD if it doesn't exist
            orders = self._find_orders_by_study_uid(study_instance_uid.strip())
            for order in orders:
                # Avoid duplicates if we already matched on accession number
                if not any(existing_order.id == order.id for existing_order, _ in matches):
                    matches.append((order, "STUDY_INSTANCE_UID"))
                    log.info("Found order by study instance UID", order_id=order.id)
        
        # Strategy 3: Future - Patient ID + Modality + Date composite matching
        # This would be useful for orders that don't have accession numbers populated
        # or where the DICOM accession number doesn't match the order system
        
        if not matches:
            log.debug("No matching orders found for DICOM object")
        else:
            log.info("Found matching orders", match_count=len(matches))
            
        return matches
    
    def record_dicom_evidence(
        self,
        *,
        matched_orders: List[Tuple[ImagingOrder, str]],
        dataset: pydicom.Dataset,
        applied_rule_names: List[str],
        applied_rule_ids: List[int],
        destination_results: Dict[str, Dict[str, Any]],
        processing_successful: bool,
        source_identifier: str
    ) -> List[int]:
        """
        Record evidence that DICOM objects matching orders were processed.
        
        Args:
            matched_orders: List of (order, match_rule) tuples from find_matching_orders()
            dataset: The processed DICOM dataset
            applied_rule_names: List of rule names that processed this DICOM
            applied_rule_ids: List of rule IDs that processed this DICOM  
            destination_results: Dict of destination results from processing
            processing_successful: Whether processing was overall successful
            source_identifier: Identifier of the source system
            
        Returns:
            List of evidence IDs that were created/updated
        """
        evidence_ids = []
        
        # Extract DICOM identifiers
        sop_instance_uid = getattr(dataset, 'SOPInstanceUID', f'Unknown_{id(dataset)}')
        study_instance_uid = getattr(dataset, 'StudyInstanceUID', None)
        series_instance_uid = getattr(dataset, 'SeriesInstanceUID', None)
        accession_number = getattr(dataset, 'AccessionNumber', None)
        
        # Serialize rule information
        rule_names_str = ','.join(applied_rule_names) if applied_rule_names else None
        rule_ids_str = ','.join(map(str, applied_rule_ids)) if applied_rule_ids else None
        
        # Serialize destination results
        destination_results_str = json.dumps(destination_results) if destination_results else None
        
        log = self.logger.bind(
            sop_instance_uid=sop_instance_uid,
            matched_order_count=len(matched_orders)
        )
        
        # Record evidence for each matched order
        for order, match_rule in matched_orders:
            try:
                evidence = crud_order_dicom_evidence.create_or_update_evidence_with_events(
                    db=self.db,
                    imaging_order_id=order.id,
                    sop_instance_uid=sop_instance_uid,
                    study_instance_uid=study_instance_uid,
                    series_instance_uid=series_instance_uid,
                    accession_number=accession_number,
                    match_rule=match_rule,
                    applied_rule_names=rule_names_str,
                    applied_rule_ids=rule_ids_str,
                    destination_results=destination_results_str,
                    processing_successful=processing_successful,
                    source_identifier=source_identifier
                )
                
                evidence_ids.append(evidence.id)
                
                log.info(
                    "Recorded DICOM evidence for order",
                    order_id=order.id,
                    evidence_id=evidence.id,
                    match_rule=match_rule,
                    accession_number=order.accession_number
                )
                
            except Exception as e:
                log.error(
                    "Failed to record DICOM evidence",
                    order_id=order.id,
                    match_rule=match_rule,
                    error=str(e),
                    exc_info=True
                )
                # Continue processing other matched orders even if one fails
                continue
        
        return evidence_ids
    
    def _find_orders_by_study_uid(self, study_instance_uid: str) -> List[ImagingOrder]:
        """
        Find orders by study instance UID.
        
        This is for cases where the order was pre-populated with a study UID
        (e.g., from a prior exam or pre-scheduled procedure).
        """
        return self.db.query(ImagingOrder).filter(
            ImagingOrder.study_instance_uid == study_instance_uid
        ).all()
    
    def process_dicom_with_order_matching(
        self,
        *,
        dataset: pydicom.Dataset,
        applied_rule_names: List[str],
        applied_rule_ids: List[int], 
        destination_results: Dict[str, Dict[str, Any]],
        processing_successful: bool,
        source_identifier: str
    ) -> Optional[Dict[str, Any]]:
        """
        Complete workflow: Find matching orders and record evidence.
        
        This is the main entry point called from the DICOM processing pipeline.
        
        Returns:
            Dict with matching results, or None if no matches found:
            {
                'matched_orders_count': int,
                'evidence_ids': List[int],
                'match_rules_used': List[str]
            }
        """
        log = self.logger.bind(
            sop_instance_uid=getattr(dataset, 'SOPInstanceUID', 'Unknown'),
            source_identifier=source_identifier
        )
        
        # Find matching orders
        matched_orders = self.find_matching_orders(dataset)
        
        if not matched_orders:
            log.debug("No orders matched this DICOM object - no evidence recorded")
            return None
        
        # Record evidence for all matched orders  
        evidence_ids = self.record_dicom_evidence(
            matched_orders=matched_orders,
            dataset=dataset,
            applied_rule_names=applied_rule_names,
            applied_rule_ids=applied_rule_ids,
            destination_results=destination_results,
            processing_successful=processing_successful,
            source_identifier=source_identifier
        )
        
        match_rules_used = [match_rule for _, match_rule in matched_orders]
        
        result = {
            'matched_orders_count': len(matched_orders),
            'evidence_ids': evidence_ids,
            'match_rules_used': match_rules_used
        }
        
        log.info(
            "Completed order matching workflow",
            **result
        )
        
        return result


def create_order_matching_service(db_session: Session) -> OrderMatchingService:
    """Factory function to create an OrderMatchingService instance."""
    return OrderMatchingService(db_session)
