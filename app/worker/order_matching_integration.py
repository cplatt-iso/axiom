# app/worker/order_matching_integration.py
"""
Integration layer for order matching in the DICOM processing pipeline.
This module provides helper functions to integrate order matching seamlessly
into existing executor workflows.
"""

import structlog
from typing import Dict, Any, List, Optional
from sqlalchemy.orm import Session
import pydicom

from app.services.order_matching_service import create_order_matching_service

logger = structlog.get_logger(__name__)


def process_order_matching_for_dicom(
    *,
    db_session: Session,
    dataset: pydicom.Dataset,
    applied_rules: List[str],
    destination_statuses: Dict[str, Dict[str, Any]],
    processing_successful: bool,
    source_identifier: str
) -> Optional[Dict[str, Any]]:
    """
    Process order matching for a DICOM object that has been through the rules engine.
    
    This function should be called by executors after DICOM processing is complete
    but before the final return/cleanup.
    
    Args:
        db_session: Active database session
        dataset: The processed DICOM dataset  
        applied_rules: List of rule names that processed this DICOM (from orchestrator)
        destination_statuses: Dict of destination results from processing
        processing_successful: Whether overall processing was successful
        source_identifier: Source system identifier
        
    Returns:
        Dict with order matching results or None if no orders matched:
        {
            'matched_orders_count': int,
            'evidence_ids': List[int], 
            'match_rules_used': List[str]
        }
    """
    log = logger.bind(
        sop_instance_uid=getattr(dataset, 'SOPInstanceUID', 'Unknown'),
        source_identifier=source_identifier
    )
    
    try:
        # Create order matching service
        order_service = create_order_matching_service(db_session)
        
        # Extract rule IDs from applied_rules strings
        # applied_rules format is like: ["RuleSetName/RuleName (ID:123)", ...]
        applied_rule_ids = []
        applied_rule_names = []
        
        for rule_str in applied_rules:
            # Extract rule name and ID from the string
            try:
                # Split on " (ID:" to separate name from ID
                if " (ID:" in rule_str:
                    name_part, id_part = rule_str.rsplit(" (ID:", 1)
                    rule_id = int(id_part.rstrip(")"))
                    applied_rule_ids.append(rule_id)
                    applied_rule_names.append(name_part)
                else:
                    # Fallback if format is unexpected
                    applied_rule_names.append(rule_str)
            except (ValueError, IndexError) as e:
                log.warning("Failed to parse rule string", rule_str=rule_str, error=str(e))
                applied_rule_names.append(rule_str)
        
        # Process the order matching workflow
        result = order_service.process_dicom_with_order_matching(
            dataset=dataset,
            applied_rule_names=applied_rule_names,
            applied_rule_ids=applied_rule_ids,
            destination_results=destination_statuses,
            processing_successful=processing_successful,
            source_identifier=source_identifier
        )
        
        if result:
            log.info(
                "Order matching completed successfully",
                matched_orders=result['matched_orders_count'],
                evidence_records=len(result['evidence_ids']),
                match_rules=result['match_rules_used']
            )
        else:
            log.debug("No orders matched this DICOM object")
            
        return result
        
    except Exception as e:
        log.error(
            "Failed to process order matching",
            error=str(e),
            exc_info=True
        )
        # Don't fail the entire processing pipeline if order matching fails
        # This is supplementary functionality
        return None


def commit_order_matching_session(db_session: Session) -> bool:
    """
    Commit the database session after order matching operations.
    
    Args:
        db_session: Database session to commit
        
    Returns:
        True if commit was successful, False otherwise
    """
    log = logger.bind()
    
    try:
        db_session.commit()
        log.debug("Order matching database session committed successfully")
        return True
    except Exception as e:
        log.error("Failed to commit order matching session", error=str(e), exc_info=True)
        try:
            db_session.rollback()
            log.debug("Order matching session rolled back after commit failure")
        except Exception as rollback_err:
            log.error("Failed to rollback after commit failure", error=str(rollback_err))
        return False
