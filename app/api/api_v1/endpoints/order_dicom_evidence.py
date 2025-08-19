# app/api/api_v1/endpoints/order_dicom_evidence.py
"""
API endpoints for order DICOM evidence tracking.
Provides views of which DICOM objects matched and fulfilled specific orders.
"""

from typing import Optional, List
from fastapi import APIRouter, Depends, Query, HTTPException, status
from sqlalchemy.orm import Session

from app.crud.crud_order_dicom_evidence import crud_order_dicom_evidence
from app.crud.crud_imaging_order import imaging_order
from app.db.models.user import User
from app.schemas.order_dicom_evidence import OrderDicomEvidenceRead, OrderDicomEvidenceSummary
from app.api import deps

router = APIRouter()


@router.get(
    "/",
    response_model=List[OrderDicomEvidenceRead],
    summary="List Recent Order DICOM Evidence"
)
def list_recent_evidence(
    db: Session = Depends(deps.get_db),
    limit: int = Query(100, ge=1, le=1000, description="Maximum number of records to return"),
    successful_only: Optional[bool] = Query(None, description="Filter by processing success status"),
    accession_number: Optional[str] = Query(None, description="Filter by order accession number"),
    current_user: User = Depends(deps.get_current_active_user)
):
    """List recent order DICOM evidence."""
    return crud_order_dicom_evidence.get_recent_evidence(
        db, limit=limit, successful_only=successful_only, accession_number=accession_number
    )


@router.get(
    "/order/{order_id}",
    response_model=List[OrderDicomEvidenceRead],
    summary="Get Evidence for Specific Order"
)
def get_order_evidence(
    order_id: int,
    db: Session = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user)
):
    """Get all DICOM evidence for a specific order."""
    # Verify order exists
    order = imaging_order.get(db, id=order_id)
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Imaging order with ID {order_id} not found"
        )
    
    return crud_order_dicom_evidence.get_by_order_id(db, imaging_order_id=order_id)


@router.get(
    "/order/{order_id}/summary",
    response_model=OrderDicomEvidenceSummary,
    summary="Get Evidence Summary for Order"
)
def get_order_evidence_summary(
    order_id: int,
    db: Session = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user)
):
    """Get summary statistics for DICOM evidence of a specific order."""
    # Verify order exists
    order = imaging_order.get(db, id=order_id)
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Imaging order with ID {order_id} not found"
        )
    
    return crud_order_dicom_evidence.get_summary_for_order(db, imaging_order_id=order_id)


@router.get(
    "/order/{order_id}/count",
    response_model=int,
    summary="Get Evidence Count for Order (Most Efficient)"
)
def get_order_evidence_count(
    order_id: int,
    db: Session = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user)
):
    """Get the count of DICOM evidence objects for a specific order (most efficient method)."""
    # Verify order exists
    order = imaging_order.get(db, id=order_id)
    if not order:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Imaging order with ID {order_id} not found"
        )
    
    return crud_order_dicom_evidence.get_evidence_count_for_order(db, imaging_order_id=order_id)


@router.get(
    "/sop/{sop_instance_uid}",
    response_model=OrderDicomEvidenceRead,
    summary="Get Evidence by SOP Instance UID"
)
def get_evidence_by_sop_uid(
    sop_instance_uid: str,
    db: Session = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user)
):
    """Get evidence for a specific DICOM object by SOP Instance UID."""
    evidence = crud_order_dicom_evidence.get_by_sop_instance_uid(
        db, sop_instance_uid=sop_instance_uid
    )
    
    if not evidence:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"No evidence found for SOP Instance UID: {sop_instance_uid}"
        )
    
    return evidence


@router.get(
    "/study/{study_instance_uid}",
    response_model=List[OrderDicomEvidenceRead],
    summary="Get Evidence for Study"
)
def get_evidence_by_study_uid(
    study_instance_uid: str,
    db: Session = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_user)
):
    """Get all evidence for DICOM objects in a specific study."""
    return crud_order_dicom_evidence.get_evidence_for_study(
        db, study_instance_uid=study_instance_uid
    )
