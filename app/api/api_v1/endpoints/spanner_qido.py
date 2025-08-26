# app/api/api_v1/endpoints/spanner_qido.py
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, status, Query, Request, Path
from sqlalchemy.orm import Session
import structlog

from app import crud, schemas
from app.db import models
from app.api import deps
from app.services.spanner_engine import SpannerEngine

logger = structlog.get_logger(__name__)
router = APIRouter()


@router.get(
    "/studies",
    summary="Search for Studies Across Multiple Sources",
    description="QIDO-RS studies search with query spanning across configured DICOM sources.",
    response_model=List[Dict[str, Any]],
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid query parameters."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
async def search_studies_spanning(
    request: Request,
    db: Session = Depends(deps.get_db),
    # Standard QIDO-RS query parameters
    PatientID: Optional[str] = Query(None, description="Patient ID"),
    PatientName: Optional[str] = Query(None, description="Patient Name"),
    StudyInstanceUID: Optional[str] = Query(None, description="Study Instance UID"),
    StudyDate: Optional[str] = Query(None, description="Study Date (YYYYMMDD or range)"),
    StudyTime: Optional[str] = Query(None, description="Study Time"),
    AccessionNumber: Optional[str] = Query(None, description="Accession Number"),
    ModalitiesInStudy: Optional[str] = Query(None, description="Modalities in Study"),
    StudyDescription: Optional[str] = Query(None, description="Study Description"),
    # Spanner-specific parameters
    spanner_config_id: Optional[int] = Query(None, description="Specific spanner configuration to use"),
    limit: Optional[int] = Query(None, ge=1, le=1000, description="Maximum number of results"),
    offset: Optional[int] = Query(0, ge=0, description="Number of results to skip"),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> List[Dict[str, Any]]:
    """
    Search for studies across multiple DICOM sources using query spanning.
    
    This endpoint implements the QIDO-RS /studies search but spreads the query
    across multiple configured sources and aggregates the results.
    """
    logger.info(f"User {current_user.id} performing spanning QIDO studies search")
    
    # Build query filters from parameters
    query_filters = {}
    
    if PatientID:
        query_filters['PatientID'] = PatientID
    if PatientName:
        query_filters['PatientName'] = PatientName
    if StudyInstanceUID:
        query_filters['StudyInstanceUID'] = StudyInstanceUID
    if StudyDate:
        query_filters['StudyDate'] = StudyDate
    if StudyTime:
        query_filters['StudyTime'] = StudyTime
    if AccessionNumber:
        query_filters['AccessionNumber'] = AccessionNumber
    if ModalitiesInStudy:
        query_filters['ModalitiesInStudy'] = ModalitiesInStudy
    if StudyDescription:
        query_filters['StudyDescription'] = StudyDescription
    
    # Get spanner configuration
    if spanner_config_id:
        spanner_config = crud.crud_spanner_config.get(db, id=spanner_config_id)
        if not spanner_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Spanner config {spanner_config_id} not found"
            )
    else:
        # Use the first available QIDO-enabled spanner config
        configs = crud.crud_spanner_config.get_multi(db, include_disabled=False)
        spanner_config = next((c for c in configs if c.supports_qido), None)
        
        if not spanner_config:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No QIDO-enabled spanner configurations available"
            )
    
    try:
        # Execute spanning query
        spanner_engine = SpannerEngine(db)
        spanning_result = await spanner_engine.execute_spanning_query(
            spanner_config_id=spanner_config.id,
            query_type="QIDO",
            query_level="STUDY",
            query_filters=query_filters,
            requesting_ip=request.client.host if request.client else None
        )
        
        results = spanning_result.get('results', [])
        
        # Apply pagination if requested
        if offset or limit:
            start_idx = offset or 0
            end_idx = start_idx + limit if limit else len(results)
            results = results[start_idx:end_idx]
        
        # Add spanning metadata to response headers
        # Note: In a real implementation, you might want to use custom headers
        logger.info(f"QIDO spanning query completed: {spanning_result['metadata']}")
        
        return results
        
    except Exception as e:
        logger.error(f"Error in spanning QIDO studies search: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error executing spanning query"
        )


@router.get(
    "/studies/{study_uid}/series",
    summary="Search for Series in Study Across Multiple Sources",
    description="QIDO-RS series search within a study with query spanning.",
    response_model=List[Dict[str, Any]],
)
async def search_series_spanning(
    request: Request,
    study_uid: str = Path(..., description="Study Instance UID"),
    db: Session = Depends(deps.get_db),
    # Standard QIDO-RS query parameters for series
    SeriesInstanceUID: Optional[str] = Query(None, description="Series Instance UID"),
    Modality: Optional[str] = Query(None, description="Modality"),
    SeriesNumber: Optional[str] = Query(None, description="Series Number"),
    SeriesDescription: Optional[str] = Query(None, description="Series Description"),
    # Spanner-specific parameters
    spanner_config_id: Optional[int] = Query(None, description="Specific spanner configuration to use"),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> List[Dict[str, Any]]:
    """Search for series within a study across multiple sources."""
    
    # Build query filters
    query_filters = {'StudyInstanceUID': study_uid}
    
    if SeriesInstanceUID:
        query_filters['SeriesInstanceUID'] = SeriesInstanceUID
    if Modality:
        query_filters['Modality'] = Modality
    if SeriesNumber:
        query_filters['SeriesNumber'] = SeriesNumber
    if SeriesDescription:
        query_filters['SeriesDescription'] = SeriesDescription
    
    # Get spanner configuration (same logic as studies)
    if spanner_config_id:
        spanner_config = crud.crud_spanner_config.get(db, id=spanner_config_id)
        if not spanner_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Spanner config {spanner_config_id} not found"
            )
    else:
        configs = crud.crud_spanner_config.get_multi(db, include_disabled=False)
        spanner_config = next((c for c in configs if c.supports_qido), None)
        
        if not spanner_config:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No QIDO-enabled spanner configurations available"
            )
    
    try:
        # Execute spanning query
        spanner_engine = SpannerEngine(db)
        spanning_result = await spanner_engine.execute_spanning_query(
            spanner_config_id=spanner_config.id,
            query_type="QIDO",
            query_level="SERIES",
            query_filters=query_filters,
            requesting_ip=request.client.host if request.client else None
        )
        
        return spanning_result.get('results', [])
        
    except Exception as e:
        logger.error(f"Error in spanning QIDO series search: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error executing spanning query"
        )


@router.get(
    "/studies/{study_uid}/series/{series_uid}/instances",
    summary="Search for Instances in Series Across Multiple Sources",
    description="QIDO-RS instances search within a series with query spanning.",
    response_model=List[Dict[str, Any]],
)
async def search_instances_spanning(
    request: Request,
    study_uid: str = Path(..., description="Study Instance UID"),
    series_uid: str = Path(..., description="Series Instance UID"),
    db: Session = Depends(deps.get_db),
    # Standard QIDO-RS query parameters for instances
    SOPInstanceUID: Optional[str] = Query(None, description="SOP Instance UID"),
    SOPClassUID: Optional[str] = Query(None, description="SOP Class UID"),
    InstanceNumber: Optional[str] = Query(None, description="Instance Number"),
    # Spanner-specific parameters
    spanner_config_id: Optional[int] = Query(None, description="Specific spanner configuration to use"),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> List[Dict[str, Any]]:
    """Search for instances within a series across multiple sources."""
    
    # Build query filters
    query_filters = {
        'StudyInstanceUID': study_uid,
        'SeriesInstanceUID': series_uid
    }
    
    if SOPInstanceUID:
        query_filters['SOPInstanceUID'] = SOPInstanceUID
    if SOPClassUID:
        query_filters['SOPClassUID'] = SOPClassUID
    if InstanceNumber:
        query_filters['InstanceNumber'] = InstanceNumber
    
    # Get spanner configuration (same logic as studies)
    if spanner_config_id:
        spanner_config = crud.crud_spanner_config.get(db, id=spanner_config_id)
        if not spanner_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Spanner config {spanner_config_id} not found"
            )
    else:
        configs = crud.crud_spanner_config.get_multi(db, include_disabled=False)
        spanner_config = next((c for c in configs if c.supports_qido), None)
        
        if not spanner_config:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No QIDO-enabled spanner configurations available"
            )
    
    try:
        # Execute spanning query
        spanner_engine = SpannerEngine(db)
        spanning_result = await spanner_engine.execute_spanning_query(
            spanner_config_id=spanner_config.id,
            query_type="QIDO",
            query_level="INSTANCE",
            query_filters=query_filters,
            requesting_ip=request.client.host if request.client else None
        )
        
        return spanning_result.get('results', [])
        
    except Exception as e:
        logger.error(f"Error in spanning QIDO instances search: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error executing spanning query"
        )


@router.get(
    "/metadata",
    summary="Get Spanning Query Metadata",
    description="Get information about available spanner configurations and their capabilities.",
    response_model=Dict[str, Any],
)
def get_spanning_metadata(
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Dict[str, Any]:
    """Get metadata about available spanning configurations."""
    
    configs = crud.crud_spanner_config.get_multi(db, include_disabled=False)
    qido_configs = [c for c in configs if c.supports_qido]
    
    metadata = {
        "available_spanner_configs": len(qido_configs),
        "supported_query_levels": ["STUDY", "SERIES", "INSTANCE"],
        "supported_protocols": ["QIDO-RS"],
        "configurations": [
            {
                "id": config.id,
                "name": config.name,
                "description": config.description,
                "max_concurrent_sources": config.max_concurrent_sources,
                "query_timeout_seconds": config.query_timeout_seconds,
                "failure_strategy": config.failure_strategy,
                "deduplication_strategy": config.deduplication_strategy,
                "source_count": len(crud.crud_spanner_source_mapping.get_multi_by_spanner(
                    db, spanner_config_id=config.id, include_disabled=False
                ))
            }
            for config in qido_configs
        ]
    }
    
    return metadata
