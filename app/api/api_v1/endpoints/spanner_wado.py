# app/api/api_v1/endpoints/spanner_wado.py
import logging
from typing import Dict, Any, Optional, List
from datetime import datetime
import asyncio

from fastapi import APIRouter, Depends, HTTPException, status, Query, Request, Path, Response
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session

from app import crud, schemas
from app.db import models
from app.api import deps
from app.services.spanner_engine import SpannerEngine

logger = logging.getLogger(__name__)
router = APIRouter()


@router.get(
    "/studies/{study_uid}",
    summary="Retrieve Study Across Multiple Sources",
    description="WADO-RS study retrieval with automatic source selection.",
    responses={
        status.HTTP_200_OK: {"description": "Study data retrieved successfully."},
        status.HTTP_404_NOT_FOUND: {"description": "Study not found in any source."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
async def retrieve_study_spanning(
    request: Request,
    study_uid: str = Path(..., description="Study Instance UID"),
    spanner_config_id: Optional[int] = Query(None, description="Specific spanner configuration to use"),
    preferred_source: Optional[str] = Query(None, description="Preferred source AE title"),
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
):
    """
    Retrieve a complete study from the first available source.
    
    Uses spanner configuration to determine which sources to check and in what order.
    If preferred_source is specified, it will be tried first.
    """
    logger.info(f"User {current_user.id} retrieving study {study_uid} via spanning")
    
    # Get spanner configuration
    if spanner_config_id:
        spanner_config = crud.crud_spanner_config.get(db, id=spanner_config_id)
        if not spanner_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Spanner config {spanner_config_id} not found"
            )
    else:
        # Use the first available WADO-enabled spanner config
        configs = crud.crud_spanner_config.get_multi(db, include_disabled=False)
        spanner_config = next((c for c in configs if c.supports_wado), None)
        
        if not spanner_config:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No WADO-enabled spanner configurations available"
            )
    
    try:
        # For WADO retrieval, we use a different approach than querying
        # We need to find the study first, then retrieve from the best source
        spanner_engine = SpannerEngine(db)
        
        # First, do a quick query to find which sources have this study
        query_result = await spanner_engine.execute_spanning_query(
            spanner_config_id=spanner_config.id,
            query_type="C-FIND",  # Use C-FIND to locate the study
            query_level="STUDY",
            query_filters={'StudyInstanceUID': study_uid},
            requesting_ip=request.client.host if request.client else None
        )
        
        if not query_result.get('results'):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Study {study_uid} not found in any configured source"
            )
        
        # Select the best source for retrieval
        available_sources = set()
        for result in query_result['results']:
            if 'source_ae_title' in result:
                available_sources.add(result['source_ae_title'])
        
        # If preferred source is available, use it
        if preferred_source and preferred_source in available_sources:
            target_source = preferred_source
        else:
            # Use the first available source (in priority order if configured)
            source_mappings = crud.crud_spanner_source_mapping.get_multi_by_spanner(
                db, spanner_config_id=spanner_config.id, include_disabled=False
            )
            # Sort by priority (lower number = higher priority)
            source_mappings.sort(key=lambda x: x.priority or 999)
            
            target_source = None
            for mapping in source_mappings:
                # Get the appropriate source based on mapping type
                source_ae_title = None
                if mapping.source_type == "dimse-qr" and mapping.dimse_qr_source:
                    source_ae_title = mapping.dimse_qr_source.remote_ae_title
                elif mapping.source_type == "dicomweb" and mapping.dicomweb_source:
                    source_ae_title = str(mapping.dicomweb_source.source_name)  # type: ignore[arg-type]
                elif mapping.source_type == "google_healthcare" and mapping.google_healthcare_source:
                    source_ae_title = mapping.google_healthcare_source.name
                
                if source_ae_title and source_ae_title in available_sources:
                    target_source = source_ae_title
                    break
            
            if not target_source:
                target_source = next(iter(available_sources))
        
        # Now retrieve the study from the selected source
        # This would integrate with your existing WADO-RS implementation
        # For now, return metadata about the retrieval
        
        return {
            "message": f"Study {study_uid} would be retrieved from source {target_source}",
            "study_uid": study_uid,
            "selected_source": target_source,
            "available_sources": list(available_sources),
            "retrieval_method": "WADO-RS",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in spanning WADO study retrieval: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error executing spanning retrieval"
        )


@router.get(
    "/studies/{study_uid}/series/{series_uid}",
    summary="Retrieve Series Across Multiple Sources",
    description="WADO-RS series retrieval with automatic source selection.",
)
async def retrieve_series_spanning(
    request: Request,
    study_uid: str = Path(..., description="Study Instance UID"),
    series_uid: str = Path(..., description="Series Instance UID"),
    db: Session = Depends(deps.get_db),
    spanner_config_id: Optional[int] = Query(None, description="Specific spanner configuration to use"),
    preferred_source: Optional[str] = Query(None, description="Preferred source AE title"),
    current_user: models.User = Depends(deps.get_current_active_user),
):
    """Retrieve a series from the first available source."""
    
    # Get spanner configuration (same logic as study retrieval)
    if spanner_config_id:
        spanner_config = crud.crud_spanner_config.get(db, id=spanner_config_id)
        if not spanner_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Spanner config {spanner_config_id} not found"
            )
    else:
        configs = crud.crud_spanner_config.get_multi(db, include_disabled=False)
        spanner_config = next((c for c in configs if c.supports_wado), None)
        
        if not spanner_config:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No WADO-enabled spanner configurations available"
            )
    
    try:
        spanner_engine = SpannerEngine(db)
        
        # Query to find which sources have this series
        query_result = await spanner_engine.execute_spanning_query(
            spanner_config_id=spanner_config.id,
            query_type="C-FIND",
            query_level="SERIES",
            query_filters={
                'StudyInstanceUID': study_uid,
                'SeriesInstanceUID': series_uid
            },
            requesting_ip=request.client.host if request.client else None
        )
        
        if not query_result.get('results'):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Series {series_uid} not found in any configured source"
            )
        
        # Source selection logic (same as study)
        available_sources = set()
        for result in query_result['results']:
            if 'source_ae_title' in result:
                available_sources.add(result['source_ae_title'])
        
        if preferred_source and preferred_source in available_sources:
            target_source = preferred_source
        else:
            source_mappings = crud.crud_spanner_source_mapping.get_multi_by_spanner(
                db, spanner_config_id=spanner_config.id, include_disabled=False
            )
            source_mappings.sort(key=lambda x: x.priority or 999)
            
            target_source = None
            for mapping in source_mappings:
                # Get the appropriate source based on mapping type
                source_ae_title = None
                if mapping.source_type == "dimse-qr" and mapping.dimse_qr_source:
                    source_ae_title = mapping.dimse_qr_source.remote_ae_title
                elif mapping.source_type == "dicomweb" and mapping.dicomweb_source:
                    source_ae_title = str(mapping.dicomweb_source.source_name)  # type: ignore[arg-type]
                elif mapping.source_type == "google_healthcare" and mapping.google_healthcare_source:
                    source_ae_title = mapping.google_healthcare_source.name
                
                if source_ae_title and source_ae_title in available_sources:
                    target_source = source_ae_title
                    break
            
            if not target_source:
                target_source = next(iter(available_sources))
        
        return {
            "message": f"Series {series_uid} would be retrieved from source {target_source}",
            "study_uid": study_uid,
            "series_uid": series_uid,
            "selected_source": target_source,
            "available_sources": list(available_sources),
            "retrieval_method": "WADO-RS",
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in spanning WADO series retrieval: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error executing spanning retrieval"
        )


@router.get(
    "/studies/{study_uid}/series/{series_uid}/instances/{instance_uid}",
    summary="Retrieve Instance Across Multiple Sources",
    description="WADO-RS instance retrieval with automatic source selection.",
)
async def retrieve_instance_spanning(
    request: Request,
    study_uid: str = Path(..., description="Study Instance UID"),
    series_uid: str = Path(..., description="Series Instance UID"),
    instance_uid: str = Path(..., description="SOP Instance UID"),
    db: Session = Depends(deps.get_db),
    spanner_config_id: Optional[int] = Query(None, description="Specific spanner configuration to use"),
    preferred_source: Optional[str] = Query(None, description="Preferred source AE title"),
    accept_header: Optional[str] = Query(None, description="DICOM content type preferences"),
    current_user: models.User = Depends(deps.get_current_active_user),
):
    """Retrieve an instance from the first available source."""
    
    # Get spanner configuration
    if spanner_config_id:
        spanner_config = crud.crud_spanner_config.get(db, id=spanner_config_id)
        if not spanner_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Spanner config {spanner_config_id} not found"
            )
    else:
        configs = crud.crud_spanner_config.get_multi(db, include_disabled=False)
        spanner_config = next((c for c in configs if c.supports_wado), None)
        
        if not spanner_config:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No WADO-enabled spanner configurations available"
            )
    
    try:
        spanner_engine = SpannerEngine(db)
        
        # Query to find which sources have this instance
        query_result = await spanner_engine.execute_spanning_query(
            spanner_config_id=spanner_config.id,
            query_type="C-FIND",
            query_level="INSTANCE",
            query_filters={
                'StudyInstanceUID': study_uid,
                'SeriesInstanceUID': series_uid,
                'SOPInstanceUID': instance_uid
            },
            requesting_ip=request.client.host if request.client else None
        )
        
        if not query_result.get('results'):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Instance {instance_uid} not found in any configured source"
            )
        
        # Source selection logic
        available_sources = set()
        for result in query_result['results']:
            if 'source_ae_title' in result:
                available_sources.add(result['source_ae_title'])
        
        if preferred_source and preferred_source in available_sources:
            target_source = preferred_source
        else:
            source_mappings = crud.crud_spanner_source_mapping.get_multi_by_spanner(
                db, spanner_config_id=spanner_config.id, include_disabled=False
            )
            source_mappings.sort(key=lambda x: x.priority or 999)
            
            target_source = None
            for mapping in source_mappings:
                # Get the appropriate source based on mapping type
                source_ae_title = None
                if mapping.source_type == "dimse-qr" and mapping.dimse_qr_source:
                    source_ae_title = mapping.dimse_qr_source.remote_ae_title
                elif mapping.source_type == "dicomweb" and mapping.dicomweb_source:
                    source_ae_title = str(mapping.dicomweb_source.source_name)  # type: ignore[arg-type]
                elif mapping.source_type == "google_healthcare" and mapping.google_healthcare_source:
                    source_ae_title = mapping.google_healthcare_source.name
                
                if source_ae_title and source_ae_title in available_sources:
                    target_source = source_ae_title
                    break
            
            if not target_source:
                target_source = next(iter(available_sources))
        
        # In a real implementation, you would now:
        # 1. Connect to the selected source
        # 2. Retrieve the actual DICOM instance
        # 3. Stream it back to the client with proper content-type headers
        
        return {
            "message": f"Instance {instance_uid} would be retrieved from source {target_source}",
            "study_uid": study_uid,
            "series_uid": series_uid,
            "instance_uid": instance_uid,
            "selected_source": target_source,
            "available_sources": list(available_sources),
            "retrieval_method": "WADO-RS",
            "content_type_requested": accept_header,
            "timestamp": datetime.utcnow().isoformat()
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in spanning WADO instance retrieval: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error executing spanning retrieval"
        )


@router.get(
    "/studies/{study_uid}/metadata",
    summary="Get Study Metadata from Best Source",
    description="Retrieve study metadata using source selection logic.",
)
async def get_study_metadata_spanning(
    request: Request,
    study_uid: str = Path(..., description="Study Instance UID"),
    db: Session = Depends(deps.get_db),
    spanner_config_id: Optional[int] = Query(None, description="Specific spanner configuration to use"),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Dict[str, Any]:
    """Get comprehensive metadata for a study from the best available source."""
    
    # Get spanner configuration
    if spanner_config_id:
        spanner_config = crud.crud_spanner_config.get(db, id=spanner_config_id)
        if not spanner_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Spanner config {spanner_config_id} not found"
            )
    else:
        configs = crud.crud_spanner_config.get_multi(db, include_disabled=False)
        spanner_config = next((c for c in configs if c.supports_wado), None)
        
        if not spanner_config:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No WADO-enabled spanner configurations available"
            )
    
    try:
        spanner_engine = SpannerEngine(db)
        
        # Get study metadata from all available sources
        query_result = await spanner_engine.execute_spanning_query(
            spanner_config_id=spanner_config.id,
            query_type="C-FIND",
            query_level="STUDY",
            query_filters={'StudyInstanceUID': study_uid},
            requesting_ip=request.client.host if request.client else None
        )
        
        if not query_result.get('results'):
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Study {study_uid} not found in any configured source"
            )
        
        # Combine metadata from all sources
        metadata = {
            "study_uid": study_uid,
            "sources": [],
            "consolidated_metadata": {},
            "retrieval_timestamp": datetime.utcnow().isoformat()
        }
        
        for result in query_result['results']:
            source_info = {
                "source_ae_title": result.get('source_ae_title', 'Unknown'),
                "metadata": result
            }
            metadata["sources"].append(source_info)
        
        # Create consolidated metadata (use first result as base)
        if query_result['results']:
            metadata["consolidated_metadata"] = query_result['results'][0].copy()
            metadata["consolidated_metadata"]["available_source_count"] = len(query_result['results'])
        
        return metadata
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting spanning study metadata: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving study metadata"
        )
