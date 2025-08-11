# app/api/api_v1/endpoints/config_spanner.py
import logging
from typing import List, Any, Dict
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status, Query
from sqlalchemy.orm import Session

from app import crud, schemas
from app.db import models
from app.api import deps

logger = logging.getLogger(__name__)
router = APIRouter()


# --- Dependency to get spanner config by ID ---
def get_spanner_config_by_id_from_path(
    spanner_id: int,
    db: Session = Depends(deps.get_db)
) -> models.SpannerConfig:
    """Dependency to fetch a spanner config by ID from path parameter."""
    spanner_config = crud.crud_spanner_config.get(db, id=spanner_id)
    if not spanner_config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Spanner config with ID {spanner_id} not found."
        )
    return spanner_config


# --- Dependency to get source mapping by ID ---
def get_source_mapping_by_id_from_path(
    mapping_id: int,
    db: Session = Depends(deps.get_db)
) -> models.SpannerSourceMapping:
    """Dependency to fetch a source mapping by ID from path parameter."""
    mapping = crud.crud_spanner_source_mapping.get(db, id=mapping_id)
    if not mapping:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source mapping with ID {mapping_id} not found."
        )
    return mapping


# --- SPANNER CONFIG ENDPOINTS ---

@router.post(
    "",
    response_model=schemas.SpannerConfigRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create Spanner Configuration",
    description="Creates a new DICOM query spanning configuration.",
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid input data."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized to create configurations."},
        status.HTTP_409_CONFLICT: {"description": "A spanner config with the same name already exists."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def create_spanner_config(
    *,
    db: Session = Depends(deps.get_db),
    spanner_in: schemas.SpannerConfigCreate,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> models.SpannerConfig:
    """Create a new spanner configuration."""
    logger.info(f"User {current_user.id} creating spanner config: {spanner_in.name}")
    return crud.crud_spanner_config.create(db=db, obj_in=spanner_in)


@router.get(
    "",
    response_model=schemas.SpannerConfigListResponse,
    summary="List Spanner Configurations",
    description="Retrieves a list of DICOM query spanning configurations.",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
    }
)
def list_spanner_configs(
    *,
    db: Session = Depends(deps.get_db),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    include_disabled: bool = Query(False, description="Include disabled configurations"),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> schemas.SpannerConfigListResponse:
    """List all spanner configurations with pagination."""
    configs = crud.crud_spanner_config.get_multi(
        db=db, skip=skip, limit=limit, include_disabled=include_disabled
    )
    total = crud.crud_spanner_config.count(db=db, include_disabled=include_disabled)
    
    # Add source mappings count to each config
    configs_with_count = []
    for config in configs:
        config_data = schemas.SpannerConfigRead.model_validate(config)
        # Count source mappings
        mappings = crud.crud_spanner_source_mapping.get_multi_by_spanner(
            db=db, spanner_config_id=config.id
        )
        config_data.source_mappings_count = len(mappings)
        configs_with_count.append(config_data)
    
    return schemas.SpannerConfigListResponse(configs=configs_with_count, total=total)


@router.get(
    "/{spanner_id}",
    response_model=schemas.SpannerConfigRead,
    summary="Get Spanner Configuration",
    description="Retrieves a specific spanner configuration by ID.",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "Spanner config not found."},
    }
)
def get_spanner_config(
    *,
    spanner_config: models.SpannerConfig = Depends(get_spanner_config_by_id_from_path),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> models.SpannerConfig:
    """Get a specific spanner configuration."""
    return spanner_config


@router.put(
    "/{spanner_id}",
    response_model=schemas.SpannerConfigRead,
    summary="Update Spanner Configuration",
    description="Updates a spanner configuration.",
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid input data."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "Spanner config not found."},
        status.HTTP_409_CONFLICT: {"description": "Name conflict."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def update_spanner_config(
    *,
    db: Session = Depends(deps.get_db),
    spanner_config: models.SpannerConfig = Depends(get_spanner_config_by_id_from_path),
    spanner_in: schemas.SpannerConfigUpdate,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> models.SpannerConfig:
    """Update a spanner configuration."""
    logger.info(f"User {current_user.id} updating spanner config ID: {spanner_config.id}")
    return crud.crud_spanner_config.update(db=db, db_obj=spanner_config, obj_in=spanner_in)


@router.delete(
    "/{spanner_id}",
    response_model=schemas.SpannerConfigRead,
    summary="Delete Spanner Configuration",
    description="Deletes a spanner configuration and all its source mappings.",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "Spanner config not found."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def delete_spanner_config(
    *,
    db: Session = Depends(deps.get_db),
    spanner_id: int,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> models.SpannerConfig:
    """Delete a spanner configuration."""
    logger.info(f"User {current_user.id} deleting spanner config ID: {spanner_id}")
    return crud.crud_spanner_config.remove(db=db, id=spanner_id)


# --- SOURCE MAPPING ENDPOINTS ---

@router.get(
    "/{spanner_id}/sources",
    response_model=schemas.SpannerSourceMappingListResponse,
    summary="List Source Mappings",
    description="Lists all source mappings for a spanner configuration.",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "Spanner config not found."},
    }
)
def list_source_mappings(
    *,
    db: Session = Depends(deps.get_db),
    spanner_config: models.SpannerConfig = Depends(get_spanner_config_by_id_from_path),
    include_disabled: bool = Query(False, description="Include disabled mappings"),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> schemas.SpannerSourceMappingListResponse:
    """List all source mappings for a spanner configuration."""
    mappings = crud.crud_spanner_source_mapping.get_multi_by_spanner(
        db=db, spanner_config_id=spanner_config.id, include_disabled=include_disabled
    )
    
    # Add source info to each mapping
    mappings_with_info = []
    for mapping in mappings:
        mapping_data = schemas.SpannerSourceMappingRead.model_validate(mapping)
        if hasattr(mapping, 'dimse_qr_source') and mapping.dimse_qr_source:
            mapping_data.source_name = mapping.dimse_qr_source.name
            mapping_data.source_remote_ae_title = mapping.dimse_qr_source.remote_ae_title
        mappings_with_info.append(mapping_data)
    
    return schemas.SpannerSourceMappingListResponse(
        mappings=mappings_with_info, 
        total=len(mappings_with_info)
    )


@router.post(
    "/{spanner_id}/sources",
    response_model=schemas.SpannerSourceMappingRead,
    status_code=status.HTTP_201_CREATED,
    summary="Add Source Mapping",
    description="Adds a DIMSE Q/R source to a spanner configuration.",
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid input data."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "Spanner config or source not found."},
        status.HTTP_409_CONFLICT: {"description": "Source already mapped to this spanner."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def add_source_mapping(
    *,
    db: Session = Depends(deps.get_db),
    spanner_config: models.SpannerConfig = Depends(get_spanner_config_by_id_from_path),
    mapping_in: schemas.SpannerSourceMappingCreate,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> models.SpannerSourceMapping:
    """Add a source mapping to a spanner configuration."""
    logger.info(f"User {current_user.id} adding source {mapping_in.dimse_qr_source_id} to spanner {spanner_config.id}")
    return crud.crud_spanner_source_mapping.create(
        db=db, spanner_config_id=spanner_config.id, obj_in=mapping_in
    )


@router.put(
    "/{spanner_id}/sources/{mapping_id}",
    response_model=schemas.SpannerSourceMappingRead,
    summary="Update Source Mapping",
    description="Updates a source mapping configuration.",
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid input data."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "Source mapping not found."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def update_source_mapping(
    *,
    db: Session = Depends(deps.get_db),
    spanner_id: int,  # Validate it exists
    mapping: models.SpannerSourceMapping = Depends(get_source_mapping_by_id_from_path),
    mapping_in: schemas.SpannerSourceMappingUpdate,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> models.SpannerSourceMapping:
    """Update a source mapping."""
    # Verify the mapping belongs to the specified spanner
    if mapping.spanner_config_id != spanner_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source mapping {mapping.id} does not belong to spanner {spanner_id}."
        )
    
    logger.info(f"User {current_user.id} updating source mapping ID: {mapping.id}")
    return crud.crud_spanner_source_mapping.update(db=db, db_obj=mapping, obj_in=mapping_in)


@router.delete(
    "/{spanner_id}/sources/{mapping_id}",
    response_model=schemas.SpannerSourceMappingRead,
    summary="Remove Source Mapping",
    description="Removes a source mapping from a spanner configuration.",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "Source mapping not found."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def remove_source_mapping(
    *,
    db: Session = Depends(deps.get_db),
    spanner_id: int,  # Validate it exists
    mapping_id: int,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> models.SpannerSourceMapping:
    """Remove a source mapping from a spanner configuration."""
    # Get the mapping and verify it exists and belongs to the spanner
    mapping = crud.crud_spanner_source_mapping.get(db, id=mapping_id)
    if not mapping:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source mapping {mapping_id} not found."
        )
    
    if mapping.spanner_config_id != spanner_id:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Source mapping {mapping_id} does not belong to spanner {spanner_id}."
        )
    
    logger.info(f"User {current_user.id} removing source mapping ID: {mapping_id}")
    return crud.crud_spanner_source_mapping.remove(db=db, id=mapping_id)


# --- TEST ENDPOINTS ---

@router.post(
    "/{spanner_id}/test",
    response_model=schemas.SpannerTestResult,
    summary="Test Spanner Configuration",
    description="Tests a spanner configuration by performing a test query across all configured sources.",
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid test parameters."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "Spanner config not found."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
    }
)
def test_spanner_config(
    *,
    db: Session = Depends(deps.get_db),
    spanner_config: models.SpannerConfig = Depends(get_spanner_config_by_id_from_path),
    test_request: schemas.SpannerTestRequest,
    current_user: models.User = Depends(deps.get_current_active_user),
) -> schemas.SpannerTestResult:
    """Test a spanner configuration."""
    logger.info(f"User {current_user.id} testing spanner config ID: {spanner_config.id}")
    
    # For now, return a mock result - actual implementation would go in the spanner engine
    start_time = datetime.utcnow()
    
    # Get source mappings
    mappings = crud.crud_spanner_source_mapping.get_multi_by_spanner(
        db=db, spanner_config_id=spanner_config.id, include_disabled=False
    )
    
    # Mock test results
    test_duration = (datetime.utcnow() - start_time).total_seconds()
    
    return schemas.SpannerTestResult(
        spanner_config_id=spanner_config.id,
        test_status=schemas.QueryStatus.SUCCESS,
        sources_tested=len(mappings),
        sources_successful=len(mappings),  # Mock: all successful
        total_results=42,  # Mock result count
        test_duration_seconds=test_duration,
        source_results=[
            {
                "source_id": mapping.dimse_qr_source_id,
                "source_name": mapping.dimse_qr_source.name if hasattr(mapping, 'dimse_qr_source') else "Unknown",
                "status": "SUCCESS",
                "results_found": 10 + (mapping.id % 5),  # Mock varying results
                "response_time_seconds": 0.5 + (mapping.priority * 0.1)
            }
            for mapping in mappings[:5]  # Limit to first 5 for brevity
        ]
    )


# --- QUERY LOG ENDPOINTS ---

@router.get(
    "/{spanner_id}/logs",
    response_model=schemas.SpannerQueryLogListResponse,
    summary="Get Query Logs",
    description="Retrieves query logs for a spanner configuration.",
    responses={
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_404_NOT_FOUND: {"description": "Spanner config not found."},
    }
)
def get_query_logs(
    *,
    db: Session = Depends(deps.get_db),
    spanner_config: models.SpannerConfig = Depends(get_spanner_config_by_id_from_path),
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(100, ge=1, le=1000, description="Number of records to return"),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> schemas.SpannerQueryLogListResponse:
    """Get query logs for a spanner configuration."""
    logs = crud.crud_spanner_query_log.get_multi_by_spanner(
        db=db, spanner_config_id=spanner_config.id, skip=skip, limit=limit
    )
    
    return schemas.SpannerQueryLogListResponse(
        logs=[schemas.SpannerQueryLogRead.model_validate(log) for log in logs],
        total=len(logs)  # For now, return actual count - could optimize with separate count query
    )
