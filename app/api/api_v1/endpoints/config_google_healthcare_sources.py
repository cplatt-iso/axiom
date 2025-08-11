# backend/app/api/api_v1/endpoints/config_google_healthcare_sources.py
from typing import Any, List, Optional, Dict
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app import crud, schemas
from app.api import deps
from app.db.models.user import User # Import User model for permission checks if needed

router = APIRouter()

@router.get("/", response_model=List[schemas.GoogleHealthcareSourceRead])
def read_google_healthcare_sources(
    db: Session = Depends(deps.get_db),
    skip: int = 0,
    limit: int = 100,
    enabled_only: bool = False, # Optional filter
    active_only: bool = False, # Optional filter
    current_user: User = Depends(deps.get_current_active_superuser), # Require superuser for config access
) -> Any:
    """
    Retrieve Google Healthcare DICOM Store source configurations.
    Superusers can filter by enabled or active status.
    """
    if active_only:
        sources = crud.google_healthcare_source.get_multi_active(db, skip=skip, limit=limit)
    elif enabled_only:
         sources = crud.google_healthcare_source.get_multi_enabled(db, skip=skip, limit=limit)
    else:
        sources = crud.google_healthcare_source.get_multi(db, skip=skip, limit=limit)
    return sources


@router.post("/", response_model=schemas.GoogleHealthcareSourceRead, status_code=status.HTTP_201_CREATED)
def create_google_healthcare_source(
    *,
    db: Session = Depends(deps.get_db),
    source_in: schemas.GoogleHealthcareSourceCreate,
    current_user: User = Depends(deps.get_current_active_superuser),
) -> Any:
    """
    Create new Google Healthcare DICOM Store source configuration. (Superuser only)
    """
    try:
        source = crud.google_healthcare_source.create(db=db, obj_in=source_in)
    except ValueError as e:
        # Catch potential duplicate name error from CRUD
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    return source


@router.put("/{id}", response_model=schemas.GoogleHealthcareSourceRead)
def update_google_healthcare_source(
    *,
    db: Session = Depends(deps.get_db),
    id: int,
    source_in: schemas.GoogleHealthcareSourceUpdate,
    current_user: User = Depends(deps.get_current_active_superuser),
) -> Any:
    """
    Update a Google Healthcare DICOM Store source configuration. (Superuser only)
    """
    source = crud.google_healthcare_source.get(db=db, id=id)
    if not source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Google Healthcare Source not found")
    try:
        source = crud.google_healthcare_source.update(db=db, db_obj=source, obj_in=source_in)
    except ValueError as e:
         # Catch potential duplicate name or active/enabled validation errors
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    return source


@router.get("/{id}", response_model=schemas.GoogleHealthcareSourceRead)
def read_google_healthcare_source(
    *,
    db: Session = Depends(deps.get_db),
    id: int,
    current_user: User = Depends(deps.get_current_active_superuser),
) -> Any:
    """
    Get Google Healthcare DICOM Store source configuration by ID. (Superuser only)
    """
    source = crud.google_healthcare_source.get(db=db, id=id)
    if not source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Google Healthcare Source not found")
    return source


@router.delete("/{id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_google_healthcare_source(
    *,
    db: Session = Depends(deps.get_db),
    id: int,
    current_user: User = Depends(deps.get_current_active_superuser),
) -> None:
    """
    Delete a Google Healthcare DICOM Store source configuration. (Superuser only)
    """
    source = crud.google_healthcare_source.get(db=db, id=id)
    if not source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Google Healthcare Source not found")
    # Add checks here if deletion should be prevented (e.g., if used in active rules)
    # Example:
    # if crud.rule.is_google_healthcare_source_used(db, source_id=id):
    #     raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot delete source, it is currently used in rules.")
    crud.google_healthcare_source.remove(db=db, id=id)
    return None # Return None for 204 status code


# --- Connection Test Endpoint ---
@router.post(
    "/{id}/test-connection",
    response_model=Dict[str, Any],
    summary="Test Google Healthcare Source Connection",
    description="Tests the connection to a Google Healthcare source and updates its health status.",
    responses={
        status.HTTP_200_OK: {"description": "Connection test completed (check response for results)."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized to test connections."},
        status.HTTP_404_NOT_FOUND: {"description": "Google Healthcare source with the specified ID not found."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error during connection test."},
    }
)
async def test_google_healthcare_connection(
    *,
    id: int,
    db: Session = Depends(deps.get_db),
    current_user: User = Depends(deps.get_current_active_superuser),
) -> Dict[str, Any]:
    """
    Tests the connection to a Google Healthcare source and updates its health status.
    
    This endpoint performs a connection test by making API calls to the Google Healthcare API
    and updates the source's health status based on the results.
    """
    # Get the source first
    db_source = crud.google_healthcare_source.get(db=db, id=id)
    if not db_source:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Google Healthcare source not found"
        )
    
    import logging
    logger = logging.getLogger(__name__)
    logger.info(f"User {current_user.email} testing connection for Google Healthcare source ID {id} (project: '{db_source.gcp_project_id}', dataset: '{db_source.gcp_dataset_id}').")
    
    try:
        from app.services.connection_test_service import ConnectionTestService
        
        # Perform the connection test
        health_status, error_message = await ConnectionTestService.test_google_healthcare_connection(db_source)
        
        # Update the health status in the database
        await ConnectionTestService.update_source_health_status(
            db_session=db,
            source_type="google_healthcare",
            source_id=id,
            health_status=health_status,
            error_message=error_message
        )
        
        # Prepare response
        response = {
            "source_id": id,
            "gcp_project_id": db_source.gcp_project_id,
            "gcp_dataset_id": db_source.gcp_dataset_id,
            "gcp_dicom_store_id": db_source.gcp_dicom_store_id,
            "health_status": health_status.value,
            "test_timestamp": datetime.now(timezone.utc).isoformat(),
            "success": health_status == schemas.enums.HealthStatus.OK,
        }
        
        if error_message:
            response["error_message"] = error_message
        
        logger.info(f"Connection test completed for Google Healthcare source {id}: {health_status.value}")
        return response
        
    except Exception as e:
        logger.error(f"Error testing Google Healthcare connection for source {id}: {str(e)}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to test connection: {str(e)}"
        )
