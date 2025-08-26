# app/api/api_v1/endpoints/config_crosswalk.py
import logging
from typing import List, Any, Dict, Optional, Tuple # <-- ADD Tuple HERE

# --- ADDED: Import JSONResponse ---
from fastapi import APIRouter, Depends, HTTPException, status, Query, Body, Response
import structlog
from fastapi.responses import JSONResponse
# --- END ADDED ---
from sqlalchemy.orm import Session

from app import crud, schemas
from app.db import models # Correct import for models
from app.api import deps
# Use try-except block for crosswalk imports
try:
    from app.crosswalk import service as crosswalk_service
    from app.crosswalk import tasks as crosswalk_tasks
    CROSSWALK_ENABLED = True
except ImportError:
    logging.getLogger(__name__).warning("Crosswalk modules not found or import failed. Crosswalk API endpoints might be disabled or limited.")
    CROSSWALK_ENABLED = False
    # Define dummy modules/functions if needed
    class DummyTaskResult: id = "disabled" # type: ignore
    
    # Create a dummy service CLASS
    class _DummyCrosswalkServiceModule:
        @staticmethod
        def test_connection(*args, **kwargs) -> Tuple[bool, str]:
            return (False, "Crosswalk feature disabled due to import error.")
        
        # Add other methods from the real crosswalk_service if they are called elsewhere
        # and need a dummy implementation. For example:
        # @staticmethod
        # def get_crosswalk_value_sync(*args, **kwargs) -> Optional[Dict[str, Any]]:
        #     logger.warning("Dummy get_crosswalk_value_sync called because real service failed to import.")
        #     return None

    crosswalk_service = _DummyCrosswalkServiceModule() # Instantiate the dummy class
    
    # Create a dummy tasks module with the necessary structure
    class _DummyTaskCallable:
        @staticmethod
        def delay(*args, **kwargs) -> DummyTaskResult: # type: ignore
            return DummyTaskResult()

    class _DummyCrosswalkTasksModule:
        sync_crosswalk_source_task = _DummyTaskCallable()

    crosswalk_tasks = _DummyCrosswalkTasksModule()


logger = structlog.get_logger(__name__)
router = APIRouter()

# === Crosswalk Data Source Endpoints ===

@router.post(
    "/data-sources",
    response_model=schemas.CrosswalkDataSourceRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create Crosswalk Data Source Configuration",
    dependencies=[Depends(deps.require_role("Admin"))],
    tags=["Configuration - Crosswalk"]
)
def create_crosswalk_data_source(
    *,
    db: Session = Depends(deps.get_db),
    source_in: schemas.CrosswalkDataSourceCreate,
) -> models.CrosswalkDataSource:
    """Adds a new external database configuration for crosswalking."""
    try:
        return crud.crud_crosswalk_data_source.create(db=db, obj_in=source_in)
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error creating crosswalk data source '{source_in.name}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server error creating data source.")


@router.get(
    "/data-sources",
    response_model=List[schemas.CrosswalkDataSourceRead],
    summary="List Crosswalk Data Source Configurations",
    dependencies=[Depends(deps.get_current_active_user)],
    tags=["Configuration - Crosswalk"]
)
def read_crosswalk_data_sources(
    db: Session = Depends(deps.get_db),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
) -> List[models.CrosswalkDataSource]:
    """Retrieves a list of configured crosswalk data sources."""
    return crud.crud_crosswalk_data_source.get_multi(db=db, skip=skip, limit=limit)


@router.get(
    "/data-sources/{source_id}",
    response_model=schemas.CrosswalkDataSourceRead,
    summary="Get Crosswalk Data Source by ID",
    dependencies=[Depends(deps.get_current_active_user)],
    tags=["Configuration - Crosswalk"]
)
def read_crosswalk_data_source(
    source_id: int,
    db: Session = Depends(deps.get_db),
) -> models.CrosswalkDataSource:
    """Retrieves details for a specific crosswalk data source."""
    db_source = crud.crud_crosswalk_data_source.get(db, id=source_id)
    if not db_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data Source not found")
    return db_source


@router.put(
    "/data-sources/{source_id}",
    response_model=schemas.CrosswalkDataSourceRead,
    summary="Update Crosswalk Data Source Configuration",
    dependencies=[Depends(deps.require_role("Admin"))],
    tags=["Configuration - Crosswalk"]
)
def update_crosswalk_data_source(
    source_id: int,
    *,
    db: Session = Depends(deps.get_db),
    source_in: schemas.CrosswalkDataSourceUpdate,
) -> models.CrosswalkDataSource:
    """Updates an existing crosswalk data source configuration."""
    db_source = crud.crud_crosswalk_data_source.get(db, id=source_id)
    if not db_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data Source not found")
    try:
        return crud.crud_crosswalk_data_source.update(db=db, db_obj=db_source, obj_in=source_in)
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error updating crosswalk data source ID {source_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server error updating data source.")


@router.delete(
    "/data-sources/{source_id}",
    response_model=schemas.CrosswalkDataSourceRead,
    summary="Delete Crosswalk Data Source Configuration",
    dependencies=[Depends(deps.require_role("Admin"))],
    tags=["Configuration - Crosswalk"]
)
def delete_crosswalk_data_source(
    source_id: int,
    db: Session = Depends(deps.get_db),
) -> models.CrosswalkDataSource:
    """Deletes a crosswalk data source configuration."""
    try:
        return crud.crud_crosswalk_data_source.remove(db=db, id=source_id)
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error deleting crosswalk data source ID {source_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server error deleting data source.")


@router.post(
    "/data-sources/{source_id}/test",
    summary="Test Crosswalk Data Source Connection",
    # Remove default status code - it will be set in the response
    # status_code=status.HTTP_200_OK,
    dependencies=[Depends(deps.require_role("Admin"))],
    tags=["Configuration - Crosswalk"],
    response_model=Dict[str, Any] # Response model is correct
)
def test_crosswalk_data_source_connection( # Removed async
    source_id: int,
    db: Session = Depends(deps.get_db),
    # --- REMOVED response parameter ---
) -> JSONResponse: # <-- Return JSONResponse
    """Tests the database connection defined in the data source config."""
    if not CROSSWALK_ENABLED:
         raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Crosswalk feature is not fully enabled/imported.")

    db_source = crud.crud_crosswalk_data_source.get(db, id=source_id)
    if not db_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data Source not found")

    logger.info(f"Testing connection for data source ID: {source_id}, Name: {db_source.name}")
    try:
        success, message = crosswalk_service.test_connection(db_source) # Removed await
        resp_status_code = status.HTTP_200_OK if success else status.HTTP_400_BAD_REQUEST
        # --- Return JSONResponse with dynamic status code ---
        return JSONResponse(
            status_code=resp_status_code,
            content={"success": success, "message": message}
        )
        # --- End Return JSONResponse ---
    except Exception as e:
        logger.error(f"Unexpected error testing connection for source ID {source_id}: {e}", exc_info=True)
        # Return error response using JSONResponse
        return JSONResponse(
             status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
             content={"success": False, "message": f"Unexpected server error during connection test: {e}"}
        )


@router.post(
    "/data-sources/{source_id}/sync",
    summary="Trigger Manual Sync for Crosswalk Data Source",
    status_code=status.HTTP_202_ACCEPTED,
    dependencies=[Depends(deps.require_role("Admin"))],
    tags=["Configuration - Crosswalk"],
    response_model=Dict[str, str]
)
async def trigger_manual_sync(
    source_id: int,
    db: Session = Depends(deps.get_db),
):
    """Queues a background task to immediately sync the crosswalk data source."""
    if not CROSSWALK_ENABLED:
        raise HTTPException(status_code=status.HTTP_501_NOT_IMPLEMENTED, detail="Crosswalk feature is not fully enabled/imported.")

    db_source = crud.crud_crosswalk_data_source.get(db, id=source_id)
    if not db_source:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Data Source not found")
    if not db_source.is_enabled:
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Cannot sync a disabled data source.")

    logger.info(f"Manually triggering sync task for data source ID: {source_id}, Name: {db_source.name}")
    try:
        from app.worker.celery_app import app as celery_app_instance # Import the app instance
        logger.info(f"API attempting to send task using broker: {celery_app_instance.connection().as_uri(include_password=False)}") # Log without password
        task_result = crosswalk_tasks.sync_crosswalk_source_task.delay(source_id) # type: ignore
        return {"message": "Sync task queued successfully.", "task_id": task_result.id}
    except Exception as e:
        logger.error(f"Failed to queue sync task for source ID {source_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Failed to queue sync task.")

# === Crosswalk Map Endpoints ===

@router.post(
    "/mappings",
    response_model=schemas.CrosswalkMapRead,
    status_code=status.HTTP_201_CREATED,
    summary="Create Crosswalk Mapping",
    dependencies=[Depends(deps.require_role("Admin"))],
    tags=["Configuration - Crosswalk"]
)
def create_crosswalk_map(
    *,
    db: Session = Depends(deps.get_db),
    map_in: schemas.CrosswalkMapCreate,
) -> models.CrosswalkMap:
    """Defines a new crosswalk mapping configuration."""
    try:
        return crud.crud_crosswalk_map.create(db=db, obj_in=map_in)
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error creating crosswalk map '{map_in.name}': {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server error creating map.")

@router.get(
    "/mappings",
    response_model=List[schemas.CrosswalkMapRead],
    summary="List Crosswalk Mappings",
    dependencies=[Depends(deps.get_current_active_user)],
    tags=["Configuration - Crosswalk"]
)
def read_crosswalk_maps(
    db: Session = Depends(deps.get_db),
    data_source_id: Optional[int] = Query(None, description="Filter by data source ID"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=500),
) -> List[models.CrosswalkMap]:
    """Retrieves a list of configured crosswalk mappings."""
    if data_source_id:
         return crud.crud_crosswalk_map.get_by_data_source(db=db, data_source_id=data_source_id, skip=skip, limit=limit)
    else:
         return crud.crud_crosswalk_map.get_multi(db=db, skip=skip, limit=limit)

@router.get(
    "/mappings/{map_id}",
    response_model=schemas.CrosswalkMapRead,
    summary="Get Crosswalk Map by ID",
    dependencies=[Depends(deps.get_current_active_user)],
    tags=["Configuration - Crosswalk"]
)
def read_crosswalk_map(
    map_id: int,
    db: Session = Depends(deps.get_db),
) -> models.CrosswalkMap:
    """Retrieves details for a specific crosswalk map."""
    db_map = crud.crud_crosswalk_map.get(db, id=map_id)
    if not db_map:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Map not found")
    return db_map

@router.put(
    "/mappings/{map_id}",
    response_model=schemas.CrosswalkMapRead,
    summary="Update Crosswalk Mapping",
    dependencies=[Depends(deps.require_role("Admin"))],
    tags=["Configuration - Crosswalk"]
)
def update_crosswalk_map(
    map_id: int,
    *,
    db: Session = Depends(deps.get_db),
    map_in: schemas.CrosswalkMapUpdate,
) -> models.CrosswalkMap:
    """Updates an existing crosswalk mapping configuration."""
    db_map = crud.crud_crosswalk_map.get(db, id=map_id)
    if not db_map:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Map not found")
    try:
        return crud.crud_crosswalk_map.update(db=db, db_obj=db_map, obj_in=map_in)
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error updating crosswalk map ID {map_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server error updating map.")

@router.delete(
    "/mappings/{map_id}",
    response_model=schemas.CrosswalkMapRead,
    summary="Delete Crosswalk Mapping",
    dependencies=[Depends(deps.require_role("Admin"))],
    tags=["Configuration - Crosswalk"]
)
def delete_crosswalk_map(
    map_id: int,
    db: Session = Depends(deps.get_db),
) -> models.CrosswalkMap:
    """Deletes a crosswalk mapping configuration."""
    try:
        return crud.crud_crosswalk_map.remove(db=db, id=map_id)
    except HTTPException as http_exc:
        raise http_exc
    except Exception as e:
        logger.error(f"Unexpected error deleting crosswalk map ID {map_id}: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Server error deleting map.")
