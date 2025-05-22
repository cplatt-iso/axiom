# backend/app/api/api_v1/endpoints/data_browser.py

from typing import Any, List, Literal

from fastapi import APIRouter, Depends, HTTPException, Query, Body
from sqlalchemy.orm import Session

# --- CORRECTED IMPORTS ---
from app import crud, schemas # Keep schemas import
from app.db import models # Import models module explicitly
# --- END CORRECTION ---

from app.api import deps
from app.services import data_browser_service
from app.schemas.data_browser import DataBrowserQueryResponse, DataBrowserQueryRequest, QueryLevel

router = APIRouter()

AllowedSourceType = Literal["dicomweb", "dimse-qr", "google_healthcare"]

@router.post("/query", response_model=DataBrowserQueryResponse)
async def run_data_browser_query(
    *,
    db: Session = Depends(deps.get_db),
    query_request: DataBrowserQueryRequest = Body(...),
    # Type hint using the explicitly imported models module
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Any:
    """
    Execute a query against a configured data source.
    """
    base_logger = data_browser_service.logger # Get the logger from the service
    log = base_logger
    # Check if the logger has a 'bind' method (like structlog)
    if hasattr(base_logger, 'bind'):
        try:
            log = base_logger.bind( # type: ignore
                user_id=current_user.id,
                source_id=query_request.source_id,
                source_type=query_request.source_type,
                query_level=query_request.query_level.value if query_request.query_level else QueryLevel.STUDY.value
            )
        except Exception: # Fallback if bind fails unexpectedly
            log = base_logger # Use the original logger

    # The Pylance errors for missing attributes (execute_query, SourceNotFoundError, etc.)
    # indicate these are not defined or exposed in app.services.data_browser_service.
    # They must be fixed in that module.

    try:
        log.info("Received data browser query request")
        # This line assumes data_browser_service.execute_query is defined.
        response = await data_browser_service.execute_query( # type: ignore
            db=db,
            source_id=query_request.source_id,
            source_type=query_request.source_type,
            query_params=query_request.query_parameters,
            query_level=query_request.query_level or QueryLevel.STUDY
        )
        log.info("Data browser query execution finished in service")
        return response

    # This assumes data_browser_service.SourceNotFoundError is defined.
    except data_browser_service.SourceNotFoundError as e: # type: ignore
         log.warning(f"Data browser query failed: Source not found. Error: {e}")
         raise HTTPException(status_code=404, detail=str(e))
    # This assumes data_browser_service.InvalidParameterError is defined.
    except data_browser_service.InvalidParameterError as e: # type: ignore
         log.warning(f"Data browser query failed: Invalid parameters. Error: {e}")
         raise HTTPException(status_code=400, detail=str(e))
    # This assumes data_browser_service.RemoteConnectionError and RemoteQueryError are defined.
    except (data_browser_service.RemoteConnectionError, data_browser_service.RemoteQueryError) as e: # type: ignore
         log.error(f"Data browser query failed: Remote error. Error: {e}", exc_info=False)
         raise HTTPException(status_code=502, detail=str(e))
    # This assumes data_browser_service.QueryServiceError is defined.
    except data_browser_service.QueryServiceError as e: # type: ignore
         log.error(f"Data browser query failed: Generic service error. Error: {e}", exc_info=True)
         raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        log.exception("Unexpected error processing data browser query") # Logs exception with traceback
        raise HTTPException(status_code=500, detail="An unexpected error occurred while processing the query.")
