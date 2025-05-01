# app/api/api_v1/endpoints/data_browser.py
# Use structlog if available
try:
    import structlog # type: ignore
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

from typing import List, Dict, Any, Optional, Literal

from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.orm import Session

from app import crud, schemas
from app.db import models
from app.api import deps
from app.services import data_browser_service

router = APIRouter()

@router.post(
    "/query",
    response_model=schemas.data_browser.DataBrowserQueryResponse,
    summary="Query a Configured Scraper Source",
    description="Performs an on-demand query (QIDO-RS or C-FIND) against a configured scraper source.",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(deps.get_current_active_user)],
    tags=["Data Browser"],
    responses={
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid query parameters or source configuration."},
        status.HTTP_404_NOT_FOUND: {"description": "Configured source ID not found."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Backend error during query."},
        status.HTTP_503_SERVICE_UNAVAILABLE: {"description": "Remote source unavailable or timed out."},
    }
)
async def query_data_source(
    *,
    db: Session = Depends(deps.get_db),
    query_request: schemas.data_browser.DataBrowserQueryRequest = Body(...)
    # current_user: models.User = Depends(deps.get_current_active_user)
) -> schemas.data_browser.DataBrowserQueryResponse:
    """
    Submits a query request based on source_id AND source_type.
    """
    log = logger.bind(
        source_id=query_request.source_id,
        source_type=query_request.source_type,
        query_params=query_request.query_parameters,
        query_level=query_request.query_level
    )
    log.info("Received data browser query request")

    try:
        # --- PASS source_type TO THE SERVICE ---
        results_response = await data_browser_service.execute_query(
            db=db,
            source_id=query_request.source_id,
            source_type=query_request.source_type, # <-- Pass type explicitly
            query_params=query_request.query_parameters,
            query_level=query_request.query_level
        )
        log.info(
            "Query completed",
            query_status=results_response.query_status,
            source_name=results_response.source_name, # Log name from response
            result_count=len(results_response.results)
        )
        return results_response

    except data_browser_service.SourceNotFoundError as e:
        log.warning("Data browser query failed: Source not found")
        # Use info from request for error detail if available
        detail_msg = f"Source not found (Type: {query_request.source_type}, ID: {query_request.source_id})"
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=detail_msg)

    except data_browser_service.QueryServiceError as e:
         log.error("Data browser query failed", error_message=str(e), exc_info=False)
         status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
         if isinstance(e, data_browser_service.RemoteConnectionError): status_code = status.HTTP_503_SERVICE_UNAVAILABLE
         elif isinstance(e, data_browser_service.RemoteQueryError): status_code = status.HTTP_502_BAD_GATEWAY # Or 503/500
         elif isinstance(e, data_browser_service.InvalidParameterError): status_code = status.HTTP_400_BAD_REQUEST

         # --- Construct error response USING REQUEST INFO ---
         # Attempt to get source name/type info even on error
         source_info = data_browser_service.get_source_info_for_response(db, query_request.source_id, query_request.source_type)

         return schemas.data_browser.DataBrowserQueryResponse(
              query_status="error",
              message=f"Query Failed: {e}",
              source_id=query_request.source_id,
              source_name=source_info["name"], # Use looked-up or default name
              source_type=source_info["type"], # Use looked-up or default type
              results=[]
          )

    except Exception as e:
        # Catch totally unexpected errors
        log.exception("Unexpected error during data browser query")
        # --- Use request info for better error context ---
        source_info = data_browser_service.get_source_info_for_response(db, query_request.source_id, query_request.source_type)
        return schemas.data_browser.DataBrowserQueryResponse(
             query_status="error",
             message=f"An unexpected server error occurred: {e}",
             source_id=query_request.source_id,
             source_name=source_info["name"],
             source_type=source_info["type"],
             results=[]
         )
