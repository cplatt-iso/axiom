# app/api/api_v1/endpoints/data_browser.py
import logging
from typing import List, Dict, Any, Optional

from fastapi import APIRouter, Depends, HTTPException, status, Body
from sqlalchemy.orm import Session

from app import crud, schemas # Import top-level crud and schemas
from app.db import models     # Import models directly from app.db

from app.api import deps # Import API dependencies
from app.services import data_browser_service # Import the new service (will create next)

logger = logging.getLogger(__name__)
router = APIRouter()

@router.post(
    "/query",
    response_model=schemas.data_browser.DataBrowserQueryResponse,
    summary="Query a Configured Scraper Source",
    description="Performs an on-demand query (QIDO-RS or C-FIND) against a configured scraper source.",
    status_code=status.HTTP_200_OK,
    dependencies=[Depends(deps.get_current_active_user)], # Require login
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
    # current_user: models.User = Depends(deps.get_current_active_user) # Available if needed
) -> schemas.data_browser.DataBrowserQueryResponse:
    """
    Submits a query request to the backend to fetch study data from a
    specific configured DIMSE Q/R or DICOMweb source.
    """
    logger.info(f"Received data browser query request for source ID: {query_request.source_id}")
    logger.debug(f"Query Parameters: {query_request.query_parameters}")

    try:
        results_response = await data_browser_service.execute_query(
            db=db,
            source_id=query_request.source_id,
            query_params=query_request.query_parameters
        )
        logger.info(f"Query for source {query_request.source_id} completed with status: {results_response.query_status}, found {len(results_response.results)} results.")
        # Return the successful response (contains status/message/results)
        return results_response

    except data_browser_service.SourceNotFoundError as e:
        logger.warning(f"Data browser query failed: Source not found (ID: {query_request.source_id})")
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(e))
    except data_browser_service.QueryServiceError as e:
         # Handle errors originating from the query service (validation, connection, remote errors)
         logger.error(f"Data browser query failed for source ID {query_request.source_id}: {e}", exc_info=False) # Less verbose for common errors
         # Determine appropriate HTTP status code based on the error type if possible
         # For now, use 503 for potential remote issues, 400 for bad params, 500 otherwise
         # (This could be refined in the service layer)
         status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
         if "timeout" in str(e).lower() or "connection refused" in str(e).lower() or "unavailable" in str(e).lower():
              status_code = status.HTTP_503_SERVICE_UNAVAILABLE
         elif "invalid parameter" in str(e).lower() or "unsupported query" in str(e).lower():
              status_code = status.HTTP_400_BAD_REQUEST

         # Return an error response conforming to the schema
         return schemas.data_browser.DataBrowserQueryResponse(
              query_status="error",
              message=f"Query Failed: {e}",
              source_id=query_request.source_id,
              # Provide dummy/placeholder source info if lookup failed early
              source_name="Unknown",
              source_type="Unknown",
              results=[]
          )

         # Or raise HTTPException (might be simpler, but less informative in response body)
         # raise HTTPException(status_code=status_code, detail=f"Query failed: {e}")

    except Exception as e:
        # Catch unexpected errors
        logger.exception(f"Unexpected error during data browser query for source ID {query_request.source_id}: {e}")
        # Return an error response conforming to the schema
        return schemas.data_browser.DataBrowserQueryResponse(
             query_status="error",
             message=f"An unexpected server error occurred: {e}",
             source_id=query_request.source_id,
             source_name="Unknown",
             source_type="Unknown",
             results=[]
         )
        # Or raise HTTPException
        # raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="An unexpected server error occurred.")
