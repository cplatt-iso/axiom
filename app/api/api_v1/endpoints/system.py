# app/api/api_v1/endpoints/system.py
from typing import List

from fastapi import APIRouter, Depends

from app.core.config import settings
from app.api import deps # For authentication dependency

router = APIRouter()

@router.get(
    "/input-sources",
    response_model=List[str],
    summary="List Known Input Sources",
    description="Retrieve the list of configured identifiers for known system input sources (e.g., 'dicom_scp_main', 'api_json'). Requires authentication.",
    # Add permissions dependency if needed later, for now just require login
    dependencies=[Depends(deps.get_current_active_user)]
)
def list_input_sources() -> List[str]:
    """
    Returns the list of known input source identifiers from the application settings.
    """
    return settings.KNOWN_INPUT_SOURCES
