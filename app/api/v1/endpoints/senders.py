from typing import List
from fastapi import APIRouter
from app.schemas.sender import Sender

router = APIRouter()

AVAILABLE_SENDERS = [
    {
        "identifier": "pynetdicom",
        "name": "pynetdicom",
        "description": "Default sender using the pynetdicom library."
    },
    {
        "identifier": "dcm4che",
        "name": "dcm4che",
        "description": "Sender using the dcm4che toolkit's storescu utility."
    }
]

@router.get("/", response_model=List[Sender])
def list_senders():
    """
    Lists all available DICOM senders in the system.
    """
    return AVAILABLE_SENDERS
