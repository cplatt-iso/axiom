# app/api/api_v1/endpoints/dicomweb.py
import logging
import tempfile
import os
import uuid
from io import BytesIO
from email import message_from_bytes
from email.policy import default as default_policy
# --- ADDED Typing import ---
from typing import List, Tuple, Optional, Dict, Any
# --- END ADDED ---

import pydicom
from pydicom.errors import InvalidDicomError
from fastapi import APIRouter, Depends, Request, HTTPException, status, Response
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from app.db import models
from app import schemas
from app.api import deps
from app.worker.tasks import process_stow_instance_task
from app.schemas.dicomweb import STOWResponse, ReferencedSOP, FailedSOP, FailureReasonCode
from app.core.config import settings

logger = logging.getLogger(__name__)

router = APIRouter()

# Helper function to parse multipart/related DICOM content
def parse_multipart_related(content_type_header: str, body: bytes) -> List[bytes]:
    """
    Parses a 'multipart/related' request body containing DICOM instances.
    Args: content_type_header, body. Returns: List of byte strings. Raises: ValueError.
    """
    if not content_type_header or 'multipart/related' not in content_type_header.lower():
        raise ValueError("Content-Type must be multipart/related")
    headers = f"Content-Type: {content_type_header}\r\n\r\n".encode()
    full_message_bytes = headers + body
    try:
        msg = message_from_bytes(full_message_bytes, policy=default_policy)
        dicom_parts = []
        if msg.is_multipart():
            for part in msg.iter_parts():
                if part.get_content_type().lower() == 'application/dicom':
                    payload = part.get_payload(decode=True)
                    if payload: dicom_parts.append(payload)
                    else: logger.warning("Encountered empty DICOM part in multipart message.")
                else: logger.warning(f"Skipping non-DICOM part with Content-Type: {part.get_content_type()}")
        else: raise ValueError("Request body is not a valid multipart message")
        if not dicom_parts: raise ValueError("No 'application/dicom' parts found in the multipart message")
        return dicom_parts
    except Exception as e:
        logger.error(f"Error parsing multipart/related content: {e}", exc_info=True)
        raise ValueError(f"Failed to parse multipart/related content: {e}")


@router.post(
    "/studies",
    response_model=schemas.dicomweb.STOWResponse,
    status_code=status.HTTP_200_OK,
    responses={
        status.HTTP_200_OK: {"description": "Instances processed (check body for details)."},
        status.HTTP_202_ACCEPTED: {"description": "Instances accepted for asynchronous processing."},
        status.HTTP_400_BAD_REQUEST: {"description": "Invalid request."},
        status.HTTP_401_UNAUTHORIZED: {"description": "Authentication required."},
        status.HTTP_403_FORBIDDEN: {"description": "Not authorized."},
        status.HTTP_409_CONFLICT: {"description": "Conflict detected."},
        status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error."},
        status.HTTP_507_INSUFFICIENT_STORAGE: {"description": "Insufficient storage."},
    },
    summary="Store DICOM Instances (STOW-RS)",
    description="Accepts DICOM instances via POST with Content-Type `multipart/related; type=\"application/dicom\"`."
)
async def store_instances(
    *,
    request: Request,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
):
    """
    DICOMweb STOW-RS endpoint. Receives instances, validates, saves temporarily,
    and queues for async processing. Requires auth.
    """
    # --- Prepare Association Info ---
    source_ip = request.client.host if request.client else None
    # For STOW-RS, we don't have explicit Calling/Called AE from the protocol
    # We can use the authenticated user/key info as a stand-in if needed, or leave null
    calling_ae_title = f"USER_{current_user.id}" # Example
    called_ae_title = "AXIOM_STOW_RS" # Our service endpoint AE
    association_info: Dict[str, Any] = {
        "calling_ae_title": calling_ae_title,
        "called_ae_title": called_ae_title,
        "source_ip": source_ip,
    }
    logger.info(f"STOW-RS request received from {source_ip} by user {current_user.id}. Assoc Info: {association_info}")
    # --- End Prepare ---

    content_type = request.headers.get("content-type")
    if not content_type:
         raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Content-Type header is missing.")

    # 1. Parse Request Body
    try:
        body = await request.body()
        if not body: raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="Request body is empty.")
        dicom_parts = parse_multipart_related(content_type, body)
        logger.info(f"Received {len(dicom_parts)} DICOM parts from {source_ip}")
    except ValueError as e:
        logger.error(f"STOW-RS request parsing error: {e}", exc_info=True)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"Failed to parse multipart/related request: {str(e)}")
    except Exception as e:
         logger.error(f"Unexpected error reading STOW-RS request body: {e}", exc_info=True)
         raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="Error reading request body.")

    successful_sops: List[ReferencedSOP] = []
    failed_sops: List[FailedSOP] = []
    temp_files_to_clean: List[str] = []

    # 2. Process Each DICOM Part
    for i, dicom_bytes in enumerate(dicom_parts):
        temp_filepath = None
        ds = None
        sop_class_uid = "Unknown"
        sop_instance_uid = f"Unknown_{uuid.uuid4()}"

        try:
            # 2a. Save to Temporary File
            temp_dir = settings.TEMP_DIR
            with tempfile.NamedTemporaryFile(delete=False, suffix=".dcm", prefix="stow_", dir=temp_dir) as tf:
                tf.write(dicom_bytes)
                temp_filepath = tf.name
                temp_files_to_clean.append(temp_filepath)
            logger.debug(f"Saved STOW part {i+1} to temporary file: {temp_filepath}")

            # 2b. Basic DICOM Validation
            try:
                ds = pydicom.dcmread(BytesIO(dicom_bytes), stop_before_pixels=True, force=True)
                sop_class_uid = getattr(ds, 'SOPClassUID', 'Unknown')
                sop_instance_uid = getattr(ds, 'SOPInstanceUID', f'Unknown_{uuid.uuid4()}')
                logger.debug(f"Successfully parsed metadata for SOPInstanceUID: {sop_instance_uid}")
            except InvalidDicomError as e:
                logger.warning(f"Invalid DICOM data received in part {i+1} (UID: {sop_instance_uid}): {e}")
                failed_sops.append(FailedSOP(
                    **{
                        "00081150": sop_class_uid,  # Alias for ReferencedSOPClassUID
                        "00081155": sop_instance_uid, # Alias for ReferencedSOPInstanceUID
                        "00081197": FailureReasonCode.ErrorCannotUnderstand, # Alias for FailureReason
                    },
                    ReasonDetail=f"Invalid DICOM format: {e}" # Field name, no alias
                ))
                continue
            except Exception as e:
                 logger.error(f"Error parsing DICOM metadata for part {i+1} (UID: {sop_instance_uid}): {e}", exc_info=True)
                 failed_sops.append(FailedSOP(
                    **{
                        "00081150": sop_class_uid,
                        "00081155": sop_instance_uid,
                        "00081197": FailureReasonCode.ProcessingFailure,
                    },
                    ReasonDetail=f"Error parsing DICOM metadata: {e}"
                 ))
                 continue

            # 2c. Queue for Asynchronous Processing
            try:
                # --- Pass association_info to the task ---
                task_result = process_stow_instance_task.delay( # type: ignore[operator]
                    temp_filepath=temp_filepath,
                    association_info=association_info # Pass the dict here
                )
                # --- END Pass ---
                temp_files_to_clean.remove(temp_filepath)
                logger.info(f"Queued STOW instance {sop_instance_uid} for processing. Task ID: {task_result.id}")
                successful_sops.append(ReferencedSOP(**{"00081150": sop_class_uid, "00081155": sop_instance_uid}))

            except Exception as e:
                logger.error(f"Failed to queue STOW instance {sop_instance_uid} (path: {temp_filepath}): {e}", exc_info=True)
                failed_sops.append(FailedSOP(
                    **{
                        "00081150": sop_class_uid,
                        "00081155": sop_instance_uid,
                        "00081197": FailureReasonCode.QueuingFailed,
                    },
                    ReasonDetail=f"Failed to queue instance for processing: {e}"
                ))

        except Exception as e:
             logger.error(f"Unexpected error processing STOW part {i+1} (UID: {sop_instance_uid}): {e}", exc_info=True)
             if not any(fs.ReferencedSOPInstanceUID == sop_instance_uid for fs in failed_sops): # This check uses field name, which is fine for instances
                 failed_sops.append(FailedSOP(
                    **{
                        "00081150": sop_class_uid,
                        "00081155": sop_instance_uid,
                        "00081197": FailureReasonCode.ProcessingFailure,
                    },
                    ReasonDetail=f"Unexpected error during processing part: {e}"
                 ))

    # Cleanup for files that failed BEFORE successful queuing
    for failed_path in temp_files_to_clean:
        if os.path.exists(failed_path):
            try: os.remove(failed_path); logger.warning(f"Cleaned up temporary file due to pre-queueing failure: {failed_path}")
            except OSError as rm_err: logger.error(f"Failed to clean up temporary file {failed_path}: {rm_err}")

    # 3. Construct and Return Response
    stow_response = STOWResponse(
        **{
            "00081199": successful_sops if successful_sops else None, # Alias for ReferencedSOPSequence
            "00081198": failed_sops if failed_sops else None,         # Alias for FailedSOPSequence
            # "00081195": None # Optional: TransactionUID, can be omitted if not explicitly set
        }
    )
    final_status_code = status.HTTP_200_OK
    if failed_sops and not successful_sops: logger.warning(f"STOW-RS request from {source_ip} resulted in all {len(failed_sops)} instances failing pre-processing or queuing.")
    elif failed_sops: logger.warning(f"STOW-RS request from {source_ip} completed with {len(successful_sops)} queued, {len(failed_sops)} failed pre-processing/queuing.")
    else: logger.info(f"STOW-RS request from {source_ip} completed successfully, queuing {len(successful_sops)} instances.")

    return JSONResponse(
        status_code=final_status_code,
        content=stow_response.model_dump(by_alias=True, exclude_none=True)
    )
