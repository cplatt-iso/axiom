# app/worker/utils/dicom_utils.py
import re
import structlog
from typing import Optional

import pydicom
from pydicom.tag import BaseTag, Tag
from pydicom.datadict import tag_for_keyword # No need for repeater_has_keyword here

logger = structlog.get_logger(__name__)

def parse_dicom_tag(tag_str: str) -> Optional[BaseTag]:
    """
    Parses a DICOM tag string (keyword or GGGG,EEEE) into a pydicom Tag object.
    Returns None if parsing fails.
    """
    if not isinstance(tag_str, str):
        # logger.warning(f"Invalid type for tag string: {type(tag_str)}, value: {tag_str}") # Too noisy for a util
        return None
    
    tag_str = tag_str.strip()

    # Match GGGG,EEEE format (allow optional parentheses and spaces, case-insensitive hex)
    match_ge = re.fullmatch(r"^\(?\s*([0-9a-fA-F]{4})\s*,\s*([0-9a-fA-F]{4})\s*\)?$", tag_str)
    if match_ge:
        try:
            group = int(match_ge.group(1), 16)
            element = int(match_ge.group(2), 16)
            return Tag(group, element)
        except (ValueError, IndexError):
            # This should not happen if regex matches, but defensive
            logger.debug(f"Could not parse hex group/element from '{tag_str}' despite regex match.")
            return None # Should be unreachable

    # Match DICOM Keyword format (alphanumeric)
    # Using re.fullmatch to ensure the entire string is a keyword
    if re.fullmatch(r"^[a-zA-Z0-9]+$", tag_str):
        try:
            # pydicom's tag_for_keyword handles known keywords.
            # It returns None if the keyword is not found.
            tag_val = tag_for_keyword(tag_str)
            if tag_val is not None:
                return Tag(tag_val)
            else:
                # It's a valid keyword format, but not in the dictionary.
                # This can happen for private tags if used by keyword.
                # Depending on policy, this could be an error or allowed.
                # For now, if not in dict, consider it unparsable as a standard keyword.
                logger.debug(f"Input '{tag_str}' looks like a keyword but is not in the DICOM dictionary.")
                return None
        except Exception as e: # Catch any unexpected error from pydicom
             logger.warning(f"Error converting keyword '{tag_str}' to tag: {e}")
             return None
    
    # If neither format matches
    logger.debug(f"Could not parse DICOM tag from string: '{tag_str}'. Invalid format.")
    return None

# Add other general DICOM utilities here as needed, for example:
# def safe_get_dicom_value(ds: pydicom.Dataset, tag: BaseTag, default: Any = None) -> Any: ...
# def format_dicom_datetime(dt_value: Optional[str]) -> Optional[datetime]: ...
