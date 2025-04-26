# app/schemas/crosswalk.py
from pydantic import BaseModel, Field, field_validator, ConfigDict, Json, SecretStr, EmailStr, ValidationInfo as FieldValidationInfo # <-- IMPORT ADDED
from typing import Optional, Dict, Any, List
from datetime import datetime
import json as pyjson

# Re-import enums defined in models
from app.db.models.crosswalk import CrosswalkDbType, CrosswalkSyncStatus

# --- CrosswalkDataSource Schemas ---

class CrosswalkDataSourceBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    db_type: CrosswalkDbType
    # Use Dict for connection_details, will be validated further
    connection_details: Dict[str, Any] = Field(..., description="DB connection parameters (host, port, user, password_secret, dbname)")
    target_table: str = Field(..., min_length=1, description="Name of the table/view containing crosswalk data.")
    sync_interval_seconds: int = Field(default=3600, gt=0)
    is_enabled: bool = True

    # Add validator for connection_details based on db_type
    @field_validator('connection_details')
    @classmethod
    def validate_connection_details(cls, v: Dict[str, Any], info: FieldValidationInfo) -> Dict[str, Any]: # <-- Type hint now valid
        db_type = info.data.get('db_type')
        if not db_type:
            # This case might not happen if db_type is required and validated first,
            # but good practice to check.
            # Can't raise validation error *during* validation of another field easily in v2 model_validator 'before'
            # Consider using model_validator(mode='after') if cross-field validation is complex
            # For now, assume db_type is present due to field requirement.
            pass
            # raise ValueError("db_type must be provided to validate connection_details")


        required_keys = {'host', 'port', 'user', 'password_secret', 'dbname'}
        missing_keys = required_keys - v.keys()
        if missing_keys:
            raise ValueError(f"Missing required connection keys: {', '.join(missing_keys)}")

        if not isinstance(v.get('host'), str) or not v['host']:
            raise ValueError("'host' must be a non-empty string.")
        # Ensure port is an integer before checking range
        port = v.get('port')
        if not isinstance(port, int):
             try:
                 port = int(port) # Try converting if it's a string/number-like
                 v['port'] = port # Store the converted int back
             except (ValueError, TypeError):
                  raise ValueError("'port' must be an integer.")
        if not (0 < port < 65536):
             raise ValueError("'port' must be an integer between 1 and 65535.")

        if not isinstance(v.get('user'), str): # Allow potentially empty user? Check DB requirements.
             raise ValueError("'user' must be a string.")
        if not isinstance(v.get('password_secret'), str): # Password should be stored as string here, handled as SecretStr elsewhere
            raise ValueError("'password_secret' must be a string.")
        if not isinstance(v.get('dbname'), str) or not v['dbname']:
            raise ValueError("'dbname' must be a non-empty string.")

        # Add type-specific checks if needed (e.g., different keys for Oracle)
        # Example for MSSQL if driver needed (adjust key name as needed)
        # if db_type == CrosswalkDbType.MSSQL and 'odbc_driver' not in v:
        #     logger.warning("ODBC driver not specified for MSSQL, will use default.") # Use logger if available

        return v


class CrosswalkDataSourceCreate(CrosswalkDataSourceBase):
    pass # Inherits validation

class CrosswalkDataSourceUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    # db_type probably shouldn't be updated easily? Or require re-validation of connection_details
    connection_details: Optional[Dict[str, Any]] = Field(None, description="Full replacement for connection details.")
    target_table: Optional[str] = Field(None, min_length=1)
    sync_interval_seconds: Optional[int] = Field(None, gt=0)
    is_enabled: Optional[bool] = None

    # Re-apply validator if connection_details can be updated
    # This needs careful handling if db_type *isn't* updated simultaneously.
    # For simplicity, maybe disallow updating connection_details here, or require db_type too.
    # If allowing update, need to fetch existing db_type to validate correctly.
    # Example (if db_type is NOT updatable via this schema):
    # @field_validator('connection_details')
    # @classmethod
    # def validate_update_connection_details(cls, v: Optional[Dict[str, Any]], info: FieldValidationInfo) -> Optional[Dict[str, Any]]:
    #     if v is None: return None
    #     # Here you would need the existing db_type somehow, which isn't directly
    #     # available in the validator context easily without looking up the DB object.
    #     # It's often simpler to handle complex cross-field updates in the CRUD layer.
    #     # Minimal validation: ensure it's a dict if provided
    #     if not isinstance(v, dict): raise ValueError("Connection details must be an object.")
    #     return v


class CrosswalkDataSourceRead(CrosswalkDataSourceBase):
    id: int
    created_at: datetime
    updated_at: datetime
    last_sync_status: CrosswalkSyncStatus
    last_sync_time: Optional[datetime] = None
    last_sync_error: Optional[str] = None
    last_sync_row_count: Optional[int] = None

    # Exclude sensitive password from read schema
    @field_validator('connection_details', mode='before')
    @classmethod
    def mask_password(cls, v: Any) -> Dict[str, Any]:
        if isinstance(v, dict) and 'password_secret' in v:
            v_copy = v.copy()
            v_copy['password_secret'] = '********' # Mask password
            return v_copy
        # Return as is if not a dict or password not present
        return v if isinstance(v, dict) else {}

    model_config = ConfigDict(from_attributes=True)


# --- CrosswalkMap Schemas ---

# Helper for list-of-dict JSON validation
def _validate_json_list_of_dict(v: Any) -> List[Dict[str, Any]]:
    if v is None: return [] # Default to empty list
    if isinstance(v, list): # Already a list
        if all(isinstance(item, dict) for item in v): return v
        else: raise ValueError("Input must be a list of dictionaries.")
    if isinstance(v, str): # Try parsing from JSON string
        try:
            parsed = pyjson.loads(v)
            if isinstance(parsed, list) and all(isinstance(item, dict) for item in parsed): return parsed
            else: raise ValueError("JSON string must represent a list of dictionaries.")
        except pyjson.JSONDecodeError: raise ValueError("Invalid JSON string format.")
    raise TypeError("Input must be a list of dictionaries or a valid JSON string representing one.")

# Helper for list-of-string JSON validation
def _validate_json_list_of_string(v: Any) -> List[str]:
    if v is None: return [] # Default to empty list
    if isinstance(v, list): # Already a list
        if all(isinstance(item, str) for item in v): return v
        else: raise ValueError("Input must be a list of strings.")
    if isinstance(v, str): # Try parsing from JSON string
        try:
            parsed = pyjson.loads(v)
            if isinstance(parsed, list) and all(isinstance(item, str) for item in parsed): return parsed
            else: raise ValueError("JSON string must represent a list of strings.")
        except pyjson.JSONDecodeError: raise ValueError("Invalid JSON string format.")
    raise TypeError("Input must be a list of strings or a valid JSON string representing one.")

class CrosswalkMapBase(BaseModel):
    name: str = Field(..., min_length=1, max_length=100)
    description: Optional[str] = None
    is_enabled: bool = True
    data_source_id: int
    match_columns: List[Dict[str, str]] = Field(
        ...,
        min_length=1,
        description='List of mappings: [{"column_name": "mrn", "dicom_tag": "0010,0020"}]'
    )
    cache_key_columns: List[str] = Field(
        ...,
        min_length=1,
        description='List of source table column names used to build the unique cache key.'
    )
    replacement_mapping: List[Dict[str, str]] = Field(
        ...,
        min_length=1,
        description='List of mappings: [{"source_column": "new_mrn", "dicom_tag": "0010,0020", "dicom_vr": "LO"}]'
    )
    cache_ttl_seconds: Optional[int] = Field(None, gt=0)
    on_cache_miss: str = Field("fail", pattern="^(fail|query_db|log_only)$")

    # JSON Validators (simplified for example)
    _validate_match_columns = field_validator('match_columns', mode='before')(_validate_json_list_of_dict)
    _validate_cache_key_columns = field_validator('cache_key_columns', mode='before')(_validate_json_list_of_string)
    _validate_replacement_mapping = field_validator('replacement_mapping', mode='before')(_validate_json_list_of_dict)

    # Add more specific validation for the dict structures if needed
    # Example: Check required keys within the dictionaries
    @field_validator('match_columns')
    @classmethod
    def check_match_column_items(cls, v: List[Dict[str, str]]) -> List[Dict[str, str]]:
        for item in v:
            if 'column_name' not in item or 'dicom_tag' not in item:
                raise ValueError("Each item in 'match_columns' must have 'column_name' and 'dicom_tag'.")
            if not isinstance(item['column_name'], str) or not item['column_name']:
                 raise ValueError("'column_name' must be a non-empty string.")
            if not isinstance(item['dicom_tag'], str) or not item['dicom_tag']:
                 raise ValueError("'dicom_tag' must be a non-empty string.")
            # Add DICOM tag format validation if desired
        return v

    @field_validator('replacement_mapping')
    @classmethod
    def check_replacement_mapping_items(cls, v: List[Dict[str, str]]) -> List[Dict[str, str]]:
        for item in v:
            if 'source_column' not in item or 'dicom_tag' not in item:
                raise ValueError("Each item in 'replacement_mapping' must have 'source_column' and 'dicom_tag'.")
            if not isinstance(item['source_column'], str) or not item['source_column']:
                 raise ValueError("'source_column' must be a non-empty string.")
            if not isinstance(item['dicom_tag'], str) or not item['dicom_tag']:
                 raise ValueError("'dicom_tag' must be a non-empty string.")
            # VR is optional, but if present, validate it
            if 'dicom_vr' in item and (not isinstance(item['dicom_vr'], str) or len(item['dicom_vr']) != 2 or not item['dicom_vr'].isupper()):
                 raise ValueError("Optional 'dicom_vr' must be a 2-character uppercase string if provided.")
        return v

class CrosswalkMapCreate(CrosswalkMapBase):
    pass

class CrosswalkMapUpdate(BaseModel):
    name: Optional[str] = Field(None, min_length=1, max_length=100)
    description: Optional[str] = None
    is_enabled: Optional[bool] = None
    # data_source_id: Optional[int] = None # Usually not updated?
    match_columns: Optional[List[Dict[str, str]]] = Field(None, min_length=1)
    cache_key_columns: Optional[List[str]] = Field(None, min_length=1)
    replacement_mapping: Optional[List[Dict[str, str]]] = Field(None, min_length=1)
    cache_ttl_seconds: Optional[int] = Field(None, gt=0)
    on_cache_miss: Optional[str] = Field(None, pattern="^(fail|query_db|log_only)$")

    # Re-apply JSON validators if needed for update
    _validate_match_columns_up = field_validator('match_columns', mode='before')(_validate_json_list_of_dict)
    _validate_cache_key_columns_up = field_validator('cache_key_columns', mode='before')(_validate_json_list_of_string)
    _validate_replacement_mapping_up = field_validator('replacement_mapping', mode='before')(_validate_json_list_of_dict)

    # Re-apply item structure validation if needed
    _check_match_items_up = field_validator('match_columns')(CrosswalkMapBase.check_match_column_items)
    _check_replace_items_up = field_validator('replacement_mapping')(CrosswalkMapBase.check_replacement_mapping_items)


# Add DataSource nested info for Read schema
class CrosswalkDataSourceInfoForMap(BaseModel):
    id: int
    name: str
    db_type: CrosswalkDbType

class CrosswalkMapRead(CrosswalkMapBase):
    id: int
    created_at: datetime
    updated_at: datetime
    data_source: CrosswalkDataSourceInfoForMap # Nested source info

    model_config = ConfigDict(from_attributes=True)
