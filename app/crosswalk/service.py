# app/crosswalk/service.py
import logging
from typing import Optional, List, Dict, Any, Tuple
import redis
import json as pyjson # Use standard json library
from sqlalchemy import create_engine, text, inspect as sql_inspect, Column, MetaData, Table # Import necessary SQLAlchemy components
from sqlalchemy.engine import URL as SQLAlchemyURL, Engine # Import Engine
from sqlalchemy.exc import SQLAlchemyError
from pydantic import SecretStr

from app.db import models
from app.core.config import settings # Import settings for Redis URL

logger = logging.getLogger(__name__)

# --- Redis Connection ---
try:
    redis_client = redis.Redis.from_url(settings.REDIS_URL, decode_responses=True) # decode_responses for string keys/values
    redis_client.ping()
    logger.info(f"Redis client connected successfully to {settings.REDIS_URL}")
except redis.exceptions.ConnectionError as e:
    logger.error(f"Failed to connect to Redis at {settings.REDIS_URL}: {e}. Crosswalk caching will be unavailable.")
    redis_client = None
except Exception as e:
     logger.error(f"An unexpected error occurred initializing Redis client: {e}")
     redis_client = None

# --- Constants ---
CACHE_NOT_FOUND_MARKER = "__AXIOM_CW_NOT_FOUND__"
DEFAULT_CACHE_TTL_SECONDS = 3600 # 1 hour default TTL for lookups/not_found

# --- Database Connection Helper ---
def get_db_engine(config: models.CrosswalkDataSource) -> Engine:
    """Creates a SQLAlchemy engine based on the data source config."""
    details = config.connection_details
    drivername = ""
    odbc_driver = None # Specific for MSSQL via pyodbc

    if config.db_type == models.CrosswalkDbType.POSTGRES:
        drivername = "postgresql+psycopg"
    elif config.db_type == models.CrosswalkDbType.MYSQL:
        # --- Use mysql-connector-python driver name ---
        drivername = "mysql+mysqlconnector"
        # --- End Use ---
    elif config.db_type == models.CrosswalkDbType.MSSQL:
        drivername = "mssql+pyodbc"
        odbc_driver = details.get('odbc_driver', 'ODBC Driver 17 for SQL Server')
        logger.debug(f"Using ODBC Driver: {odbc_driver}")
    else:
        raise ValueError(f"Unsupported database type: {config.db_type.value}")

    try:
        password = details.get('password_secret', '')
        url_params = {}
        if odbc_driver:
             url_params['driver'] = odbc_driver

        url = SQLAlchemyURL.create(
            drivername=drivername,
            username=details.get('user'),
            password=password, # Pass password directly
            host=details.get('host'),
            port=details.get('port'),
            database=details.get('dbname'),
            query=url_params
        )
        # Use minimal pool size for potentially infrequent connections during lookup
        return create_engine(url, pool_size=1, max_overflow=2, pool_recycle=300)
    except Exception as e:
        logger.error(f"Failed to build SQLAlchemy URL or Engine for {config.name}: {e}", exc_info=True)
        raise ConnectionError(f"Failed to configure connection for {config.name}: {e}") from e

# --- Test Connection (Synchronous) ---
def test_connection(config: models.CrosswalkDataSource) -> Tuple[bool, str]:
    """Tests the database connection."""
    engine = None
    try:
        engine = get_db_engine(config)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        logger.info(f"Connection test successful for data source: {config.name}")
        return True, "Connection successful."
    except SQLAlchemyError as e:
        error_msg = f"Connection test failed for {config.name}: {e}"
        logger.error(error_msg, exc_info=False) # Less verbose logging for common connection errors
        return False, f"Connection failed: Check credentials, host, port, DB name, and network connectivity. Error: {e}"
    except Exception as e:
        error_msg = f"Unexpected error testing connection for {config.name}: {e}"
        logger.error(error_msg, exc_info=True)
        return False, f"Unexpected error: {e}"
    finally:
        if engine:
            engine.dispose()

# --- Fetch Data (Synchronous - for pre-warming/debug) ---
def fetch_data_from_source(config: models.CrosswalkDataSource) -> Tuple[Optional[List[Dict]], Optional[str]]:
    """Fetches data from the configured target table (Potentially ALL rows - use with caution)."""
    logger.warning(f"Executing fetch_data_from_source for {config.name} - Fetching potentially large dataset for sync/pre-warming.")
    engine = None
    try:
        engine = get_db_engine(config)
        with engine.connect() as connection:
            # WARNING: Still fetching ALL rows if called. Requires chunking in task.
            stmt = text(f"SELECT * FROM {config.target_table}") # Use model's target_table directly
            result = connection.execute(stmt)
            fetched_data = [row._mapping for row in result]
        logger.info(f"Fetched {len(fetched_data)} rows from {config.name}:{config.target_table} for sync/pre-warming.")
        return fetched_data, None
    except SQLAlchemyError as e:
        error_msg = f"Database error during full fetch from {config.name}:{config.target_table}: {e}"
        logger.error(error_msg, exc_info=True)
        return None, f"DB Error: {e}"
    except Exception as e:
        error_msg = f"Unexpected error during full fetch from {config.name}:{config.target_table}: {e}"
        logger.error(error_msg, exc_info=True)
        return None, f"Unexpected Error: {e}"
    finally:
        if engine:
            engine.dispose()

# --- Build Cache Key (Helper - Synchronous) ---
def build_cache_key(prefix: str, key_columns: List[str], row: Dict[str, Any]) -> Optional[str]:
    """Builds a Redis key from specified columns in a row."""
    key_parts = [prefix]
    try:
        for col in key_columns:
            # Check if column exists in the row (case-sensitive from DB might matter)
            if col not in row:
                 logger.warning(f"Key column '{col}' not found in fetched row: {list(row.keys())}. Cannot build cache key.")
                 return None
            value = row.get(col)
            if value is None:
                 logger.debug(f"Value for key column '{col}' is NULL in row. Cannot build cache key.")
                 return None # Skip rows missing a key part
            key_parts.append(str(value)) # Ensure parts are strings
        # Use lowercase key for consistency? Optional.
        # return ":".join(key_parts).lower()
        return ":".join(key_parts)
    except Exception as e:
        logger.error(f"Error building cache key for row {row} using columns {key_columns}: {e}")
        return None

# --- Update Cache (Synchronous - for pre-warming) ---
# Stores the processed replacement dictionary, not the raw DB row.
def update_cache(config: models.CrosswalkDataSource, data: List[Dict]) -> Tuple[int, Optional[str]]:
    """
    Updates the Redis cache with fetched data (for pre-warming).
    Stores the *replacement dictionary* for each row, keyed by lookup values.
    """
    if not redis_client:
        return 0, "Redis client not available."

    cache_prefix = f"crosswalk:{config.id}"
    logger.info(f"Updating Redis cache (pre-warming) for source ID {config.id} (prefix: {cache_prefix}) with {len(data)} rows.")

    # Fetch the associated map config to get key columns and replacement mapping
    map_config: Optional[models.CrosswalkMap] = None
    # Limitation: Assumes only one map per source for pre-warming. Needs refinement if multiple maps use the same source.
    if hasattr(config, 'crosswalk_maps') and config.crosswalk_maps:
         map_config = config.crosswalk_maps[0]
         logger.warning(f"Pre-warming cache for source {config.id} using the first associated map: '{map_config.name}' (ID: {map_config.id}).")
    else:
         # Fallback: Query DB if relationship not loaded
         from app.db.session import SessionLocal
         db = None
         try:
             db = SessionLocal()
             map_config = db.query(models.CrosswalkMap).filter(models.CrosswalkMap.data_source_id == config.id).first()
             if map_config:
                 logger.warning(f"Pre-warming cache for source {config.id} using first map found in DB: '{map_config.name}' (ID: {map_config.id}).")
         finally:
              if db: db.close()

    if not map_config:
        msg = f"No CrosswalkMap found for data source ID {config.id}. Cannot pre-warm cache."
        logger.error(msg)
        return 0, msg

    cache_key_columns = map_config.cache_key_columns
    replacement_mapping = map_config.replacement_mapping
    if not cache_key_columns:
        msg = f"Map '{map_config.name}' for source ID {config.id} has no cache_key_columns defined."; logger.error(msg); return 0, msg
    if not replacement_mapping:
        msg = f"Map '{map_config.name}' for source ID {config.id} has no replacement_mapping defined."; logger.error(msg); return 0, msg

    logger.debug(f"Pre-warming cache using key columns: {cache_key_columns}")
    pipe = redis_client.pipeline()
    keys_added = 0
    errors_encountered = 0

    for row in data:
        # Build the key used for LOOKUP
        lookup_key = build_cache_key(cache_prefix, cache_key_columns, row)
        if lookup_key:
            try:
                # --- Construct the VALUE to store (the replacement dict) ---
                replacement_dict: Dict[str, Any] = {}
                for mapping in replacement_mapping:
                    source_col = mapping.get('source_column')
                    target_tag = mapping.get('dicom_tag')
                    target_vr = mapping.get('dicom_vr')
                    if source_col and target_tag:
                        # Check if source_col actually exists in the row from DB
                        if source_col not in row:
                            logger.warning(f"Source column '{source_col}' defined in replacement mapping not found in fetched row keys: {list(row.keys())} for map '{map_config.name}'. Skipping this part of replacement.")
                            continue
                        value_from_db = row.get(source_col) # Get value from *this* row
                        replacement_dict[target_tag] = {
                            "value": value_from_db,
                            "vr": target_vr # Include VR (might be None)
                        }
                # --- End Construct Value ---

                value_to_store = pyjson.dumps(replacement_dict) # Store the replacement dict as JSON
                ttl = map_config.cache_ttl_seconds or config.sync_interval_seconds + 600 # Default TTL
                pipe.setex(lookup_key, ttl, value_to_store)
                keys_added += 1
            except TypeError as e:
                 logger.error(f"Failed to serialize replacement dict to JSON for cache key {lookup_key}: {e} - Row: {row}")
                 errors_encountered += 1
            except Exception as e:
                logger.error(f"Error adding key {lookup_key} to Redis pipeline: {e}")
                errors_encountered += 1
        else:
             errors_encountered +=1 # Could not build lookup key

    try:
        results = pipe.execute()
        failed_ops = sum(1 for res in results if isinstance(res, Exception) or res is False)
        if failed_ops > 0 or errors_encountered > 0:
            err_msg = f"Cache pre-warming for source {config.id} finished with {failed_ops} pipeline errors and {errors_encountered} skipped/failed rows."; logger.warning(err_msg); return keys_added, err_msg
        else:
            logger.info(f"Successfully pre-warmed cache for source {config.id}. Added/Updated {keys_added} keys."); return keys_added, None
    except redis.exceptions.RedisError as e: logger.error(f"Redis pipeline execution failed during pre-warming for source {config.id}: {e}"); return 0, f"Redis Error: {e}"
    except Exception as e: logger.error(f"Unexpected error executing Redis pipeline during pre-warming for source {config.id}: {e}"); return 0, f"Unexpected Pipeline Error: {e}"


# --- Live Lookup Function (Synchronous) ---
def get_crosswalk_value_sync(map_config: models.CrosswalkMap, match_values: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Synchronous lookup. Attempts to get crosswalk result from Redis cache.
    If missed and policy is 'query_db', performs a live query against the external DB.
    Caches the result (including "not found").
    Returns the dictionary of replacement values {dicom_tag: {value: ..., vr: ...}} or None.
    """
    # 1. --- Build Cache Key ---
    cache_prefix = f"crosswalk:{map_config.data_source_id}"
    tag_to_col_map: Dict[str, str] = {mc['dicom_tag']: mc['column_name'] for mc in map_config.match_columns}
    key_value_parts = []
    try:
        for cache_col_name in map_config.cache_key_columns:
            dicom_tag_for_col = next((tag for tag, col in tag_to_col_map.items() if col == cache_col_name), None)
            if not dicom_tag_for_col:
                 logger.error(f"SYNC: Config error map '{map_config.name}': Cache key col '{cache_col_name}' not found in match_columns.")
                 return None # Configuration error
            value = match_values.get(dicom_tag_for_col)
            if value is None:
                 logger.debug(f"SYNC: Cache key part missing for tag '{dicom_tag_for_col}' (col '{cache_col_name}') for map '{map_config.name}'. Cannot build cache key.")
                 return None # Input data missing required value
            key_part = str(value[0]) if isinstance(value, list) and value else str(value)
            key_value_parts.append(key_part)
    except Exception as e:
        logger.error(f"SYNC: Error preparing cache key parts for map '{map_config.name}': {e}", exc_info=True)
        return None # Error during key prep

    if not key_value_parts:
         logger.warning(f"SYNC: No cache key parts could be determined for map '{map_config.name}'.")
         return None # Cannot form key

    cache_key = f"{cache_prefix}:{':'.join(key_value_parts)}"
    logger.debug(f"SYNC: Crosswalk lookup using cache key: {cache_key}")

    # 2. --- Check Redis Cache ---
    cached_data_str: Optional[str] = None
    cache_hit = False
    if redis_client:
        try:
            cached_data_str = redis_client.get(cache_key)
            if cached_data_str is not None:
                cache_hit = True
                logger.debug(f"SYNC: Cache HIT for key {cache_key}")
        except redis.exceptions.RedisError as e:
            logger.error(f"SYNC: Redis error getting key {cache_key} for map '{map_config.name}': {e}")
            # Treat as cache miss, proceed based on policy

    # 3. --- Process Cache Result or Handle Miss ---
    if cache_hit and cached_data_str:
        if cached_data_str == CACHE_NOT_FOUND_MARKER:
            logger.debug(f"SYNC: Cached 'not found' marker hit for key {cache_key}.")
            return None # Explicitly not found
        else:
            try:
                replacement_values = pyjson.loads(cached_data_str) # Expecting the replacement dict
                if isinstance(replacement_values, dict):
                    logger.debug(f"SYNC: Returning replacements from cache for key {cache_key}")
                    return replacement_values
                else:
                    logger.error(f"SYNC: Cached data for key {cache_key} is not a dictionary: {cached_data_str[:100]}...")
            except pyjson.JSONDecodeError as e:
                logger.error(f"SYNC: Failed to decode cached JSON for key {cache_key}: {e}. Data: {cached_data_str[:100]}...")
            except Exception as e:
                 logger.error(f"SYNC: Error processing cached data for key {cache_key}: {e}", exc_info=True)
            # Fall through to DB query if cache data was invalid

    # --- Cache Miss ---
    logger.debug(f"SYNC: Cache MISS for key {cache_key}")
    if map_config.on_cache_miss == 'fail':
        logger.warning(f"SYNC: Crosswalk lookup failed (cache miss, policy=fail) for map '{map_config.name}', key: {cache_key}")
        return None
    elif map_config.on_cache_miss == 'log_only':
        logger.info(f"SYNC: Crosswalk lookup skipped (cache miss, policy=log_only) for map '{map_config.name}', key: {cache_key}")
        return None
    elif map_config.on_cache_miss == 'query_db':
        logger.info(f"SYNC: Cache miss for map '{map_config.name}', policy=query_db. Querying external DB...")
        engine = None
        replacement_values: Optional[Dict[str, Any]] = None
        db_hit_result_str_for_cache: str = ""
        try:
            # Ensure data_source relationship is loaded if needed
            data_source_config = map_config.data_source
            if not data_source_config:
                 # Should not happen if using joinedload in CRUD, but handle defensively
                 logger.error(f"SYNC: Cannot query DB for map '{map_config.name}' - data_source config not loaded.")
                 return None

            engine = get_db_engine(data_source_config)

            # Build SELECT clause
            select_cols_set = {mapping['source_column'] for mapping in map_config.replacement_mapping}
            if not select_cols_set:
                 logger.error(f"SYNC: No source_column defined in replacement_mapping for map '{map_config.name}'. Cannot query DB.")
                 return None
            # --- Adjust Quoting based on DB Type ---
            quote_char = '`' if data_source_config.db_type == models.CrosswalkDbType.MYSQL else '"'
            select_clause = ", ".join([f'{quote_char}{col}{quote_char}' for col in select_cols_set])
            # --- End Adjustment ---

            # Build WHERE clause
            where_conditions = []
            params = {}
            param_idx = 1
            for match_info in map_config.match_columns:
                col_name = match_info['column_name']
                tag_str = match_info['dicom_tag']
                input_value = match_values.get(tag_str)
                query_value = input_value[0] if isinstance(input_value, list) and input_value else input_value
                if query_value is None:
                    logger.error(f"SYNC: Missing input value for tag '{tag_str}' (column '{col_name}') needed for DB query.")
                    return None
                param_name = f"param_{param_idx}"
                # --- Adjust Quoting based on DB Type ---
                where_conditions.append(f'{quote_char}{col_name}{quote_char} = :{param_name}')
                # --- End Adjustment ---
                params[param_name] = query_value
                param_idx += 1

            if not where_conditions:
                logger.error(f"SYNC: Could not build WHERE clause for map '{map_config.name}'."); return None
            where_clause = " AND ".join(where_conditions)

            # Construct and execute query
            # --- Adjust Quoting for table name ---
            target_table_quoted = f'{quote_char}{data_source_config.target_table}{quote_char}'
            full_query = text(f"SELECT {select_clause} FROM {target_table_quoted} WHERE {where_clause}")
            # --- End Adjustment ---
            logger.debug(f"SYNC: Executing DB query for map '{map_config.name}': {full_query} with params: {params}")

            with engine.connect() as connection:
                result = connection.execute(full_query, params)
                found_rows = [row._mapping for row in result]

            # --- Process DB Result ---
            if len(found_rows) == 0:
                logger.info(f"SYNC: DB query for map '{map_config.name}' returned no results for key values: {key_value_parts}")
                replacement_values = None
                db_hit_result_str_for_cache = CACHE_NOT_FOUND_MARKER
            elif len(found_rows) == 1:
                db_row = found_rows[0]
                logger.info(f"SYNC: DB query for map '{map_config.name}' found one result.")
                current_replacement_values: Dict[str, Any] = {}
                for mapping in map_config.replacement_mapping:
                    source_col = mapping.get('source_column')
                    target_tag = mapping.get('dicom_tag')
                    target_vr = mapping.get('dicom_vr')
                    if source_col and target_tag:
                        # Check if source_col exists in the returned row
                        if source_col not in db_row:
                             logger.warning(f"SYNC: Source column '{source_col}' in replacement_mapping not found in DB result columns: {list(db_row.keys())} for map '{map_config.name}'. Skipping.")
                             continue
                        value_from_db = db_row.get(source_col)
                        current_replacement_values[target_tag] = {"value": value_from_db, "vr": target_vr}
                replacement_values = current_replacement_values
                try:
                    db_hit_result_str_for_cache = pyjson.dumps(replacement_values)
                except TypeError as e:
                    logger.error(f"SYNC: Failed to serialize DB result dictionary to JSON for caching (map '{map_config.name}'): {e}")
                    db_hit_result_str_for_cache = "" # Don't cache bad data
            else:
                logger.error(f"SYNC: DB query for map '{map_config.name}' returned {len(found_rows)} rows (expected 0 or 1) for key values: {key_value_parts}. Ambiguous mapping.")
                replacement_values = None
                db_hit_result_str_for_cache = "" # Don't cache ambiguous results

            # --- Update Cache ---
            if redis_client and db_hit_result_str_for_cache:
                try:
                    ttl = map_config.cache_ttl_seconds or DEFAULT_CACHE_TTL_SECONDS
                    redis_client.setex(cache_key, ttl, db_hit_result_str_for_cache)
                    logger.debug(f"SYNC: Cached DB lookup result for key {cache_key} with TTL {ttl}s.")
                except redis.exceptions.RedisError as e:
                    logger.error(f"SYNC: Failed to cache DB lookup result for key {cache_key}: {e}")

            return replacement_values

        except SQLAlchemyError as e:
            logger.error(f"SYNC: Database error during live query for map '{map_config.name}': {e}", exc_info=True)
            return None
        except Exception as e:
            logger.error(f"SYNC: Unexpected error during live DB query for map '{map_config.name}': {e}", exc_info=True)
            return None
        finally:
            if engine:
                engine.dispose()
    else:
        logger.error(f"SYNC: Invalid on_cache_miss policy '{map_config.on_cache_miss}' for map '{map_config.name}'.")
        return None
