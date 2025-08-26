# app/crosswalk/tasks.py
import logging
import structlog
import time
from celery import shared_task
from sqlalchemy.sql import func, text
from sqlalchemy.exc import SQLAlchemyError

from app.db.session import SessionLocal
from app.db import models
from app.crud import crud_crosswalk_data_source
from app.crosswalk import service as crosswalk_service

logger = structlog.get_logger(__name__)

SYNC_CHUNK_SIZE = 10000
PERFORM_PREWARMING_SYNC = True

@shared_task(name="sync_all_enabled_crosswalk_sources")
def sync_all_enabled_crosswalk_sources():
    # ... (no changes needed here) ...
    logger.info("Beat Task: Checking for enabled crosswalk sources...")
    db = SessionLocal()
    enabled_sources = []
    try:
        enabled_sources = crud_crosswalk_data_source.get_enabled_sources(db)
        logger.info(f"Found {len(enabled_sources)} enabled crosswalk sources.")
        for source in enabled_sources:
            logger.info(f"Queueing sync/health check task for source ID: {source.id}, Name: {source.name}")
            sync_crosswalk_source_task.delay(source.id) # type: ignore
    except Exception as e:
        logger.error(f"Error querying/queueing crosswalk sources: {e}", exc_info=True)
    finally:
        db.close()
    return f"Queued tasks for {len(enabled_sources)} sources."


@shared_task(name="sync_crosswalk_source_task", acks_late=True, max_retries=1, default_retry_delay=300)
def sync_crosswalk_source_task(source_id: int):
    """
    Celery task to perform health check and optionally pre-warm cache
    for a single crosswalk data source.
    """
    logger.info(f"Starting sync/health check task for Crosswalk Data Source ID: {source_id}")
    start_time = time.monotonic()
    db = SessionLocal()
    source_config = None
    task_result_msg = f"Task completed for source ID {source_id}."

    try:
        source_config = crud_crosswalk_data_source.get(db, id=source_id)
        if not source_config:
            logger.error(f"Sync Task Error: Data source ID {source_id} not found.")
            return f"Failed: Source ID {source_id} not found."
        if not source_config.is_enabled:
             logger.info(f"Sync Task Skipped: Data source ID {source_id} ('{source_config.name}') is disabled.")
             return f"Skipped: Source ID {source_id} disabled."

        # --- Perform Health Check ---
        logger.debug(f"Performing connection test for source ID {source_id}...")
        connected, conn_msg = crosswalk_service.test_connection(source_config)
        if not connected:
            logger.error(f"Health check failed for source ID {source_id}: {conn_msg}")
            crud_crosswalk_data_source.update_sync_status(
                db=db, source_id=source_id, status=models.CrosswalkSyncStatus.FAILED, error_message=f"Connection Test Failed: {conn_msg}"
            )
            return f"Failed: Connection test failed for source ID {source_id}."
        else:
             logger.info(f"Health check successful for source ID {source_id}.")
             crud_crosswalk_data_source.update_sync_status(
                 db=db, source_id=source_id, status=models.CrosswalkSyncStatus.PENDING,
                 error_message=None
             )

        # --- Optional Pre-warming Sync ---
        if PERFORM_PREWARMING_SYNC:
            logger.info(f"Starting cache pre-warming for source ID {source_id}...")
            crud_crosswalk_data_source.update_sync_status(
                db=db, source_id=source_id, status=models.CrosswalkSyncStatus.SYNCING
            )

            engine = None
            total_rows_processed = 0
            batch_errors = []
            offset = 0

            try:
                engine = crosswalk_service.get_db_engine(source_config)
                # --- UPDATE ORDER BY COLUMN for MySQL ---
                # Use the actual primary key of the crosswalk table. Backticks for MySQL.
                order_col = "`old_mrn`"
                logger.debug(f"Using ordering column: {order_col} for chunking source {source_id}")
                # --- End Update ---

                with engine.connect() as connection:
                    while True:
                        logger.info(f"Fetching pre-warming chunk for source {source_id}, offset {offset}, size {SYNC_CHUNK_SIZE}")
                        try:
                            # Use the defined order column
                            stmt = text(f"SELECT * FROM {source_config.target_table} ORDER BY {order_col} LIMIT {SYNC_CHUNK_SIZE} OFFSET {offset}")
                            result = connection.execute(stmt)
                            chunk_data = [dict(row._mapping) for row in result] # Convert RowMapping to dict
                        except SQLAlchemyError as db_err:
                             logger.error(f"Database error fetching chunk for source {source_id} at offset {offset}: {db_err}")
                             batch_errors.append(f"DB Error offset {offset}: {db_err}")
                             break

                        if not chunk_data:
                            logger.info(f"No more data found for source {source_id} at offset {offset}.")
                            break

                        logger.debug(f"Fetched {len(chunk_data)} rows for pre-warming source {source_id}, offset {offset}.")

                        rows_in_chunk, cache_err = crosswalk_service.update_cache(source_config, chunk_data)
                        if cache_err:
                            logger.error(f"Error pre-warming cache for chunk (source {source_id}, offset {offset}): {cache_err}")
                            batch_errors.append(f"Cache Error offset {offset}: {cache_err}")
                        total_rows_processed += rows_in_chunk
                        offset += SYNC_CHUNK_SIZE
                        # time.sleep(0.05)

                final_status = models.CrosswalkSyncStatus.SUCCESS if not batch_errors else models.CrosswalkSyncStatus.FAILED
                final_error_msg = "; ".join(batch_errors) if batch_errors else None
                crud_crosswalk_data_source.update_sync_status(
                    db=db, source_id=source_id, status=final_status, error_message=final_error_msg, row_count=total_rows_processed
                )
                task_result_msg = f"Sync {final_status.value}: Processed {total_rows_processed} rows for source ID {source_id}. Errors: {len(batch_errors)}"
                logger.info(task_result_msg)

            except Exception as sync_exc:
                 logger.error(f"Critical error during sync processing for source ID {source_id}: {sync_exc}", exc_info=True)
                 crud_crosswalk_data_source.update_sync_status(db=db, source_id=source_id, status=models.CrosswalkSyncStatus.FAILED, error_message=f"Sync Processing Error: {sync_exc}")
                 task_result_msg = f"Failed: Sync processing error for source ID {source_id}."
            finally:
                 if engine: engine.dispose()
        else:
             logger.info(f"Skipping cache pre-warming for source ID {source_id} as per configuration.")
             task_result_msg = f"Health check completed for source ID {source_id}."

    except Exception as e:
        logger.critical(f"Sync Task CRITICAL Error for source ID {source_id}: {e}", exc_info=True)
        if source_config:
            try: crud_crosswalk_data_source.update_sync_status(db=db, source_id=source_id, status=models.CrosswalkSyncStatus.FAILED, error_message=f"Task Error: {e}")
            except Exception as db_err: logger.error(f"Failed to update error status during critical failure handling for source ID {source_id}: {db_err}")
        raise e
    finally:
        if db: db.close()
        end_time = time.monotonic()
        logger.info(f"Finished sync/health check task for source ID {source_id} in {end_time - start_time:.2f} seconds.")

    return task_result_msg
