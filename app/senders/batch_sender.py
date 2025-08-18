# app/senders/batch_sender.py
import time
# import schedule  # Commented out - install with: pip install schedule
import threading
from sqlalchemy.orm import Session
from app import crud
from app.db.models.exam_batch import ExamBatch
from app.db.session import SessionLocal
import pydicom
import structlog

logger = structlog.get_logger(__name__)


def send_batch(db: Session, batch: ExamBatch):
    logger.info(f"Processing batch {batch.id} for study {batch.study_instance_uid}")

    # Update status to sending
    crud.crud_exam_batch.crud_exam_batch.update_status(db, db_obj=batch, status="SENDING")
    db.commit()

    datasets = []
    for instance in batch.instances:
        try:
            ds = pydicom.dcmread(instance.processed_filepath)
            datasets.append(ds)
            logger.info(f"Loaded DICOM file: {instance.processed_filepath}")
        except Exception as e:
            logger.error(f"Failed to load DICOM file {instance.processed_filepath}: {e}")
            # Update batch status to failed and return
            crud.crud_exam_batch.crud_exam_batch.update_status(db, db_obj=batch, status="FAILED")
            db.commit()
            return

    if not datasets:
        logger.warning(f"No valid DICOM files found for batch {batch.id}")
        crud.crud_exam_batch.crud_exam_batch.update_status(db, db_obj=batch, status="FAILED")
        db.commit()
        return

    # Send all datasets in a single association
    try:
        # Here you would implement the actual DICOM sending logic
        # using your preferred DICOM library (pynetdicom, etc.)
        logger.info(f"Sending {len(datasets)} instances for study {batch.study_instance_uid}")

        # For now, just simulate success
        crud.crud_exam_batch.crud_exam_batch.update_status(db, db_obj=batch, status="COMPLETED")
        db.commit()
        logger.info(f"Successfully sent batch {batch.id}")

    except Exception as e:
        logger.error(f"Failed to send batch {batch.id}: {e}")
        crud.crud_exam_batch.crud_exam_batch.update_status(db, db_obj=batch, status="FAILED")
        db.commit()


def check_for_ready_batches():
    db = SessionLocal()
    try:
        ready_batches = crud.crud_exam_batch.crud_exam_batch.get_ready_for_sending(db)
        for batch in ready_batches:
            send_batch(db, batch)
    finally:
        db.close()


def run_scheduler():
    # Alternative simple scheduler without the schedule library
    while True:
        try:
            check_for_ready_batches()
            time.sleep(10)  # Check every 10 seconds
        except Exception as e:
            logger.error(f"Error in scheduler: {e}")
            time.sleep(10)


if __name__ == "__main__":
    logger.info("Starting batch sender scheduler...")
    scheduler_thread = threading.Thread(target=run_scheduler)
    scheduler_thread.start()
