#!/usr/bin/env python3
"""
Exam Batch Completion Service

This service monitors exam_batches for studies that appear to be complete
and marks them as READY for sending. It runs as a background service.

Study completion strategies:
1. Time-based: No new instances for X seconds
2. MPPS-based: Expected instance count reached (future)
3. Manual: Operator marks complete (future)
"""

import asyncio
from datetime import datetime, timezone
import structlog

from app.db.session import SessionLocal
from app.db.models.exam_batch import ExamBatch, ExamBatchInstance
from app.core.config import settings

logger = structlog.get_logger(__name__)


class ExamBatchCompletionService:
    def __init__(self):
        self.running = False
        self.completion_timeout = getattr(settings, 'EXAM_BATCH_COMPLETION_TIMEOUT', 30)  # seconds
        self.check_interval = getattr(settings, 'EXAM_BATCH_CHECK_INTERVAL', 10)  # seconds

    async def start(self):
        """Start the completion monitoring service."""
        self.running = True
        logger.info("Starting Exam Batch Completion Service",
                    completion_timeout=self.completion_timeout,
                    check_interval=self.check_interval)

        while self.running:
            try:
                await self.check_pending_batches()
                await asyncio.sleep(self.check_interval)
            except Exception as e:
                logger.error("Error in completion service loop", error=str(e), exc_info=True)
                await asyncio.sleep(5)  # Brief pause on error

    def stop(self):
        """Stop the service."""
        self.running = False
        logger.info("Stopping Exam Batch Completion Service")

    async def check_pending_batches(self):
        """Check all pending batches to see if any should be marked as READY."""
        db = SessionLocal()
        try:
            # Get all PENDING batches
            pending_batches = db.query(ExamBatch).filter(
                ExamBatch.status == "PENDING"
            ).all()

            if not pending_batches:
                return

            logger.debug(f"Checking {len(pending_batches)} pending batches for completion")

            now = datetime.now(timezone.utc)
            completed_batches = []

            for batch in pending_batches:
                # Get the latest instance creation time for this batch
                latest_instance = db.query(ExamBatchInstance)\
                    .filter(ExamBatchInstance.batch_id == batch.id)\
                    .order_by(ExamBatchInstance.created_at.desc())\
                    .first()

                if not latest_instance:
                    # No instances yet, skip
                    continue

                # Check if enough time has passed since the last instance
                time_since_last = now - latest_instance.created_at
                if time_since_last.total_seconds() >= self.completion_timeout:
                    completed_batches.append(batch)

            # Mark completed batches as READY
            for batch in completed_batches:
                instance_count = db.query(ExamBatchInstance)\
                    .filter(ExamBatchInstance.batch_id == batch.id).count()

                logger.info("Marking exam batch as READY",
                            batch_id=batch.id,
                            study_uid=batch.study_instance_uid,
                            destination_id=batch.destination_id,
                            instance_count=instance_count)

                batch.status = "READY"
                batch.updated_at = now

            if completed_batches:
                db.commit()
                logger.info(f"Marked {len(completed_batches)} batches as READY for sending")

        except Exception as e:
            logger.error("Error checking pending batches", error=str(e), exc_info=True)
            db.rollback()
        finally:
            db.close()


# Global service instance
completion_service = ExamBatchCompletionService()


async def start_completion_service():
    """Start the completion service - called from main app startup."""
    await completion_service.start()


def stop_completion_service():
    """Stop the completion service - called from app shutdown."""
    completion_service.stop()


if __name__ == "__main__":
    """Run standalone for testing."""
    async def main():
        try:
            await start_completion_service()
        except KeyboardInterrupt:
            stop_completion_service()

    asyncio.run(main())
