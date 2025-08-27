
import sys
import os
import logging
from sqlalchemy import select

# Add the project root to the Python path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.db.session import SessionLocal
from app.db.models import DimseListenerConfig

try:
    import structlog
    from app.core.logging_config import configure_json_logging
    configure_json_logging("inspect_listeners")
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

def inspect_dimse_listeners():
    db = None
    try:
        logger.info("Connecting to the database...")
        db = SessionLocal()
        logger.info("Database connection successful. Querying all DIMSE listener configurations...")
        
        statement = select(DimseListenerConfig)
        listeners = list(db.execute(statement).scalars().all())
        
        if not listeners:
            logger.warning("No DIMSE listener configurations found in the database.")
            return

        logger.info(f"Found {len(listeners)} listener(s):")
        for listener in listeners:
            logger.info(
                f"  - ID: {listener.id}, "
                f"Name: '{listener.name}', "
                f"Instance ID: '{listener.instance_id}', "
                f"AE Title: '{listener.ae_title}', "
                f"Port: {listener.port}, "
                f"Is Enabled: {listener.is_enabled}"
            )
            
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
    finally:
        if db:
            db.close()
            logger.info("Database connection closed.")

if __name__ == "__main__":
    inspect_dimse_listeners()
