# app/services/dimse/spanner_scp.py
import asyncio
import logging
import structlog
from typing import Dict, Any, Optional, List
from pynetdicom import AE, evt  # type: ignore[attr-defined]
from pynetdicom.sop_class import (
    PatientRootQueryRetrieveInformationModelFind,  # type: ignore[attr-defined]
    StudyRootQueryRetrieveInformationModelFind,  # type: ignore[attr-defined]
    PatientStudyOnlyQueryRetrieveInformationModelFind  # type: ignore[attr-defined]
)
from pydicom import Dataset
from pydicom.uid import generate_uid
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app.db import models
from app.services.spanner_engine import SpannerEngine
from app import crud

logger = structlog.get_logger(__name__)


class SpannerSCP:
    """
    DICOM SCP service that accepts C-FIND queries and routes them through the spanner engine.
    This is the entry point for external DICOM clients to perform spanning queries.
    """
    
    def __init__(self, ae_title: str = "AXIOM_SPANNER", port: int = 11112):
        self.ae_title = ae_title
        self.port = port
        self.ae = AE(ae_title=ae_title)
        self.db_session_factory = SessionLocal
        
        # Add supported presentation contexts for C-FIND
        self.ae.add_supported_context(PatientRootQueryRetrieveInformationModelFind)
        self.ae.add_supported_context(StudyRootQueryRetrieveInformationModelFind)
        self.ae.add_supported_context(PatientStudyOnlyQueryRetrieveInformationModelFind)
        
        # Set up event handlers
        self.ae.on_c_find = self.handle_c_find  # type: ignore[attr-defined]
        
        logger.info(f"Initialized Spanner SCP with AE Title: {ae_title}, Port: {port}")
    
    def start_server(self):
        """Start the DIMSE SCP server."""
        logger.info(f"Starting Spanner SCP server on port {self.port}")
        self.ae.start_server(('', self.port), block=True)
    
    def handle_c_find(self, event):
        """
        Handle incoming C-FIND requests and route them through the spanner.
        
        This is where the magic happens - we take a standard DICOM C-FIND request
        and spread it across multiple sources using the spanner engine.
        """
        logger.info(f"Received C-FIND request from {event.assoc.requestor.ae_title}")
        
        db = None  # Initialize db variable
        try:
            # Get database session
            db = self.db_session_factory()
            
            # Extract query information from the DICOM dataset
            query_dataset = event.identifier
            query_level = self._get_query_level(event.context.abstract_syntax)
            query_filters = self._dataset_to_filters(query_dataset)
            
            # Get the default spanner configuration
            # TODO: Add logic to select spanner config based on requesting AE or other criteria
            spanner_config = self._get_spanner_config_for_request(
                db, event.assoc.requestor.ae_title, event.assoc.requestor.address
            )
            
            if not spanner_config:
                logger.warning(f"No spanner config found for {event.assoc.requestor.ae_title}")
                yield 0xC000  # Failure - Unable to process
                return
            
            # Execute spanning query using asyncio.run to handle async call
            spanner_engine = SpannerEngine(db)
            spanning_result = asyncio.run(spanner_engine.execute_spanning_query(
                spanner_config_id=spanner_config.id,
                query_type="C-FIND",
                query_level=query_level,
                query_filters=query_filters,
                requesting_ae_title=event.assoc.requestor.ae_title,
                requesting_ip=event.assoc.requestor.address
            ))
            
            # Convert results back to DICOM datasets and yield them
            results = spanning_result.get('results', [])
            logger.info(f"Spanning query returned {len(results)} results")
            
            for result in results:
                response_dataset = self._result_to_dataset(result, query_dataset)
                yield (0x0000, response_dataset)  # Success status
            
            # Log the query completion
            logger.info(f"C-FIND spanning query completed: {spanning_result['metadata']}")
            
        except Exception as e:
            logger.error(f"Error processing C-FIND request: {e}", exc_info=True)
            yield 0xC000  # Failure - Unable to process
        
        finally:
            if db:
                db.close()
    
    def _get_query_level(self, abstract_syntax: str) -> str:
        """Determine the query level from the abstract syntax."""
        if "Patient" in abstract_syntax:
            return "PATIENT"
        elif "Study" in abstract_syntax:
            return "STUDY"
        else:
            return "STUDY"  # Default to STUDY level
    
    def _dataset_to_filters(self, dataset: Dataset) -> Dict[str, Any]:
        """Convert a DICOM dataset to query filters."""
        filters = {}
        
        # Common DICOM tags that are used in queries
        tag_mappings = {
            'PatientID': (0x0010, 0x0020),
            'PatientName': (0x0010, 0x0010),
            'StudyInstanceUID': (0x0020, 0x000D),
            'StudyDate': (0x0008, 0x0020),
            'StudyTime': (0x0008, 0x0030),
            'AccessionNumber': (0x0008, 0x0050),
            'ModalitiesInStudy': (0x0008, 0x0061),
            'SeriesInstanceUID': (0x0020, 0x000E),
            'Modality': (0x0008, 0x0060),
            'SOPInstanceUID': (0x0008, 0x0018),
        }
        
        for dicom_name, tag in tag_mappings.items():
            if tag in dataset:
                value = dataset[tag].value
                if value:  # Only include non-empty values
                    filters[dicom_name] = str(value)
        
        return filters
    
    def _result_to_dataset(self, result: Dict[str, Any], query_dataset: Dataset) -> Dataset:
        """Convert a query result back to a DICOM dataset."""
        response_ds = Dataset()
        
        # Copy the query retrieve level
        if (0x0008, 0x0052) in query_dataset:
            response_ds.QueryRetrieveLevel = query_dataset.QueryRetrieveLevel
        
        # Map common fields back to DICOM tags
        field_mappings = {
            'PatientID': (0x0010, 0x0020),
            'PatientName': (0x0010, 0x0010),
            'StudyInstanceUID': (0x0020, 0x000D),
            'StudyDate': (0x0008, 0x0020),
            'StudyTime': (0x0008, 0x0030),
            'AccessionNumber': (0x0008, 0x0050),
            'ModalitiesInStudy': (0x0008, 0x0061),
            'SeriesInstanceUID': (0x0020, 0x000E),
            'Modality': (0x0008, 0x0060),
            'SOPInstanceUID': (0x0008, 0x0018),
            'StudyDescription': (0x0008, 0x1030),
            'SeriesDescription': (0x0008, 0x103E),
            'NumberOfStudyRelatedSeries': (0x0020, 0x1206),
            'NumberOfStudyRelatedInstances': (0x0020, 0x1208),
        }
        
        for field_name, tag in field_mappings.items():
            if field_name in result:
                setattr(response_ds, field_name, result[field_name])
        
        # Add metadata about the spanning operation
        if result.get('_source_names'):
            # Add a private tag to indicate which sources contributed (for debugging)
            response_ds.add_new((0x7777, 0x0001), 'LO', ', '.join(result['_source_names']))
        
        return response_ds
    
    def _get_spanner_config_for_request(
        self, 
        db: Session, 
        requesting_ae: str, 
        requesting_ip: str
    ) -> Optional[models.SpannerConfig]:
        """
        Determine which spanner configuration to use for this request.
        
        For now, this just returns the first enabled spanner config.
        In the future, you could add logic to select based on:
        - Requesting AE Title
        - IP address
        - Time of day
        - Query level
        - etc.
        """
        configs = crud.crud_spanner_config.get_multi(db, include_disabled=False, limit=1)
        
        if configs and configs[0].supports_cfind:
            return configs[0]
        
        return None


class SpannerSCPManager:
    """
    Manager class to handle multiple spanner SCP instances.
    Useful if you want to run different spanner configs on different ports.
    """
    
    def __init__(self):
        self.scps: Dict[str, SpannerSCP] = {}
    
    def add_scp(self, name: str, ae_title: str, port: int) -> SpannerSCP:
        """Add a new SCP instance."""
        scp = SpannerSCP(ae_title=ae_title, port=port)
        self.scps[name] = scp
        return scp
    
    def start_scp(self, name: str):
        """Start a specific SCP."""
        if name in self.scps:
            logger.info(f"Starting SCP: {name}")
            self.scps[name].start_server()
        else:
            raise ValueError(f"SCP {name} not found")
    
    def start_all_scps(self):
        """Start all configured SCPs."""
        import threading
        
        for name, scp in self.scps.items():
            thread = threading.Thread(target=scp.start_server, name=f"SCP-{name}")
            thread.daemon = True
            thread.start()
            logger.info(f"Started SCP {name} in background thread")


# Factory functions for easy usage
def create_default_spanner_scp() -> SpannerSCP:
    """Create a spanner SCP with default settings."""
    return SpannerSCP(ae_title="AXIOM_SPANNER", port=11112)


def create_spanner_scp_manager() -> SpannerSCPManager:
    """Create a spanner SCP manager."""
    return SpannerSCPManager()
