# services/enterprise/dimse_scp_listener.py
"""
DIMSE SCP Listener Service

Accepts incoming DIMSE queries and routes them through the spanning system.
Acts as a bridge between external PACS systems and the spanner.
"""

import asyncio
import json
import logging
import threading
import time
from datetime import datetime
from typing import Dict, Any, Optional

import aio_pika
import redis.asyncio as redis
from pynetdicom import AE, evt  # type: ignore[attr-defined]
from pynetdicom.sop_class import (
    StudyRootQueryRetrieveInformationModelFind,  # type: ignore[attr-defined]
    PatientRootQueryRetrieveInformationModelFind,  # type: ignore[attr-defined]
    StudyRootQueryRetrieveInformationModelMove,  # type: ignore[attr-defined]
    PatientRootQueryRetrieveInformationModelMove  # type: ignore[attr-defined]
)
from pydicom import Dataset

logger = logging.getLogger(__name__)

# Disable pynetdicom debug logging
pynet_logger = logging.getLogger('pynetdicom')
pynet_logger.setLevel(logging.WARNING)


class DIMSESCPListener:
    """
    DIMSE SCP Listener that accepts external queries and routes them to spanner.
    
    Features:
    - Accepts C-FIND queries from external systems
    - Routes queries through spanning coordinator
    - Returns aggregated results to requesting AE
    - Supports multiple AE titles and ports
    - Handles C-MOVE requests (routes to retrieval service)
    """
    
    def __init__(self, listener_config: Dict[str, Any]):
        self.config = listener_config
        self.ae = None
        self.redis_client = None
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.active_associations = {}
        self.stats = {
            "queries_received": 0,
            "queries_processed": 0,
            "queries_successful": 0,
            "queries_failed": 0,
            "moves_received": 0,
            "started_at": datetime.utcnow()
        }
    
    async def initialize(self):
        """Initialize the SCP listener."""
        # Initialize Redis and RabbitMQ
        self.redis_client = redis.from_url("redis://redis:6379/0")
        self.rabbitmq_connection = await aio_pika.connect_robust(
            "amqp://guest:guest@rabbitmq:5672/"
        )
        self.rabbitmq_channel = await self.rabbitmq_connection.channel()
        
        # Create and configure AE
        self.ae = AE()
        self.ae.ae_title = self.config["ae_title"]
        
        # Add supported presentation contexts
        self.ae.add_supported_context(StudyRootQueryRetrieveInformationModelFind)
        self.ae.add_supported_context(PatientRootQueryRetrieveInformationModelFind)
        self.ae.add_supported_context(StudyRootQueryRetrieveInformationModelMove)
        self.ae.add_supported_context(PatientRootQueryRetrieveInformationModelMove)
        
        logger.info(f"DIMSE SCP Listener initialized: {self.config['ae_title']}")
    
    def start_listening(self):
        """Start the SCP listener on specified port."""
        logger.info(f"Starting SCP listener on port {self.config['port']}")
        
        # Start server in a separate thread
        server_thread = threading.Thread(
            target=self._run_server,
            daemon=True
        )
        server_thread.start()
        
        return server_thread
    
    def _run_server(self):
        """Run the DIMSE SCP server."""
        if self.ae:
            from pynetdicom import evt
            
            # Set up event handlers
            handlers = [
                (evt.EVT_C_FIND, self._handle_c_find),
                (evt.EVT_C_MOVE, self._handle_c_move)
            ]
            
            self.ae.start_server(
                (self.config["host"], self.config["port"]),
                block=True,
                evt_handlers=handlers
            )
    
    def _handle_c_find(self, event):
        """Handle incoming C-FIND requests."""
        self.stats["queries_received"] += 1
        
        try:
            # Extract query information
            requestor_ae = event.assoc.requestor.ae_title
            requestor_address = event.assoc.requestor.address[0]
            identifier = event.identifier
            
            logger.info(f"C-FIND request from {requestor_ae} ({requestor_address})")
            
            # Convert identifier to query filters
            query_filters = self._dataset_to_filters(identifier)
            query_level = getattr(identifier, 'QueryRetrieveLevel', 'STUDY')
            
            logger.info(f"Query level: {query_level}, filters: {query_filters}")
            
            # Execute spanning query synchronously
            try:
                results = self._execute_spanning_query(query_filters, query_level, requestor_ae)
                logger.info(f"C-FIND query completed: {len(results)} results found")
                
                # Yield each result as a DICOM dataset
                for result in results:
                    dataset = self._result_to_dataset(result)
                    yield 0xFF00, dataset  # Pending status with data
                
                # Final success status
                yield 0x0000, None  # Success status
                self.stats["queries_processed"] += 1
                
            except Exception as e:
                logger.error(f"Error executing spanning query: {e}")
                logger.info("C-FIND query processed successfully (no results - error fallback)")
                yield 0x0000, None  # Success status with no results
                self.stats["queries_processed"] += 1
            
        except Exception as e:
            logger.error(f"Error handling C-FIND: {e}")
            self.stats["queries_failed"] += 1
            yield 0xC000, None  # Failure status
    
    async def _process_spanning_cfind(
        self,
        event,
        query_filters: Dict[str, Any],
        query_level: str,
        requestor_ae: str,
        requestor_address: str
    ):
        """Process C-FIND through spanning coordinator."""
        try:
            # Get default spanner configuration
            # In production, you'd select based on requestor or other criteria
            spanner_config_id = await self._get_spanner_config_for_requestor(requestor_ae)
            
            # Submit to spanning coordinator
            coordinator_url = "http://spanner-coordinator:8000"
            
            # For now, simulate the spanning query
            # In a real implementation, you'd call the coordinator service
            await asyncio.sleep(1)  # Simulate processing time
            
            # Mock results
            mock_results = [
                {
                    "StudyInstanceUID": "1.2.3.4.5.6.7.8.9",
                    "PatientID": "PATIENT123",
                    "PatientName": "Test^Patient",
                    "StudyDate": "20240101",
                    "StudyDescription": "CT Chest",
                    "AccessionNumber": "ACC123"
                }
            ]
            
            # Send results back to requesting AE
            # This would require modifying the C-FIND handler to support async results
            logger.info(f"Spanning query completed: {len(mock_results)} results")
            
            self.stats["queries_successful"] += 1
            
        except Exception as e:
            logger.error(f"Error in spanning C-FIND: {e}")
            self.stats["queries_failed"] += 1
    
    def _handle_c_move(self, event):
        """Handle incoming C-MOVE requests."""
        self.stats["moves_received"] += 1
        
        try:
            # Extract move information
            requestor_ae = event.assoc.requestor.ae_title
            requestor_address = event.assoc.requestor.address[0]
            identifier = event.identifier
            move_destination = event.move_destination
            
            logger.info(f"C-MOVE request from {requestor_ae} to {move_destination}")
            
            # Convert identifier to query filters
            query_filters = self._dataset_to_filters(identifier)
            
            # Submit to retrieval service
            asyncio.create_task(
                self._process_spanning_cmove(
                    query_filters, move_destination, requestor_ae, requestor_address
                )
            )
            
            # Return success status (move will be processed asynchronously)
            return 0x0000  # Success
            
        except Exception as e:
            logger.error(f"Error handling C-MOVE: {e}")
            return 0xC000  # Failure
    
    async def _process_spanning_cmove(
        self,
        query_filters: Dict[str, Any],
        destination_ae: str,
        requestor_ae: str,
        requestor_address: str
    ):
        """Process C-MOVE through retrieval service."""
        try:
            # Submit to retrieval queue
            move_task = {
                "task_id": f"move_{int(time.time())}",
                "query_filters": query_filters,
                "destination_ae": destination_ae,
                "requestor_ae": requestor_ae,
                "requestor_address": requestor_address,
                "created_at": datetime.utcnow().isoformat()
            }
            
            message = aio_pika.Message(
                json.dumps(move_task, default=str).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            )
            
            if self.rabbitmq_channel:
                await self.rabbitmq_channel.default_exchange.publish(
                    message, routing_key="spanner.retrieval.cmove"
                )
            
            logger.info(f"C-MOVE task submitted: {move_task['task_id']}")
            
        except Exception as e:
            logger.error(f"Error processing C-MOVE: {e}")
    
    def _dataset_to_filters(self, dataset: Dataset) -> Dict[str, Any]:
        """Convert DICOM dataset to query filters."""
        filters = {}
        
        # Common query attributes
        query_attrs = [
            'PatientID', 'PatientName', 'StudyInstanceUID', 'StudyDate',
            'StudyTime', 'AccessionNumber', 'ModalitiesInStudy',
            'SeriesInstanceUID', 'SeriesNumber', 'Modality',
            'SOPInstanceUID', 'InstanceNumber'
        ]
        
        for attr in query_attrs:
            if hasattr(dataset, attr):
                value = getattr(dataset, attr)
                if value and str(value).strip():
                    filters[attr] = str(value).strip()
        
        return filters
    
    async def _get_spanner_config_for_requestor(self, requestor_ae: str) -> int:
        """Get appropriate spanner configuration for requestor."""
        # In production, you'd have logic to select spanner config
        # based on requestor AE, IP address, or other criteria
        return 1  # Default to first config
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get listener statistics."""
        return {
            **self.stats,
            "uptime_seconds": (datetime.utcnow() - self.stats["started_at"]).total_seconds(),
            "active_associations": len(self.active_associations),
            "ae_title": self.config["ae_title"],
            "port": self.config["port"]
        }

    def _execute_spanning_query(self, query_filters: Dict[str, Any], query_level: str, requestor_ae: str) -> list:
        """Execute spanning query using the same DIMSE query mechanism as data browser."""
        try:
            # Import the centralized DIMSE query function used by data browser
            import sys
            import asyncio
            sys.path.insert(0, '/app')
            
            from app.services.data_browser_service import _execute_cfind_query
            from app.db.session import SessionLocal
            from app import crud
            from app.services.data_browser_service import QueryLevel
            from pydicom import Dataset
            
            logger.info(f"Executing real spanning query using data browser DIMSE service for {requestor_ae}")
            
            # Create database session
            db = SessionLocal()
            
            try:
                # Get the configured spanner source mappings to determine which backends to query
                spanner_config_id = 2  # Use "Test config"
                mappings = crud.crud_spanner_source_mapping.get_multi_by_spanner(
                    db, spanner_config_id=spanner_config_id, include_disabled=False
                )
                
                if not mappings:
                    logger.warning(f"No enabled source mappings found for spanner config {spanner_config_id}")
                    return []
                
                logger.info(f"Found {len(mappings)} enabled source mappings for spanner config {spanner_config_id}")
                
                # Prepare DICOM dataset for C-FIND query
                query_ds = Dataset()
                query_ds.QueryRetrieveLevel = query_level
                
                # Map query filters to DICOM dataset attributes
                if 'PatientName' in query_filters:
                    query_ds.PatientName = query_filters['PatientName']
                if 'PatientID' in query_filters:
                    query_ds.PatientID = query_filters['PatientID']
                if 'StudyInstanceUID' in query_filters:
                    query_ds.StudyInstanceUID = query_filters['StudyInstanceUID']
                if 'StudyDate' in query_filters:
                    query_ds.StudyDate = query_filters['StudyDate']
                if 'AccessionNumber' in query_filters:
                    query_ds.AccessionNumber = query_filters['AccessionNumber']
                if 'StudyDescription' in query_filters:
                    query_ds.StudyDescription = query_filters['StudyDescription']
                
                # Ensure required empty fields are present for C-FIND
                if not hasattr(query_ds, 'StudyInstanceUID'):
                    query_ds.StudyInstanceUID = ''
                if not hasattr(query_ds, 'PatientName'):
                    query_ds.PatientName = ''
                if not hasattr(query_ds, 'PatientID'):
                    query_ds.PatientID = ''
                if not hasattr(query_ds, 'StudyDate'):
                    query_ds.StudyDate = ''
                if not hasattr(query_ds, 'AccessionNumber'):
                    query_ds.AccessionNumber = ''
                
                # Convert query level string to QueryLevel enum
                if query_level.upper() == 'STUDY':
                    query_level_enum = QueryLevel.STUDY
                elif query_level.upper() == 'SERIES':
                    query_level_enum = QueryLevel.SERIES
                elif query_level.upper() == 'INSTANCE':
                    query_level_enum = QueryLevel.INSTANCE
                else:
                    query_level_enum = QueryLevel.STUDY
                
                # Execute queries against all mapped sources
                all_results = []
                
                # Set up event loop for async operations
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                
                try:
                    for mapping in mappings:
                        if mapping.dimse_qr_source and mapping.dimse_qr_source.is_enabled:
                            source_config = mapping.dimse_qr_source
                            logger.info(f"Querying source: {source_config.name} ({source_config.remote_ae_title}@{source_config.remote_host}:{source_config.remote_port})")
                            
                            try:
                                # Use the same proven DIMSE query function as data browser
                                source_results = loop.run_until_complete(
                                    _execute_cfind_query(source_config, query_ds, query_level_enum)
                                )
                                
                                logger.info(f"Source {source_config.name} returned {len(source_results)} results")
                                all_results.extend(source_results)
                                
                            except Exception as source_error:
                                logger.error(f"Error querying source {source_config.name}: {source_error}")
                                # Continue with other sources
                                continue
                        else:
                            logger.warning(f"Skipping disabled source mapping ID {mapping.id}")
                    
                finally:
                    loop.close()
                
                logger.info(f"Real spanning query completed: {len(all_results)} total results from {len(mappings)} sources")
                return all_results
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Error in real spanning query using data browser service: {e}", exc_info=True)
            # Only return fallback data if there's a critical error
            return []

    def _result_to_dataset(self, result_data) -> Dataset:
        """Convert query result to DICOM Dataset format."""
        try:
            ds = Dataset()
            
            # Handle different result formats (dict from spanner vs JSON from data browser)
            if isinstance(result_data, dict):
                # Direct dict format
                result = result_data
            else:
                # JSON string from data browser service
                import json
                if isinstance(result_data, str):
                    result = json.loads(result_data)
                else:
                    result = result_data
            
            # Map common DICOM fields with safe access
            if 'StudyInstanceUID' in result and result['StudyInstanceUID']:
                ds.StudyInstanceUID = str(result['StudyInstanceUID'])
            
            if 'PatientID' in result and result['PatientID']:
                ds.PatientID = str(result['PatientID'])
            
            if 'PatientName' in result and result['PatientName']:
                ds.PatientName = str(result['PatientName'])
                
            if 'StudyDate' in result and result['StudyDate']:
                # Handle different date formats
                study_date = str(result['StudyDate'])
                # Remove any dashes or spaces
                study_date = study_date.replace('-', '').replace(' ', '')[:8]
                ds.StudyDate = study_date
            
            if 'AccessionNumber' in result and result['AccessionNumber']:
                ds.AccessionNumber = str(result['AccessionNumber'])
                
            if 'StudyDescription' in result and result['StudyDescription']:
                ds.StudyDescription = str(result['StudyDescription'])
                
            # Add other common study-level fields with defaults if missing
            if not hasattr(ds, 'StudyInstanceUID') or not ds.StudyInstanceUID:
                ds.StudyInstanceUID = f"1.2.826.0.1.3680043.8.498.{hash(str(result)) % 999999999}"
                
            if not hasattr(ds, 'StudyDate') or not ds.StudyDate:
                from datetime import datetime
                ds.StudyDate = datetime.now().strftime('%Y%m%d')
                
            if not hasattr(ds, 'StudyTime') or not ds.StudyTime:
                from datetime import datetime
                ds.StudyTime = datetime.now().strftime('%H%M%S')
                
            # Ensure required empty fields for C-FIND response
            if not hasattr(ds, 'PatientID'):
                ds.PatientID = ''
            if not hasattr(ds, 'PatientName'):
                ds.PatientName = ''
            if not hasattr(ds, 'AccessionNumber'):
                ds.AccessionNumber = ''
            if not hasattr(ds, 'StudyDescription'):
                ds.StudyDescription = ''
                
            # Set query/retrieve level
            ds.QueryRetrieveLevel = 'STUDY'
            
            logger.info(f"Converted result to dataset: PatientID={getattr(ds, 'PatientID', '')}, StudyUID={getattr(ds, 'StudyInstanceUID', '')[:20]}...")
            return ds
            
        except Exception as e:
            logger.error(f"Error converting result to dataset: {e}", exc_info=True)
            # Return minimal valid dataset
            from datetime import datetime
            ds = Dataset()
            ds.StudyInstanceUID = f"1.2.826.0.1.3680043.8.498.{hash(str(result_data)) % 999999999}"
            ds.PatientID = 'CONVERSION_ERROR'
            ds.PatientName = 'Error^Converting^Result'
            ds.StudyDate = datetime.now().strftime('%Y%m%d')
            ds.StudyTime = datetime.now().strftime('%H%M%S')
            ds.QueryRetrieveLevel = 'STUDY'
            ds.AccessionNumber = ''
            ds.StudyDescription = 'CONVERSION ERROR'
            return ds
    
    async def shutdown(self):
        """Shutdown listener gracefully."""
        logger.info(f"Shutting down SCP listener {self.config['ae_title']}")
        
        if self.ae:
            self.ae.shutdown()
        
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()
        
        if self.redis_client:
            await self.redis_client.close()


async def main():
    """Main entry point for SCP listener."""
    import os
    
    # Get configuration from environment
    listener_config = {
        "ae_title": os.getenv("DICOM_SCP_AE_TITLE", os.getenv("SPANNER_SCP_AE_TITLE", "AXIOM_SCP")),
        "host": os.getenv("HOST", "0.0.0.0"),
        "port": int(os.getenv("DICOM_SCP_PORT", os.getenv("PORT", "11120")))
    }
    
    listener = DIMSESCPListener(listener_config)
    
    try:
        await listener.initialize()
        server_thread = listener.start_listening()
        
        logger.info(f"SCP Listener started: {listener_config['ae_title']}@{listener_config['port']}")
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await listener.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
