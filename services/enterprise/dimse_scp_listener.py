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
        
        # Bind event handlers
        self.ae.on_c_find = self._handle_c_find  # type: ignore[attr-defined]
        self.ae.on_c_move = self._handle_c_move  # type: ignore[attr-defined]
        
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
            self.ae.start_server(
                (self.config["host"], self.config["port"]),
                block=True
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
            
            # Submit query to spanner (async)
            asyncio.create_task(
                self._process_spanning_cfind(
                    event, query_filters, query_level, requestor_ae, requestor_address
                )
            )
            
            # For now, return pending status
            # In a real implementation, you'd yield results as they come in
            yield 0xFF00, None  # Pending status
            
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
        "ae_title": os.getenv("SPANNER_SCP_AE_TITLE", "AXIOM_SCP"),
        "host": os.getenv("HOST", "0.0.0.0"),
        "port": int(os.getenv("PORT", "11112"))
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
