# services/enterprise/dimse_query_worker.py
"""
DIMSE Query Worker Service

Handles individual DIMSE queries against PACS systems.
Consumes tasks from RabbitMQ and publishes results back.
"""

import asyncio
import json
import logging
import time
from datetime import datetime
from typing import Dict, List, Optional, Any

import aio_pika
from pynetdicom import AE, debug_logger
from pydicom import Dataset
from sqlalchemy.orm import Session

from app.db.session import SessionLocal
from app import crud
from app.services.data_browser_service import (
    _build_find_identifier,
    QueryServiceError,
    SourceNotFoundError,
    RemoteConnectionError,
    RemoteQueryError
)

logger = logging.getLogger(__name__)

# Disable pynetdicom debug logging in production
debug_logger.setLevel(logging.WARNING)


class DIMSEQueryWorker:
    """
    Worker service for executing DIMSE queries against PACS systems.
    
    Features:
    - Consumes query tasks from RabbitMQ
    - Executes C-FIND queries against PACS
    - Handles connection timeouts and retries
    - Publishes results back to coordinator
    - Connection pooling for performance
    """
    
    def __init__(self, worker_id: str = None):
        self.worker_id = worker_id or f"dimse-worker-{int(time.time())}"
        self.rabbitmq_connection = None
        self.rabbitmq_channel = None
        self.ae_connections = {}  # Connection pool
        self.stats = {
            "queries_processed": 0,
            "queries_successful": 0,
            "queries_failed": 0,
            "started_at": datetime.utcnow()
        }
    
    async def initialize(self):
        """Initialize RabbitMQ connection and start consuming."""
        self.rabbitmq_connection = await aio_pika.connect_robust(
            "amqp://guest:guest@rabbitmq:5672/"
        )
        self.rabbitmq_channel = await self.rabbitmq_connection.channel()
        
        # Set QoS to process one message at a time
        await self.rabbitmq_channel.set_qos(prefetch_count=1)
        
        # Start consuming from DIMSE query queue
        queue = await self.rabbitmq_channel.declare_queue(
            "spanner.query.dimse", durable=True
        )
        
        await queue.consume(self._process_query_task)
        
        logger.info(f"DIMSE Query Worker {self.worker_id} initialized and consuming")
    
    async def _process_query_task(self, message: aio_pika.IncomingMessage):
        """Process a single DIMSE query task."""
        async with message.process():
            try:
                task_data = json.loads(message.body.decode())
                result = await self._execute_dimse_query(task_data)
                
                # Publish result
                await self._publish_result(result)
                
                self.stats["queries_processed"] += 1
                if result["success"]:
                    self.stats["queries_successful"] += 1
                else:
                    self.stats["queries_failed"] += 1
                
            except Exception as e:
                logger.error(f"Error processing query task: {e}")
                # Publish error result
                error_result = {
                    "query_id": task_data.get("query_id"),
                    "task_id": task_data.get("task_id"),
                    "source_id": task_data.get("source_id"),
                    "success": False,
                    "error": str(e),
                    "data": [],
                    "worker_id": self.worker_id,
                    "completed_at": datetime.utcnow().isoformat()
                }
                await self._publish_result(error_result)
                self.stats["queries_failed"] += 1
    
    async def _execute_dimse_query(self, task_data: Dict[str, Any]) -> Dict[str, Any]:
        """Execute a DIMSE query against a PACS system."""
        start_time = time.time()
        
        query_id = task_data["query_id"]
        task_id = task_data["task_id"]
        source_id = task_data["source_id"]
        source_config = task_data["source_config"]
        query_type = task_data["query_type"]
        query_level = task_data["query_level"]
        query_filters = task_data["query_filters"]
        timeout_seconds = task_data.get("timeout_seconds", 30)
        
        logger.info(f"Executing DIMSE query {task_id} for source {source_id}")
        
        try:
            # Get or create AE connection
            ae = await self._get_ae_connection(source_config)
            
            # Build query dataset
            query_ds = self._build_query_dataset(query_level, query_filters)
            
            # Execute C-FIND
            results = await self._execute_cfind(
                ae, source_config, query_ds, query_level, timeout_seconds
            )
            
            duration = time.time() - start_time
            
            return {
                "query_id": query_id,
                "task_id": task_id,
                "source_id": source_id,
                "success": True,
                "data": results,
                "result_count": len(results),
                "duration_seconds": duration,
                "worker_id": self.worker_id,
                "completed_at": datetime.utcnow().isoformat()
            }
            
        except Exception as e:
            duration = time.time() - start_time
            logger.error(f"DIMSE query {task_id} failed: {e}")
            
            return {
                "query_id": query_id,
                "task_id": task_id,
                "source_id": source_id,
                "success": False,
                "error": str(e),
                "data": [],
                "duration_seconds": duration,
                "worker_id": self.worker_id,
                "completed_at": datetime.utcnow().isoformat()
            }
    
    async def _get_ae_connection(self, source_config: Dict[str, Any]) -> AE:
        """Get or create an AE connection for a source."""
        source_key = f"{source_config['host']}:{source_config['port']}"
        
        if source_key not in self.ae_connections:
            # Create new AE
            ae = AE()
            ae.ae_title = f"SPANNER_{self.worker_id}"
            
            # Add supported contexts for C-FIND
            from pynetdicom.sop_class import (
                StudyRootQueryRetrieveInformationModelFind,
                PatientRootQueryRetrieveInformationModelFind
            )
            
            ae.add_requested_context(StudyRootQueryRetrieveInformationModelFind)
            ae.add_requested_context(PatientRootQueryRetrieveInformationModelFind)
            
            self.ae_connections[source_key] = ae
        
        return self.ae_connections[source_key]
    
    def _build_query_dataset(self, query_level: str, query_filters: Dict[str, Any]) -> Dataset:
        """Build a DICOM dataset for C-FIND query."""
        ds = Dataset()
        ds.QueryRetrieveLevel = query_level.upper()
        
        # Set required return attributes based on level
        if query_level.upper() == "STUDY":
            required_attrs = [
                "StudyInstanceUID", "PatientID", "PatientName", "StudyDate",
                "StudyTime", "AccessionNumber", "StudyDescription",
                "ModalitiesInStudy", "NumberOfStudyRelatedSeries"
            ]
        elif query_level.upper() == "SERIES":
            required_attrs = [
                "StudyInstanceUID", "SeriesInstanceUID", "SeriesNumber",
                "Modality", "SeriesDescription", "NumberOfSeriesRelatedInstances"
            ]
        elif query_level.upper() == "INSTANCE":
            required_attrs = [
                "StudyInstanceUID", "SeriesInstanceUID", "SOPInstanceUID",
                "InstanceNumber", "SOPClassUID"
            ]
        else:
            required_attrs = ["StudyInstanceUID", "PatientID"]
        
        # Set filter values
        for key, value in query_filters.items():
            if hasattr(ds, key):
                setattr(ds, key, str(value))
        
        # Ensure required attributes are present (empty for return)
        for attr in required_attrs:
            if not hasattr(ds, attr):
                setattr(ds, attr, "")
        
        return ds
    
    async def _execute_cfind(
        self,
        ae: AE,
        source_config: Dict[str, Any],
        query_ds: Dataset,
        query_level: str,
        timeout_seconds: int
    ) -> List[Dict[str, Any]]:
        """Execute C-FIND query and return results."""
        results = []
        
        # Establish association
        assoc = ae.associate(
            source_config["host"],
            source_config["port"],
            ae_title=source_config["ae_title"],
            max_pdu=16384
        )
        
        if not assoc.is_established:
            raise ConnectionError(f"Failed to establish association with {source_config['ae_title']}")
        
        try:
            # Determine SOP Class based on query level
            from pynetdicom.sop_class import (
                StudyRootQueryRetrieveInformationModelFind,
                PatientRootQueryRetrieveInformationModelFind
            )
            
            sop_class = StudyRootQueryRetrieveInformationModelFind
            
            # Send C-FIND request
            responses = assoc.send_c_find(query_ds, sop_class)
            
            for status, identifier in responses:
                if status:
                    # Check status
                    if status.Status in [0x0000, 0xFF00]:  # Success or Pending
                        if identifier:
                            # Convert Dataset to dict
                            result_dict = {}
                            for elem in identifier:
                                if elem.keyword:
                                    if elem.VR in ['PN', 'LO', 'SH', 'CS', 'UI', 'ST', 'LT']:
                                        result_dict[elem.keyword] = str(elem.value) if elem.value else ""
                                    elif elem.VR in ['IS', 'DS']:
                                        result_dict[elem.keyword] = elem.value
                                    elif elem.VR == 'DA':
                                        result_dict[elem.keyword] = str(elem.value) if elem.value else ""
                                    elif elem.VR == 'TM':
                                        result_dict[elem.keyword] = str(elem.value) if elem.value else ""
                                    else:
                                        result_dict[elem.keyword] = str(elem.value) if elem.value else ""
                            
                            if result_dict:
                                # Add source metadata
                                result_dict["_source_ae_title"] = source_config["ae_title"]
                                result_dict["_source_host"] = source_config["host"]
                                result_dict["_source_port"] = source_config["port"]
                                results.append(result_dict)
                    
                    elif status.Status in [0xFE00]:  # Cancel
                        break
                    else:
                        logger.warning(f"C-FIND warning status: 0x{status.Status:04X}")
        
        finally:
            # Release association
            assoc.release()
        
        return results
    
    async def _publish_result(self, result: Dict[str, Any]):
        """Publish query result to results queue."""
        message = aio_pika.Message(
            json.dumps(result, default=str).encode(),
            delivery_mode=aio_pika.DeliveryMode.PERSISTENT
        )
        
        await self.rabbitmq_channel.default_exchange.publish(
            message, routing_key="spanner.results"
        )
    
    async def get_stats(self) -> Dict[str, Any]:
        """Get worker statistics."""
        return {
            **self.stats,
            "uptime_seconds": (datetime.utcnow() - self.stats["started_at"]).total_seconds(),
            "active_connections": len(self.ae_connections)
        }
    
    async def shutdown(self):
        """Shutdown worker gracefully."""
        logger.info(f"Shutting down DIMSE worker {self.worker_id}")
        
        # Close all AE connections
        for ae in self.ae_connections.values():
            # AE connections don't need explicit closing
            pass
        
        if self.rabbitmq_connection:
            await self.rabbitmq_connection.close()


async def main():
    """Main entry point for DIMSE worker."""
    import os
    worker_id = os.getenv("WORKER_ID", f"dimse-worker-{int(time.time())}")
    
    worker = DIMSEQueryWorker(worker_id)
    
    try:
        await worker.initialize()
        
        # Keep running
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Received shutdown signal")
    finally:
        await worker.shutdown()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
