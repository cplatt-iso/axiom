# services/enterprise/dimse_query_worker.py
"""
DIMSE Query Worker Service

Handles individual DIMSE queries against PACS systems.
Consumes tasks from RabbitMQ and publishes results back.
"""

import asyncio
import json
import logging
import ssl
import tempfile
import os
import time
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
import aio_pika
from aio_pika.abc import AbstractChannel, AbstractConnection, AbstractIncomingMessage
from pynetdicom import AE  # type: ignore[attr-defined]
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
from app.core import gcp_utils
from app.core.logging_config import configure_json_logging
from app.services.network.dimse.scu_service import TlsConfigError
import structlog

# Configure JSON logging for enterprise service
configure_json_logging("dimse_query_worker")
logger = structlog.get_logger(__name__)

# Disable pynetdicom debug logging in production
pynet_logger = logging.getLogger('pynetdicom')
pynet_logger.setLevel(logging.WARNING)


def _prepare_worker_tls_context(
    tls_ca_cert_secret: Optional[str],
    tls_client_cert_secret: Optional[str],
    tls_client_key_secret: Optional[str]
) -> Tuple[Optional[ssl.SSLContext], List[str]]:
    """Prepare TLS context for DIMSE worker queries."""
    temp_files_created: List[str] = []
    ssl_context: Optional[ssl.SSLContext] = None
    
    if not tls_ca_cert_secret:
        logger.warning("TLS enabled but no CA certificate secret provided")
        return None, temp_files_created
    
    try:
        # Fetch CA certificate
        ca_cert_content = gcp_utils.get_secret(tls_ca_cert_secret)
        ca_cert_fd, ca_cert_path = tempfile.mkstemp(suffix="-ca.pem", text=True)
        with os.fdopen(ca_cert_fd, 'w') as f:
            f.write(ca_cert_content)
        temp_files_created.append(ca_cert_path)
        
        # Create SSL context
        ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH, cafile=ca_cert_path)
        ssl_context.check_hostname = False
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        
        # Add client certificate if provided (for mTLS)
        if tls_client_cert_secret and tls_client_key_secret:
            # Fetch client certificate
            client_cert_content = gcp_utils.get_secret(tls_client_cert_secret)
            client_cert_fd, client_cert_path = tempfile.mkstemp(suffix="-cert.pem", text=True)
            with os.fdopen(client_cert_fd, 'w') as f:
                f.write(client_cert_content)
            temp_files_created.append(client_cert_path)
            
            # Fetch client key
            client_key_content = gcp_utils.get_secret(tls_client_key_secret)
            client_key_fd, client_key_path = tempfile.mkstemp(suffix="-key.pem", text=True)
            with os.fdopen(client_key_fd, 'w') as f:
                f.write(client_key_content)
            temp_files_created.append(client_key_path)
            
            ssl_context.load_cert_chain(certfile=client_cert_path, keyfile=client_key_path)
            logger.info("TLS context configured with client certificate for mTLS")
        else:
            logger.info("TLS context configured for server authentication only")
            
        return ssl_context, temp_files_created
        
    except Exception as e:
        # Clean up temp files on error
        for path in temp_files_created:
            try:
                if os.path.exists(path):
                    os.remove(path)
            except OSError:
                pass
        logger.error(f"Failed to prepare TLS context: {e}")
        raise Exception(f"TLS context preparation failed: {e}") from e


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
    
    def __init__(self, worker_id: Optional[str] = None):
        self.worker_id = worker_id or f"dimse-worker-{int(time.time())}"
        self.rabbitmq_connection: Optional[AbstractConnection] = None
        self.rabbitmq_channel: Optional[AbstractChannel] = None
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
    
    async def _process_query_task(self, message: AbstractIncomingMessage):
        """Process a single DIMSE query task."""
        task_data = None
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
                    "query_id": task_data.get("query_id") if task_data else None,
                    "task_id": task_data.get("task_id") if task_data else None,
                    "source_id": task_data.get("source_id") if task_data else None,
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
            ae_instance = AE()
            # Use the configured local AE title for this specific source
            # This is the AE title that our SCU will use when connecting to the remote PACS
            # Priority: source's local_ae_title > spanner config's scu_ae_title > environment default
            ae_title = source_config.get("local_ae_title")
            if not ae_title:
                ae_title = source_config.get("spanner_scu_ae_title", "AXIOM_SPAN")
            ae_instance.ae_title = ae_title
            
            # Add supported contexts for C-FIND
            # Use UID strings directly since import paths seem to be incorrect
            study_root_find_uid = "1.2.840.10008.5.1.4.1.2.2.1"
            patient_root_find_uid = "1.2.840.10008.5.1.4.1.2.1.1"
            
            ae_instance.add_requested_context(study_root_find_uid)
            ae_instance.add_requested_context(patient_root_find_uid)
            
            self.ae_connections[source_key] = ae_instance
        
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
        ssl_context = None
        temp_files = []
        assoc = None
        
        try:
            # Prepare TLS context if needed
            if source_config.get("tls_enabled", False):
                logger.info(f"TLS enabled for source {source_config['remote_ae_title']}, preparing TLS context")
                ssl_context, temp_files = _prepare_worker_tls_context(
                    source_config.get("tls_ca_cert_secret_name"),
                    source_config.get("tls_client_cert_secret_name"),
                    source_config.get("tls_client_key_secret_name")
                )
            
            # Prepare TLS arguments for association
            tls_args = None
            if ssl_context:
                # TLS args require hostname as string or None, but pynetdicom expects string
                hostname = source_config["host"] if ssl_context.check_hostname else source_config["host"]
                tls_args = (ssl_context, hostname)
            
            # Establish association
            if tls_args:
                assoc = ae.associate(
                    source_config["host"],
                    source_config["port"],
                    ae_title=source_config["remote_ae_title"],
                    tls_args=tls_args,
                    max_pdu=16384
                )
            else:
                assoc = ae.associate(
                    source_config["host"],
                    source_config["port"],
                    ae_title=source_config["remote_ae_title"],
                    max_pdu=16384
                )
            
            if not assoc or not assoc.is_established:
                raise ConnectionError(f"Failed to establish association with {source_config['remote_ae_title']}")
            
            # Determine SOP Class based on query level - use UID directly
            sop_class = "1.2.840.10008.5.1.4.1.2.2.1"  # Study Root Query/Retrieve Information Model - FIND
            
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
                                result_dict["_source_ae_title"] = source_config["remote_ae_title"]
                                result_dict["_source_host"] = source_config["host"]
                                result_dict["_source_port"] = source_config["port"]
                                results.append(result_dict)
                    
                    elif status.Status in [0xFE00]:  # Cancel
                        break
                    else:
                        logger.warning(f"C-FIND warning status: 0x{status.Status:04X}")
        
        except Exception as e:
            logger.error(f"Error during DIMSE query execution: {e}")
            raise
        
        finally:
            # Release association
            if assoc and assoc.is_established:
                assoc.release()
            
            # Clean up temporary TLS files
            for temp_file in temp_files:
                try:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                except OSError as e:
                    logger.warning(f"Failed to cleanup TLS temp file {temp_file}: {e}")
        
        return results
    
    async def _publish_result(self, result: Dict[str, Any]):
        """Publish query result to results queue."""
        if not self.rabbitmq_channel:
            logger.error("RabbitMQ channel not initialized")
            return
            
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
