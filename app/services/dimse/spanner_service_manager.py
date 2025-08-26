# app/services/dimse/spanner_service_manager.py
import logging
import structlog
import threading
import time
from typing import Dict, List, Optional, Any
from contextlib import contextmanager

from pynetdicom import ae
from sqlalchemy.orm import Session

from app import crud
from app.db.models import DimseListenerConfig
from app.schemas.dimse_listener_config import DimseListenerConfigCreate
from app.services.dimse.spanner_scp import SpannerSCP
from app.services.dimse.cmove_proxy import CMoveProxyService
from app.services.spanner_engine import SpannerEngine

logger = structlog.get_logger(__name__)


class SpannerServiceManager:
    """
    Manages DIMSE services for the query spanning system.
    
    Coordinates:
    - SCP listeners for incoming queries
    - C-MOVE proxy services
    - Service health monitoring
    - Configuration management
    """
    
    def __init__(self, db_session_factory):
        self.db_session_factory = db_session_factory
        self.active_services: Dict[str, Any] = {}
        self.service_threads: Dict[str, threading.Thread] = {}
        self.shutdown_event = threading.Event()
        self.health_check_thread = None
        
    @contextmanager
    def get_db_session(self):
        """Get a database session with proper cleanup."""
        session = self.db_session_factory()
        try:
            yield session
        finally:
            session.close()
    
    def start_spanner_services(self):
        """Start all configured spanner DIMSE services."""
        logger.info("Starting spanner DIMSE services")
        
        try:
            with self.get_db_session() as db:
                # Get all enabled spanner configurations
                spanner_configs = crud.crud_spanner_config.get_multi(
                    db, include_disabled=False
                )
                
                for config in spanner_configs:
                    if config.supports_cfind:
                        self._start_scp_service(config)
                    
                    if config.supports_cmove:
                        self._start_cmove_service(config)
                
                # Start health monitoring
                self._start_health_monitoring()
                
                logger.info(f"Started {len(self.active_services)} spanner services")
                
        except Exception as e:
            logger.error(f"Error starting spanner services: {e}", exc_info=True)
            raise
    
    def _start_scp_service(self, spanner_config):
        """Start an SCP service for a spanner configuration."""
        service_id = f"scp_{spanner_config.id}"
        
        try:
            with self.get_db_session() as db:
                # Create SCP service
                scp_service = SpannerSCP(
                    ae_title=spanner_config.scp_ae_title,
                    port=getattr(spanner_config, 'scp_port', 11112)
                )
                
                # Get or create DIMSE listener configuration
                listener = self._get_or_create_listener(db, spanner_config)
                
                if not listener:
                    logger.warning(f"No listener configuration for spanner {spanner_config.id}")
                    return
                
                # Start the service in a separate thread
                service_thread = threading.Thread(
                    target=self._run_scp_service,
                    args=(scp_service, listener, service_id),
                    name=f"SpannerSCP-{spanner_config.id}",
                    daemon=True
                )
                
                self.active_services[service_id] = {
                    'type': 'SCP',
                    'service': scp_service,
                    'config': spanner_config,
                    'listener': listener,
                    'status': 'starting',
                    'start_time': time.time()
                }
                
                service_thread.start()
                self.service_threads[service_id] = service_thread
                
                logger.info(f"Started SCP service for spanner config {spanner_config.id}")
                
        except Exception as e:
            logger.error(f"Error starting SCP service for config {spanner_config.id}: {e}")
    
    def _start_cmove_service(self, spanner_config):
        """Start a C-MOVE proxy service for a spanner configuration."""
        service_id = f"cmove_{spanner_config.id}"
        
        try:
            with self.get_db_session() as db:
                # Create C-MOVE proxy service
                cmove_service = CMoveProxyService(db)
                
                self.active_services[service_id] = {
                    'type': 'CMOVE',
                    'service': cmove_service,
                    'config': spanner_config,
                    'status': 'ready',
                    'start_time': time.time()
                }
                
                logger.info(f"Started C-MOVE service for spanner config {spanner_config.id}")
                
        except Exception as e:
            logger.error(f"Error starting C-MOVE service for config {spanner_config.id}: {e}")
    
    def _run_scp_service(self, scp_service, listener, service_id):
        """Run an SCP service until shutdown."""
        try:
            # Update status
            self.active_services[service_id]['status'] = 'running'
            
            # Start the SCP
            scp_service.start_scp(
                ae_title=listener.ae_title,
                host=listener.host,
                port=listener.port
            )
            
            # Wait for shutdown signal
            while not self.shutdown_event.is_set():
                time.sleep(1)
            
            # Stop the SCP
            scp_service.stop_scp()
            self.active_services[service_id]['status'] = 'stopped'
            
        except Exception as e:
            logger.error(f"Error running SCP service {service_id}: {e}", exc_info=True)
            self.active_services[service_id]['status'] = 'error'
            self.active_services[service_id]['error'] = str(e)
    
    def _get_or_create_listener(self, db: Session, spanner_config) -> Optional[DimseListenerConfig]:
        """Get or create a DIMSE listener for the spanner configuration."""
        
        # Look for existing listener for this spanner config
        listener_name = f"spanner_{spanner_config.id}_scp"
        
        listener = crud.crud_dimse_listener_config.get_by_name(db, name=listener_name)
        
        if not listener:
            # Create a new listener configuration
            # Use available port (start from 11112 for spanner services)
            base_port = 11112
            used_ports = {l.port for l in crud.crud_dimse_listener_config.get_multi(db)}
            
            port = base_port
            while port in used_ports:
                port += 1
            
            listener_data = DimseListenerConfigCreate(
                name=listener_name,
                ae_title=f'SPANNER_{spanner_config.id}',
                port=port,
                listener_type='pynetdicom',
                description=f'Auto-created listener for spanner config {spanner_config.name}',
                is_enabled=True,
                instance_id=None,
                tls_enabled=False,
                tls_cert_secret_name=None,
                tls_key_secret_name=None,
                tls_ca_cert_secret_name=None
            )
            
            listener = crud.crud_dimse_listener_config.create(db, obj_in=listener_data)
            db.commit()
            
            logger.info(f"Created DIMSE listener for spanner config {spanner_config.id}: {listener.ae_title}:{listener.port}")
        
        return listener
    
    def _start_health_monitoring(self):
        """Start health monitoring for all services."""
        if self.health_check_thread and self.health_check_thread.is_alive():
            return
        
        self.health_check_thread = threading.Thread(
            target=self._health_monitor_loop,
            name="SpannerHealthMonitor",
            daemon=True
        )
        self.health_check_thread.start()
        logger.info("Started spanner service health monitoring")
    
    def _health_monitor_loop(self):
        """Health monitoring loop."""
        while not self.shutdown_event.is_set():
            try:
                self._check_service_health()
                time.sleep(30)  # Check every 30 seconds
            except Exception as e:
                logger.error(f"Error in health monitoring: {e}", exc_info=True)
                time.sleep(5)
    
    def _check_service_health(self):
        """Check health of all active services."""
        current_time = time.time()
        
        for service_id, service_info in self.active_services.items():
            try:
                # Check if thread is still alive (for SCP services)
                if service_id in self.service_threads:
                    thread = self.service_threads[service_id]
                    if not thread.is_alive():
                        logger.warning(f"Service thread {service_id} has died")
                        service_info['status'] = 'dead'
                        service_info['error'] = 'Thread died unexpectedly'
                
                # Update uptime
                service_info['uptime_seconds'] = current_time - service_info['start_time']
                
            except Exception as e:
                logger.error(f"Error checking health of service {service_id}: {e}")
    
    def stop_spanner_services(self):
        """Stop all spanner services."""
        logger.info("Stopping spanner DIMSE services")
        
        # Signal shutdown
        self.shutdown_event.set()
        
        # Stop all SCP services
        for service_id, service_info in self.active_services.items():
            try:
                if service_info['type'] == 'SCP':
                    service_info['service'].stop_scp()
                    logger.info(f"Stopped SCP service {service_id}")
            except Exception as e:
                logger.error(f"Error stopping service {service_id}: {e}")
        
        # Wait for threads to finish
        for service_id, thread in self.service_threads.items():
            try:
                thread.join(timeout=5)
                if thread.is_alive():
                    logger.warning(f"Service thread {service_id} did not stop gracefully")
            except Exception as e:
                logger.error(f"Error joining thread {service_id}: {e}")
        
        # Stop health monitoring
        if self.health_check_thread and self.health_check_thread.is_alive():
            self.health_check_thread.join(timeout=5)
        
        self.active_services.clear()
        self.service_threads.clear()
        
        logger.info("All spanner services stopped")
    
    def get_service_status(self) -> Dict[str, Any]:
        """Get status of all spanner services."""
        status = {
            'total_services': len(self.active_services),
            'services': {},
            'summary': {
                'running': 0,
                'error': 0,
                'stopped': 0,
                'starting': 0
            }
        }
        
        for service_id, service_info in self.active_services.items():
            service_status = {
                'type': service_info['type'],
                'status': service_info['status'],
                'config_id': service_info['config'].id,
                'config_name': service_info['config'].name,
                'uptime_seconds': service_info.get('uptime_seconds', 0),
                'error': service_info.get('error')
            }
            
            if service_info['type'] == 'SCP' and 'listener' in service_info:
                listener = service_info['listener']
                service_status['ae_title'] = listener.ae_title
                service_status['endpoint'] = f"{listener.host}:{listener.port}"
            
            status['services'][service_id] = service_status
            
            # Update summary
            service_status_key = service_info['status']
            if service_status_key in status['summary']:
                status['summary'][service_status_key] += 1
        
        return status
    
    def restart_service(self, service_id: str) -> bool:
        """Restart a specific service."""
        if service_id not in self.active_services:
            return False
        
        try:
            service_info = self.active_services[service_id]
            config = service_info['config']
            
            # Stop the service
            if service_info['type'] == 'SCP':
                service_info['service'].stop_scp()
                if service_id in self.service_threads:
                    self.service_threads[service_id].join(timeout=5)
                    del self.service_threads[service_id]
            
            # Remove from active services
            del self.active_services[service_id]
            
            # Restart based on type
            if service_info['type'] == 'SCP':
                self._start_scp_service(config)
            elif service_info['type'] == 'CMOVE':
                self._start_cmove_service(config)
            
            logger.info(f"Restarted service {service_id}")
            return True
            
        except Exception as e:
            logger.error(f"Error restarting service {service_id}: {e}", exc_info=True)
            return False
