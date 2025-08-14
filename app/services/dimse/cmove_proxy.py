# app/services/dimse/cmove_proxy.py
import logging
import threading
import time
from typing import Dict, List, Optional, Any, Callable
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from enum import Enum

from pynetdicom import ae, evt
from pynetdicom.sop_class import (
    StudyRootQueryRetrieveInformationModelMove,  # type: ignore[attr-defined]
    PatientRootQueryRetrieveInformationModelMove,  # type: ignore[attr-defined]
    StudyRootQueryRetrieveInformationModelGet,  # type: ignore[attr-defined]
    PatientRootQueryRetrieveInformationModelGet,  # type: ignore[attr-defined]
)
from pydicom import Dataset
from sqlalchemy.orm import Session

from app import crud
from app.db.models import User
from app.services.spanner_engine import SpannerEngine

logger = logging.getLogger(__name__)


class CMoveStrategy(str, Enum):
    DIRECT = "DIRECT"      # Connect client directly to source
    PROXY = "PROXY"        # Proxy all data through spanner
    HYBRID = "HYBRID"      # Try direct first, fallback to proxy


@dataclass
class CMoveRequest:
    """Represents a C-MOVE request with spanner context."""
    study_uid: str
    series_uid: Optional[str] = None
    instance_uid: Optional[str] = None
    destination_ae: Optional[str] = None
    priority: int = 2  # Medium priority
    move_strategy: CMoveStrategy = CMoveStrategy.HYBRID
    spanner_config_id: Optional[int] = None
    requesting_ip: Optional[str] = None
    max_concurrent_moves: int = 3


@dataclass
class CMoveResult:
    """Result of a C-MOVE operation."""
    success: bool
    total_instances: int = 0
    moved_instances: int = 0
    failed_instances: int = 0
    source_ae_title: Optional[str] = None
    error_message: Optional[str] = None
    strategy_used: Optional[CMoveStrategy] = None
    duration_seconds: float = 0.0


class CMoveProxyService:
    """
    Handles C-MOVE requests across multiple DICOM sources using spanner configuration.
    
    Supports multiple strategies:
    - DIRECT: Provide client with source details for direct connection
    - PROXY: Retrieve data through spanner and forward to destination
    - HYBRID: Try direct first, fallback to proxy on failure
    """
    
    def __init__(self, db: Session):
        self.db = db
        self.spanner_engine = SpannerEngine(db)
        self.active_moves: Dict[str, threading.Event] = {}
        self.move_stats: Dict[str, Dict] = {}
    
    async def execute_cmove_spanning(
        self,
        move_request: CMoveRequest,
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> CMoveResult:
        """
        Execute a C-MOVE request using spanner configuration.
        
        Args:
            move_request: The C-MOVE request details
            progress_callback: Optional callback for progress updates
            
        Returns:
            CMoveResult with operation status and statistics
        """
        start_time = time.time()
        move_id = f"{move_request.study_uid}_{int(start_time)}"
        
        logger.info(f"Starting spanning C-MOVE operation {move_id}")
        
        try:
            # Get spanner configuration
            if not move_request.spanner_config_id:
                return CMoveResult(
                    success=False,
                    error_message="Spanner config ID is required"
                )
                
            spanner_config = crud.crud_spanner_config.get(
                self.db, id=move_request.spanner_config_id
            )
            if not spanner_config:
                return CMoveResult(
                    success=False,
                    error_message=f"Spanner config {move_request.spanner_config_id} not found"
                )
            
            # First, find which sources have the requested data
            query_filters = {'StudyInstanceUID': move_request.study_uid}
            if move_request.series_uid:
                query_filters['SeriesInstanceUID'] = move_request.series_uid
            if move_request.instance_uid:
                query_filters['SOPInstanceUID'] = move_request.instance_uid
            
            query_level = "STUDY"
            if move_request.instance_uid:
                query_level = "INSTANCE"
            elif move_request.series_uid:
                query_level = "SERIES"
            
            # Query spanning to find available sources
            query_result = await self.spanner_engine.execute_spanning_query(
                spanner_config_id=spanner_config.id,
                query_type="C-FIND",
                query_level=query_level,
                query_filters=query_filters,
                requesting_ip=move_request.requesting_ip
            )
            
            if not query_result.get('results'):
                return CMoveResult(
                    success=False,
                    error_message="No sources found containing the requested data"
                )
            
            # Select the best source for the move operation
            source_info = self._select_move_source(
                spanner_config, query_result['results'], move_request
            )
            
            if not source_info:
                return CMoveResult(
                    success=False,
                    error_message="No suitable source found for C-MOVE operation"
                )
            
            # Execute the move using the selected strategy
            if move_request.move_strategy == CMoveStrategy.DIRECT:
                result = self._execute_direct_move(move_request, source_info, progress_callback)
            elif move_request.move_strategy == CMoveStrategy.PROXY:
                result = self._execute_proxy_move(move_request, source_info, progress_callback)
            else:  # HYBRID
                # Try direct first
                result = self._execute_direct_move(move_request, source_info, progress_callback)
                if not result.success:
                    logger.info(f"Direct move failed for {move_id}, falling back to proxy")
                    result = self._execute_proxy_move(move_request, source_info, progress_callback)
                    result.strategy_used = CMoveStrategy.HYBRID
            
            result.duration_seconds = time.time() - start_time
            
            # Log the operation
            self._log_move_operation(spanner_config.id, move_request, result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error in spanning C-MOVE operation {move_id}: {e}", exc_info=True)
            return CMoveResult(
                success=False,
                error_message=f"Internal error: {str(e)}",
                duration_seconds=time.time() - start_time
            )
    
    def _select_move_source(
        self,
        spanner_config,
        query_results: List[Dict],
        move_request: CMoveRequest
    ) -> Optional[Dict]:
        """Select the best source for C-MOVE operation based on configuration."""
        
        # Get source mappings to understand priorities
        source_mappings = crud.crud_spanner_source_mapping.get_multi_by_spanner(
            self.db, spanner_config_id=spanner_config.id, include_disabled=False
        )
        
        # Create a priority map
        priority_map = {}
        for mapping in source_mappings:
            # Get the appropriate source based on mapping type
            source_ae_title = None
            if mapping.source_type == "dimse-qr" and mapping.dimse_qr_source:
                source_ae_title = mapping.dimse_qr_source.remote_ae_title
            elif mapping.source_type == "dicomweb" and mapping.dicomweb_source:
                source_ae_title = mapping.dicomweb_source.source_name
            elif mapping.source_type == "google_healthcare" and mapping.google_healthcare_source:
                source_ae_title = mapping.google_healthcare_source.name
            
            if source_ae_title:  # type: ignore
                priority_map[source_ae_title] = mapping.priority or 999
        
        # Find sources that have the data
        available_sources = []
        for result in query_results:
            source_ae = result.get('source_ae_title')
            if source_ae and source_ae in priority_map:
                available_sources.append({
                    'ae_title': source_ae,
                    'priority': priority_map[source_ae],
                    'result_data': result
                })
        
        if not available_sources:
            return None
        
        # Sort by priority (lower number = higher priority)
        available_sources.sort(key=lambda x: x['priority'])
        
        # Select the highest priority source
        selected_source = available_sources[0]
        
        # Get full source configuration
        source_mapping = None
        for mapping in source_mappings:
            # Get the appropriate source based on mapping type
            source_ae_title = None
            if mapping.source_type == "dimse-qr" and mapping.dimse_qr_source:
                source_ae_title = mapping.dimse_qr_source.remote_ae_title
            elif mapping.source_type == "dicomweb" and mapping.dicomweb_source:
                source_ae_title = mapping.dicomweb_source.source_name
            elif mapping.source_type == "google_healthcare" and mapping.google_healthcare_source:
                source_ae_title = mapping.google_healthcare_source.name
            
            if source_ae_title == selected_source['ae_title']:
                source_mapping = mapping
                break
        
        if not source_mapping:
            return None
        
        # Get the actual source object based on mapping type
        source_obj = None
        if source_mapping.source_type == "dimse-qr" and source_mapping.dimse_qr_source:
            source_obj = source_mapping.dimse_qr_source
        elif source_mapping.source_type == "dicomweb" and source_mapping.dicomweb_source:
            source_obj = source_mapping.dicomweb_source
        elif source_mapping.source_type == "google_healthcare" and source_mapping.google_healthcare_source:
            source_obj = source_mapping.google_healthcare_source
        
        return {
            'source': source_obj,
            'mapping': source_mapping,
            'query_result': selected_source['result_data']
        }
    
    def _execute_direct_move(
        self,
        move_request: CMoveRequest,
        source_info: Dict,
        progress_callback: Optional[Callable]
    ) -> CMoveResult:
        """
        Execute a direct C-MOVE by providing connection details to client.
        
        Note: In a real implementation, this would involve coordinating with
        the client to establish a direct connection to the source.
        """
        logger.info(f"Executing direct C-MOVE to {source_info['source'].ae_title}")
        
        # For direct moves, we essentially return connection information
        # The actual implementation would depend on your client coordination mechanism
        
        return CMoveResult(
            success=True,
            total_instances=1,  # Placeholder
            moved_instances=1,  # Placeholder
            source_ae_title=source_info['source'].ae_title,
            strategy_used=CMoveStrategy.DIRECT,
            error_message="Direct move coordination - actual implementation needed"
        )
    
    def _execute_proxy_move(
        self,
        move_request: CMoveRequest,
        source_info: Dict,
        progress_callback: Optional[Callable]
    ) -> CMoveResult:
        """
        Execute a proxy C-MOVE by retrieving data and forwarding to destination.
        
        This is the most complex strategy as it requires:
        1. C-GET/C-MOVE from source to spanner
        2. C-STORE from spanner to destination
        """
        logger.info(f"Executing proxy C-MOVE from {source_info['source'].ae_title}")
        
        source = source_info['source']
        
        try:
            # Step 1: Retrieve data from source using C-GET
            retrieved_instances = self._retrieve_from_source(move_request, source)
            
            if not retrieved_instances:
                return CMoveResult(
                    success=False,
                    error_message="Failed to retrieve instances from source",
                    source_ae_title=source.ae_title,
                    strategy_used=CMoveStrategy.PROXY
                )
            
            # Step 2: Forward instances to destination using C-STORE
            stored_count = self._store_to_destination(
                move_request, retrieved_instances, progress_callback
            )
            
            return CMoveResult(
                success=stored_count > 0,
                total_instances=len(retrieved_instances),
                moved_instances=stored_count,
                failed_instances=len(retrieved_instances) - stored_count,
                source_ae_title=source.ae_title,
                strategy_used=CMoveStrategy.PROXY
            )
            
        except Exception as e:
            logger.error(f"Error in proxy C-MOVE: {e}", exc_info=True)
            return CMoveResult(
                success=False,
                error_message=f"Proxy move error: {str(e)}",
                source_ae_title=source.ae_title,
                strategy_used=CMoveStrategy.PROXY
            )
    
    def _retrieve_from_source(self, move_request: CMoveRequest, source) -> List[Dataset]:
        """Retrieve DICOM instances from source using C-GET."""
        
        # Build query dataset
        query_ds = Dataset()
        query_ds.StudyInstanceUID = move_request.study_uid
        
        if move_request.series_uid:
            query_ds.SeriesInstanceUID = move_request.series_uid
        if move_request.instance_uid:
            query_ds.SOPInstanceUID = move_request.instance_uid
        
        # In a real implementation, you would:
        # 1. Create AE with C-GET capability
        # 2. Connect to source
        # 3. Send C-GET request
        # 4. Collect received instances
        
        logger.info(f"Would retrieve instances from {source.ae_title} using C-GET")
        
        # Placeholder - return empty list for now
        return []
    
    def _store_to_destination(
        self,
        move_request: CMoveRequest,
        instances: List[Dataset],
        progress_callback: Optional[Callable]
    ) -> int:
        """Store DICOM instances to destination using C-STORE."""
        
        stored_count = 0
        
        # In a real implementation, you would:
        # 1. Create AE with C-STORE capability
        # 2. Connect to destination AE
        # 3. Send C-STORE for each instance
        # 4. Track success/failure
        
        for i, instance in enumerate(instances):
            try:
                # Placeholder for actual C-STORE operation
                logger.info(f"Would store instance {i+1}/{len(instances)} to {move_request.destination_ae}")
                
                if progress_callback:
                    progress_callback(f"Storing instance {i+1}/{len(instances)}", i+1, len(instances))
                
                stored_count += 1
                
            except Exception as e:
                logger.error(f"Failed to store instance {i+1}: {e}")
        
        return stored_count
    
    def _log_move_operation(
        self,
        spanner_config_id: int,
        move_request: CMoveRequest,
        result: CMoveResult
    ):
        """Log the C-MOVE operation to the database."""
        
        try:
            log_data = {
                'spanner_config_id': spanner_config_id,
                'query_type': 'C-MOVE',
                'query_level': 'STUDY',  # Could be more specific
                'status': 'SUCCESS' if result.success else 'FAILURE',
                'sources_queried': 1,
                'sources_successful': 1 if result.success else 0,
                'total_results_found': result.total_instances,
                'deduplicated_results': result.moved_instances,
                'query_duration_seconds': result.duration_seconds,
                'query_filters': {
                    'StudyInstanceUID': move_request.study_uid,
                    'destination_ae': move_request.destination_ae,
                    'strategy': result.strategy_used.value if result.strategy_used else 'UNKNOWN'
                },
                'requesting_ip': move_request.requesting_ip,
                'error_message': result.error_message
            }
            
            crud.crud_spanner_query_log.create_log(self.db, **log_data)
            self.db.commit()
            
        except Exception as e:
            logger.error(f"Failed to log C-MOVE operation: {e}")
    
    def get_active_moves(self) -> Dict[str, Dict]:
        """Get information about currently active C-MOVE operations."""
        return self.move_stats.copy()
    
    def cancel_move(self, move_id: str) -> bool:
        """Cancel an active C-MOVE operation."""
        if move_id in self.active_moves:
            self.active_moves[move_id].set()  # Signal cancellation
            return True
        return False
