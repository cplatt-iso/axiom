# app/api/api_v1/endpoints/spanner_services.py
import logging
from typing import Dict, Any, List, Optional

from fastapi import APIRouter, Depends, HTTPException, status, BackgroundTasks
from sqlalchemy.orm import Session

from app import crud, schemas
from app.db import models
from app.api import deps
from app.services.dimse.cmove_proxy import CMoveProxyService, CMoveRequest, CMoveStrategy

logger = logging.getLogger(__name__)
router = APIRouter()

# This would be injected from your application context
# For now, we'll create a placeholder
spanner_service_manager = None


@router.get(
    "/status",
    summary="Get Spanner Services Status",
    description="Get the current status of all spanner DIMSE services.",
    response_model=Dict[str, Any],
)
def get_spanner_services_status(
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Dict[str, Any]:
    """Get status of all running spanner services."""
    
    if not spanner_service_manager:
        return {
            "message": "Spanner service manager not initialized",
            "services": {},
            "total_services": 0
        }
    
    try:
        status = spanner_service_manager.get_service_status()
        return status
        
    except Exception as e:
        logger.error(f"Error getting spanner services status: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving service status"
        )


@router.post(
    "/start",
    summary="Start Spanner Services",
    description="Start all configured spanner DIMSE services.",
    response_model=Dict[str, Any],
)
def start_spanner_services(
    background_tasks: BackgroundTasks,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Dict[str, Any]:
    """Start all spanner services in the background."""
    
    if not spanner_service_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Spanner service manager not available"
        )
    
    try:
        # Start services in background
        background_tasks.add_task(spanner_service_manager.start_spanner_services)
        
        return {
            "message": "Spanner services startup initiated",
            "status": "starting"
        }
        
    except Exception as e:
        logger.error(f"Error starting spanner services: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error starting spanner services"
        )


@router.post(
    "/stop",
    summary="Stop Spanner Services",
    description="Stop all running spanner DIMSE services.",
    response_model=Dict[str, Any],
)
def stop_spanner_services(
    background_tasks: BackgroundTasks,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Dict[str, Any]:
    """Stop all spanner services."""
    
    if not spanner_service_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Spanner service manager not available"
        )
    
    try:
        # Stop services in background
        background_tasks.add_task(spanner_service_manager.stop_spanner_services)
        
        return {
            "message": "Spanner services shutdown initiated",
            "status": "stopping"
        }
        
    except Exception as e:
        logger.error(f"Error stopping spanner services: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error stopping spanner services"
        )


@router.post(
    "/restart/{service_id}",
    summary="Restart Specific Service",
    description="Restart a specific spanner service by ID.",
    response_model=Dict[str, Any],
)
def restart_spanner_service(
    service_id: str,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Dict[str, Any]:
    """Restart a specific spanner service."""
    
    if not spanner_service_manager:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Spanner service manager not available"
        )
    
    try:
        success = spanner_service_manager.restart_service(service_id)
        
        if success:
            return {
                "message": f"Service {service_id} restarted successfully",
                "service_id": service_id,
                "status": "restarted"
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Service {service_id} not found"
            )
            
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error restarting service {service_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error restarting service {service_id}"
        )


@router.post(
    "/cmove",
    summary="Execute C-MOVE Request",
    description="Execute a C-MOVE request using spanner configuration.",
    response_model=Dict[str, Any],
)
def execute_cmove_spanning(
    study_uid: str,
    destination_ae: str,
    spanner_config_id: int,
    series_uid: Optional[str] = None,
    instance_uid: Optional[str] = None,
    move_strategy: CMoveStrategy = CMoveStrategy.HYBRID,
    priority: int = 2,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Dict[str, Any]:
    """Execute a C-MOVE request using the spanner engine."""
    
    try:
        # Validate spanner configuration exists
        spanner_config = crud.crud_spanner_config.get(db, id=spanner_config_id)
        if not spanner_config:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Spanner config {spanner_config_id} not found"
            )
        
        if not spanner_config.supports_cmove:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Spanner config {spanner_config_id} does not support C-MOVE"
            )
        
        # Create C-MOVE request
        move_request = CMoveRequest(
            study_uid=study_uid,
            series_uid=series_uid,
            instance_uid=instance_uid,
            destination_ae=destination_ae,
            priority=priority,
            move_strategy=move_strategy,
            spanner_config_id=spanner_config_id
        )
        
        # Execute the move
        cmove_service = CMoveProxyService(db)
        result = cmove_service.execute_cmove_spanning(move_request)
        
        return {
            "success": result.success,
            "study_uid": study_uid,
            "destination_ae": destination_ae,
            "strategy_used": result.strategy_used.value if result.strategy_used else None,
            "total_instances": result.total_instances,
            "moved_instances": result.moved_instances,
            "failed_instances": result.failed_instances,
            "source_ae_title": result.source_ae_title,
            "duration_seconds": result.duration_seconds,
            "error_message": result.error_message
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error executing C-MOVE spanning: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error executing C-MOVE request"
        )


@router.get(
    "/cmove/active",
    summary="Get Active C-MOVE Operations",
    description="Get information about currently active C-MOVE operations.",
    response_model=Dict[str, Any],
)
def get_active_cmove_operations(
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Dict[str, Any]:
    """Get active C-MOVE operations across all spanner services."""
    
    try:
        active_moves = {}
        
        # Get active moves from all C-MOVE services
        if spanner_service_manager:
            service_status = spanner_service_manager.get_service_status()
            
            for service_id, service_info in service_status.get('services', {}).items():
                if service_info['type'] == 'CMOVE':
                    # In a real implementation, you would query the service for active moves
                    # For now, return placeholder data
                    active_moves[service_id] = {
                        "service_id": service_id,
                        "config_id": service_info['config_id'],
                        "active_move_count": 0,  # Placeholder
                        "status": service_info['status']
                    }
        
        return {
            "total_active_moves": len(active_moves),
            "services": active_moves
        }
        
    except Exception as e:
        logger.error(f"Error getting active C-MOVE operations: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving active C-MOVE operations"
        )


@router.post(
    "/cmove/cancel/{move_id}",
    summary="Cancel C-MOVE Operation",
    description="Cancel a specific C-MOVE operation.",
    response_model=Dict[str, Any],
)
def cancel_cmove_operation(
    move_id: str,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> Dict[str, Any]:
    """Cancel a specific C-MOVE operation."""
    
    try:
        # In a real implementation, you would:
        # 1. Find which service is handling the move
        # 2. Call the service's cancel method
        # 3. Return the result
        
        # Placeholder implementation
        return {
            "message": f"C-MOVE operation {move_id} cancellation requested",
            "move_id": move_id,
            "status": "cancellation_requested"
        }
        
    except Exception as e:
        logger.error(f"Error cancelling C-MOVE operation {move_id}: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Error cancelling C-MOVE operation {move_id}"
        )


@router.get(
    "/logs",
    summary="Get Spanner Service Logs",
    description="Get recent logs from spanner services.",
    response_model=List[Dict[str, Any]],
)
def get_spanner_service_logs(
    limit: int = 100,
    service_type: Optional[str] = None,
    config_id: Optional[int] = None,
    db: Session = Depends(deps.get_db),
    current_user: models.User = Depends(deps.get_current_active_user),
) -> List[Dict[str, Any]]:
    """Get recent logs from spanner query operations."""
    
    try:
        # Get query logs from database
        query_logs = crud.crud_spanner_query_log.get_multi(
            db, skip=0, limit=limit
        )
        
        logs = []
        for log in query_logs:
            log_data = {
                "id": log.id,
                "spanner_config_id": log.spanner_config_id,
                "query_type": log.query_type,
                "query_level": log.query_level,
                "total_results": log.total_results,
                "successful_results": log.successful_results,
                "failed_results": log.failed_results,
                "duration_seconds": log.duration_seconds,
                "requesting_ip": log.requesting_ip,
                "error_message": log.error_message,
                "created_at": log.created_at.isoformat() if log.created_at else None
            }
            
            # Apply filters
            if service_type and log.query_type != service_type:
                continue
            if config_id and log.spanner_config_id != config_id:
                continue
            
            logs.append(log_data)
        
        return logs
        
    except Exception as e:
        logger.error(f"Error getting spanner service logs: {e}", exc_info=True)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Error retrieving service logs"
        )
