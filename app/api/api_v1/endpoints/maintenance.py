from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query, Path as PathParam
from sqlalchemy.orm import Session

from app.api import deps
from app.crud import crud_maintenance
from app.schemas.maintenance import (
    MaintenanceConfigRead,
    MaintenanceConfigCreate,
    MaintenanceConfigUpdate,
    MaintenanceConfigWithDataCleaner,
    MaintenanceTaskRead,
    MaintenanceTaskWithDetails,
    DataCleanerConfigRead,
    DataCleanerConfigCreate,
    DataCleanerConfigUpdate,
    MaintenanceStatusResponse,
    TaskExecutionRequest,
    TaskExecutionResponse
)
from app.db.models.maintenance import MaintenanceTaskType, MaintenanceTaskStatus
from app.services.maintenance_service import maintenance_service
from app.schemas.maintenance import MaintenanceTaskCreate
from datetime import datetime, timedelta

router = APIRouter()


@router.get("/status", response_model=MaintenanceStatusResponse)
def get_maintenance_status(
    db: Session = Depends(deps.get_db),
    current_user = Depends(deps.get_current_active_user)
):
    """
    Get overall maintenance system status.
    
    Perfect for the dashboard when the boss asks "Is our digital janitor still working?"
    """
    # Get all configs
    all_configs = crud_maintenance.maintenance_config.get_multi(db, limit=1000)
    active_configs = [c for c in all_configs if c.enabled]
    
    # Get running tasks
    running_tasks = crud_maintenance.maintenance_task.get_running_tasks(db)
    
    # Get recent completions and failures
    recent_completions = crud_maintenance.maintenance_task.get_recent_completions(db, hours=24)
    recent_failures = crud_maintenance.maintenance_task.get_recent_failures(db, hours=24)
    
    return MaintenanceStatusResponse(
        total_configs=len(all_configs),
        active_configs=len(active_configs),
        running_tasks=len(running_tasks),
        last_24h_completions=len(recent_completions),
        last_24h_failures=len(recent_failures),
        configs=[MaintenanceConfigRead.model_validate(config) for config in all_configs]
    )


@router.get("/configs", response_model=List[MaintenanceConfigWithDataCleaner])
def list_maintenance_configs(
    task_type: Optional[MaintenanceTaskType] = Query(None, description="Filter by task type"),
    enabled_only: bool = Query(False, description="Only return enabled configs"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(deps.get_db),
    current_user = Depends(deps.get_current_active_user)
):
    """
    Get all maintenance configurations.
    
    Use this to see what cleaning tasks are set up and whether they're
    actually doing their job or just sitting around like lazy interns.
    """
    if task_type:
        configs = crud_maintenance.maintenance_config.get_by_task_type(db, task_type=task_type.value)
    elif enabled_only:
        configs = crud_maintenance.maintenance_config.get_enabled_configs(db)
    else:
        configs = crud_maintenance.maintenance_config.get_multi(db, skip=skip, limit=limit)
    
    # Enrich configs with data cleaner details if applicable
    enriched_configs = []
    for config in configs:
        config_dict = MaintenanceConfigRead.model_validate(config).model_dump()
        
        if config.task_type == MaintenanceTaskType.DATA_CLEANER:
            data_cleaner_config = crud_maintenance.data_cleaner_config.get_by_maintenance_config_id(
                db, maintenance_config_id=config.id
            )
            if data_cleaner_config:
                config_dict["data_cleaner_config"] = DataCleanerConfigRead.model_validate(data_cleaner_config)
        
        enriched_configs.append(MaintenanceConfigWithDataCleaner(**config_dict))
    
    return enriched_configs


@router.post("/configs", response_model=MaintenanceConfigRead, status_code=status.HTTP_201_CREATED)
def create_maintenance_config(
    config_in: MaintenanceConfigCreate,
    db: Session = Depends(deps.get_db),
    current_user = Depends(deps.get_current_active_user)
):
    """
    Create a new maintenance configuration.
    
    Because sometimes you need to teach the janitor about new messes to clean up.
    """
    # Set initial next_run time
    config = crud_maintenance.maintenance_config.create(db, obj_in=config_in)
    
    next_run = datetime.utcnow() + timedelta(seconds=config.monitor_interval_seconds)
    crud_maintenance.maintenance_config.update_run_times(
        db,
        config_id=config.id,
        last_run=datetime.utcnow(),
        next_run=next_run
    )
    
    return config


@router.get("/configs/{config_id}", response_model=MaintenanceConfigWithDataCleaner)
def get_maintenance_config(
    config_id: int = PathParam(..., description="Maintenance config ID"),
    db: Session = Depends(deps.get_db),
    current_user = Depends(deps.get_current_active_user)
):
    """Get a specific maintenance configuration by ID"""
    config = crud_maintenance.maintenance_config.get(db, id=config_id)
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Maintenance config with ID {config_id} not found"
        )
    
    config_dict = MaintenanceConfigRead.model_validate(config).model_dump()
    
    if config.task_type == MaintenanceTaskType.DATA_CLEANER:
        data_cleaner_config = crud_maintenance.data_cleaner_config.get_by_maintenance_config_id(
            db, maintenance_config_id=config.id
        )
        if data_cleaner_config:
            config_dict["data_cleaner_config"] = DataCleanerConfigRead.model_validate(data_cleaner_config)
    
    return MaintenanceConfigWithDataCleaner(**config_dict)


@router.put("/configs/{config_id}", response_model=MaintenanceConfigRead)
def update_maintenance_config(
    config_in: MaintenanceConfigUpdate,
    config_id: int = PathParam(..., description="Maintenance config ID"),
    db: Session = Depends(deps.get_db),
    current_user = Depends(deps.get_current_active_user)
):
    """
    Update a maintenance configuration.
    
    For when you need to retrain the janitor or change their schedule.
    """
    config = crud_maintenance.maintenance_config.get(db, id=config_id)
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Maintenance config with ID {config_id} not found"
        )
    
    config = crud_maintenance.maintenance_config.update(db, db_obj=config, obj_in=config_in)
    return config


@router.delete("/configs/{config_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_maintenance_config(
    config_id: int = PathParam(..., description="Maintenance config ID"),
    db: Session = Depends(deps.get_db),
    current_user = Depends(deps.get_current_active_user)
):
    """
    Delete a maintenance configuration.
    
    For when you decide you don't need that particular janitor anymore.
    """
    config = crud_maintenance.maintenance_config.get(db, id=config_id)
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Maintenance config with ID {config_id} not found"
        )
    
    crud_maintenance.maintenance_config.remove(db, id=config_id)


@router.post("/configs/{config_id}/run", response_model=TaskExecutionResponse)
def trigger_maintenance_task(
    request: TaskExecutionRequest,
    config_id: int = PathParam(..., description="Maintenance config ID"),
    db: Session = Depends(deps.get_db),
    current_user = Depends(deps.get_current_active_user)
):
    """
    Manually trigger a maintenance task.
    
    For when you can't wait for the scheduled cleanup and need the janitor
    to drop everything and clean up that mess RIGHT NOW.
    """
    config = crud_maintenance.maintenance_config.get(db, id=config_id)
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Maintenance config with ID {config_id} not found"
        )
    
    if not config.enabled:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot run disabled maintenance config"
        )
    
    # Check for running tasks unless force_run is True
    if not request.force_run:
        running_tasks = crud_maintenance.maintenance_task.get_running_tasks(db)
        if any(task.config_id == config_id for task in running_tasks):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail="A task for this config is already running. Use force_run=true to override."
            )
    
    # Create and start the task
    task_create = MaintenanceTaskCreate(
        config_id=config_id,
        task_type=config.task_type
    )
    task = crud_maintenance.maintenance_task.create(db, obj_in=task_create)
    
    # Queue the task for execution (in a real implementation, you'd use a task queue)
    # For now, we'll just mark it as ready and let the maintenance service pick it up
    
    return TaskExecutionResponse(
        task_id=task.id,
        message=f"Task queued for execution: {config.name}",
        started=False
    )


@router.get("/tasks", response_model=List[MaintenanceTaskWithDetails])
def list_maintenance_tasks(
    config_id: Optional[int] = Query(None, description="Filter by config ID"),
    status: Optional[MaintenanceTaskStatus] = Query(None, description="Filter by status"),
    hours: int = Query(24, ge=1, le=168, description="Look back this many hours"),
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=1000),
    db: Session = Depends(deps.get_db),
    current_user = Depends(deps.get_current_active_user)
):
    """
    Get maintenance task execution history.
    
    Perfect for when you want to see if the janitor has been doing their job
    or just playing solitaire in the supply closet.
    """
    cutoff_time = datetime.utcnow() - timedelta(hours=hours)
    
    # Build query based on filters
    query = db.query(crud_maintenance.maintenance_task.model)
    query = query.filter(crud_maintenance.maintenance_task.model.created_at >= cutoff_time)
    
    if config_id:
        query = query.filter(crud_maintenance.maintenance_task.model.config_id == config_id)
    
    if status:
        query = query.filter(crud_maintenance.maintenance_task.model.status == status.value)
    
    tasks = query.order_by(crud_maintenance.maintenance_task.model.created_at.desc()).offset(skip).limit(limit).all()
    
    # Enrich with config details
    enriched_tasks = []
    for task in tasks:
        task_dict = MaintenanceTaskRead.model_validate(task).model_dump()
        
        # Add config details
        config = crud_maintenance.maintenance_config.get(db, id=task.config_id)
        if config:
            task_dict["config"] = MaintenanceConfigRead.model_validate(config)
        
        enriched_tasks.append(MaintenanceTaskWithDetails(**task_dict))
    
    return enriched_tasks


@router.get("/tasks/{task_id}", response_model=MaintenanceTaskWithDetails)
def get_maintenance_task(
    task_id: int = PathParam(..., description="Maintenance task ID"),
    db: Session = Depends(deps.get_db),
    current_user = Depends(deps.get_current_active_user)
):
    """Get details of a specific maintenance task execution"""
    task = crud_maintenance.maintenance_task.get(db, id=task_id)
    if not task:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Maintenance task with ID {task_id} not found"
        )
    
    task_dict = MaintenanceTaskRead.model_validate(task).model_dump()
    
    # Add config details
    config = crud_maintenance.maintenance_config.get(db, id=task.config_id)
    if config:
        task_dict["config"] = MaintenanceConfigRead.model_validate(config)
    
    return MaintenanceTaskWithDetails(**task_dict)


@router.get("/stats", response_model=Dict[str, Any])
def get_maintenance_stats(
    config_id: Optional[int] = Query(None, description="Get stats for specific config"),
    days: int = Query(30, ge=1, le=365, description="Look back this many days"),
    db: Session = Depends(deps.get_db),
    current_user = Depends(deps.get_current_active_user)
):
    """
    Get maintenance statistics.
    
    For when the boss wants numbers to prove the janitor is worth their salary.
    """
    return crud_maintenance.maintenance_task.get_task_stats(db, config_id=config_id, days=days)


# Data Cleaner specific endpoints
@router.post("/data-cleaner", response_model=DataCleanerConfigRead, status_code=status.HTTP_201_CREATED)
def create_data_cleaner_config(
    config_in: DataCleanerConfigCreate,
    db: Session = Depends(deps.get_db),
    current_user = Depends(deps.get_current_active_user)
):
    """
    Create a data cleaner configuration.
    
    For setting up a specialized janitor who only cleans up DICOM files.
    """
    # Verify the maintenance config exists and is a data cleaner type
    maintenance_config = crud_maintenance.maintenance_config.get(db, id=config_in.maintenance_config_id)
    if not maintenance_config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Maintenance config with ID {config_in.maintenance_config_id} not found"
        )
    
    if maintenance_config.task_type != MaintenanceTaskType.DATA_CLEANER:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Maintenance config must be of type DATA_CLEANER"
        )
    
    # Check if config already exists
    existing_config = crud_maintenance.data_cleaner_config.get_by_maintenance_config_id(
        db, maintenance_config_id=config_in.maintenance_config_id
    )
    if existing_config:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Data cleaner config already exists for this maintenance config"
        )
    
    config = crud_maintenance.data_cleaner_config.create(db, obj_in=config_in)
    return config


@router.get("/data-cleaner/{config_id}", response_model=DataCleanerConfigRead)
def get_data_cleaner_config(
    config_id: int = PathParam(..., description="Data cleaner config ID"),
    db: Session = Depends(deps.get_db),
    current_user = Depends(deps.get_current_active_user)
):
    """Get a specific data cleaner configuration"""
    config = crud_maintenance.data_cleaner_config.get(db, id=config_id)
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Data cleaner config with ID {config_id} not found"
        )
    return config


@router.put("/data-cleaner/{config_id}", response_model=DataCleanerConfigRead)
def update_data_cleaner_config(
    config_in: DataCleanerConfigUpdate,
    config_id: int = PathParam(..., description="Data cleaner config ID"),
    db: Session = Depends(deps.get_db),
    current_user = Depends(deps.get_current_active_user)
):
    """
    Update a data cleaner configuration.
    
    For retraining your specialized DICOM janitor when they keep
    deleting the wrong files.
    """
    config = crud_maintenance.data_cleaner_config.get(db, id=config_id)
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Data cleaner config with ID {config_id} not found"
        )
    
    config = crud_maintenance.data_cleaner_config.update(db, db_obj=config, obj_in=config_in)
    return config


@router.delete("/data-cleaner/{config_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_data_cleaner_config(
    config_id: int = PathParam(..., description="Data cleaner config ID"),
    db: Session = Depends(deps.get_db),
    current_user = Depends(deps.get_current_active_user)
):
    """Delete a data cleaner configuration"""
    config = crud_maintenance.data_cleaner_config.get(db, id=config_id)
    if not config:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Data cleaner config with ID {config_id} not found"
        )
    
    crud_maintenance.data_cleaner_config.remove(db, id=config_id)
