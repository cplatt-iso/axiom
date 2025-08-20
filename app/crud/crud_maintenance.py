from typing import List, Optional, Dict, Any
from sqlalchemy.orm import Session
from sqlalchemy import and_, desc, func
from datetime import datetime, timedelta

from app.crud.base import CRUDBase
from app.db.models.maintenance import MaintenanceConfig, MaintenanceTask, DataCleanerConfig, MaintenanceTaskStatus
from app.schemas.maintenance import (
    MaintenanceConfigCreate,
    MaintenanceConfigUpdate,
    MaintenanceTaskCreate,
    MaintenanceTaskUpdate,
    DataCleanerConfigCreate,
    DataCleanerConfigUpdate
)


class CRUDMaintenanceConfig(CRUDBase[MaintenanceConfig, MaintenanceConfigCreate, MaintenanceConfigUpdate]):
    def get_by_task_type(self, db: Session, *, task_type: str) -> List[MaintenanceConfig]:
        """Get all configs for a specific task type"""
        return db.query(MaintenanceConfig).filter(MaintenanceConfig.task_type == task_type).all()

    def get_enabled_configs(self, db: Session) -> List[MaintenanceConfig]:
        """Get all enabled maintenance configs"""
        return db.query(MaintenanceConfig).filter(MaintenanceConfig.enabled == True).all()

    def get_configs_due_for_run(self, db: Session, current_time: Optional[datetime] = None) -> List[MaintenanceConfig]:
        """Get configs that are due for their next run"""
        if current_time is None:
            current_time = datetime.utcnow()
        
        return db.query(MaintenanceConfig).filter(
            and_(
                MaintenanceConfig.enabled == True,
                MaintenanceConfig.next_run <= current_time
            )
        ).all()

    def update_run_times(
        self, 
        db: Session, 
        *, 
        config_id: int, 
        last_run: datetime, 
        next_run: datetime
    ) -> Optional[MaintenanceConfig]:
        """Update the last_run and next_run times for a config"""
        config = db.query(MaintenanceConfig).filter(MaintenanceConfig.id == config_id).first()
        if config:
            config.last_run = last_run
            config.next_run = next_run
            db.commit()
            db.refresh(config)
        return config


class CRUDMaintenanceTask(CRUDBase[MaintenanceTask, MaintenanceTaskCreate, MaintenanceTaskUpdate]):
    def get_by_config_id(self, db: Session, *, config_id: int, limit: int = 100) -> List[MaintenanceTask]:
        """Get tasks for a specific config, ordered by most recent first"""
        return (
            db.query(MaintenanceTask)
            .filter(MaintenanceTask.config_id == config_id)
            .order_by(desc(MaintenanceTask.created_at))
            .limit(limit)
            .all()
        )

    def get_running_tasks(self, db: Session) -> List[MaintenanceTask]:
        """Get all currently running tasks"""
        return db.query(MaintenanceTask).filter(MaintenanceTask.status == MaintenanceTaskStatus.RUNNING).all()

    def get_recent_completions(self, db: Session, *, hours: int = 24) -> List[MaintenanceTask]:
        """Get tasks completed in the last N hours"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        return (
            db.query(MaintenanceTask)
            .filter(
                and_(
                    MaintenanceTask.status == MaintenanceTaskStatus.COMPLETED,
                    MaintenanceTask.completed_at >= cutoff_time
                )
            )
            .order_by(desc(MaintenanceTask.completed_at))
            .all()
        )

    def get_recent_failures(self, db: Session, *, hours: int = 24) -> List[MaintenanceTask]:
        """Get tasks that failed in the last N hours"""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        return (
            db.query(MaintenanceTask)
            .filter(
                and_(
                    MaintenanceTask.status == MaintenanceTaskStatus.FAILED,
                    MaintenanceTask.completed_at >= cutoff_time
                )
            )
            .order_by(desc(MaintenanceTask.completed_at))
            .all()
        )

    def get_task_stats(self, db: Session, *, config_id: Optional[int] = None, days: int = 30) -> Dict[str, Any]:
        """Get statistics for tasks"""
        cutoff_time = datetime.utcnow() - timedelta(days=days)
        
        query = db.query(MaintenanceTask).filter(MaintenanceTask.created_at >= cutoff_time)
        if config_id:
            query = query.filter(MaintenanceTask.config_id == config_id)
        
        tasks = query.all()
        
        stats = {
            "total_runs": len(tasks),
            "successful_runs": len([t for t in tasks if t.status == MaintenanceTaskStatus.COMPLETED]),
            "failed_runs": len([t for t in tasks if t.status == MaintenanceTaskStatus.FAILED]),
            "total_files_processed": sum(t.files_processed for t in tasks),
            "total_bytes_freed": sum(t.bytes_freed for t in tasks),
            "total_errors": sum(t.errors_encountered for t in tasks),
            "average_runtime_seconds": None
        }
        
        # Calculate average runtime for completed tasks
        completed_tasks = [t for t in tasks if t.status == MaintenanceTaskStatus.COMPLETED and t.started_at and t.completed_at]
        if completed_tasks:
            total_runtime = sum(
                (t.completed_at - t.started_at).total_seconds() 
                for t in completed_tasks
                if t.started_at is not None and t.completed_at is not None
            )
            stats["average_runtime_seconds"] = total_runtime / len(completed_tasks)
        
        return stats

    def start_task(self, db: Session, *, task_id: int) -> Optional[MaintenanceTask]:
        """Mark a task as started"""
        task = db.query(MaintenanceTask).filter(MaintenanceTask.id == task_id).first()
        if task:
            task.status = MaintenanceTaskStatus.RUNNING
            task.started_at = datetime.utcnow()
            db.commit()
            db.refresh(task)
        return task

    def complete_task(
        self, 
        db: Session, 
        *, 
        task_id: int, 
        success: bool = True, 
        files_processed: int = 0,
        bytes_freed: int = 0,
        errors_encountered: int = 0,
        execution_details: Optional[Dict[str, Any]] = None,
        error_message: Optional[str] = None
    ) -> Optional[MaintenanceTask]:
        """Mark a task as completed (successfully or with failure)"""
        task = db.query(MaintenanceTask).filter(MaintenanceTask.id == task_id).first()
        if task:
            task.status = MaintenanceTaskStatus.COMPLETED if success else MaintenanceTaskStatus.FAILED
            task.completed_at = datetime.utcnow()
            task.files_processed = files_processed
            task.bytes_freed = bytes_freed
            task.errors_encountered = errors_encountered
            if execution_details:
                task.execution_details = execution_details
            if error_message:
                task.error_message = error_message
            db.commit()
            db.refresh(task)
        return task


class CRUDDataCleanerConfig(CRUDBase[DataCleanerConfig, DataCleanerConfigCreate, DataCleanerConfigUpdate]):
    def get_by_maintenance_config_id(self, db: Session, *, maintenance_config_id: int) -> Optional[DataCleanerConfig]:
        """Get data cleaner config by maintenance config ID"""
        return (
            db.query(DataCleanerConfig)
            .filter(DataCleanerConfig.maintenance_config_id == maintenance_config_id)
            .first()
        )

    def get_configs_for_directory(self, db: Session, *, target_directory: str) -> List[DataCleanerConfig]:
        """Get all data cleaner configs monitoring a specific directory"""
        return (
            db.query(DataCleanerConfig)
            .filter(DataCleanerConfig.target_directory == target_directory)
            .all()
        )


# Create instances of CRUD classes
maintenance_config = CRUDMaintenanceConfig(MaintenanceConfig)
maintenance_task = CRUDMaintenanceTask(MaintenanceTask)
data_cleaner_config = CRUDDataCleanerConfig(DataCleanerConfig)
