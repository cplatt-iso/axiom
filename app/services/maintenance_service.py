"""
Maintenance Service - The Digital Janitor

This service coordinates all maintenance tasks in the system.
Think of it as the grumpy but reliable janitor who keeps everything clean
and running smoothly while complaining about the mess everyone else makes.
"""

import os
import shutil
import asyncio
import logging
import fnmatch
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta
from sqlalchemy.orm import Session

import structlog
from app.core.config import settings
from app.db.session import SessionLocal
from app.crud import crud_maintenance
from app.db.models.maintenance import (
    MaintenanceConfig,
    MaintenanceTask,
    DataCleanerConfig,
    MaintenanceTaskType,
    MaintenanceTaskStatus,
    CleanupMode,
    DeletionStrategy
)
from app.schemas.maintenance import MaintenanceTaskCreate

logger = structlog.get_logger(__name__)


class MaintenanceService:
    """
    The main maintenance service coordinator.
    
    This is the head janitor - it knows all the maintenance tasks,
    schedules them, and makes sure they don't step on each other's toes.
    """
    
    def __init__(self):
        self.running = False
        self.task_handlers = {
            MaintenanceTaskType.DATA_CLEANER: DataCleanerTask()
        }
    
    async def start(self):
        """Start the maintenance service"""
        self.running = True
        logger.info("Maintenance service starting up - time to clean house!")
        
        # Initialize default configurations if this is first run
        await self._initialize_default_configs()
        
        # Start the main monitoring loop
        while self.running:
            try:
                await self._run_maintenance_cycle()
                await asyncio.sleep(60)  # Check every minute
            except Exception as e:
                logger.error(f"Error in maintenance cycle: {e}", exc_info=True)
                await asyncio.sleep(60)  # Don't spam errors
    
    async def stop(self):
        """Stop the maintenance service"""
        self.running = False
        logger.info("Maintenance service shutting down - leaving the mess for tomorrow")
    
    async def _run_maintenance_cycle(self):
        """Run one maintenance cycle - check for due tasks and execute them"""
        with SessionLocal() as db:
            # Get configs that are due for execution
            due_configs = crud_maintenance.maintenance_config.get_configs_due_for_run(db)
            
            for config in due_configs:
                try:
                    await self._execute_maintenance_task(db, config)
                except Exception as e:
                    logger.error(
                        f"Failed to execute maintenance task {config.name}: {e}",
                        config_id=config.id,
                        task_type=config.task_type,
                        exc_info=True
                    )
    
    async def _execute_maintenance_task(self, db: Session, config: MaintenanceConfig):
        """Execute a specific maintenance task"""
        # Check if there's already a running task for this config
        running_tasks = crud_maintenance.maintenance_task.get_running_tasks(db)
        if any(task.config_id == config.id for task in running_tasks):
            logger.debug(f"Task {config.name} already running, skipping")
            return
        
        # Create a new task execution record
        task_create = MaintenanceTaskCreate(
            config_id=config.id,
            task_type=config.task_type
        )
        task = crud_maintenance.maintenance_task.create(db, obj_in=task_create)
        
        try:
            # Start the task
            crud_maintenance.maintenance_task.start_task(db, task_id=task.id)
            logger.info(f"Starting maintenance task: {config.name}", task_id=task.id)
            
            # Get the appropriate handler
            handler = self.task_handlers.get(config.task_type)
            if not handler:
                raise ValueError(f"No handler found for task type: {config.task_type}")
            
            # Execute the task
            result = await handler.execute(db, config, task)
            
            # Mark task as completed
            crud_maintenance.maintenance_task.complete_task(
                db,
                task_id=task.id,
                success=True,
                files_processed=result.get('files_processed', 0),
                bytes_freed=result.get('bytes_freed', 0),
                errors_encountered=result.get('errors_encountered', 0),
                execution_details=result
            )
            
            # Update config run times
            next_run = datetime.utcnow() + timedelta(seconds=config.monitor_interval_seconds)
            crud_maintenance.maintenance_config.update_run_times(
                db,
                config_id=config.id,
                last_run=datetime.utcnow(),
                next_run=next_run
            )
            
            logger.info(
                f"Completed maintenance task: {config.name}",
                task_id=task.id,
                files_processed=result.get('files_processed', 0),
                bytes_freed=result.get('bytes_freed', 0)
            )
            
        except Exception as e:
            # Mark task as failed
            crud_maintenance.maintenance_task.complete_task(
                db,
                task_id=task.id,
                success=False,
                error_message=str(e)
            )
            logger.error(
                f"Maintenance task {config.name} failed: {e}",
                task_id=task.id,
                config_id=config.id,
                exc_info=True
            )
            raise
    
    async def _initialize_default_configs(self):
        """Initialize default maintenance configurations on first startup"""
        with SessionLocal() as db:
            # Check if we already have configurations
            existing_configs = crud_maintenance.maintenance_config.get_multi(db, limit=1)
            if existing_configs:
                logger.debug("Maintenance configs already exist, skipping initialization")
                return
            
            logger.info("Initializing default maintenance configurations")
            
            # Load defaults from YAML file
            await self._load_default_configs_from_file(db)
    
    async def _load_default_configs_from_file(self, db: Session):
        """Load default configurations from YAML file"""
        import yaml
        from pathlib import Path
        
        # Look for defaults file in the backend root directory
        defaults_file = Path("/app/maintenance_defaults.yaml")  # Docker path
        if not defaults_file.exists():
            # Try local development path
            defaults_file = Path("maintenance_defaults.yaml")
        
        if not defaults_file.exists():
            logger.warning("No maintenance defaults file found, creating built-in defaults")
            await self._create_builtin_default_configs(db)
            return
        
        try:
            with open(defaults_file, 'r') as f:
                defaults = yaml.safe_load(f)
            
            maintenance_defaults = defaults.get('maintenance_defaults', {})
            logger.info(f"Loading {len(maintenance_defaults)} default maintenance configurations from file")
            
            for config_key, config_data in maintenance_defaults.items():
                try:
                    await self._create_config_from_defaults(db, config_key, config_data)
                    logger.info(f"Created default maintenance config: {config_data.get('name', config_key)}")
                except Exception as e:
                    logger.error(f"Failed to create default config {config_key}: {e}")
        
        except Exception as e:
            logger.error(f"Failed to load maintenance defaults from file: {e}")
            # Fall back to built-in defaults
            await self._create_builtin_default_configs(db)
    
    async def _create_config_from_defaults(self, db: Session, config_key: str, config_data: dict):
        """Create a maintenance config from defaults data"""
        from app.schemas.maintenance import MaintenanceConfigCreate, DataCleanerConfigCreate
        
        # Create the main maintenance config
        maintenance_config_create = MaintenanceConfigCreate(
            task_type=MaintenanceTaskType(config_data["task_type"]),
            name=config_data["name"],
            description=config_data.get("description"),
            enabled=config_data.get("enabled", True),
            monitor_interval_seconds=config_data.get("monitor_interval_seconds", 300),
            config={}  # We'll store specifics in the DataCleanerConfig table
        )
        
        maintenance_config = crud_maintenance.maintenance_config.create(
            db, obj_in=maintenance_config_create
        )
        
        # Set initial next_run time
        next_run = datetime.utcnow() + timedelta(minutes=5)  # Start in 5 minutes
        crud_maintenance.maintenance_config.update_run_times(
            db,
            config_id=maintenance_config.id,
            last_run=datetime.utcnow(),
            next_run=next_run
        )
        
        # Create the specific data cleaner config if this is a data cleaner task
        if maintenance_config.task_type == MaintenanceTaskType.DATA_CLEANER:
            data_cleaner_create = DataCleanerConfigCreate(
                maintenance_config_id=maintenance_config.id,
                target_directory=config_data["target_directory"],
                cleanup_mode=CleanupMode(config_data["cleanup_mode"]),
                deletion_strategy=DeletionStrategy(config_data.get("deletion_strategy", "oldest_first")),
                high_water_mark_bytes=config_data.get("high_water_mark_bytes"),
                target_free_space_percentage=config_data.get("target_free_space_percentage"),
                minimum_file_age_hours=config_data.get("minimum_file_age_hours", 1),
                minimum_free_space_gb=config_data.get("minimum_free_space_gb", 1.0),
                dry_run=config_data.get("dry_run", True),
                file_pattern=config_data.get("file_pattern"),
                exclude_pattern=config_data.get("exclude_pattern"),
                batch_size=config_data.get("batch_size", 100),
                max_files_per_run=config_data.get("max_files_per_run", 10000)
            )
            
            crud_maintenance.data_cleaner_config.create(db, obj_in=data_cleaner_create)
    
    async def _create_builtin_default_configs(self, db: Session):
        """Create built-in default configurations as fallback"""
        logger.info("Creating built-in default maintenance configurations")
        
        # Built-in defaults - same as what was in _create_default_data_cleaner_configs
        default_configs = [
            {
                "name": "DICOM Incoming Directory Cleaner",
                "description": "Cleans up the incoming DICOM directory to prevent disk space issues",
                "target_directory": str(settings.DICOM_STORAGE_PATH),
                "cleanup_mode": CleanupMode.FREE_SPACE_PERCENTAGE,
                "target_free_space_percentage": 10.0,  # Keep 10% free space
                "deletion_strategy": DeletionStrategy.OLDEST_FIRST,
                "minimum_file_age_hours": 1,  # Don't delete files newer than 1 hour
                "dry_run": True,  # Start in dry run mode for safety
                "monitor_interval_seconds": 300  # Check every 5 minutes
            },
            {
                "name": "DICOM Processed Directory Cleaner",
                "description": "Manages the processed DICOM directory with high water mark",
                "target_directory": str(settings.FILESYSTEM_STORAGE_PATH),
                "cleanup_mode": CleanupMode.HIGH_WATER_MARK,
                "high_water_mark_bytes": 50 * 1024 * 1024 * 1024,  # 50GB max
                "deletion_strategy": DeletionStrategy.OLDEST_FIRST,
                "minimum_file_age_hours": 24,  # Don't delete files newer than 24 hours
                "dry_run": True,
                "monitor_interval_seconds": 900  # Check every 15 minutes
            }
        ]
        
        for config_data in default_configs:
            try:
                await self._create_config_from_defaults(db, config_data["name"], config_data)
            except Exception as e:
                logger.error(f"Failed to create built-in default config {config_data['name']}: {e}")


class DataCleanerTask:
    """
    The data cleaner task - specializes in deleting files like a digital Marie Kondo.
    
    "Does this DICOM spark joy? No? DELETE!"
    """
    
    async def execute(self, db: Session, config: MaintenanceConfig, task: MaintenanceTask) -> Dict[str, Any]:
        """Execute the data cleaner task"""
        
        # Get the specific data cleaner configuration
        data_config = crud_maintenance.data_cleaner_config.get_by_maintenance_config_id(
            db, maintenance_config_id=config.id
        )
        
        if not data_config:
            raise ValueError(f"No data cleaner config found for maintenance config {config.id}")
        
        logger.info(
            f"Starting data cleanup for directory: {data_config.target_directory}",
            cleanup_mode=data_config.cleanup_mode,
            deletion_strategy=data_config.deletion_strategy,
            dry_run=data_config.dry_run
        )
        
        # Check if target directory exists
        target_path = Path(data_config.target_directory)
        if not target_path.exists():
            logger.warning(f"Target directory does not exist: {data_config.target_directory}")
            return {
                "files_processed": 0,
                "bytes_freed": 0,
                "errors_encountered": 0,
                "message": f"Target directory does not exist: {data_config.target_directory}"
            }
        
        # Determine if cleanup is needed
        cleanup_needed, reason = self._should_cleanup(data_config, target_path)
        
        result = {
            "files_processed": 0,
            "bytes_freed": 0,
            "errors_encountered": 0,
            "cleanup_needed": cleanup_needed,
            "reason": reason,
            "dry_run": data_config.dry_run,
            "deleted_files": [],
            "skipped_files": [],
            "errors": []
        }
        
        if not cleanup_needed:
            logger.info(f"No cleanup needed: {reason}")
            return result
        
        # Get files to potentially delete
        candidate_files = self._get_candidate_files(data_config, target_path)
        logger.info(f"Found {len(candidate_files)} candidate files for cleanup")
        
        if not candidate_files:
            result["message"] = "No candidate files found for cleanup"
            return result
        
        # Sort files according to deletion strategy
        sorted_files = self._sort_files_by_strategy(data_config, candidate_files)
        
        # Delete files until cleanup goal is met
        bytes_to_free = self._calculate_bytes_to_free(data_config, target_path)
        bytes_freed = 0
        files_processed = 0
        
        for file_path in sorted_files:
            if files_processed >= data_config.max_files_per_run:
                logger.info(f"Reached max files per run limit: {data_config.max_files_per_run}")
                break
            
            try:
                file_size = file_path.stat().st_size
                
                if data_config.dry_run:
                    logger.info(f"DRY RUN: Would delete {file_path} ({file_size} bytes)")
                    result["deleted_files"].append({
                        "path": str(file_path),
                        "size": file_size,
                        "action": "would_delete"
                    })
                else:
                    file_path.unlink()
                    logger.debug(f"Deleted file: {file_path} ({file_size} bytes)")
                    result["deleted_files"].append({
                        "path": str(file_path),
                        "size": file_size,
                        "action": "deleted"
                    })
                
                bytes_freed += file_size
                files_processed += 1
                
                # Check if we've freed enough space
                if data_config.cleanup_mode != CleanupMode.KEEP_EMPTY and bytes_freed >= bytes_to_free:
                    logger.info(f"Cleanup goal achieved. Freed {bytes_freed} bytes")
                    break
                    
            except Exception as e:
                logger.error(f"Error deleting file {file_path}: {e}")
                result["errors"].append({
                    "path": str(file_path),
                    "error": str(e)
                })
                result["errors_encountered"] += 1
        
        result["files_processed"] = files_processed
        result["bytes_freed"] = bytes_freed
        
        logger.info(
            f"Data cleanup completed",
            files_processed=files_processed,
            bytes_freed=bytes_freed,
            errors=result["errors_encountered"],
            dry_run=data_config.dry_run
        )
        
        return result
    
    def _should_cleanup(self, config: DataCleanerConfig, target_path: Path) -> Tuple[bool, str]:
        """Determine if cleanup is needed based on the cleanup mode"""
        
        if config.cleanup_mode == CleanupMode.KEEP_EMPTY:
            return True, "KEEP_EMPTY mode - always cleanup"
        
        elif config.cleanup_mode == CleanupMode.HIGH_WATER_MARK:
            if config.high_water_mark_bytes is None:
                return False, "High water mark not configured"
            current_size = self._get_directory_size(target_path)
            if current_size > config.high_water_mark_bytes:
                return True, f"Directory size ({current_size} bytes) exceeds high water mark ({config.high_water_mark_bytes} bytes)"
            else:
                return False, f"Directory size ({current_size} bytes) is below high water mark ({config.high_water_mark_bytes} bytes)"
        
        elif config.cleanup_mode == CleanupMode.FREE_SPACE_PERCENTAGE:
            if config.target_free_space_percentage is None:
                return False, "Target free space percentage not configured"
            disk_usage = shutil.disk_usage(target_path)
            free_percentage = (disk_usage.free / disk_usage.total) * 100
            
            if free_percentage < config.target_free_space_percentage:
                return True, f"Free space ({free_percentage:.1f}%) is below target ({config.target_free_space_percentage:.1f}%)"
            else:
                return False, f"Free space ({free_percentage:.1f}%) is above target ({config.target_free_space_percentage:.1f}%)"
        
        return False, "Unknown cleanup mode"
    
    def _get_candidate_files(self, config: DataCleanerConfig, target_path: Path) -> List[Path]:
        """Get list of files that are candidates for deletion"""
        candidates = []
        cutoff_time = datetime.utcnow() - timedelta(hours=config.minimum_file_age_hours)
        
        for file_path in target_path.rglob("*"):
            if not file_path.is_file():
                continue
            
            # Check file age
            file_mtime = datetime.fromtimestamp(file_path.stat().st_mtime)
            if file_mtime > cutoff_time:
                continue
            
            # Check file pattern
            if config.file_pattern and not fnmatch.fnmatch(file_path.name, config.file_pattern):
                continue
            
            # Check exclude pattern
            if config.exclude_pattern and fnmatch.fnmatch(file_path.name, config.exclude_pattern):
                continue
            
            candidates.append(file_path)
        
        return candidates
    
    def _sort_files_by_strategy(self, config: DataCleanerConfig, files: List[Path]) -> List[Path]:
        """Sort files according to the deletion strategy"""
        
        if config.deletion_strategy == DeletionStrategy.OLDEST_FIRST:
            # Try to extract study date from directory structure first
            # Fallback to file modification time
            return sorted(files, key=lambda f: self._get_study_date_from_path(f) or f.stat().st_mtime)
        
        elif config.deletion_strategy == DeletionStrategy.FIFO:
            # Sort by file creation/modification time (oldest first)
            return sorted(files, key=lambda f: f.stat().st_mtime)
        
        elif config.deletion_strategy == DeletionStrategy.LIFO:
            # Sort by file creation/modification time (newest first)
            return sorted(files, key=lambda f: f.stat().st_mtime, reverse=True)
        
        return files
    
    def _get_study_date_from_path(self, file_path: Path) -> Optional[float]:
        """
        Extract study date from directory structure.
        
        Assumes structure like: .../YYYYMMDD/... or similar
        Returns timestamp if found, None otherwise
        """
        for part in file_path.parts:
            if len(part) == 8 and part.isdigit():
                try:
                    year = int(part[:4])
                    month = int(part[4:6])
                    day = int(part[6:8])
                    study_date = datetime(year, month, day)
                    return study_date.timestamp()
                except ValueError:
                    continue
        return None
    
    def _calculate_bytes_to_free(self, config: DataCleanerConfig, target_path: Path) -> int:
        """Calculate how many bytes need to be freed"""
        
        if config.cleanup_mode == CleanupMode.KEEP_EMPTY:
            # Return a very large number instead of infinity for int return type
            return self._get_directory_size(target_path)  # Delete everything
        
        elif config.cleanup_mode == CleanupMode.HIGH_WATER_MARK:
            if config.high_water_mark_bytes is None:
                return 0
            current_size = self._get_directory_size(target_path)
            return max(0, current_size - config.high_water_mark_bytes)
        
        elif config.cleanup_mode == CleanupMode.FREE_SPACE_PERCENTAGE:
            if config.target_free_space_percentage is None:
                return 0
            disk_usage = shutil.disk_usage(target_path)
            target_free_bytes = int(disk_usage.total * (config.target_free_space_percentage / 100))
            return max(0, target_free_bytes - disk_usage.free)
        
        return 0
    
    def _get_directory_size(self, directory: Path) -> int:
        """Get the total size of all files in a directory"""
        total_size = 0
        for file_path in directory.rglob("*"):
            if file_path.is_file():
                try:
                    total_size += file_path.stat().st_size
                except OSError:
                    # Skip files we can't stat
                    continue
        return total_size


# Global maintenance service instance
maintenance_service = MaintenanceService()
