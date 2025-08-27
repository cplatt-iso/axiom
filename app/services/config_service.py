from typing import Dict, Any, Optional
from sqlalchemy.orm import Session
from app import crud
from app.core.config import settings

try:
    import structlog
    logger = structlog.get_logger(__name__)
except ImportError:
    import logging
    logger = logging.getLogger(__name__)

class ConfigService:
    """Service for dynamic configuration management"""
    
    @staticmethod
    def get_effective_setting(db: Session, setting_key: str, default_value: Any = None) -> Any:
        """
        Get the effective value for a setting, checking database overrides first,
        then falling back to the settings object, then the provided default.
        """
        # Check for database override
        db_setting = crud.system_setting.get_by_key(db, key=setting_key)
        if db_setting:
            return ConfigService._parse_db_value(db_setting.value, type(default_value))
        
        # Check settings object
        if hasattr(settings, setting_key):
            return getattr(settings, setting_key)
        
        # Return provided default
        return default_value
    
    @staticmethod
    def get_all_effective_settings(db: Session) -> Dict[str, Any]:
        """Get all effective settings as a dictionary"""
        effective_settings = {}
        
        # Get all database overrides
        db_settings = crud.system_setting.get_multi(db, limit=1000)
        db_overrides = {s.key: s.value for s in db_settings}
        
        # Get all settings from the settings object
        settings_dict = settings.model_dump()
        
        # Merge, with database taking precedence
        for key, value in settings_dict.items():
            if key in db_overrides:
                effective_settings[key] = ConfigService._parse_db_value(
                    db_overrides[key], type(value)
                )
            else:
                effective_settings[key] = value
        
        return effective_settings
    
    @staticmethod
    def _parse_db_value(value: str, target_type: type) -> Any:
        """Parse a database string value to the target type"""
        if target_type == bool:
            return value.lower() in ("true", "1", "yes", "on")
        elif target_type == int:
            try:
                return int(value)
            except ValueError:
                return 0
        elif target_type == float:
            try:
                return float(value)
            except ValueError:
                return 0.0
        else:
            return value

# Global instance
config_service = ConfigService()
