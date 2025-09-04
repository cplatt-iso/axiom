"""
ILM Policy Engine
Generates and manages Elasticsearch Index Lifecycle Management policies
"""

from typing import Dict, Any, Optional, TYPE_CHECKING, Union, List
from app.log_management.models import RetentionTier

if TYPE_CHECKING:
    from app.log_management.models import LogRetentionPolicy


class PolicyEngine:
    """
    Generates ILM policies based on retention requirements
    """
    
    @staticmethod
    def generate_ilm_policy(
        tier: RetentionTier,
        hot_days: int = 30,
        warm_days: int = 180,
        cold_days: int = 365,
        delete_days: int = 2555,
        max_index_size_gb: int = 10,
        max_index_age_days: int = 30,
        storage_class_hot: str = "fast-ssd",
        storage_class_warm: str = "standard",
        storage_class_cold: str = "cold-storage"
    ) -> Dict[str, Any]:
        """
        Generate ILM policy JSON based on retention parameters
        """
        
        policy: Dict[str, Any] = {
            "phases": {
                "hot": {
                    "actions": {
                        "rollover": {
                            "max_size": f"{max_index_size_gb}gb",
                            "max_age": f"{max_index_age_days}d"
                        },
                        "set_priority": {
                            "priority": 100
                        }
                    }
                }
            }
        }
        
        # Add warm phase if needed
        if warm_days > hot_days:
            policy["phases"]["warm"] = {
                "min_age": f"{hot_days}d",
                "actions": {
                    "allocate": {
                        "number_of_replicas": 0  # Reduce replicas to save space
                    },
                    "forcemerge": {
                        "max_num_segments": 1  # Optimize for storage
                    },
                    "set_priority": {
                        "priority": 50
                    }
                }
            }
            
            # Add storage class allocation for Kubernetes
            if storage_class_warm != storage_class_hot:
                policy["phases"]["warm"]["actions"]["allocate"]["require"] = {
                    "storage_class": storage_class_warm
                }
        
        # Add cold phase if needed
        if cold_days > warm_days:
            policy["phases"]["cold"] = {
                "min_age": f"{warm_days}d",
                "actions": {
                    "allocate": {
                        "number_of_replicas": 0
                    },
                    "set_priority": {
                        "priority": 0
                    }
                }
            }
            
            # Add storage class allocation for Kubernetes
            if storage_class_cold != storage_class_warm:
                policy["phases"]["cold"]["actions"]["allocate"]["require"] = {
                    "storage_class": storage_class_cold
                }
        
        # Add delete phase
        if delete_days > 0:
            policy["phases"]["delete"] = {
                "min_age": f"{delete_days}d",
                "actions": {
                    "delete": {}
                }
            }
        
        return policy
    
    @staticmethod
    def generate_ilm_policy_from_db(db_policy: "LogRetentionPolicy") -> Dict[str, Any]:
        """
        Generate ILM policy from database model
        """
        return PolicyEngine.generate_ilm_policy(
            tier=db_policy.tier,
            hot_days=db_policy.hot_days,
            warm_days=db_policy.warm_days,
            cold_days=db_policy.cold_days,
            delete_days=db_policy.delete_days,
            max_index_size_gb=db_policy.max_index_size_gb,
            max_index_age_days=db_policy.max_index_age_days,
            storage_class_hot=db_policy.storage_class_hot,
            storage_class_warm=db_policy.storage_class_warm,
            storage_class_cold=db_policy.storage_class_cold
        )
    
    @staticmethod
    def get_predefined_policies() -> Dict[str, Dict[str, Any]]:
        """
        Get predefined policies for common use cases
        """
        return {
            "critical_logs": PolicyEngine.generate_ilm_policy(
                tier=RetentionTier.CRITICAL,
                hot_days=30,
                warm_days=180,
                cold_days=365,
                delete_days=2555,  # 7 years
                max_index_size_gb=10
            ),
            "operational_logs": PolicyEngine.generate_ilm_policy(
                tier=RetentionTier.OPERATIONAL,
                hot_days=7,
                warm_days=90,
                cold_days=365,
                delete_days=730,  # 2 years
                max_index_size_gb=5
            ),
            "debug_logs": PolicyEngine.generate_ilm_policy(
                tier=RetentionTier.DEBUG,
                hot_days=1,
                warm_days=7,
                cold_days=30,
                delete_days=90,  # 3 months
                max_index_size_gb=2
            )
        }
    
    @staticmethod
    def generate_index_template_config(
        pattern: str,
        ilm_policy: str,
        alias: str,
        priority: int = 100
    ) -> Dict[str, Any]:
        """
        Generate index template configuration
        """
        return {
            "index_patterns": [pattern],
            "template": {
                "settings": {
                    "index": {
                        "lifecycle": {
                            "name": ilm_policy,
                            "rollover_alias": alias
                        },
                        "number_of_shards": 1,
                        "number_of_replicas": 1,
                        "codec": "best_compression"  # Save storage space
                    }
                },
                "mappings": {
                    "properties": {
                        "@timestamp": {
                            "type": "date"
                        },
                        "container_name": {
                            "type": "keyword"
                        },
                        "service": {
                            "type": "keyword"
                        },
                        "level": {
                            "type": "keyword"
                        },
                        "message": {
                            "type": "text",
                            "analyzer": "standard"
                        },
                        "log": {
                            "type": "text",
                            "analyzer": "standard"
                        },
                        "source": {
                            "type": "keyword"
                        }
                    }
                }
            },
            "priority": priority
        }
    
    @staticmethod
    def generate_kubernetes_cronjob(
        name: str,
        schedule: str,
        image: str,
        command: list,
        env_vars: Optional[Dict[str, str]] = None,
        resources: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Generate Kubernetes CronJob manifest for log archival
        """
        return {
            "apiVersion": "batch/v1",
            "kind": "CronJob",
            "metadata": {
                "name": name,
                "namespace": "logging"
            },
            "spec": {
                "schedule": schedule,
                "jobTemplate": {
                    "spec": {
                        "template": {
                            "spec": {
                                "containers": [{
                                    "name": "log-archiver",
                                    "image": image,
                                    "command": command,
                                    "env": [
                                        {"name": k, "value": v} 
                                        for k, v in (env_vars or {}).items()
                                    ],
                                    "resources": resources or {
                                        "requests": {
                                            "cpu": "100m",
                                            "memory": "256Mi"
                                        },
                                        "limits": {
                                            "cpu": "500m", 
                                            "memory": "1Gi"
                                        }
                                    }
                                }],
                                "restartPolicy": "OnFailure"
                            }
                        }
                    }
                },
                "successfulJobsHistoryLimit": 3,
                "failedJobsHistoryLimit": 1
            }
        }
    
    @staticmethod
    def generate_medical_imaging_policies() -> Dict[str, Dict[str, Any]]:
        """
        Generate medical imaging specific policies for HIPAA compliance
        """
        return {
            "critical_dicom": PolicyEngine.generate_ilm_policy(
                tier=RetentionTier.CRITICAL,
                hot_days=30,
                warm_days=180,
                cold_days=365,
                delete_days=2555,  # 7+ years for HIPAA
                max_index_size_gb=50,
                max_index_age_days=1
            ),
            "api_security": PolicyEngine.generate_ilm_policy(
                tier=RetentionTier.CRITICAL,
                hot_days=30,
                warm_days=90,
                cold_days=365,
                delete_days=2555,
                max_index_size_gb=10,
                max_index_age_days=1
            ),
            "system_operations": PolicyEngine.generate_ilm_policy(
                tier=RetentionTier.OPERATIONAL,
                hot_days=7,
                warm_days=30,
                cold_days=180,
                delete_days=730,  # 2 years
                max_index_size_gb=20,
                max_index_age_days=1
            ),
            "debug_development": PolicyEngine.generate_ilm_policy(
                tier=RetentionTier.DEBUG,
                hot_days=1,
                warm_days=7,
                cold_days=30,
                delete_days=90,  # 3 months
                max_index_size_gb=5,
                max_index_age_days=1
            )
        }
    
    @staticmethod
    def generate_index_template(
        template_name: str,
        index_pattern: Union[str, List[str]],
        ilm_policy_name: str,
        priority: int = 100
    ) -> Dict[str, Any]:
        """
        Generate index template for Elasticsearch
        """
        # Ensure index_pattern is a list
        if isinstance(index_pattern, str):
            index_patterns = [index_pattern]
        else:
            index_patterns = index_pattern
            
        return {
            "index_patterns": index_patterns,
            "template": {
                "settings": {
                    "index": {
                        "lifecycle": {
                            "name": ilm_policy_name
                        },
                        "number_of_shards": 1,
                        "number_of_replicas": 1,
                        "codec": "best_compression"
                    }
                },
                "mappings": {
                    "properties": {
                        "@timestamp": {
                            "type": "date"
                        },
                        "container_name": {
                            "type": "keyword"
                        },
                        "service": {
                            "type": "keyword"
                        },
                        "level": {
                            "type": "keyword"
                        },
                        "message": {
                            "type": "text",
                            "analyzer": "standard"
                        }
                    }
                }
            },
            "priority": priority,
            "_meta": {
                "description": f"Template for {template_name} with ILM policy {ilm_policy_name}"
            }
        }
