"""
Elasticsearch client wrapper for log management operations
Supports both Docker Compose and Kubernetes environments
"""

import json
import logging
from app.core.config import settings
from elasticsearch import Elasticsearch
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
import json
import logging

logger = logging.getLogger(__name__)


class ElasticsearchLogManager:
    """
    Manages Elasticsearch operations for log retention and archival
    """
    
    def __init__(self):
        self.client = self._get_client()
        
    def _get_client(self) -> Elasticsearch:
        """Get Elasticsearch client with proper configuration"""
        
        # Kubernetes service discovery
        if settings.KUBERNETES_MODE:
            host = "elasticsearch-service.logging.svc.cluster.local"
            port = 9200
        else:
            # Docker Compose mode
            host = settings.ELASTICSEARCH_HOST or "localhost"
            port = settings.ELASTICSEARCH_PORT or 9200
            
        # SSL configuration
        if settings.ELASTICSEARCH_SSL_ENABLED:
            client = Elasticsearch(
                hosts=[f"https://{host}:{port}"],
                basic_auth=("elastic", str(settings.ELASTICSEARCH_PASSWORD)),
                verify_certs=False,  # TODO: Implement proper cert verification
                ssl_show_warn=False
            )
        else:
            client = Elasticsearch(
                hosts=[f"http://{host}:{port}"]
            )
            
        return client
    
    async def create_ilm_policy(self, policy_name: str, policy_config: Dict[str, Any]) -> bool:
        """Create or update an Index Lifecycle Management policy"""
        try:
            response = self.client.ilm.put_lifecycle(
                name=policy_name,
                body=policy_config
            )
            logger.info(f"ILM policy '{policy_name}' created/updated successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create ILM policy '{policy_name}': {str(e)}")
            return False
    
    async def get_ilm_policies(self) -> Dict[str, Any]:
        """Get all ILM policies"""
        try:
            response = self.client.ilm.get_lifecycle()
            # ObjectApiResponse can be used as a dict directly in most Elasticsearch versions
            return dict(response.body) if hasattr(response, 'body') else response  # type: ignore
        except Exception as e:
            logger.error(f"Failed to get ILM policies: {str(e)}")
            return {}
    
    async def create_index_template(self, template_name: str, pattern: str, 
                                 ilm_policy: str, settings: Optional[Dict[str, Any]] = None) -> bool:
        """Create index template with ILM policy"""
        template_config = {
            "index_patterns": [pattern],
            "template": {
                "settings": {
                    "index.lifecycle.name": ilm_policy,
                    "index.lifecycle.rollover_alias": f"{pattern.replace('*', 'alias')}",
                    **(settings or {})
                }
            }
        }
        
        try:
            self.client.indices.put_template(
                name=template_name,
                body=template_config
            )
            logger.info(f"Index template '{template_name}' created successfully")
            return True
        except Exception as e:
            logger.error(f"Failed to create index template '{template_name}': {str(e)}")
            return False
    
    async def get_index_stats(self) -> Dict[str, Any]:
        """Get comprehensive index statistics"""
        try:
            stats = self.client.indices.stats(index="*")
            # Handle ObjectApiResponse conversion properly
            return stats.body if hasattr(stats, 'body') else stats  # type: ignore
        except Exception as e:
            logger.error(f"Failed to get index stats: {str(e)}")
            return {}
    
    async def get_service_log_counts(self) -> List[Dict[str, Any]]:
        """Get log counts by service/container"""
        query = {
            "size": 0,
            "aggs": {
                "services": {
                    "terms": {
                        "field": "container_name.keyword",
                        "size": 100
                    },
                    "aggs": {
                        "earliest": {
                            "min": {
                                "field": "@timestamp"
                            }
                        },
                        "latest": {
                            "max": {
                                "field": "@timestamp"
                            }
                        }
                    }
                }
            }
        }
        
        try:
            response = self.client.search(index="*", body=query)
            services = []
            for bucket in response["aggregations"]["services"]["buckets"]:
                services.append({
                    "container_name": bucket["key"],
                    "log_count": bucket["doc_count"],
                    "earliest_log": bucket["earliest"]["value_as_string"] if bucket["earliest"]["value"] else None,
                    "latest_log": bucket["latest"]["value_as_string"] if bucket["latest"]["value"] else None
                })
            return services
        except Exception as e:
            logger.error(f"Failed to get service log counts: {str(e)}")
            return []
    
    async def get_storage_stats(self) -> Dict[str, Any]:
        """Get overall storage statistics"""
        try:
            # Get cluster stats
            cluster_stats = self.client.cluster.stats()
            
            # Get index stats for size breakdown
            index_stats = await self.get_index_stats()
            
            total_size = 0
            total_indices = 0
            
            if index_stats and "indices" in index_stats:
                for index_name, stats in index_stats["indices"].items():
                    total_size += stats["total"]["store"]["size_in_bytes"]
                    total_indices += 1
            
            # Get date range of logs
            date_query = {
                "size": 0,
                "aggs": {
                    "oldest": {
                        "min": {
                            "field": "@timestamp"
                        }
                    },
                    "newest": {
                        "max": {
                            "field": "@timestamp"  
                        }
                    }
                }
            }
            
            date_response = self.client.search(index="*", body=date_query)
            
            return {
                "total_indices": total_indices,
                "total_size_gb": total_size / (1024**3),
                "hot_size_gb": 0,  # TODO: Calculate based on ILM phase
                "warm_size_gb": 0,
                "cold_size_gb": 0,
                "oldest_log_date": date_response["aggregations"]["oldest"]["value_as_string"] if date_response["aggregations"]["oldest"]["value"] else None,
                "newest_log_date": date_response["aggregations"]["newest"]["value_as_string"] if date_response["aggregations"]["newest"]["value"] else None
            }
            
        except Exception as e:
            logger.error(f"Failed to get storage stats: {str(e)}")
            return {
                "total_indices": 0,
                "total_size_gb": 0,
                "hot_size_gb": 0,
                "warm_size_gb": 0,
                "cold_size_gb": 0,
                "oldest_log_date": None,
                "newest_log_date": None
            }
    
    async def search_logs(self, query: Dict[str, Any], index: str = "*") -> Dict[str, Any]:
        """Execute arbitrary log search query"""
        try:
            response = self.client.search(index=index, body=query)
            # Handle ObjectApiResponse conversion properly
            return response.body if hasattr(response, 'body') else response  # type: ignore
        except Exception as e:
            logger.error(f"Search failed: {str(e)}")
            return {"hits": {"total": {"value": 0}, "hits": []}}
    
    async def delete_old_indices(self, pattern: str, older_than_days: int) -> List[str]:
        """Delete indices older than specified days"""
        cutoff_date = datetime.utcnow() - timedelta(days=older_than_days)
        
        try:
            # Get all indices matching pattern
            response = self.client.cat.indices(index=pattern, format="json")
            indices = list(response) if response else []
            deleted = []
            
            for index in indices:
                # Extract date from index name or use creation date
                # This is a simplified approach - you might need more sophisticated date parsing
                if isinstance(index, dict):
                    index_name = index.get("index", "")
                else:
                    index_name = str(index)
                
                # Check if we should delete this index
                # This is where you'd implement your date logic
                # For now, just log what we would delete
                logger.info(f"Would delete index {index_name} (older than {older_than_days} days)")
                
            return deleted
            
        except Exception as e:
            logger.error(f"Failed to delete old indices: {str(e)}")
            return []
    
    async def create_snapshot(self, repository: str, snapshot_name: str, 
                           indices: List[str]) -> bool:
        """Create snapshot of specified indices"""
        snapshot_config = {
            "indices": ",".join(indices),
            "ignore_unavailable": True,
            "include_global_state": False
        }
        
        try:
            response = self.client.snapshot.create(
                repository=repository,
                snapshot=snapshot_name,
                body=snapshot_config
            )
            logger.info(f"Snapshot '{snapshot_name}' created in repository '{repository}'")
            return True
        except Exception as e:
            logger.error(f"Failed to create snapshot: {str(e)}")
            return False
