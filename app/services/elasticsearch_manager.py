"""
Elasticsearch Manager Service for log management integration.

Provides high-level interface for managing Elasticsearch ILM policies,
index templates, and cluster operations for medical imaging log compliance.
"""

import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone

from elasticsearch import AsyncElasticsearch
from elasticsearch.exceptions import NotFoundError, RequestError

from ..core.config import settings

logger = logging.getLogger(__name__)


class ElasticsearchManager:
    """Service for managing Elasticsearch operations for log management."""
    
    def __init__(self):
        """Initialize Elasticsearch client with SSL and authentication."""
        self.client = AsyncElasticsearch(
            hosts=["https://axiom-elasticsearch:9200"],
            basic_auth=("elastic", "furt-murt-dirt-dink"),
            verify_certs=False,  # For development - use proper certs in production
            ssl_show_warn=False,
            request_timeout=30,
        )
    
    async def health_check(self) -> Dict[str, Any]:
        """Check Elasticsearch cluster health."""
        try:
            health = await self.client.cluster.health()
            return {
                "status": health["status"],
                "cluster_name": health["cluster_name"],
                "number_of_nodes": health["number_of_nodes"],
                "active_primary_shards": health["active_primary_shards"],
                "active_shards": health["active_shards"],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            logger.error(f"Elasticsearch health check failed: {e}")
            raise
    
    async def create_ilm_policy(self, policy_name: str, policy: Dict[str, Any]) -> bool:
        """Create or update an ILM policy in Elasticsearch."""
        try:
            response = await self.client.ilm.put_lifecycle(
                name=policy_name,
                policy=policy
            )
            logger.info(f"Created/updated ILM policy: {policy_name}")
            return response.get("acknowledged", False)
        except Exception as e:
            logger.error(f"Failed to create ILM policy {policy_name}: {e}")
            raise
    
    async def update_ilm_policy(self, policy_name: str, policy: Dict[str, Any]) -> bool:
        """Update an existing ILM policy."""
        return await self.create_ilm_policy(policy_name, policy)
    
    async def delete_ilm_policy(self, policy_name: str) -> bool:
        """Delete an ILM policy."""
        try:
            response = await self.client.ilm.delete_lifecycle(name=policy_name)
            logger.info(f"Deleted ILM policy: {policy_name}")
            return response.get("acknowledged", False)
        except NotFoundError:
            logger.warning(f"ILM policy {policy_name} not found for deletion")
            return True  # Consider it successful if it doesn't exist
        except Exception as e:
            logger.error(f"Failed to delete ILM policy {policy_name}: {e}")
            raise
    
    async def list_ilm_policies(self) -> List[Dict[str, Any]]:
        """List all ILM policies in Elasticsearch with full details."""
        try:
            response = await self.client.ilm.get_lifecycle()
            policies = []
            
            for name, policy_data in response.items():
                policies.append({
                    "name": name,
                    "version": policy_data.get("version", 1),
                    "modified_date": policy_data.get("modified_date"),
                    "policy": policy_data.get("policy", {})
                })
            
            return policies
        except Exception as e:
            logger.error(f"Failed to list ILM policies: {e}")
            raise
    
    async def list_ilm_policy_names(self) -> List[str]:
        """List all ILM policy names in Elasticsearch."""
        try:
            response = await self.client.ilm.get_lifecycle()
            return list(response.keys())
        except Exception as e:
            logger.error(f"Failed to list ILM policy names: {e}")
            raise
    
    async def get_ilm_policy(self, policy_name: str) -> Optional[Dict[str, Any]]:
        """Get a specific ILM policy."""
        try:
            response = await self.client.ilm.get_lifecycle(name=policy_name)
            return response.get(policy_name)
        except NotFoundError:
            return None
        except Exception as e:
            logger.error(f"Failed to get ILM policy {policy_name}: {e}")
            raise
    
    async def create_index_template(
        self, template_name: str, template: Dict[str, Any]
    ) -> bool:
        """Create or update an index template."""
        try:
            response = await self.client.indices.put_index_template(
                name=template_name,
                **template
            )
            logger.info(f"Created/updated index template: {template_name}")
            return response.get("acknowledged", False)
        except Exception as e:
            logger.error(f"Failed to create index template {template_name}: {e}")
            raise
    
    async def delete_index_template(self, template_name: str) -> bool:
        """Delete an index template."""
        try:
            response = await self.client.indices.delete_index_template(
                name=template_name
            )
            logger.info(f"Deleted index template: {template_name}")
            return response.get("acknowledged", False)
        except NotFoundError:
            logger.warning(f"Index template {template_name} not found for deletion")
            return True
        except Exception as e:
            logger.error(f"Failed to delete index template {template_name}: {e}")
            raise
    
    async def list_index_templates(self) -> List[Dict[str, Any]]:
        """List all index templates."""
        try:
            response = await self.client.indices.get_index_template()
            templates = []
            
            for template_data in response.get("index_templates", []):
                templates.append({
                    "name": template_data["name"],
                    "index_patterns": template_data["index_template"]["index_patterns"],
                    "priority": template_data["index_template"].get("priority", 0),
                    "composed_of": template_data["index_template"].get("composed_of", [])
                })
            
            return templates
        except Exception as e:
            logger.error(f"Failed to list index templates: {e}")
            raise
    
    async def list_indices(self, pattern: str = "*") -> List[Dict[str, Any]]:
        """List indices with metadata."""
        try:
            response = await self.client.cat.indices(
                index=pattern,
                format="json",
                h="index,status,health,docs.count,store.size,creation.date.string"
            )
            
            indices = []
            for index_data in response:
                indices.append({
                    "name": index_data.get("index"),
                    "status": index_data.get("status"),
                    "health": index_data.get("health"),
                    "docs_count": int(index_data.get("docs.count", 0) or 0),
                    "store_size": index_data.get("store.size"),
                    "creation_date": index_data.get("creation.date.string")
                })
            
            return indices
        except Exception as e:
            logger.error(f"Failed to list indices: {e}")
            raise
    
    async def get_cluster_stats(self) -> Dict[str, Any]:
        """Get comprehensive cluster statistics."""
        try:
            stats = await self.client.cluster.stats()
            
            return {
                "cluster_name": stats["cluster_name"],
                "status": stats["status"],
                "nodes": stats["nodes"]["count"]["total"],
                "indices_count": stats["indices"]["count"],
                "docs_count": stats["indices"]["docs"]["count"],
                "store_size_bytes": stats["indices"]["store"]["size_in_bytes"],
                "timestamp": datetime.now(timezone.utc).isoformat()
            }
        except Exception as e:
            logger.error(f"Failed to get cluster stats: {e}")
            raise
    
    async def get_indices_stats(
        self, index_pattern: str = "*"
    ) -> Dict[str, Any]:
        """Get statistics for indices matching a pattern."""
        try:
            response = await self.client.indices.stats(index=index_pattern)
            
            stats = {
                "total_indices": 0,
                "total_docs": 0,
                "total_size_bytes": 0,
                "indices": []
            }
            
            for index_name, index_data in response.get("indices", {}).items():
                index_stats = {
                    "name": index_name,
                    "docs_count": index_data["total"]["docs"]["count"],
                    "size_bytes": index_data["total"]["store"]["size_in_bytes"],
                    "primaries": index_data["primaries"]["docs"]["count"],
                }
                
                stats["indices"].append(index_stats)
                stats["total_docs"] += index_stats["docs_count"]
                stats["total_size_bytes"] += index_stats["size_bytes"]
                stats["total_indices"] += 1
            
            return stats
        except Exception as e:
            logger.error(f"Failed to get indices stats for pattern {index_pattern}: {e}")
            raise
    
    async def apply_ilm_policy_to_index(
        self, index_pattern: str, policy_name: str
    ) -> bool:
        """Apply an ILM policy to existing indices."""
        try:
            response = await self.client.ilm.retry(index=index_pattern)
            logger.info(f"Applied ILM policy {policy_name} to {index_pattern}")
            return True
        except Exception as e:
            logger.error(f"Failed to apply ILM policy to indices: {e}")
            raise
    
    async def get_index_ilm_status(
        self, index_pattern: str
    ) -> List[Dict[str, Any]]:
        """Get ILM status for indices."""
        try:
            response = await self.client.ilm.explain_lifecycle(index=index_pattern)
            
            status_list = []
            for index_name, index_data in response.get("indices", {}).items():
                status_list.append({
                    "index": index_name,
                    "managed": index_data.get("managed", False),
                    "policy": index_data.get("policy"),
                    "phase": index_data.get("phase"),
                    "action": index_data.get("action"),
                    "step": index_data.get("step"),
                    "phase_time": index_data.get("phase_time")
                })
            
            return status_list
        except Exception as e:
            logger.error(f"Failed to get ILM status: {e}")
            raise
    
    async def search_logs(
        self,
        index_pattern: str,
        query: Dict[str, Any],
        size: int = 100,
        sort: Optional[List[Dict[str, Any]]] = None
    ) -> Dict[str, Any]:
        """Search logs with a query."""
        try:
            search_body = {
                "query": query,
                "size": size
            }
            
            if sort:
                search_body["sort"] = sort
            
            response = await self.client.search(
                index=index_pattern,
                body=search_body
            )
            
            return {
                "total_hits": response["hits"]["total"]["value"],
                "hits": response["hits"]["hits"],
                "took_ms": response["took"],
                "timed_out": response["timed_out"]
            }
        except Exception as e:
            logger.error(f"Failed to search logs: {e}")
            raise
    
    async def aggregate_logs(
        self,
        index_pattern: str,
        aggregation: Dict[str, Any],
        query: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Perform aggregation on logs."""
        try:
            search_body = {
                "size": 0,
                "aggs": aggregation
            }
            
            if query:
                search_body["query"] = query
            
            response = await self.client.search(
                index=index_pattern,
                body=search_body
            )
            
            return response.get("aggregations", {})
        except Exception as e:
            logger.error(f"Failed to aggregate logs: {e}")
            raise
    
    async def close(self):
        """Close the Elasticsearch client."""
        await self.client.close()
    
    def __del__(self):
        """Ensure client is closed when object is destroyed."""
        try:
            import asyncio
            loop = asyncio.get_event_loop()
            if loop.is_running():
                loop.create_task(self.close())
        except Exception:
            pass
