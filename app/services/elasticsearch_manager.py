"""
Elasticsearch Manager Service for log management integration.

Provides high-level interface for managing Elasticsearch ILM policies,
index templates, and cluster operations for medical imaging log compliance.
"""

import json
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone, timedelta
from collections import defaultdict

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

    async def get_enhanced_analytics(self, db_policies: List[Any] = None) -> Dict[str, Any]:
        """Get comprehensive analytics including policy efficiency correlation."""
        
        # Get basic analytics
        analytics = await self._get_basic_analytics()
        
        # Add enhanced analytics
        analytics["storage_distribution"] = await self._get_storage_by_tier()
        analytics["ingestion_trends"] = await self._get_daily_ingestion_rates()
        analytics["service_breakdown"] = await self._get_service_breakdown()
        analytics["log_level_distribution"] = await self._get_log_level_distribution()
        analytics["retention_compliance"] = await self._get_retention_compliance()
        
        # Add policy efficiency if policies provided
        if db_policies:
            analytics["policy_efficiency"] = await self.get_policy_efficiency_analysis(db_policies)
        else:
            analytics["policy_efficiency"] = {
                "coverage_percentage": 0,
                "recommendations": ["No database policies provided for analysis"]
            }
        
        return analytics

    async def _get_basic_analytics(self) -> Dict[str, Any]:
        """Get basic analytics including timestamp and common metrics."""
        try:
            # Get cluster stats
            cluster_stats = await self.client.cluster.stats()
            
            # Get axiom indices stats
            indices_stats = await self.client.indices.stats(index="axiom*")
            
            total_indices = len(indices_stats["indices"])
            total_documents = cluster_stats["indices"]["docs"]["count"]
            total_storage_bytes = cluster_stats["indices"]["store"]["size_in_bytes"]
            
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "total_indices": total_indices,
                "total_documents": total_documents,
                "total_storage_gb": round(total_storage_bytes / (1024**3), 3),
                "cluster_health": cluster_stats["status"]
            }
            
        except Exception as e:
            logger.error(f"Failed to get basic analytics: {e}")
            return {
                "timestamp": datetime.now(timezone.utc).isoformat(),
                "total_indices": 0,
                "total_documents": 0,
                "total_storage_gb": 0,
                "cluster_health": "unknown"
            }

    async def _get_storage_by_tier(self) -> Dict[str, Any]:
        """Get storage breakdown by age-based tiers (since ILM not implemented yet)."""
        try:
            # Get all axiom indices with their stats
            indices_stats = await self.client.indices.stats(index="axiom*")
            
            # Define age-based tiers for analysis
            now = datetime.now(timezone.utc)
            tier_breakdown = {
                "hot": {"size_bytes": 0, "indices": [], "description": "< 30 days old"},
                "warm": {"size_bytes": 0, "indices": [], "description": "30-180 days old"}, 
                "cold": {"size_bytes": 0, "indices": [], "description": "180+ days old"},
                "archived": {"size_bytes": 0, "indices": [], "description": "No ILM policy applied"}
            }
            
            total_storage = 0
            
            for index_name, stats in indices_stats["indices"].items():
                size_bytes = stats["total"]["store"]["size_in_bytes"]
                doc_count = stats["total"]["docs"]["count"]
                total_storage += size_bytes
                
                # Determine age-based tier from index name
                try:
                    # Extract date from index name
                    if "axiom-flow-" in index_name:
                        date_part = index_name.split("axiom-flow-")[1]
                    else:
                        date_part = index_name.split("axiom-")[1]
                    
                    date_components = date_part.split(".")
                    if len(date_components) == 3:
                        index_date = datetime.strptime(f"{date_components[0]}-{date_components[1].zfill(2)}-{date_components[2].zfill(2)}", "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        age_days = (now - index_date).days
                        
                        # Classify by age
                        if age_days < 30:
                            tier = "hot"
                        elif age_days < 180:
                            tier = "warm"
                        else:
                            tier = "cold"
                    else:
                        tier = "archived"  # Can't determine age
                        
                except (IndexError, ValueError):
                    tier = "archived"  # Unknown pattern
                
                tier_breakdown[tier]["size_bytes"] += size_bytes
                tier_breakdown[tier]["indices"].append({
                    "name": index_name,
                    "size_bytes": size_bytes,
                    "doc_count": doc_count,
                    "age_days": age_days if 'age_days' in locals() else None
                })
            
            # Calculate percentages and convert to GB
            for tier_name, tier_data in tier_breakdown.items():
                size_gb = tier_data["size_bytes"] / (1024**3)
                percentage = (tier_data["size_bytes"] / total_storage * 100) if total_storage > 0 else 0
                
                tier_breakdown[tier_name].update({
                    "size_gb": round(size_gb, 3),
                    "percentage": round(percentage, 2),
                    "indices_count": len(tier_data["indices"]),
                    "avg_index_size_gb": round(size_gb / len(tier_data["indices"]), 3) if tier_data["indices"] else 0
                })
            
            # Calculate potential savings with proper ILM
            hot_cost_multiplier = 10  # Hot storage is ~10x more expensive
            warm_cost_multiplier = 3
            cold_cost_multiplier = 1
            
            current_cost_units = total_storage * hot_cost_multiplier  # Everything on hot storage
            optimized_cost_units = (
                tier_breakdown["hot"]["size_bytes"] * hot_cost_multiplier +
                tier_breakdown["warm"]["size_bytes"] * warm_cost_multiplier +
                tier_breakdown["cold"]["size_bytes"] * cold_cost_multiplier +
                tier_breakdown["archived"]["size_bytes"] * cold_cost_multiplier
            )
            
            potential_savings = round(((current_cost_units - optimized_cost_units) / current_cost_units * 100), 2) if current_cost_units > 0 else 0
            
            return {
                "total_storage_gb": round(total_storage / (1024**3), 3),
                "by_tier": tier_breakdown,
                "analysis": {
                    "ilm_implemented": False,
                    "potential_cost_savings_percent": potential_savings,
                    "recommendation": "Implement ILM policies to move older data to cheaper storage tiers",
                    "oldest_index_age_days": max([idx.get("age_days", 0) for tier in tier_breakdown.values() for idx in tier["indices"]], default=0)
                }
            }
        except Exception as e:
            logger.error(f"Failed to get storage by tier: {e}")
            return {"total_storage_gb": 0, "by_tier": {}, "analysis": {}}

    async def _get_log_level_distribution(self) -> Dict[str, Any]:
        """Get distribution of log levels across all axiom indices."""
        try:
            search_body = {
                "size": 0,
                "aggs": {
                    "log_levels": {
                        "terms": {
                            "field": "level",
                            "size": 10,
                            "missing": "UNKNOWN"
                        }
                    }
                }
            }
            
            response = await self.client.search(
                index="axiom*",
                body=search_body
            )
            
            buckets = response["aggregations"]["log_levels"]["buckets"]
            total_logs = sum(bucket["doc_count"] for bucket in buckets)
            
            distribution = {}
            for bucket in buckets:
                level = bucket["key"]
                count = bucket["doc_count"]
                percentage = (count / total_logs * 100) if total_logs > 0 else 0
                distribution[level] = {
                    "count": count,
                    "percentage": round(percentage, 2)
                }
            
            return {
                "total_logs": total_logs,
                "distribution": distribution
            }
        except Exception as e:
            logger.error(f"Failed to get log level distribution: {e}")
            return {"total_logs": 0, "distribution": {}}

    async def _get_daily_ingestion_rates(self) -> Dict[str, Any]:
        """Get daily log ingestion rates with actual storage calculations."""
        try:
            end_date = datetime.now(timezone.utc)
            start_date = end_date - timedelta(days=30)
            
            # Get detailed index statistics grouped by date
            indices_stats = await self.client.indices.stats(index="axiom*")
            
            daily_data = defaultdict(lambda: {"document_count": 0, "storage_bytes": 0, "indices": []})
            
            # Parse index names and aggregate by date
            for index_name, stats in indices_stats["indices"].items():
                # Extract date from index name (axiom-2025.09.08 or axiom-flow-2025.09.08)
                try:
                    if "axiom-flow-" in index_name:
                        date_part = index_name.split("axiom-flow-")[1]  # 2025.09.08
                    else:
                        date_part = index_name.split("axiom-")[1]       # 2025.09.08
                    
                    # Convert to YYYY-MM-DD format
                    date_components = date_part.split(".")
                    if len(date_components) == 3:
                        date_str = f"{date_components[0]}-{date_components[1].zfill(2)}-{date_components[2].zfill(2)}"
                        index_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                        
                        # Only include last 30 days
                        if start_date <= index_date <= end_date:
                            doc_count = stats["total"]["docs"]["count"]
                            size_bytes = stats["total"]["store"]["size_in_bytes"]
                            
                            daily_data[date_str]["document_count"] += doc_count
                            daily_data[date_str]["storage_bytes"] += size_bytes
                            daily_data[date_str]["indices"].append({
                                "name": index_name,
                                "docs": doc_count,
                                "size_bytes": size_bytes
                            })
                except (IndexError, ValueError):
                    # Skip indices that don't match our pattern
                    continue
            
            # Convert to sorted list and calculate rates
            sorted_data = []
            total_docs_30d = 0
            total_storage_30d = 0
            
            for date_str in sorted(daily_data.keys()):
                data = daily_data[date_str]
                storage_gb = data["storage_bytes"] / (1024**3)
                
                sorted_data.append({
                    "date": date_str,
                    "document_count": data["document_count"],
                    "storage_bytes": data["storage_bytes"],
                    "storage_gb": round(storage_gb, 3),
                    "indices_count": len(data["indices"]),
                    "avg_doc_size_bytes": round(data["storage_bytes"] / data["document_count"], 2) if data["document_count"] > 0 else 0
                })
                
                total_docs_30d += data["document_count"]
                total_storage_30d += data["storage_bytes"]
            
            # Calculate realistic averages
            days_with_data = len(sorted_data)
            avg_daily_docs = total_docs_30d / days_with_data if days_with_data > 0 else 0
            avg_daily_storage_gb = (total_storage_30d / (1024**3)) / days_with_data if days_with_data > 0 else 0
            avg_doc_size_bytes = total_storage_30d / total_docs_30d if total_docs_30d > 0 else 0
            
            return {
                "last_30_days": sorted_data,
                "average_daily_documents": round(avg_daily_docs),
                "average_daily_storage_gb": round(avg_daily_storage_gb, 3),
                "total_documents_30d": total_docs_30d,
                "total_storage_30d_gb": round(total_storage_30d / (1024**3), 3),
                "average_document_size_bytes": round(avg_doc_size_bytes, 2),
                "days_with_data": days_with_data
            }
        except Exception as e:
            logger.error(f"Failed to get daily ingestion rates: {e}")
            return {"last_30_days": [], "average_daily_documents": 0, "average_daily_storage_gb": 0}

    async def _get_retention_compliance(self) -> Dict[str, Any]:
        """Check retention policy compliance across indices."""
        try:
            # Get all axiom indices with their creation dates
            indices = await self.client.cat.indices(index="axiom*", format="json")
            
            compliance_data = {
                "compliant_indices": 0,
                "non_compliant_indices": 0,
                "total_indices": len(indices),
                "compliance_percentage": 0,
                "details": []
            }
            
            for index in indices:
                index_name = index["index"]
                creation_time = index.get("creation.date.string", "")
                
                # Check if index has ILM policy
                try:
                    ilm_explain = await self.client.ilm.explain_lifecycle(index=index_name)
                    has_policy = "policy" in ilm_explain["indices"][index_name]
                    policy_name = ilm_explain["indices"][index_name].get("policy", "none")
                    
                    if has_policy and policy_name.startswith("axiom-"):
                        compliance_data["compliant_indices"] += 1
                        is_compliant = True
                    else:
                        compliance_data["non_compliant_indices"] += 1
                        is_compliant = False
                    
                    compliance_data["details"].append({
                        "index": index_name,
                        "is_compliant": is_compliant,
                        "policy": policy_name,
                        "creation_date": creation_time
                    })
                except:
                    compliance_data["non_compliant_indices"] += 1
                    compliance_data["details"].append({
                        "index": index_name,
                        "is_compliant": False,
                        "policy": "none",
                        "creation_date": creation_time
                    })
            
            if compliance_data["total_indices"] > 0:
                compliance_data["compliance_percentage"] = round(
                    (compliance_data["compliant_indices"] / compliance_data["total_indices"]) * 100, 2
                )
            
            return compliance_data
        except Exception as e:
            logger.error(f"Failed to get retention compliance: {e}")
            return {"compliant_indices": 0, "non_compliant_indices": 0, "total_indices": 0, "compliance_percentage": 0}

    async def _get_service_breakdown(self) -> Dict[str, Any]:
        """Get breakdown of log volume by service."""
        try:
            # Get all axiom indices and aggregate by service pattern
            indices_stats = await self.client.indices.stats(index="axiom*")
            
            service_data = {}
            total_documents = 0
            total_storage = 0
            
            for index_name, stats in indices_stats["indices"].items():
                # Extract service name from index pattern (e.g., axiom-servicename-2024.01.01)
                parts = index_name.split('-')
                if len(parts) >= 2:
                    service = parts[1]  # Get the service name part
                else:
                    service = "unknown"
                
                doc_count = stats["total"]["docs"]["count"]
                size_bytes = stats["total"]["store"]["size_in_bytes"]
                
                if service not in service_data:
                    service_data[service] = {
                        "service": service,
                        "document_count": 0,
                        "storage_gb": 0,
                        "index_count": 0
                    }
                
                service_data[service]["document_count"] += doc_count
                service_data[service]["storage_gb"] += size_bytes / (1024**3)
                service_data[service]["index_count"] += 1
                
                total_documents += doc_count
                total_storage += size_bytes
            
            # Calculate percentages and round storage
            services_list = []
            for service_info in service_data.values():
                doc_percentage = (service_info["document_count"] / total_documents * 100) if total_documents > 0 else 0
                storage_percentage = (service_info["storage_gb"] / (total_storage / (1024**3)) * 100) if total_storage > 0 else 0
                
                services_list.append({
                    "service": service_info["service"],
                    "document_count": service_info["document_count"],
                    "document_percentage": round(doc_percentage, 2),
                    "storage_gb": round(service_info["storage_gb"], 3),
                    "storage_percentage": round(storage_percentage, 2),
                    "index_count": service_info["index_count"]
                })
            
            # Sort by document count descending
            services_list.sort(key=lambda x: x["document_count"], reverse=True)
            
            return {
                "services": services_list,
                "total_services": len(services_list),
                "total_documents": total_documents,
                "total_storage_gb": round(total_storage / (1024**3), 3)
            }
            
        except Exception as e:
            logger.error(f"Failed to get service breakdown: {e}")
            return {"services": [], "total_services": 0, "total_documents": 0, "total_storage_gb": 0}

    async def get_policy_efficiency_analysis(self, db_policies: List[Any]) -> Dict[str, Any]:
        """Analyze how well retention policies match actual data patterns."""
        try:
            # Get all axiom indices with their stats
            indices_stats = await self.client.indices.stats(index="axiom*")
            
            policy_coverage = {}
            uncovered_indices = []
            total_uncovered_storage = 0
            
            for policy in db_policies:
                policy_name = policy.name
                service_patterns = [p.strip() for p in policy.service_pattern.split(',')]
                
                policy_coverage[policy_name] = {
                    "patterns": service_patterns,
                    "tier": policy.tier.value,
                    "retention_days": policy.delete_days,
                    "covered_indices": [],
                    "covered_storage_gb": 0,
                    "covered_documents": 0,
                    "pattern_matches": 0
                }
            
            # Analyze each index
            for index_name, stats in indices_stats["indices"].items():
                size_bytes = stats["total"]["store"]["size_in_bytes"]
                doc_count = stats["total"]["docs"]["count"]
                
                matched_policy = None
                
                # Check which policy should cover this index
                for policy in db_policies:
                    service_patterns = [p.strip() for p in policy.service_pattern.split(',')]
                    
                    for pattern in service_patterns:
                        # Convert policy pattern to regex-like matching
                        pattern_regex = pattern.replace('*', '.*').replace('.', r'\.')
                        
                        import re
                        if re.match(pattern_regex, index_name):
                            matched_policy = policy.name
                            policy_coverage[policy.name]["covered_indices"].append({
                                "name": index_name,
                                "size_gb": round(size_bytes / (1024**3), 3),
                                "doc_count": doc_count,
                                "pattern_matched": pattern
                            })
                            policy_coverage[policy.name]["covered_storage_gb"] += size_bytes / (1024**3)
                            policy_coverage[policy.name]["covered_documents"] += doc_count
                            policy_coverage[policy.name]["pattern_matches"] += 1
                            break
                    
                    if matched_policy:
                        break
                
                # Track uncovered indices
                if not matched_policy:
                    uncovered_indices.append({
                        "name": index_name,
                        "size_gb": round(size_bytes / (1024**3), 3),
                        "doc_count": doc_count
                    })
                    total_uncovered_storage += size_bytes
            
            # Calculate efficiency metrics
            total_storage = sum(stats["total"]["store"]["size_in_bytes"] for stats in indices_stats["indices"].values())
            covered_storage = total_storage - total_uncovered_storage
            coverage_percentage = (covered_storage / total_storage * 100) if total_storage > 0 else 0
            
            # Round storage values
            for policy_name in policy_coverage:
                policy_coverage[policy_name]["covered_storage_gb"] = round(policy_coverage[policy_name]["covered_storage_gb"], 3)
            
            # Generate recommendations
            recommendations = []
            if len(uncovered_indices) > 0:
                recommendations.append(f"Create policies for {len(uncovered_indices)} uncovered indices ({round(total_uncovered_storage / (1024**3), 3)} GB)")
            
            # Check for patterns that might need attention
            large_uncovered = [idx for idx in uncovered_indices if idx["size_gb"] > 1.0]
            if large_uncovered:
                recommendations.append(f"Priority: {len(large_uncovered)} large uncovered indices need immediate attention")
            
            # Check for policy gaps
            for policy_name, data in policy_coverage.items():
                if data["pattern_matches"] == 0:
                    recommendations.append(f"Policy '{policy_name}' has no matching indices - check patterns")
            
            return {
                "coverage_percentage": round(coverage_percentage, 2),
                "total_storage_gb": round(total_storage / (1024**3), 3),
                "covered_storage_gb": round(covered_storage / (1024**3), 3),
                "uncovered_storage_gb": round(total_uncovered_storage / (1024**3), 3),
                "policy_details": policy_coverage,
                "uncovered_indices": uncovered_indices[:10],  # Top 10 for UI
                "total_uncovered_count": len(uncovered_indices),
                "recommendations": recommendations,
                "efficiency_score": round(coverage_percentage, 1)
            }
            
        except Exception as e:
            logger.error(f"Failed to analyze policy efficiency: {e}")
            return {"coverage_percentage": 0, "policy_details": {}, "recommendations": [f"Analysis failed: {str(e)}"]}
    
        """Get breakdown of logs by service/container."""
        try:
            search_body = {
                "size": 0,
                "aggs": {
                    "services": {
                        "terms": {
                            "field": "container_name.keyword",
                            "size": 20,
                            "missing": "unknown"
                        },
                        "aggs": {
                            "doc_size": {
                                "sum": {
                                    "field": "_size"
                                }
                            }
                        }
                    }
                }
            }
            
            response = await self.client.search(
                index="axiom*",
                body=search_body
            )
            
            buckets = response["aggregations"]["services"]["buckets"]
            services = []
            total_docs = sum(bucket["doc_count"] for bucket in buckets)
            
            for bucket in buckets:
                service_name = bucket["key"]
                doc_count = bucket["doc_count"]
                percentage = (doc_count / total_docs * 100) if total_docs > 0 else 0
                
                services.append({
                    "service": service_name,
                    "document_count": doc_count,
                    "percentage": round(percentage, 2)
                })
            
            return {
                "total_documents": total_docs,
                "services": services
            }
        except Exception as e:
            logger.error(f"Failed to get service breakdown: {e}")
            return {"total_documents": 0, "services": []}
    
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
