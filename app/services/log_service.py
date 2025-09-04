# app/services/log_service.py
"""
Log retrieval service for querying Elasticsearch.
Because sometimes you need to see what the fuck went wrong without SSH'ing into Kibana.
"""
import json
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Optional, Any

from elasticsearch import Elasticsearch, exceptions as es_exceptions
from pydantic import BaseModel
from app.core.config import settings
import structlog

logger = structlog.get_logger(__name__)


class LogEntry(BaseModel):
    """A single log entry from Elasticsearch."""
    timestamp: datetime
    level: str
    message: str
    service: Optional[str] = None
    container_name: Optional[str] = None
    source: Optional[str] = None
    extra_fields: Dict[str, Any] = {}


class LogQueryParams(BaseModel):
    """Parameters for log queries."""
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    level: Optional[str] = None
    service: Optional[str] = None
    container_name: Optional[str] = None
    search_text: Optional[str] = None
    limit: int = 100
    offset: int = 0


class ElasticsearchLogService:
    """
    Service for querying logs from Elasticsearch.
    
    This assumes your ELK stack is accessible and follows common conventions.
    If your setup is weird, you get to fix it.
    """
    
    def __init__(self):
        # These settings should be configurable, but for POC we'll hardcode some defaults
        self.es_host = getattr(settings, 'ELASTICSEARCH_HOST', 'elasticsearch')
        self.es_port = getattr(settings, 'ELASTICSEARCH_PORT', 9200)
        self.es_scheme = getattr(settings, 'ELASTICSEARCH_SCHEME', 'http')
        self.index_pattern = getattr(settings, 'ELASTICSEARCH_LOG_INDEX_PATTERN', 'fluentd-*')
        
        self.client = None
        self._initialize_client()
    
    def _initialize_client(self):
        """Initialize Elasticsearch client."""
        try:
            # Build connection parameters
            host_url = f"{self.es_scheme}://{self.es_host}:{self.es_port}"
            
            # Prepare connection args
            client_args = {
                "hosts": [host_url],
                "timeout": getattr(settings, 'ELASTICSEARCH_TIMEOUT_SECONDS', 10),
                "max_retries": getattr(settings, 'ELASTICSEARCH_MAX_RETRIES', 3),
                "retry_on_timeout": True
            }
            
            # Add authentication if provided
            username = getattr(settings, 'ELASTICSEARCH_USERNAME', None)
            password = getattr(settings, 'ELASTICSEARCH_PASSWORD', None)
            if username and password:
                if hasattr(password, 'get_secret_value'):
                    password = password.get_secret_value()
                client_args["basic_auth"] = (username, password)
            
            # Handle TLS/SSL settings
            verify_certs = False  # Initialize variable
            if self.es_scheme == "https":
                verify_certs = getattr(settings, 'ELASTICSEARCH_VERIFY_CERTS', False)
                ca_cert_path = getattr(settings, 'ELASTICSEARCH_CA_CERT_PATH', None)
                
                client_args["verify_certs"] = verify_certs
                
                if verify_certs and ca_cert_path:
                    # Check if CA certificate file exists
                    import os
                    if os.path.exists(ca_cert_path):
                        client_args["ca_certs"] = ca_cert_path
                        logger.info("Using CA certificate for Elasticsearch TLS verification",
                                   ca_cert_path=ca_cert_path)
                    else:
                        logger.warning("CA certificate file not found, disabling cert verification",
                                     ca_cert_path=ca_cert_path)
                        client_args["verify_certs"] = False
                        verify_certs = False
                
                if not verify_certs:
                    # Disable SSL warnings when not verifying certs
                    import urllib3
                    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
                    logger.warning("Elasticsearch TLS certificate verification disabled - insecure connection")
                else:
                    logger.info("Elasticsearch TLS certificate verification enabled - secure connection")
            
            self.client = Elasticsearch(**client_args)
            
            # Test connection
            if not self.client.ping():
                logger.warning("Could not ping Elasticsearch", 
                             host=self.es_host, port=self.es_port, scheme=self.es_scheme)
            else:
                logger.info("Successfully connected to Elasticsearch",
                           host=self.es_host, port=self.es_port, scheme=self.es_scheme,
                           tls_verified=verify_certs if self.es_scheme == "https" else False)
                
        except Exception as e:
            logger.error("Failed to initialize Elasticsearch client", 
                        error=str(e),
                        host=self.es_host, 
                        port=self.es_port, 
                        scheme=self.es_scheme)
            self.client = None
    
    def health_check(self) -> Dict[str, Any]:
        """Check if Elasticsearch is accessible."""
        if not self.client:
            return {"status": "error", "message": "Elasticsearch client not initialized"}
        
        try:
            cluster_health = self.client.cluster.health()
            
            # Get TLS verification status
            verify_certs = getattr(settings, 'ELASTICSEARCH_VERIFY_CERTS', False)
            tls_info = {
                "scheme": self.es_scheme,
                "tls_enabled": self.es_scheme == "https",
                "certificate_verification": verify_certs if self.es_scheme == "https" else None
            }
            
            return {
                "status": "ok",
                "cluster_status": cluster_health.get("status", "unknown"),
                "number_of_nodes": cluster_health.get("number_of_nodes", 0),
                "tls_info": tls_info
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}
    
    def query_logs(self, params: LogQueryParams) -> Dict[str, Any]:
        """
        Query logs from Elasticsearch based on parameters.
        
        Returns both the log entries and metadata about the query.
        """
        if not self.client:
            return {
                "entries": [],
                "total": 0,
                "error": "Elasticsearch client not available"
            }
        
        try:
            # Build the query
            query = self._build_query(params)
            
            # Execute search
            response = self.client.search(
                index=self.index_pattern,
                body=query,
                size=params.limit,
                from_=params.offset,
                sort=[{"@timestamp": {"order": "desc"}}]
            )
            
            # Parse results
            entries = []
            for hit in response['hits']['hits']:
                entry = self._parse_log_entry(hit['_source'])
                if entry:
                    entries.append(entry)
            
            return {
                "entries": [entry.dict() for entry in entries],
                "total": response['hits']['total']['value'],
                "took_ms": response['took'],
                "query": query  # For debugging
            }
            
        except es_exceptions.NotFoundError:
            return {
                "entries": [],
                "total": 0,
                "error": f"Index pattern '{self.index_pattern}' not found"
            }
        except Exception as e:
            logger.error("Error querying logs", error=str(e), params=params.dict())
            return {
                "entries": [],
                "total": 0,
                "error": str(e)
            }
    
    def _build_query(self, params: LogQueryParams) -> Dict[str, Any]:
        """Build Elasticsearch query from parameters."""
        must_clauses = []
        
        # Time range filter
        if params.start_time or params.end_time:
            time_range = {}
            if params.start_time:
                time_range["gte"] = params.start_time.isoformat()
            if params.end_time:
                time_range["lte"] = params.end_time.isoformat()
            
            must_clauses.append({
                "range": {
                    "@timestamp": time_range
                }
            })
        
        # Log level filter - handle both 'level' and 'level.keyword' fields
        if params.level:
            level_filter = {
                "bool": {
                    "should": [
                        {"term": {"level.keyword": params.level.upper()}},
                        {"term": {"level": params.level.upper()}}
                    ],
                    "minimum_should_match": 1
                }
            }
            must_clauses.append(level_filter)
        
        # Service filter - search in both service and logger fields
        if params.service:
            service_filter = {
                "bool": {
                    "should": [
                        {"term": {"service_name.keyword": params.service}},
                        {"term": {"logger.keyword": params.service}},
                        {"wildcard": {"logger.keyword": f"*{params.service}*"}}
                    ],
                    "minimum_should_match": 1
                }
            }
            must_clauses.append(service_filter)
        
        # Container name filter
        if params.container_name:
            container_filter = {
                "bool": {
                    "should": [
                        {"term": {"container_name.keyword": params.container_name}},
                        {"wildcard": {"container_name.keyword": f"*{params.container_name}*"}}
                    ],
                    "minimum_should_match": 1
                }
            }
            must_clauses.append(container_filter)
        
        # Text search - search across multiple fields
        if params.search_text:
            must_clauses.append({
                "multi_match": {
                    "query": params.search_text,
                    "fields": ["message", "log", "event", "logger"],
                    "type": "best_fields",
                    "operator": "and"
                }
            })
        
        # If no filters, default to recent logs
        if not must_clauses:
            must_clauses.append({
                "range": {
                    "@timestamp": {
                        "gte": (datetime.now(timezone.utc) - timedelta(hours=1)).isoformat()
                    }
                }
            })
        
        return {
            "query": {
                "bool": {
                    "must": must_clauses
                }
            }
        }
    
    def _parse_log_entry(self, source: Dict[str, Any]) -> Optional[LogEntry]:
        """Parse a raw Elasticsearch document into a LogEntry."""
        try:
            # Try to extract timestamp
            timestamp_str = source.get('@timestamp')
            if not timestamp_str:
                return None
            
            # Parse timestamp
            if isinstance(timestamp_str, str):
                timestamp = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
            else:
                timestamp = datetime.now(timezone.utc)  # Use timezone-aware datetime
            
            # Extract common fields
            level = source.get('level', 'INFO').upper()
            
            # For Axiom logs, the actual message is in the 'event' field
            message = (source.get('event') or 
                      source.get('message') or 
                      source.get('log', ''))
            
            # Try to get service name from logger field or container name
            service = (source.get('service_name') or 
                      source.get('logger') or
                      source.get('container_name', '').replace('/axiom-', '').replace('/', ''))
            
            container_name = source.get('container_name', '').replace('/', '')
            
            # Everything else goes into extra_fields
            extra_fields = {}
            for key, value in source.items():
                if key not in ['@timestamp', 'level', 'message', 'log', 'event', 'service_name', 'container_name', 'logger']:
                    extra_fields[key] = value
            
            return LogEntry(
                timestamp=timestamp,
                level=level,
                message=message,
                service=service,
                container_name=container_name,
                extra_fields=extra_fields
            )
            
        except Exception as e:
            logger.warning("Failed to parse log entry", error=str(e), source=source)
            return None


# Global instance
log_service = ElasticsearchLogService()
