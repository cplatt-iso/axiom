# Documentation Index - Enterprise DICOM Query Spanning

Welcome to the documentation for the Enterprise DICOM Query Spanning system built for Axiom DICOM Processor.

## 📚 Available Documentation

### Quick References
- **[Quick Start Guide](QUICK_START.md)** - Get up and running in 5 minutes
- **[API Reference](API_REFERENCE.md)** - Complete API documentation with examples

### Comprehensive Guides  
- **[Enterprise Spanning Guide](ENTERPRISE_SPANNING_GUIDE.md)** - Complete system documentation
- **[Docker Architecture](DOCKER_ARCHITECTURE.md)** - Detailed Docker Compose architecture

## 🚀 Getting Started

**New to the system?** Start here:
1. Read the [Quick Start Guide](QUICK_START.md)
2. Follow the 5-minute setup process
3. Test your first spanning query
4. Refer to [API Reference](API_REFERENCE.md) for integration

**Need comprehensive information?** Check the [Enterprise Spanning Guide](ENTERPRISE_SPANNING_GUIDE.md).

## 📖 Documentation Structure

```
docs/
├── README.md                    # This index file
├── QUICK_START.md              # 5-minute setup guide
├── API_REFERENCE.md            # Complete API documentation
├── ENTERPRISE_SPANNING_GUIDE.md # Comprehensive system guide
└── DOCKER_ARCHITECTURE.md     # Docker infrastructure details
```

## 🎯 Quick Access

### System Health Checks
```bash
# Check all services
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml ps

# Test APIs
curl http://localhost:8001/health  # Original API
curl http://localhost:8002/health  # Enterprise Coordinator
```

### Common Tasks
```bash
# Deploy enterprise services
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml up -d --build

# Scale workers
docker compose -f docker-compose.yml -f docker-compose.enterprise.yml up -d --scale dimse-query-worker=5

# View logs
docker logs -f axiom-spanner-coordinator
```

### API Endpoints
- **Original API**: http://localhost:8001 (Spanner configuration management)
- **Enterprise Coordinator**: http://localhost:8002 (Spanning query execution)

## 🔧 System Overview

### What's Been Built
Your system now includes:

✅ **Enterprise Query Spanning**: Query across multiple PACS simultaneously  
✅ **Scalable Architecture**: Microservices with horizontal scaling  
✅ **Fault Tolerance**: Continues working even if some PACS fail  
✅ **Real-time Monitoring**: Status tracking and comprehensive logging  
✅ **Non-disruptive Deployment**: Added as overlay to existing system  

### Key Components

| Component | Port | Purpose |
|-----------|------|---------|
| **Original API** | 8001 | DICOM processing + Spanner management |
| **Enterprise Coordinator** | 8002 | Spanning query orchestration |
| **DIMSE Listeners** | 11112-11114 | DICOM protocol endpoints |
| **MLLP Listener** | 2575 | HL7 message processing |

## 📊 Enterprise Scale Ready

The system is designed to handle:
- **150 locations** across your network
- **1200 modalities** spanning multiple facilities  
- **8 different PACS systems** with varying protocols
- **High availability** with distributed workers and coordinators

## 🔍 Finding Information

### By Task
- **Setup**: [Quick Start Guide](QUICK_START.md)
- **Integration**: [API Reference](API_REFERENCE.md)
- **Scaling**: [Enterprise Spanning Guide](ENTERPRISE_SPANNING_GUIDE.md) → Scaling Section
- **Troubleshooting**: [Enterprise Spanning Guide](ENTERPRISE_SPANNING_GUIDE.md) → Troubleshooting Section
- **Architecture**: [Docker Architecture](DOCKER_ARCHITECTURE.md)

### By Role
- **System Administrator**: [Docker Architecture](DOCKER_ARCHITECTURE.md) + [Enterprise Spanning Guide](ENTERPRISE_SPANNING_GUIDE.md)
- **Developer**: [API Reference](API_REFERENCE.md) + [Quick Start Guide](QUICK_START.md)
- **DevOps Engineer**: [Docker Architecture](DOCKER_ARCHITECTURE.md) + Monitoring sections
- **End User**: [Quick Start Guide](QUICK_START.md) + API examples

## 🆘 Support Resources

### Troubleshooting Steps
1. Check service health: `docker compose ps`
2. Review logs: `docker logs axiom-spanner-coordinator`
3. Test connectivity: `curl http://localhost:8002/health`
4. Consult [Troubleshooting Guide](ENTERPRISE_SPANNING_GUIDE.md#troubleshooting)

### Log Analysis
```bash
# Service logs
docker logs -f axiom-spanner-coordinator
docker logs -f backend-dimse-query-worker-1

# System status
docker stats
docker exec axiom-rabbitmq rabbitmqctl list_queues
```

### Configuration Access
```bash
# List spanner configurations
curl http://localhost:8001/spanner/configs

# List PACS sources  
curl http://localhost:8001/spanner/sources

# Check query status
curl http://localhost:8002/spanning-query/{query_id}
```

## 📈 Next Steps

### Immediate Actions
1. **Configure Your PACS**: Add your 8 PACS systems via the API
2. **Test Spanning Queries**: Verify connectivity to all systems
3. **Set Up Monitoring**: Configure alerts in your ELK stack

### Production Readiness
1. **Security**: Configure authentication and SSL certificates
2. **Scaling**: Add more workers based on load testing
3. **Backup**: Include spanner configurations in backup procedures
4. **Documentation**: Customize for your specific PACS environments

---

**🎉 Your enterprise DICOM query spanning system is ready!**

Start with the [Quick Start Guide](QUICK_START.md) to begin using your new capabilities.
