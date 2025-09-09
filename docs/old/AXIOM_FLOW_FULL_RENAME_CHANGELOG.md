# 🚀 AXIOM FLOW COMPLETE RENAMING - OPTION 2 NUCLEAR CHANGELOG

## Overview
Complete system rename from "dicom_processor" to "Axiom Flow" branding. This is a **BREAKING CHANGE** that requires full system rebuild.

## ⚡ BREAKING CHANGES

### Database Names (**REQUIRES DB RECREATION**)
- `dicom_processor_user` → `axiom_user`
- `dicom_processor_db` → `axiom_db`
- All existing database connections **WILL BREAK**

### Container Names (UPDATED ✅)
- `dicom_processor_api` → `axiom-api`
- `dicom_processor_worker` → `axiom-worker` 
- `dicom_processor_beat` → `axiom-beat`
- `dicom_processor_db` → `axiom-db`
- `dicom_processor_rabbitmq` → `axiom-rabbitmq`
- `dicom_processor_redis` → `axiom-redis`
- `dicom_processor_elasticsearch` → `axiom-elasticsearch`
- `dicom_processor_fluent_bit` → `axiom-fluent-bit`
- `dicom_processor_storescp_1` → `axiom-storescp-1`
- `dicom_processor_storescp_2` → `axiom-storescp-2`
- `dicom_processor_dcm4che_sender` → `axiom-dcm4che-sender`
- `dicom_processor_pynetdicom_sender` → `axiom-pynetdicom-sender`
- `dicom_processor_dustbin_verification` → `axiom-dustbin-verification`
- `dicom_processor_docs` → `axiom-docs`

### AE Titles (UPDATED ✅)
- `DICOM_PROCESSOR` → `AXIOM_FLOW`
- SCP listeners now use `AXIOM_SCP` branding

## 📁 FILES UPDATED

### Core Configuration
- ✅ `core.yml` - Database health checks and container names
- ✅ `core-bootstrap.yml` - Bootstrap container names and DB users
- ✅ `docker/app.yml` - Application containers (already updated)
- ✅ `docker/logging.yml` - Logging infrastructure (already updated)
- ✅ `docker/docs.yml` - Documentation container (already updated)

### Application Code
- ✅ `app/core/config.py` - Database connection defaults
- ✅ `.env` - Environment variables (already updated)
- ✅ `inject_admin.py` - Admin script database URI

### Container Definitions
- ✅ `docker/listeners/storescp_1.yml` - Already updated
- ✅ `docker/listeners/storescp_2.yml` - Already updated
- ✅ `docker/senders/dcm4che_sender.yml` - Already updated
- ✅ `docker/workers/dustbin_verification_worker.yml` - Already updated

### Documentation
- ✅ `docs/QUICK_START.md` - Updated docker exec commands
- ✅ `docs/ENTERPRISE_SPANNING_GUIDE.md` - Updated container references
- ✅ `docs/README.md` - Updated container references

### Service Naming
- ✅ `app/core/service_naming.py` - Standardized service names for logging

## 🔥 DEPLOYMENT REQUIREMENTS

### PRE-DEPLOYMENT (CRITICAL!)
```bash
# 1. STOP ALL SERVICES
./axiomctl down

# 2. BACKUP DATABASE (IF NEEDED)
docker exec axiom-db pg_dump -U axiom_user axiom_db > backup_before_rename.sql

# 3. REMOVE OLD VOLUMES (NUCLEAR OPTION!)
./axiomctl down -v  # This DESTROYS all data!
```

### DEPLOYMENT
```bash
# 1. BUILD NEW CONTAINERS
./axiomctl up -d --build

# 2. VERIFY CONTAINERS
docker ps | grep axiom-

# 3. CHECK DATABASE CONNECTION
docker exec axiom-db pg_isready -U axiom_user -d axiom_db

# 4. CHECK LOGS
curl -H "Authorization: Api-Key XXX" "https://axiom.trazen.org/api/v1/logs/recent?limit=5"
```

### POST-DEPLOYMENT VERIFICATION
```bash
# Check service discovery
curl -H "Authorization: Api-Key XXX" "https://axiom.trazen.org/api/v1/logs/services" | jq .

# Check queue status  
docker exec axiom-rabbitmq rabbitmqctl list_queues

# Check Redis status
docker exec axiom-redis redis-cli ping
```

## ⚠️ KNOWN IMPACTS

### What WILL Break
- **Database connections** until `.env` updated
- **Existing docker exec scripts** in documentation
- **Hardcoded container references** in external scripts
- **Database dumps** with old user references

### What Will Continue Working
- **Volume data** (unless `-v` flag used)
- **Port mappings** (unchanged)
- **Service discovery within containers** (uses service names)
- **DICOM processing logic** (unchanged)

## 🎯 SUCCESS METRICS

After deployment, verify:
- [ ] All `axiom-*` containers running
- [ ] Database accessible with `axiom_user`
- [ ] API responding on port 8001
- [ ] Logs endpoint returns data
- [ ] Services endpoint shows proper service names
- [ ] DICOM listeners accepting connections
- [ ] RabbitMQ queues processing

## 🚨 ROLLBACK PLAN

If things go wrong:
1. `./axiomctl down`
2. Revert all files in git: `git checkout HEAD -- .`
3. `./axiomctl up -d --build`
4. Restore database from backup if needed

---

**READY FOR NUCLEAR OPTION DEPLOYMENT! 🚀💥**
