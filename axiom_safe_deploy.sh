#!/bin/bash
# Axiom Flow Database Backup & Restore Scripts
# Created during the full rename from dicom_processor to axiom

set -e  # Exit on any error

BACKUP_FILE="axiom_db_backup_before_rename_$(date +%Y%m%d_%H%M%S).sql"
LATEST_BACKUP=$(ls -t axiom_db_backup_*.sql 2>/dev/null | head -1)

echo "🔄 Axiom Flow Database Backup & Restore Utility"
echo "================================================"

function create_backup() {
    echo "📦 Creating database backup..."
    
    # Try new container name first, fallback to old
    if docker exec axiom-db pg_dump -U axiom_user -d axiom_db --clean --if-exists > "$BACKUP_FILE" 2>/dev/null; then
        echo "✅ Backup created using new names: $BACKUP_FILE"
    elif docker exec dicom_processor_db pg_dump -U dicom_processor_user -d dicom_processor_db --clean --if-exists > "$BACKUP_FILE" 2>/dev/null; then
        echo "✅ Backup created using old names: $BACKUP_FILE"
    else
        echo "❌ Failed to create backup with both old and new container names"
        exit 1
    fi
    
    ls -lh "$BACKUP_FILE"
}

function restore_backup() {
    local backup_file=${1:-$LATEST_BACKUP}
    
    if [[ ! -f "$backup_file" ]]; then
        echo "❌ Backup file not found: $backup_file"
        exit 1
    fi
    
    echo "📥 Restoring database from: $backup_file"
    echo "⚠️  This will OVERWRITE the current database!"
    read -p "Are you sure? (y/N): " -n 1 -r
    echo
    
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        # Try new container name first, fallback to old
        if docker exec -i axiom-db psql -U axiom_user -d axiom_db < "$backup_file" 2>/dev/null; then
            echo "✅ Database restored using new names"
        elif docker exec -i dicom_processor_db psql -U dicom_processor_user -d dicom_processor_db < "$backup_file" 2>/dev/null; then
            echo "✅ Database restored using old names"
        else
            echo "❌ Failed to restore with both old and new container names"
            exit 1
        fi
    else
        echo "❌ Restore cancelled"
        exit 0
    fi
}

function safe_rename_deployment() {
    echo "🚀 Safe Axiom Flow Rename Deployment"
    echo "===================================="
    
    # Step 1: Create backup
    create_backup
    
    # Step 2: Bring down services (but keep volumes)
    echo "⏹️  Stopping services (keeping volumes)..."
    ./axiomctl down
    
    # Step 3: Rebuild with new names
    echo "🔨 Building containers with new names..."
    ./axiomctl up -d --build
    
    # Step 4: Wait for database to be ready
    echo "⏳ Waiting for database to be ready..."
    sleep 10
    
    # Step 5: Restore data to new database
    echo "📥 Restoring data to new database structure..."
    # We need to modify the backup to use new user/db names
    local modified_backup="modified_${LATEST_BACKUP}"
    sed 's/dicom_processor_user/axiom_user/g; s/dicom_processor_db/axiom_db/g' "$LATEST_BACKUP" > "$modified_backup"
    
    if docker exec -i axiom-db psql -U axiom_user -d axiom_db < "$modified_backup"; then
        echo "✅ Data successfully migrated to new database structure!"
        rm "$modified_backup"  # Clean up temporary file
    else
        echo "❌ Failed to restore data to new database"
        echo "🔄 Rolling back..."
        ./axiomctl down
        # Restore old config files and restart
        git checkout HEAD -- core.yml core-bootstrap.yml app/core/config.py inject_admin.py
        ./axiomctl up -d
        exit 1
    fi
    
    # Step 6: Verify services
    echo "🔍 Verifying services..."
    sleep 5
    
    if docker ps | grep -q "axiom-"; then
        echo "✅ New container names are working!"
    else
        echo "❌ New containers not found"
        exit 1
    fi
    
    # Step 7: Test API connectivity
    echo "🌐 Testing API connectivity..."
    if curl -f -s http://localhost:8001/api/v1/health > /dev/null; then
        echo "✅ API is responding!"
    else
        echo "⚠️  API not responding yet (may need more time to start)"
    fi
    
    echo "🎉 AXIOM FLOW RENAME DEPLOYMENT COMPLETE!"
    echo "✅ Database backed up and migrated"
    echo "✅ Containers renamed to axiom-*"
    echo "✅ Services running with new names"
    echo ""
    echo "📋 Next steps:"
    echo "1. Test your API endpoints"
    echo "2. Verify DICOM processing"
    echo "3. Check logs: curl -H 'Authorization: Api-Key XXX' 'https://axiom.trazen.org/api/v1/logs/recent'"
}

# Command line interface
case "${1:-help}" in
    "backup")
        create_backup
        ;;
    "restore")
        restore_backup "$2"
        ;;
    "safe-deploy")
        safe_rename_deployment
        ;;
    "help"|*)
        echo "Usage: $0 [backup|restore|safe-deploy]"
        echo ""
        echo "Commands:"
        echo "  backup          - Create a database backup"
        echo "  restore [file]  - Restore from backup (uses latest if no file specified)"
        echo "  safe-deploy     - Perform complete safe rename deployment"
        echo ""
        echo "Available backups:"
        ls -1t axiom_db_backup_*.sql 2>/dev/null | head -5 || echo "  No backups found"
        ;;
esac
