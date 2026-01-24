#!/bin/bash
###############################################################################
# Docker System Backup Script for Homeside-Fetcher
# Backs up Docker images, volumes (InfluxDB, Grafana), and configurations
# for full disaster recovery
###############################################################################

set -e  # Exit on error

# Configuration
BACKUP_BASE_DIR="/tmp/homeside_docker_backup"
NAS_IP="192.168.86.5"
NAS_SHARE="Backup"
NAS_USER="AutoBackup"
NAS_PASSWORD="vU2In!?=k45"
MOUNT_POINT="/mnt/nas_backup"
PROJECT_DIR="/opt/dev/homeside-fetcher"
PROJECT_NAME="homeside-fetcher"
SAVE_TO_NAS=true  # Set to false to only create local backup

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

###############################################################################
# Functions
###############################################################################

get_file_size_mb() {
    local file="$1"
    local size_bytes=$(stat -c%s "$file" 2>/dev/null || echo 0)
    echo "scale=2; $size_bytes / 1024 / 1024" | bc
}

get_dir_size_mb() {
    local dir="$1"
    local size_bytes=$(du -sb "$dir" 2>/dev/null | cut -f1)
    echo "scale=2; $size_bytes / 1024 / 1024" | bc
}

print_header() {
    echo ""
    echo "============================================================"
    echo -e "${BLUE}$1${NC}"
    echo "============================================================"
}

backup_docker_images() {
    local backup_dir="$1"
    print_header "📦 Backing up Docker images"

    mkdir -p "$backup_dir/images"

    local start_time=$(date +%s.%N)
    local total_size_mb=0
    local image_count=0

    # Get list of custom images (homeside-*)
    local custom_images=$(docker images --format "{{.Repository}}:{{.Tag}}" | grep "homeside-" || true)

    if [ -z "$custom_images" ]; then
        echo -e "   ${YELLOW}⚠️  No custom Docker images found${NC}"
    else
        echo "$custom_images" | while read -r image; do
            if [ -n "$image" ]; then
                local image_name=$(echo "$image" | tr ':/' '_')
                local image_file="$backup_dir/images/${image_name}.tar"

                echo -e "   ${GREEN}Exporting: $image${NC}"

                docker save -o "$image_file" "$image"

                local size_mb=$(get_file_size_mb "$image_file")
                echo -e "   ${GREEN}✅ Saved: ${image_name}.tar (${size_mb} MB)${NC}"

                image_count=$((image_count + 1))
            fi
        done
    fi

    # Save list of official images for reference
    docker images --format "{{.Repository}}:{{.Tag}}" | grep -E "influxdb|grafana" > "$backup_dir/images/official_images.txt" || true

    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)

    echo -e "   ${GREEN}✅ Docker images backup complete in ${duration}s${NC}"
}

backup_docker_volumes() {
    local backup_dir="$1"
    print_header "💾 Backing up Docker volumes (InfluxDB + Grafana)"

    mkdir -p "$backup_dir/volumes"

    local start_time=$(date +%s.%N)
    local total_size_mb=0
    local volume_count=0

    # Get list of homeside volumes
    local volumes=$(docker volume ls --format "{{.Name}}" | grep "homeside-fetcher" || true)

    if [ -z "$volumes" ]; then
        echo -e "   ${YELLOW}⚠️  No homeside-fetcher volumes found${NC}"
        return 0
    fi

    echo "$volumes" | while read -r volume; do
        if [ -n "$volume" ]; then
            echo -e "   ${GREEN}Backing up volume: $volume${NC}"

            local volume_file="$backup_dir/volumes/${volume}.tar.gz"

            # Use a temporary container to access the volume and create a tar archive
            docker run --rm \
                -v "$volume:/volume:ro" \
                -v "$backup_dir/volumes:/backup" \
                alpine \
                tar czf "/backup/${volume}.tar.gz" -C /volume .

            local size_mb=$(get_file_size_mb "$volume_file")
            echo -e "   ${GREEN}✅ Saved: ${volume}.tar.gz (${size_mb} MB)${NC}"

            volume_count=$((volume_count + 1))
        fi
    done

    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)

    echo -e "   ${GREEN}✅ Backed up Docker volumes in ${duration}s${NC}"
}

backup_influxdb_data() {
    local backup_dir="$1"
    print_header "📊 Creating InfluxDB backup (native format)"

    mkdir -p "$backup_dir/influxdb_backup"

    # Check if InfluxDB container is running
    if ! docker ps --format "{{.Names}}" | grep -q "homeside-influxdb"; then
        echo -e "   ${YELLOW}⚠️  InfluxDB container not running, skipping native backup${NC}"
        return 0
    fi

    local start_time=$(date +%s.%N)

    echo -e "   ${GREEN}Running InfluxDB backup command...${NC}"

    # Run influx backup inside the container
    docker exec homeside-influxdb influx backup /tmp/influx_backup \
        --org homeside \
        --token homeside_token_2026_secret 2>/dev/null || {
        echo -e "   ${YELLOW}⚠️  Native InfluxDB backup failed (will use volume backup instead)${NC}"
        return 0
    }

    # Copy backup out of container
    docker cp homeside-influxdb:/tmp/influx_backup/. "$backup_dir/influxdb_backup/"

    # Clean up backup inside container
    docker exec homeside-influxdb rm -rf /tmp/influx_backup 2>/dev/null || true

    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    local size_mb=$(get_dir_size_mb "$backup_dir/influxdb_backup")

    echo -e "   ${GREEN}✅ InfluxDB backup complete: ${size_mb} MB in ${duration}s${NC}"
}

backup_configurations() {
    local backup_dir="$1"
    print_header "📋 Backing up configurations"

    mkdir -p "$backup_dir/config"

    local file_count=0

    # Docker compose files
    if [ -f "${PROJECT_DIR}/docker-compose.yml" ]; then
        cp "${PROJECT_DIR}/docker-compose.yml" "$backup_dir/config/"
        echo -e "   ${GREEN}✅ docker-compose.yml${NC}"
        file_count=$((file_count + 1))
    fi

    # Environment file
    if [ -f "${PROJECT_DIR}/.env" ]; then
        cp "${PROJECT_DIR}/.env" "$backup_dir/config/"
        echo -e "   ${GREEN}✅ .env${NC}"
        file_count=$((file_count + 1))
    fi

    if [ -f "${PROJECT_DIR}/.env.example" ]; then
        cp "${PROJECT_DIR}/.env.example" "$backup_dir/config/"
        echo -e "   ${GREEN}✅ .env.example${NC}"
        file_count=$((file_count + 1))
    fi

    # Dockerfile
    if [ -f "${PROJECT_DIR}/Dockerfile" ]; then
        cp "${PROJECT_DIR}/Dockerfile" "$backup_dir/config/"
        echo -e "   ${GREEN}✅ Dockerfile${NC}"
        file_count=$((file_count + 1))
    fi

    # Requirements
    if [ -f "${PROJECT_DIR}/requirements.txt" ]; then
        cp "${PROJECT_DIR}/requirements.txt" "$backup_dir/config/"
        echo -e "   ${GREEN}✅ requirements.txt${NC}"
        file_count=$((file_count + 1))
    fi

    # Variables config
    if [ -f "${PROJECT_DIR}/variables_config.json" ]; then
        cp "${PROJECT_DIR}/variables_config.json" "$backup_dir/config/"
        echo -e "   ${GREEN}✅ variables_config.json${NC}"
        file_count=$((file_count + 1))
    fi

    # Save current container list
    docker ps -a --filter "name=homeside" --format "{{.Names}}\t{{.Image}}\t{{.Status}}" > "$backup_dir/config/containers.txt"
    echo -e "   ${GREEN}✅ containers list${NC}"
    file_count=$((file_count + 1))

    # Save current network configuration
    docker network ls --format "{{.Name}}\t{{.Driver}}" > "$backup_dir/config/networks.txt"
    echo -e "   ${GREEN}✅ networks list${NC}"
    file_count=$((file_count + 1))

    echo -e "   ${GREEN}✅ Backed up ${file_count} configuration file(s)${NC}"
}

backup_codebase() {
    local backup_dir="$1"
    print_header "📦 Backing up codebase"

    local code_dir="${backup_dir}/codebase"
    mkdir -p "$code_dir"

    local files_backed_up=0

    # Backup Python files
    for py_file in "${PROJECT_DIR}"/*.py; do
        if [ -f "$py_file" ]; then
            cp "$py_file" "$code_dir/"
            files_backed_up=$((files_backed_up + 1))
        fi
    done

    # Backup shell scripts
    for sh_file in "${PROJECT_DIR}"/*.sh; do
        if [ -f "$sh_file" ]; then
            cp "$sh_file" "$code_dir/"
            files_backed_up=$((files_backed_up + 1))
        fi
    done

    # Backup JSON files
    for json_file in "${PROJECT_DIR}"/*.json; do
        if [ -f "$json_file" ]; then
            cp "$json_file" "$code_dir/"
            files_backed_up=$((files_backed_up + 1))
        fi
    done

    # Remove __pycache__
    find "$code_dir" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find "$code_dir" -type f -name "*.pyc" -delete 2>/dev/null || true

    # Calculate total size
    local size_bytes=$(du -sb "$code_dir" 2>/dev/null | cut -f1)
    local total_size_mb=$(echo "scale=2; $size_bytes / 1024 / 1024" | bc)

    echo -e "   ${GREEN}✅ Backed up ${files_backed_up} files: ${total_size_mb} MB${NC}"
}

create_restore_script() {
    local backup_dir="$1"
    print_header "📝 Creating restore script"

    cat > "$backup_dir/RESTORE.sh" << 'RESTORE_EOF'
#!/bin/bash
###############################################################################
# Docker System Restore Script for Homeside-Fetcher
# Restores Docker images, volumes, and configurations from backup
###############################################################################

set -e

echo "============================================================"
echo "🔄 Homeside-Fetcher Docker System Restore"
echo "============================================================"
echo ""
echo "This will restore the Homeside-Fetcher Docker system from backup."
echo "Make sure Docker is installed and running on this system."
echo ""
read -p "Continue? (yes/no): " -r
if [[ ! $REPLY =~ ^[Yy]es$ ]]; then
    echo "Restore cancelled."
    exit 0
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo ""
echo "📦 Restoring Docker images..."
for image_file in "$SCRIPT_DIR"/images/*.tar; do
    if [ -f "$image_file" ]; then
        echo "   Loading: $(basename $image_file)"
        docker load -i "$image_file"
    fi
done

echo ""
echo "📥 Pulling official images..."
if [ -f "$SCRIPT_DIR/images/official_images.txt" ]; then
    while read -r image; do
        if [ -n "$image" ]; then
            echo "   Pulling: $image"
            docker pull "$image" || echo "   ⚠️  Failed to pull $image"
        fi
    done < "$SCRIPT_DIR/images/official_images.txt"
fi

echo ""
echo "💾 Restoring Docker volumes..."
for volume_file in "$SCRIPT_DIR"/volumes/*.tar.gz; do
    if [ -f "$volume_file" ]; then
        volume_name=$(basename "$volume_file" .tar.gz)
        echo "   Creating volume: $volume_name"
        docker volume create "$volume_name" 2>/dev/null || true

        echo "   Restoring data..."
        docker run --rm \
            -v "$volume_name:/volume" \
            -v "$SCRIPT_DIR/volumes:/backup" \
            alpine \
            sh -c "cd /volume && tar xzf /backup/${volume_name}.tar.gz"
    fi
done

echo ""
echo "📊 InfluxDB native backup available in: $SCRIPT_DIR/influxdb_backup/"
echo "   To restore: influx restore /path/to/backup --org homeside"

echo ""
echo "📋 Configuration files restored to: $SCRIPT_DIR/config/"
echo "📦 Codebase restored to: $SCRIPT_DIR/codebase/"
echo "   Review and copy these files to their proper locations:"
echo "   - docker-compose.yml"
echo "   - .env"
echo "   - All Python files"

echo ""
echo "🚀 Next steps:"
echo "   1. Copy all files to /opt/dev/homeside-fetcher/"
echo "   2. Navigate to /opt/dev/homeside-fetcher/"
echo "   3. Run: docker-compose up -d"
echo "   4. Verify containers: docker ps"
echo "   5. Check InfluxDB: http://localhost:8086"
echo "   6. Check Grafana: http://localhost:3000"

echo ""
echo "============================================================"
echo "✅ Restore preparation complete!"
echo "============================================================"
RESTORE_EOF

    chmod +x "$backup_dir/RESTORE.sh"
    echo -e "   ${GREEN}✅ Restore script created: RESTORE.sh${NC}"
}

create_readme() {
    local backup_dir="$1"

    cat > "$backup_dir/README.md" << README_EOF
# Homeside-Fetcher Docker System Backup

This backup contains everything needed to restore the Homeside-Fetcher system on a new server.

## Backup Contents

- **images/** - Custom Docker images (homeside-fetcher)
- **volumes/** - Docker volumes (InfluxDB data, Grafana config)
- **influxdb_backup/** - Native InfluxDB backup (for point-in-time restore)
- **config/** - Configuration files (docker-compose.yml, .env, etc.)
- **codebase/** - Python source files
- **RESTORE.sh** - Automated restore script

## Backup Date
Date: $(date +"%Y-%m-%d %H:%M:%S")
Server: $(hostname)

## Prerequisites for Restore

1. **Install Docker**:
   \`\`\`bash
   curl -fsSL https://get.docker.com -o get-docker.sh
   sudo sh get-docker.sh
   sudo usermod -aG docker \$USER
   \`\`\`

2. **Install Docker Compose**:
   \`\`\`bash
   sudo apt-get update
   sudo apt-get install docker-compose-plugin
   \`\`\`

## Quick Restore

1. Extract this backup archive
2. Run the restore script:
   \`\`\`bash
   cd /path/to/extracted/backup
   ./RESTORE.sh
   \`\`\`
3. Follow the on-screen instructions

## Services

- **homeside-fetcher**: Main Python app fetching heating data
- **influxdb**: Time-series database (port 8086)
- **grafana**: Visualization dashboards (port 3000)

## Default Credentials (after restore)

- **InfluxDB**: admin / homeside_admin_2026
- **Grafana**: admin / homeside_grafana_2026

## Important Notes

- This backup includes sensitive data (credentials, time-series data)
- Store securely and encrypt if transmitting over network
- Test restore procedure on non-production system first
- InfluxDB data can be large - ensure sufficient disk space

---
Backup created by: backup_docker_system.sh
README_EOF

    echo -e "   ${GREEN}✅ README created: README.md${NC}"
}

mount_nas() {
    if [ "$SAVE_TO_NAS" = false ]; then
        return 0
    fi

    echo -e "${GREEN}📂 Mounting NAS share //${NAS_IP}/${NAS_SHARE}...${NC}"

    sudo mkdir -p "$MOUNT_POINT"

    # Check if already mounted
    if mountpoint -q "$MOUNT_POINT"; then
        echo "   Already mounted"
        return 0
    fi

    # Mount using cifs
    sudo mount -t cifs "//${NAS_IP}/${NAS_SHARE}" "$MOUNT_POINT" \
        -o "username=${NAS_USER},password=${NAS_PASSWORD},vers=3.0" 2>&1

    if [ $? -eq 0 ]; then
        echo -e "   ${GREEN}✅ Mounted successfully${NC}"
        return 0
    else
        echo -e "   ${RED}❌ Mount failed${NC}"
        return 1
    fi
}

unmount_nas() {
    if [ "$SAVE_TO_NAS" = false ]; then
        return 0
    fi

    echo -e "${GREEN}📂 Unmounting NAS share...${NC}"
    sudo umount "$MOUNT_POINT" 2>/dev/null || true
    echo -e "   ${GREEN}✅ Unmounted${NC}"
}

create_final_archive() {
    local backup_dir="$1"
    print_header "📦 Creating final archive" >&2

    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local archive_name="${PROJECT_NAME}_docker_backup_${timestamp}.tar.gz"
    local archive_path="/tmp/${archive_name}"

    local start_time=$(date +%s.%N)

    echo -e "   ${GREEN}Creating compressed archive...${NC}" >&2
    tar -czf "$archive_path" -C "$(dirname $backup_dir)" "$(basename $backup_dir)"

    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    local size_mb=$(get_file_size_mb "$archive_path")

    echo -e "   ${GREEN}✅ Archive created: ${archive_name} (${size_mb} MB) in ${duration}s${NC}" >&2

    # Return via stdout only
    echo "$archive_path|$archive_name"
}

copy_to_nas() {
    if [ "$SAVE_TO_NAS" = false ]; then
        return 0
    fi

    local archive_path="$1"
    local archive_name="$2"
    print_header "📤 Copying to NAS"

    local dest_dir="${MOUNT_POINT}/${PROJECT_NAME}_docker_backups"
    sudo mkdir -p "$dest_dir"

    local dest_path="${dest_dir}/${archive_name}"

    local start_time=$(date +%s.%N)

    sudo cp "$archive_path" "$dest_path"

    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    local size_mb=$(get_file_size_mb "$dest_path")
    local speed_mbps=$(echo "scale=2; $size_mb / $duration" | bc)

    echo -e "   ${GREEN}✅ Copied to NAS: ${size_mb} MB in ${duration}s (${speed_mbps} MB/s)${NC}"
}

verify_nas_backup() {
    local archive_path="$1"
    local archive_name="$2"
    print_header "🔍 Verifying NAS backup"

    local dest_dir="${MOUNT_POINT}/${PROJECT_NAME}_docker_backups"
    local dest_path="${dest_dir}/${archive_name}"

    # Check if file exists on NAS
    if [ ! -f "$dest_path" ]; then
        echo -e "   ${RED}❌ File not found on NAS: ${dest_path}${NC}"
        return 1
    fi

    # Compare file sizes
    local local_size=$(stat -c%s "$archive_path" 2>/dev/null || echo 0)
    local nas_size=$(stat -c%s "$dest_path" 2>/dev/null || echo 0)

    if [ "$local_size" -ne "$nas_size" ]; then
        echo -e "   ${RED}❌ Size mismatch! Local: ${local_size} bytes, NAS: ${nas_size} bytes${NC}"
        return 1
    fi

    local size_mb=$(get_file_size_mb "$dest_path")
    echo -e "   ${GREEN}✅ Verified: ${archive_name} (${size_mb} MB) exists on NAS with matching size${NC}"

    return 0
}

cleanup_local_backups_after_nas_verified() {
    local archive_name="$1"
    local local_backup_dir="$2"
    print_header "🗑️  Cleaning up local backups (NAS verified)"

    # Delete all local Docker backups since NAS copy is verified
    local removed_count=0
    local removed_size_mb=0

    for local_backup in "${local_backup_dir}"/${PROJECT_NAME}_docker_backup_*.tar.gz; do
        if [ -f "$local_backup" ]; then
            local size_mb=$(get_file_size_mb "$local_backup")
            rm -f "$local_backup"
            removed_count=$((removed_count + 1))
            removed_size_mb=$(echo "$removed_size_mb + $size_mb" | bc)
            echo -e "   ${YELLOW}🗑️  Removed: $(basename $local_backup) (${size_mb} MB)${NC}"
        fi
    done

    if [ "$removed_count" -gt 0 ]; then
        echo -e "   ${GREEN}✅ Removed ${removed_count} local backup(s), freed ${removed_size_mb} MB${NC}"
    else
        echo -e "   ${GREEN}✅ No local backups to remove${NC}"
    fi
}

###############################################################################
# Main execution
###############################################################################

main() {
    local start_time=$(date +%s.%N)

    echo "============================================================"
    echo "🐳 HOMESIDE-FETCHER DOCKER BACKUP - $(date '+%Y-%m-%d %H:%M:%S')"
    echo "============================================================"

    # Create backup directory
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local backup_dir="${BACKUP_BASE_DIR}/${timestamp}"
    mkdir -p "$backup_dir"

    local mounted=false
    local archive_path=""

    # Trap to ensure cleanup
    trap 'unmount_nas; rm -rf "$backup_dir" "$archive_path" 2>/dev/null' EXIT

    # Perform backups
    backup_docker_images "$backup_dir"
    backup_docker_volumes "$backup_dir"
    backup_influxdb_data "$backup_dir"
    backup_configurations "$backup_dir"
    backup_codebase "$backup_dir"
    create_restore_script "$backup_dir"
    create_readme "$backup_dir"

    # Create final archive
    local archive_info=$(create_final_archive "$backup_dir")
    archive_path=$(echo "$archive_info" | cut -d'|' -f1)
    local archive_name=$(echo "$archive_info" | cut -d'|' -f2)
    local archive_size_mb=$(get_file_size_mb "$archive_path")

    # Copy to local backup location
    local local_backup_dir="/opt/docker/${PROJECT_NAME}/backups"
    mkdir -p "$local_backup_dir"
    cp "$archive_path" "$local_backup_dir/"
    echo -e "${GREEN}✅ Local copy saved: ${local_backup_dir}/${archive_name}${NC}"

    # Optionally copy to NAS and verify
    local nas_verified=false
    if [ "$SAVE_TO_NAS" = true ]; then
        if mount_nas; then
            mounted=true
            copy_to_nas "$archive_path" "$archive_name"

            # Verify the backup exists on NAS with correct size
            if verify_nas_backup "$archive_path" "$archive_name"; then
                nas_verified=true
            else
                echo -e "${YELLOW}⚠️  NAS verification failed, keeping local backups${NC}"
            fi
        else
            echo -e "${YELLOW}⚠️  Failed to mount NAS, backup saved locally only${NC}"
        fi
    fi

    # Clean up local backups based on NAS verification
    if [ "$nas_verified" = true ]; then
        # NAS copy verified - delete ALL local backups
        cleanup_local_backups_after_nas_verified "$archive_name" "$local_backup_dir"
    else
        # NAS not verified - keep latest 2 local backups for safety
        echo ""
        echo -e "${GREEN}🗑️  Cleaning up old local Docker backups (keeping latest 2 for safety)...${NC}"
        local backup_count=$(ls -1 "${local_backup_dir}"/${PROJECT_NAME}_docker_backup_*.tar.gz 2>/dev/null | wc -l)

        if [ "$backup_count" -gt 2 ]; then
            ls -1t "${local_backup_dir}"/${PROJECT_NAME}_docker_backup_*.tar.gz | tail -n +3 | while read -r old_backup; do
                local size_mb=$(get_file_size_mb "$old_backup")
                rm -f "$old_backup"
                echo -e "   ${YELLOW}🗑️  Removed: $(basename $old_backup) (${size_mb} MB)${NC}"
            done
            echo -e "   ${GREEN}✅ Cleaned up old local backups${NC}"
        else
            echo -e "   ${GREEN}✅ No old backups to remove (keeping latest 2)${NC}"
        fi
    fi

    # Delete temporary archive from /tmp
    if [ -f "$archive_path" ]; then
        echo ""
        echo -e "${GREEN}🗑️  Removing temporary archive from /tmp...${NC}"
        rm -f "$archive_path"
        echo -e "   ${GREEN}✅ Temporary archive deleted${NC}"
        archive_path=""
    fi

    # Success!
    local end_time=$(date +%s.%N)
    local total_duration=$(echo "$end_time - $start_time" | bc)

    echo ""
    echo "============================================================"
    echo -e "${GREEN}✅ Docker backup completed successfully in ${total_duration}s${NC}"
    echo -e "   Archive: ${archive_name}"
    echo -e "   Size: ${archive_size_mb} MB"
    echo -e "   Local: ${local_backup_dir}/${archive_name}"
    if [ "$SAVE_TO_NAS" = true ] && [ "$mounted" = true ]; then
        echo -e "   NAS: //${NAS_IP}/${NAS_SHARE}/${PROJECT_NAME}_docker_backups/${archive_name}"
    fi
    echo "============================================================"
}

# Run main function
main
