#!/bin/bash
###############################################################################
# Docker System Backup Script for Homeside-Fetcher
# Backs up Docker images, volumes (InfluxDB), and configurations
# for full disaster recovery
###############################################################################

set -e  # Exit on error

# Configuration
BACKUP_BASE_DIR="$HOME/homeside_docker_backup"
NAS_IP="192.168.86.5"
NAS_SHARE="Backup"
NAS_CREDENTIALS="/home/ulf/.nas_credentials"
MOUNT_POINT="/mnt/nas_backup"
PROJECT_DIR="/opt/dev/homeside-fetcher"
PROJECT_NAME="homeside-fetcher"

# Read container prefix from .env (used for Docker container/image/volume names)
CONTAINER_PREFIX=$(grep -m1 '^CONTAINER_PREFIX=' "${PROJECT_DIR}/.env" 2>/dev/null | cut -d= -f2-)
CONTAINER_PREFIX="${CONTAINER_PREFIX:-SvenskEB}"
SAVE_TO_NAS=true  # Set to false to only create local backup

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Source shared backup configuration (single source of truth for what to backup)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/backup_include.conf"

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

    # Get list of custom images (matching container prefix)
    local custom_images=$(docker images --format "{{.Repository}}:{{.Tag}}" | grep -i "${CONTAINER_PREFIX}\|homeside-" || true)

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
    docker images --format "{{.Repository}}:{{.Tag}}" | grep -E "influxdb" > "$backup_dir/images/official_images.txt" || true

    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)

    echo -e "   ${GREEN}✅ Docker images backup complete in ${duration}s${NC}"
}

backup_docker_volumes() {
    local backup_dir="$1"
    print_header "💾 Backing up Docker volumes (InfluxDB)"

    mkdir -p "$backup_dir/volumes"

    local start_time=$(date +%s.%N)
    local total_size_mb=0
    local volume_count=0

    # Get list of project volumes
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
    if ! docker ps --format "{{.Names}}" | grep -q "${CONTAINER_PREFIX}-influxdb"; then
        echo -e "   ${YELLOW}⚠️  InfluxDB container not running, skipping native backup${NC}"
        return 0
    fi

    local start_time=$(date +%s.%N)

    echo -e "   ${GREEN}Running InfluxDB backup command...${NC}"

    # Read token from .env
    local influx_token
    influx_token=$(grep -m1 '^INFLUXDB_TOKEN=' "${SCRIPT_DIR}/../.env" | cut -d= -f2-)

    # Run influx backup inside the container
    docker exec "${CONTAINER_PREFIX}-influxdb" influx backup /tmp/influx_backup \
        --org homeside \
        --token "${influx_token}" 2>/dev/null || {
        echo -e "   ${YELLOW}⚠️  Native InfluxDB backup failed (will use volume backup instead)${NC}"
        return 0
    }

    # Copy backup out of container
    docker cp "${CONTAINER_PREFIX}-influxdb":/tmp/influx_backup/. "$backup_dir/influxdb_backup/"

    # Clean up backup inside container
    docker exec "${CONTAINER_PREFIX}-influxdb" rm -rf /tmp/influx_backup 2>/dev/null || true

    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    local size_mb=$(get_dir_size_mb "$backup_dir/influxdb_backup")

    echo -e "   ${GREEN}✅ InfluxDB backup complete: ${size_mb} MB in ${duration}s${NC}"
}

backup_configurations() {
    local backup_dir="$1"
    print_header "📋 Backing up configurations"

    # Use shared function from backup_include.conf
    backup_config_shared "$backup_dir" "$PROJECT_DIR"

    # Additional Docker-specific info
    mkdir -p "$backup_dir/config"

    # Save current container list
    docker ps -a --filter "name=${CONTAINER_PREFIX}" --format "{{.Names}}\t{{.Image}}\t{{.Status}}" > "$backup_dir/config/containers.txt"
    echo -e "   ${GREEN}✅ containers list${NC}"

    # Save current network configuration
    docker network ls --format "{{.Name}}\t{{.Driver}}" > "$backup_dir/config/networks.txt"
    echo -e "   ${GREEN}✅ networks list${NC}"
}

backup_codebase() {
    local backup_dir="$1"
    print_header "📦 Backing up codebase"

    # Use shared function from backup_include.conf
    backup_codebase_shared "$backup_dir" "$PROJECT_DIR"
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
- **volumes/** - Docker volumes (InfluxDB data)
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

- **homeside-orchestrator**: Main Python app fetching heating data
- **influxdb**: Time-series database (port 8086)

## Default Credentials (after restore)

- **InfluxDB**: Check .env for INFLUXDB_ADMIN_PASSWORD

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

    # Check if already mounted
    if mountpoint -q "$MOUNT_POINT"; then
        echo "   Already mounted"
        return 0
    fi

    # Mount using fstab entry (user mount, no sudo needed)
    mount "$MOUNT_POINT" 2>&1

    if [ $? -eq 0 ]; then
        echo -e "   ${GREEN}✅ Mounted successfully${NC}"
        return 0
    else
        echo -e "   ${RED}❌ Mount failed - check fstab entry for ${MOUNT_POINT}${NC}"
        return 1
    fi
}

unmount_nas() {
    if [ "$SAVE_TO_NAS" = false ]; then
        return 0
    fi

    echo -e "${GREEN}📂 Unmounting NAS share...${NC}"
    umount "$MOUNT_POINT" 2>/dev/null || true
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
    mkdir -p "$dest_dir"

    local dest_path="${dest_dir}/${archive_name}"

    local start_time=$(date +%s.%N)

    cp "$archive_path" "$dest_path"

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

    # Create final archive (in /tmp)
    local archive_info=$(create_final_archive "$backup_dir")
    archive_path=$(echo "$archive_info" | cut -d'|' -f1)
    local archive_name=$(echo "$archive_info" | cut -d'|' -f2)
    local archive_size_mb=$(get_file_size_mb "$archive_path")

    # Copy to NAS and verify
    local nas_verified=false
    if [ "$SAVE_TO_NAS" = true ]; then
        if mount_nas; then
            mounted=true
            copy_to_nas "$archive_path" "$archive_name"

            # Verify the backup exists on NAS with correct size
            if verify_nas_backup "$archive_path" "$archive_name"; then
                nas_verified=true
                # NAS verified - delete local archive from /tmp
                echo ""
                echo -e "${GREEN}🗑️  NAS backup verified - removing local archive...${NC}"
                rm -f "$archive_path"
                echo -e "   ${GREEN}✅ Local archive deleted${NC}"
                archive_path=""
            else
                echo -e "${YELLOW}⚠️  NAS verification failed!${NC}"
                echo -e "${YELLOW}   Local archive kept at: ${archive_path}${NC}"
                seq_log "Warning" "Docker backup: NAS verification failed, local archive kept" \
                    "{\"BackupType\": \"docker\", \"Archive\": \"${archive_name}\", \"SizeMB\": \"${archive_size_mb}\"}"
            fi
        else
            echo -e "${RED}❌ Failed to mount NAS${NC}"
            echo -e "${YELLOW}   Local archive kept at: ${archive_path}${NC}"
            seq_log "Error" "Docker backup FAILED: could not mount NAS" \
                "{\"BackupType\": \"docker\", \"Archive\": \"${archive_name}\", \"SizeMB\": \"${archive_size_mb}\"}"
        fi
    else
        echo -e "${YELLOW}⚠️  NAS backup disabled, archive at: ${archive_path}${NC}"
    fi

    # Success!
    local end_time=$(date +%s.%N)
    local total_duration=$(echo "$end_time - $start_time" | bc)

    echo ""
    echo "============================================================"
    if [ "$nas_verified" = true ]; then
        echo -e "${GREEN}✅ Docker backup completed successfully in ${total_duration}s${NC}"
        echo -e "   Archive: ${archive_name}"
        echo -e "   Size: ${archive_size_mb} MB"
        echo -e "   NAS: //${NAS_IP}/${NAS_SHARE}/${PROJECT_NAME}_docker_backups/${archive_name}"

        seq_log "Information" "Docker backup completed: ${archive_name} (${archive_size_mb} MB) in ${total_duration}s" \
            "{\"BackupType\": \"docker\", \"Archive\": \"${archive_name}\", \"SizeMB\": \"${archive_size_mb}\", \"DurationSeconds\": \"${total_duration}\"}"
    else
        echo -e "${YELLOW}⚠️  Docker backup created but NAS transfer failed${NC}"
        echo -e "   Archive: ${archive_path}"
        echo -e "   Size: ${archive_size_mb} MB"
    fi
    echo "============================================================"
}

# Run main function
main
