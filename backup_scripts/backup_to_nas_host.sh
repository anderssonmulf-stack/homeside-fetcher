#!/bin/bash
###############################################################################
# Automated Backup to Synology NAS (Host-based)
# Backs up Homeside Fetcher codebase and configuration files to NAS via SMB
###############################################################################

set -e  # Exit on error

# Configuration
NAS_IP="192.168.86.5"
NAS_SHARE="Backup"
NAS_USER="AutoBackup"
NAS_PASSWORD="vU2In!?=k45"
BACKUP_BASE_DIR="$HOME/homeside_backup"
MOUNT_POINT="/mnt/nas_backup"
RETENTION_DAYS=30
PROJECT_DIR="/opt/dev/homeside-fetcher"
PROJECT_NAME="homeside-fetcher"

# Colors for output
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

###############################################################################
# Functions
###############################################################################

get_file_size_mb() {
    local file="$1"
    local size_bytes=$(stat -c%s "$file" 2>/dev/null || echo 0)
    echo "scale=2; $size_bytes / 1024 / 1024" | bc
}

ensure_mount_point() {
    sudo mkdir -p "$MOUNT_POINT"
}

mount_nas() {
    echo -e "${GREEN}📂 Mounting NAS share //${NAS_IP}/${NAS_SHARE}...${NC}"

    ensure_mount_point

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
    echo -e "${GREEN}📂 Unmounting NAS share...${NC}"
    sudo umount "$MOUNT_POINT" 2>/dev/null || true
    echo -e "   ${GREEN}✅ Unmounted${NC}"
}

backup_config_files() {
    local backup_dir="$1"
    echo -e "${GREEN}📋 Backing up configuration files...${NC}"

    local config_dir="${backup_dir}/config"
    mkdir -p "$config_dir"

    local files_backed_up=0

    # Copy configuration files
    if [ -f "${PROJECT_DIR}/docker-compose.yml" ]; then
        cp "${PROJECT_DIR}/docker-compose.yml" "$config_dir/"
        echo -e "   ${GREEN}✅ docker-compose.yml${NC}"
        files_backed_up=$((files_backed_up + 1))
    fi

    if [ -f "${PROJECT_DIR}/.env" ]; then
        cp "${PROJECT_DIR}/.env" "$config_dir/"
        echo -e "   ${GREEN}✅ .env${NC}"
        files_backed_up=$((files_backed_up + 1))
    fi

    if [ -f "${PROJECT_DIR}/.env.example" ]; then
        cp "${PROJECT_DIR}/.env.example" "$config_dir/"
        echo -e "   ${GREEN}✅ .env.example${NC}"
        files_backed_up=$((files_backed_up + 1))
    fi

    if [ -f "${PROJECT_DIR}/Dockerfile" ]; then
        cp "${PROJECT_DIR}/Dockerfile" "$config_dir/"
        echo -e "   ${GREEN}✅ Dockerfile${NC}"
        files_backed_up=$((files_backed_up + 1))
    fi

    if [ -f "${PROJECT_DIR}/requirements.txt" ]; then
        cp "${PROJECT_DIR}/requirements.txt" "$config_dir/"
        echo -e "   ${GREEN}✅ requirements.txt${NC}"
        files_backed_up=$((files_backed_up + 1))
    fi

    if [ -f "${PROJECT_DIR}/variables_config.json" ]; then
        cp "${PROJECT_DIR}/variables_config.json" "$config_dir/"
        echo -e "   ${GREEN}✅ variables_config.json${NC}"
        files_backed_up=$((files_backed_up + 1))
    fi

    if [ -f "${PROJECT_DIR}/README.md" ]; then
        cp "${PROJECT_DIR}/README.md" "$config_dir/"
        echo -e "   ${GREEN}✅ README.md${NC}"
        files_backed_up=$((files_backed_up + 1))
    fi

    # Backup scripts
    if [ -d "${PROJECT_DIR}/backup_scripts" ]; then
        mkdir -p "$config_dir/backup_scripts"
        cp -r "${PROJECT_DIR}/backup_scripts/"* "$config_dir/backup_scripts/"
        echo -e "   ${GREEN}✅ backup_scripts/${NC}"
        files_backed_up=$((files_backed_up + 1))
    fi

    echo -e "   ${GREEN}✅ Backed up ${files_backed_up} config files${NC}"
    return 0
}

backup_codebase() {
    local backup_dir="$1"
    echo -e "${GREEN}📦 Backing up codebase...${NC}"

    local code_dir="${backup_dir}/codebase"
    mkdir -p "$code_dir"

    local files_backed_up=0

    # Backup Python files in root
    for py_file in "${PROJECT_DIR}"/*.py; do
        if [ -f "$py_file" ]; then
            cp "$py_file" "$code_dir/"
            echo -e "   ${GREEN}✅ $(basename $py_file)${NC}"
            files_backed_up=$((files_backed_up + 1))
        fi
    done

    # Backup shell scripts in root
    for sh_file in "${PROJECT_DIR}"/*.sh; do
        if [ -f "$sh_file" ]; then
            cp "$sh_file" "$code_dir/"
            echo -e "   ${GREEN}✅ $(basename $sh_file)${NC}"
            files_backed_up=$((files_backed_up + 1))
        fi
    done

    # Backup energy_models package
    if [ -d "${PROJECT_DIR}/energy_models" ]; then
        mkdir -p "$code_dir/energy_models"
        cp -r "${PROJECT_DIR}/energy_models/"*.py "$code_dir/energy_models/" 2>/dev/null || true
        echo -e "   ${GREEN}✅ energy_models/${NC}"
        files_backed_up=$((files_backed_up + 1))
    fi

    # Backup webgui (excluding venv)
    if [ -d "${PROJECT_DIR}/webgui" ]; then
        mkdir -p "$code_dir/webgui"
        cp "${PROJECT_DIR}/webgui/"*.py "$code_dir/webgui/" 2>/dev/null || true
        cp "${PROJECT_DIR}/webgui/"*.txt "$code_dir/webgui/" 2>/dev/null || true
        cp "${PROJECT_DIR}/webgui/"*.service "$code_dir/webgui/" 2>/dev/null || true
        cp -r "${PROJECT_DIR}/webgui/templates" "$code_dir/webgui/" 2>/dev/null || true
        cp -r "${PROJECT_DIR}/webgui/static" "$code_dir/webgui/" 2>/dev/null || true
        echo -e "   ${GREEN}✅ webgui/${NC}"
        files_backed_up=$((files_backed_up + 1))
    fi

    # Backup profiles
    if [ -d "${PROJECT_DIR}/profiles" ]; then
        mkdir -p "$code_dir/profiles"
        cp "${PROJECT_DIR}/profiles/"*.json "$code_dir/profiles/" 2>/dev/null || true
        echo -e "   ${GREEN}✅ profiles/${NC}"
        files_backed_up=$((files_backed_up + 1))
    fi

    # Backup grafana dashboards
    if [ -d "${PROJECT_DIR}/grafana" ]; then
        cp -r "${PROJECT_DIR}/grafana" "$code_dir/" 2>/dev/null || true
        echo -e "   ${GREEN}✅ grafana/${NC}"
        files_backed_up=$((files_backed_up + 1))
    fi

    # Backup nginx configs
    if [ -d "${PROJECT_DIR}/nginx" ]; then
        cp -r "${PROJECT_DIR}/nginx" "$code_dir/" 2>/dev/null || true
        echo -e "   ${GREEN}✅ nginx/${NC}"
        files_backed_up=$((files_backed_up + 1))
    fi

    # Backup docs
    if [ -d "${PROJECT_DIR}/docs" ]; then
        cp -r "${PROJECT_DIR}/docs" "$code_dir/" 2>/dev/null || true
        echo -e "   ${GREEN}✅ docs/${NC}"
        files_backed_up=$((files_backed_up + 1))
    fi

    # Remove __pycache__ if any
    find "$code_dir" -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
    find "$code_dir" -type f -name "*.pyc" -delete 2>/dev/null || true

    # Calculate total size
    local size_bytes=$(du -sb "$code_dir" 2>/dev/null | cut -f1)
    local total_size_mb=$(echo "scale=2; $size_bytes / 1024 / 1024" | bc)

    echo -e "   ${GREEN}✅ Backed up ${files_backed_up} items: ${total_size_mb} MB${NC}"
    return 0
}

create_archive() {
    local backup_dir="$1"
    echo -e "${GREEN}📦 Creating compressed archive...${NC}" >&2

    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local archive_name="${PROJECT_NAME}_backup_${timestamp}.tar.gz"
    local archive_path="/tmp/${archive_name}"

    local start_time=$(date +%s.%N)

    tar -czf "$archive_path" -C "$(dirname $backup_dir)" "$(basename $backup_dir)"

    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    local size_mb=$(get_file_size_mb "$archive_path")

    echo -e "   ${GREEN}✅ Archive created: ${size_mb} MB in ${duration}s${NC}" >&2

    # Return via stdout
    echo "$archive_path|$archive_name"
}

copy_to_nas() {
    local archive_path="$1"
    local archive_name="$2"
    echo -e "${GREEN}📤 Copying to NAS...${NC}"

    local dest_dir="${MOUNT_POINT}/${PROJECT_NAME}"
    sudo mkdir -p "$dest_dir"

    local dest_path="${dest_dir}/${archive_name}"

    local start_time=$(date +%s.%N)

    sudo cp "$archive_path" "$dest_path"

    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc)
    local size_mb=$(get_file_size_mb "$dest_path")
    local speed_mbps=$(echo "scale=2; $size_mb / $duration" | bc)

    echo -e "   ${GREEN}✅ Copied to NAS: ${size_mb} MB in ${duration}s (${speed_mbps} MB/s)${NC}"

    return 0
}

cleanup_old_backups() {
    echo -e "${GREEN}🧹 Cleaning up backups older than ${RETENTION_DAYS} days...${NC}"

    local backup_dir="${MOUNT_POINT}/${PROJECT_NAME}"

    if [ ! -d "$backup_dir" ]; then
        echo "   No backup directory on NAS"
        return 0
    fi

    local removed_count=0

    # Find and remove old backups
    while IFS= read -r file; do
        local size_mb=$(get_file_size_mb "$file")
        sudo rm -f "$file"
        removed_count=$((removed_count + 1))
        echo -e "   ${YELLOW}🗑️  Removed: $(basename $file) (${size_mb} MB)${NC}"
    done < <(find "$backup_dir" -name "${PROJECT_NAME}_backup_*.tar.gz" -type f -mtime +${RETENTION_DAYS})

    if [ $removed_count -gt 0 ]; then
        echo -e "   ${GREEN}✅ Removed ${removed_count} old backup(s)${NC}"
    else
        echo -e "   ${GREEN}✅ No old backups to remove${NC}"
    fi
}

cleanup_temp_files() {
    local backup_dir="$1"
    local archive_path="$2"
    echo -e "${GREEN}🧹 Cleaning up temporary files...${NC}"

    rm -rf "$backup_dir" 2>/dev/null || true

    if [ -f "$archive_path" ]; then
        rm -f "$archive_path" 2>/dev/null || true
    fi

    echo -e "   ${GREEN}✅ Temporary files cleaned up${NC}"
}

###############################################################################
# Main execution
###############################################################################

main() {
    local start_time=$(date +%s.%N)

    echo "============================================================"
    echo "💾 HOMESIDE-FETCHER BACKUP TO NAS - $(date '+%Y-%m-%d %H:%M:%S')"
    echo "============================================================"

    # Create backup directory
    local timestamp=$(date +"%Y%m%d_%H%M%S")
    local backup_dir="${BACKUP_BASE_DIR}/${timestamp}"
    mkdir -p "$backup_dir"

    local archive_path=""

    # Trap to ensure cleanup
    trap 'unmount_nas; cleanup_temp_files "$backup_dir" "$archive_path"' EXIT

    # Backup config files
    backup_config_files "$backup_dir"

    # Backup codebase
    backup_codebase "$backup_dir"

    # Create archive
    local archive_info=$(create_archive "$backup_dir")
    archive_path=$(echo "$archive_info" | cut -d'|' -f1)
    local archive_name=$(echo "$archive_info" | cut -d'|' -f2)

    # Mount NAS
    if ! mount_nas; then
        echo -e "${RED}❌ Backup FAILED: Failed to mount NAS${NC}"
        exit 1
    fi

    # Copy to NAS
    if ! copy_to_nas "$archive_path" "$archive_name"; then
        echo -e "${RED}❌ Backup FAILED: Failed to copy to NAS${NC}"
        exit 1
    fi

    # Cleanup old backups on NAS
    cleanup_old_backups

    # Delete local archive after successful NAS transfer
    if [ -f "$archive_path" ]; then
        echo ""
        echo -e "${GREEN}🗑️  Removing local archive (successfully transferred to NAS)...${NC}"
        rm -f "$archive_path"
        echo -e "   ${GREEN}✅ Local archive deleted${NC}"
    fi

    # Success!
    local end_time=$(date +%s.%N)
    local total_duration=$(echo "$end_time - $start_time" | bc)
    local archive_size_mb=$(get_file_size_mb "${MOUNT_POINT}/${PROJECT_NAME}/$archive_name" 2>/dev/null || echo "0")

    echo ""
    echo "============================================================"
    echo -e "${GREEN}✅ Backup completed successfully in ${total_duration}s${NC}"
    echo -e "   Archive: ${archive_name}"
    echo -e "   Size: ${archive_size_mb} MB"
    echo -e "   Location: //${NAS_IP}/${NAS_SHARE}/${PROJECT_NAME}/"
    echo "============================================================"
}

# Run main function
main
