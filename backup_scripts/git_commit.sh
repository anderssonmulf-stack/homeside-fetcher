#!/bin/bash
###############################################################################
# Git Commit Script for Homeside-Fetcher
# Uses backup_include.conf as single source of truth for what to track
###############################################################################

set -e

# Configuration
PROJECT_DIR="/opt/dev/homeside-fetcher"
PROJECT_NAME="homeside-fetcher"

# Colors
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# Source shared configuration (single source of truth)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
source "${SCRIPT_DIR}/backup_include.conf"

cd "$PROJECT_DIR"

###############################################################################
# Functions
###############################################################################

show_usage() {
    echo "Usage: $0 [options] [commit message]"
    echo ""
    echo "Options:"
    echo "  -h, --help     Show this help"
    echo "  -n, --dry-run  Show what would be committed without committing"
    echo "  -p, --push     Push to remote after commit"
    echo ""
    echo "Examples:"
    echo "  $0 'Fix bug in heating controller'"
    echo "  $0 -p 'Add new feature'"
    echo "  $0 -n  # Dry run to see changes"
}

stage_files() {
    echo -e "${BLUE}📋 Staging files from backup_include.conf...${NC}"

    local staged_count=0

    # Stage root-level files matching patterns
    for pattern in "${ROOT_FILE_PATTERNS[@]}"; do
        for file in ${PROJECT_DIR}/${pattern}; do
            if [ -f "$file" ]; then
                local filename=$(basename "$file")
                # Skip .env (contains secrets)
                if [[ "$filename" == ".env" ]]; then
                    continue
                fi
                git add "$file" 2>/dev/null && staged_count=$((staged_count + 1)) || true
            fi
        done
    done

    # Stage directories
    for dir in "${BACKUP_DIRS[@]}"; do
        if [ -d "${PROJECT_DIR}/${dir}" ]; then
            # Add directory contents, respecting .gitignore
            git add "${PROJECT_DIR}/${dir}" 2>/dev/null || true
            echo -e "   ${GREEN}✅ ${dir}/${NC}"
        fi
    done

    echo -e "   ${GREEN}✅ Staged files from ${#ROOT_FILE_PATTERNS[@]} patterns and ${#BACKUP_DIRS[@]} directories${NC}"
}

show_status() {
    echo ""
    echo -e "${BLUE}📊 Git Status:${NC}"
    git status --short
}

###############################################################################
# Main
###############################################################################

DRY_RUN=false
PUSH=false
COMMIT_MSG=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -h|--help)
            show_usage
            exit 0
            ;;
        -n|--dry-run)
            DRY_RUN=true
            shift
            ;;
        -p|--push)
            PUSH=true
            shift
            ;;
        *)
            COMMIT_MSG="$1"
            shift
            ;;
    esac
done

echo "============================================================"
echo -e "${BLUE}🔄 HOMESIDE-FETCHER GIT COMMIT${NC}"
echo "============================================================"

# Stage files based on backup_include.conf
stage_files

# Show status
show_status

# Check if there are changes to commit
if git diff --cached --quiet; then
    echo ""
    echo -e "${YELLOW}⚠️  No changes to commit${NC}"
    exit 0
fi

# Dry run - just show what would be committed
if [ "$DRY_RUN" = true ]; then
    echo ""
    echo -e "${YELLOW}🔍 DRY RUN - Would commit these changes:${NC}"
    git diff --cached --stat
    exit 0
fi

# Need a commit message
if [ -z "$COMMIT_MSG" ]; then
    echo ""
    echo -e "${RED}❌ Error: Commit message required${NC}"
    echo "Usage: $0 'Your commit message'"
    exit 1
fi

# Commit
echo ""
echo -e "${GREEN}📝 Committing...${NC}"
git commit -m "$COMMIT_MSG

Co-Authored-By: Claude Opus 4.5 <noreply@anthropic.com>"

# Push if requested
if [ "$PUSH" = true ]; then
    echo ""
    echo -e "${GREEN}📤 Pushing to remote...${NC}"
    git push
fi

echo ""
echo "============================================================"
echo -e "${GREEN}✅ Done!${NC}"
echo "============================================================"
