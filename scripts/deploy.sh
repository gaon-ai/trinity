#!/bin/bash
# Deploy DAGs and Plugins to Production Airflow VM
#
# Usage:
#   ./scripts/deploy.sh              # Deploy all
#   ./scripts/deploy.sh --dry-run    # Show what would be synced
#
# Prerequisites:
#   - SSH key at ~/.ssh/airflow_vm_key
#   - VM accessible via SSH

set -e

# Configuration
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
AIRFLOW_DIR="$PROJECT_ROOT/airflow"

# VM Configuration
VM_IP="${AIRFLOW_VM_IP:-20.186.91.34}"
VM_USER="${AIRFLOW_VM_USER:-azureuser}"
SSH_KEY="${SSH_KEY_PATH:-$HOME/.ssh/airflow_vm_key}"
REMOTE_AIRFLOW_PATH="/opt/airflow"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Check prerequisites
check_prerequisites() {
    log_info "Checking prerequisites..."

    if [ ! -f "$SSH_KEY" ]; then
        log_error "SSH key not found at $SSH_KEY"
        exit 1
    fi

    if [ ! -d "$AIRFLOW_DIR/dags" ]; then
        log_error "DAGs directory not found at $AIRFLOW_DIR/dags"
        exit 1
    fi

    if [ ! -d "$AIRFLOW_DIR/plugins" ]; then
        log_error "Plugins directory not found at $AIRFLOW_DIR/plugins"
        exit 1
    fi

    log_info "Prerequisites OK"
}

# Validate DAGs before deploying
validate_dags() {
    log_info "Validating DAGs..."

    if [ -f "$SCRIPT_DIR/validate_dags.py" ]; then
        if python3 "$SCRIPT_DIR/validate_dags.py"; then
            log_info "DAG validation passed"
        else
            log_error "DAG validation failed"
            exit 1
        fi
    else
        # Fallback to basic syntax check
        log_warn "validate_dags.py not found, using basic syntax check"
        find "$AIRFLOW_DIR/dags" -name "*.py" -type f | while read file; do
            python3 -m py_compile "$file" || exit 1
        done
        log_info "Basic syntax check passed"
    fi
}

# Deploy to VM
deploy() {
    local dry_run=""
    if [ "$1" == "--dry-run" ]; then
        dry_run="--dry-run"
        log_warn "DRY RUN MODE - No changes will be made"
    fi

    log_info "Deploying to $VM_USER@$VM_IP..."

    # Sync DAGs
    log_info "Syncing DAGs..."
    rsync -avz $dry_run --delete \
        -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=no" \
        "$AIRFLOW_DIR/dags/" \
        "$VM_USER@$VM_IP:/tmp/dags/"

    # Sync plugins
    log_info "Syncing plugins..."
    rsync -avz $dry_run --delete \
        -e "ssh -i $SSH_KEY -o StrictHostKeyChecking=no" \
        "$AIRFLOW_DIR/plugins/" \
        "$VM_USER@$VM_IP:/tmp/plugins/"

    if [ -z "$dry_run" ]; then
        # Move to correct location with sudo
        log_info "Moving files to $REMOTE_AIRFLOW_PATH..."
        ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$VM_USER@$VM_IP" \
            "sudo rsync -av --delete /tmp/dags/ $REMOTE_AIRFLOW_PATH/dags/ && \
             sudo rsync -av --delete /tmp/plugins/ $REMOTE_AIRFLOW_PATH/plugins/ && \
             sudo chown -R 50000:0 $REMOTE_AIRFLOW_PATH/dags/ $REMOTE_AIRFLOW_PATH/plugins/ && \
             sudo chmod -R 755 $REMOTE_AIRFLOW_PATH/dags/ $REMOTE_AIRFLOW_PATH/plugins/"

        # Trigger DAG re-parsing
        log_info "Triggering DAG re-serialization..."
        ssh -i "$SSH_KEY" -o StrictHostKeyChecking=no "$VM_USER@$VM_IP" \
            "cd $REMOTE_AIRFLOW_PATH && sudo docker compose exec -T airflow-scheduler airflow dags reserialize" || true

        log_info "Deployment complete!"
        echo ""
        echo "Access Airflow at: http://$VM_IP:8080"
        echo "Or via SSH tunnel: ssh -i $SSH_KEY -L 8080:localhost:8080 $VM_USER@$VM_IP"
    fi
}

# Main
main() {
    echo "========================================"
    echo "  Airflow Production Deployment"
    echo "========================================"
    echo ""

    check_prerequisites
    validate_dags
    deploy "$1"
}

main "$@"
