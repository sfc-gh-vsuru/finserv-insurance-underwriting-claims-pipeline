#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TF_DIR="$SCRIPT_DIR/terraform"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

check_prerequisites() {
    log_info "Checking prerequisites..."

    if ! command -v terraform &> /dev/null; then
        log_error "terraform not found. Install: brew install terraform"
        exit 1
    fi

    if ! command -v aws &> /dev/null; then
        log_error "aws CLI not found. Install: brew install awscli"
        exit 1
    fi

    aws sts get-caller-identity > /dev/null 2>&1 || {
        log_error "AWS credentials not configured. Run: aws configure"
        exit 1
    }

    log_info "Prerequisites OK"
    aws sts get-caller-identity --query 'Account' --output text | xargs -I{} echo -e "${GREEN}[INFO]${NC} AWS Account: {}"
}

create_tfvars() {
    if [ ! -f "$TF_DIR/terraform.tfvars" ]; then
        log_warn "terraform.tfvars not found. Creating from example..."
        cp "$TF_DIR/terraform.tfvars.example" "$TF_DIR/terraform.tfvars"
        log_warn "EDIT $TF_DIR/terraform.tfvars with your passwords before proceeding!"
        log_warn "  - mysql_root_password"
        log_warn "  - mysql_cdc_password"
        echo ""
        read -p "Press Enter after editing terraform.tfvars (or Ctrl+C to abort)..."
    fi
}

deploy() {
    log_info "=== Deploying FinServ Insurance Infrastructure ==="

    check_prerequisites
    create_tfvars

    cd "$TF_DIR"

    log_info "Initializing Terraform..."
    terraform init

    log_info "Planning deployment..."
    terraform plan -out=tfplan

    echo ""
    read -p "Apply this plan? (yes/no): " CONFIRM
    if [ "$CONFIRM" != "yes" ]; then
        log_warn "Aborted."
        exit 0
    fi

    log_info "Applying Terraform..."
    terraform apply tfplan

    echo ""
    log_info "=== Deployment Complete ==="
    echo ""
    terraform output -json | python3 -c "
import json, sys
outputs = json.load(sys.stdin)
print('=' * 60)
print('  DEPLOYMENT OUTPUTS')
print('=' * 60)
for k, v in outputs.items():
    val = v.get('value', '')
    if isinstance(val, str) and len(val) > 100:
        print(f'\n  {k}:')
        for line in val.strip().split('\n'):
            print(f'    {line}')
    else:
        print(f'  {k}: {val}')
print('=' * 60)
" 2>/dev/null || terraform output

    rm -f tfplan
}

destroy() {
    log_info "=== Destroying FinServ Insurance Infrastructure ==="

    check_prerequisites
    cd "$TF_DIR"

    echo ""
    log_warn "This will DESTROY all resources (EC2, S3 bucket, etc.)"
    read -p "Are you sure? Type 'destroy' to confirm: " CONFIRM
    if [ "$CONFIRM" != "destroy" ]; then
        log_warn "Aborted."
        exit 0
    fi

    terraform destroy -auto-approve
    log_info "All resources destroyed."
}

status() {
    cd "$TF_DIR"
    if [ -f "terraform.tfstate" ]; then
        log_info "Current infrastructure state:"
        terraform output 2>/dev/null || log_warn "No outputs yet. Run deploy first."

        INSTANCE_ID=$(terraform output -raw ec2_instance_id 2>/dev/null || echo "")
        if [ -n "$INSTANCE_ID" ]; then
            echo ""
            log_info "EC2 Instance Status:"
            aws ec2 describe-instances \
                --instance-ids "$INSTANCE_ID" \
                --query 'Reservations[0].Instances[0].{State:State.Name,PublicIP:PublicIpAddress,LaunchTime:LaunchTime}' \
                --output table 2>/dev/null || log_warn "Could not query instance status"
        fi
    else
        log_warn "No Terraform state found. Run './deploy.sh deploy' first."
    fi
}

validate_mysql() {
    cd "$TF_DIR"
    INSTANCE_ID=$(terraform output -raw ec2_instance_id 2>/dev/null || echo "")
    REGION=$(terraform output -raw 2>/dev/null | grep -oP 'us-\w+-\d' | head -1 || echo "us-west-2")

    if [ -z "$INSTANCE_ID" ]; then
        log_error "No instance found. Deploy first."
        exit 1
    fi

    log_info "Validating MySQL via SSM on instance $INSTANCE_ID..."

    aws ssm send-command \
        --instance-ids "$INSTANCE_ID" \
        --document-name "AWS-RunShellScript" \
        --parameters 'commands=["mysql -u root -p$(cat /root/.mysql_root_pw 2>/dev/null || echo UNKNOWN) -e \"SHOW VARIABLES LIKE '\''gtid_mode'\''; SHOW DATABASES; USE insurance_db; SHOW TABLES;\""]' \
        --region "$REGION" \
        --output text 2>/dev/null || {
        log_warn "SSM command sent. Check AWS Console for output."
    }

    log_info "Or connect interactively:"
    echo "  aws ssm start-session --target $INSTANCE_ID --region $REGION"
}

usage() {
    echo "Usage: $0 {deploy|destroy|status|validate}"
    echo ""
    echo "Commands:"
    echo "  deploy    - Deploy EC2 (MySQL) + S3 infrastructure"
    echo "  destroy   - Tear down all infrastructure"
    echo "  status    - Show current deployment status"
    echo "  validate  - Verify MySQL is running with CDC enabled"
    echo ""
}

case "${1:-}" in
    deploy)   deploy ;;
    destroy)  destroy ;;
    status)   status ;;
    validate) validate_mysql ;;
    *)        usage ;;
esac
