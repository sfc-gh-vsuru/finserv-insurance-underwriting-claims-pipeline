import os
import subprocess
import sys
import time
import json

INSTANCE_ID = os.getenv("EC2_INSTANCE_ID", "i-XXXXXXXXXXXX")
REGION = os.getenv("AWS_REGION", "us-west-2")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "")
DATABASE = "insurance_db"
S3_BUCKET = os.getenv("S3_BUCKET", "finserv-insurance-demo-docs-XXXXXXXXXXXX")
S3_PREFIX = "mock_data"

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")

LOAD_ORDER = [
    "underwriters",
    "adjusters",
    "customers",
    "policies",
    "underwriting_decisions",
    "claims",
    "claim_payments",
    "risk_factors",
]


def run_local(cmd):
    return subprocess.run(cmd, capture_output=True, text=True)


def run_ssm(commands, timeout_secs=180):
    cmd = [
        "aws", "ssm", "send-command",
        "--instance-ids", INSTANCE_ID,
        "--document-name", "AWS-RunShellScript",
        "--parameters", json.dumps({"commands": commands, "executionTimeout": [str(timeout_secs)]}),
        "--region", REGION,
        "--output", "json",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"  ERROR sending: {result.stderr.strip()}")
        return None

    resp = json.loads(result.stdout)
    command_id = resp["Command"]["CommandId"]

    max_checks = timeout_secs // 3 + 10
    for _ in range(max_checks):
        time.sleep(3)
        check = subprocess.run([
            "aws", "ssm", "get-command-invocation",
            "--command-id", command_id,
            "--instance-id", INSTANCE_ID,
            "--region", REGION,
            "--output", "json",
        ], capture_output=True, text=True)
        if check.returncode != 0:
            continue
        inv = json.loads(check.stdout)
        status = inv.get("Status", "")
        if status in ("Success", "Failed", "TimedOut", "Cancelled"):
            return {
                "status": status,
                "stdout": inv.get("StandardOutputContent", ""),
                "stderr": inv.get("StandardErrorContent", ""),
            }
    return {"status": "Timeout", "stdout": "", "stderr": "Timed out"}


def upload_csvs_to_s3():
    print("Uploading CSV files to S3...")
    for table in LOAD_ORDER:
        csv_file = os.path.join(OUTPUT_DIR, f"{table}.csv")
        if not os.path.exists(csv_file):
            print(f"  SKIP: {table}.csv not found")
            continue
        s3_key = f"s3://{S3_BUCKET}/{S3_PREFIX}/{table}.csv"
        result = run_local(["aws", "s3", "cp", csv_file, s3_key, "--region", REGION])
        if result.returncode == 0:
            size = os.path.getsize(csv_file)
            print(f"  {table}.csv ({size:,} bytes) → S3")
        else:
            print(f"  ERROR: {result.stderr.strip()}")
            return False
    return True


def create_load_script_on_ec2():
    tables_str = " ".join(LOAD_ORDER)
    script = f"""#!/bin/bash
set -e
BUCKET="{S3_BUCKET}"
PREFIX="{S3_PREFIX}"
DB="{DATABASE}"
USER="{MYSQL_USER}"
PASS="{MYSQL_PASS}"
REGION="{REGION}"
TABLES="{tables_str}"

echo "=== Downloading CSVs from S3 ==="
for T in $TABLES; do
    aws s3 cp "s3://$BUCKET/$PREFIX/${{T}}.csv" "/tmp/${{T}}.csv" --region $REGION
    echo "Downloaded $T.csv ($(wc -l < /tmp/${{T}}.csv) lines)"
done

echo ""
echo "=== Truncating tables ==="
for T in $TABLES; do
    mysql -u $USER -p"$PASS" $DB -e "SET FOREIGN_KEY_CHECKS=0; TRUNCATE TABLE $T; SET FOREIGN_KEY_CHECKS=1;" 2>/dev/null
done
echo "Tables truncated"

echo ""
echo "=== Loading data ==="
for T in $TABLES; do
    mysql --local-infile=1 -u $USER -p"$PASS" $DB -e "LOAD DATA LOCAL INFILE '/tmp/${{T}}.csv' INTO TABLE $T FIELDS TERMINATED BY ',' OPTIONALLY ENCLOSED BY '\\\"' LINES TERMINATED BY '\\n' IGNORE 1 LINES;" 2>/dev/null
    COUNT=$(mysql -u $USER -p"$PASS" $DB -N -e "SELECT COUNT(*) FROM $T;" 2>/dev/null)
    echo "$T: $COUNT rows"
done

echo ""
echo "=== Final verification ==="
mysql -u $USER -p"$PASS" $DB -e "SELECT 'underwriters' as tbl, COUNT(*) as cnt FROM underwriters UNION ALL SELECT 'adjusters', COUNT(*) FROM adjusters UNION ALL SELECT 'customers', COUNT(*) FROM customers UNION ALL SELECT 'policies', COUNT(*) FROM policies UNION ALL SELECT 'underwriting_decisions', COUNT(*) FROM underwriting_decisions UNION ALL SELECT 'claims', COUNT(*) FROM claims UNION ALL SELECT 'claim_payments', COUNT(*) FROM claim_payments UNION ALL SELECT 'risk_factors', COUNT(*) FROM risk_factors;" 2>/dev/null

echo ""
echo "=== Cleanup ==="
rm -f /tmp/*.csv
echo "Done!"
"""
    print("Creating load script on EC2...")
    result = run_ssm([
        f"cat > /tmp/load_data.sh << 'HEREDOC'\n{script}\nHEREDOC",
        "chmod +x /tmp/load_data.sh",
        "echo 'Script created'",
    ])
    if result and result["status"] == "Success":
        print("  Load script created on EC2")
        return True
    else:
        print(f"  ERROR: {result}")
        return False


def run_load_script():
    print("\nExecuting load script on EC2 (this may take a few minutes)...")
    result = run_ssm(["bash /tmp/load_data.sh"], timeout_secs=300)
    if result is None:
        print("  ERROR: No response")
        return False

    print(result["stdout"])
    if result["stderr"]:
        errors = [l for l in result["stderr"].split("\n") if "Warning" not in l and l.strip()]
        if errors:
            print("Errors:", "\n".join(errors))

    return result["status"] == "Success"


def cleanup_s3():
    print("\nCleaning up S3 staging files...")
    result = run_local([
        "aws", "s3", "rm", f"s3://{S3_BUCKET}/{S3_PREFIX}/", "--recursive", "--region", REGION,
    ])
    print("  S3 staging files removed" if result.returncode == 0 else f"  Warning: {result.stderr.strip()}")


def main():
    print(f"Loading mock data into MySQL on EC2 ({INSTANCE_ID})")
    print(f"Strategy: Local → S3 → EC2 script → MySQL")
    print(f"Region: {REGION}\n")

    csvs = [f for f in os.listdir(OUTPUT_DIR) if f.endswith(".csv")]
    if not csvs:
        print("No CSV files found. Run generate_all.py first.")
        sys.exit(1)
    print(f"Found {len(csvs)} CSV files\n")

    if not upload_csvs_to_s3():
        sys.exit(1)

    if not create_load_script_on_ec2():
        sys.exit(1)

    if run_load_script():
        print("\nAll data loaded successfully!")
    else:
        print("\nLoad completed with errors — check output above.")

    cleanup_s3()


if __name__ == "__main__":
    main()
