import os
import subprocess
import sys
import time
import json

INSTANCE_ID = os.getenv("EC2_INSTANCE_ID", "i-XXXXXXXXXXXX")
REGION = os.getenv("AWS_REGION", "us-west-2")
MYSQL_USER = os.getenv("MYSQL_USER", "root")
MYSQL_PASS = os.getenv("MYSQL_PASS", "")
S3_BUCKET = os.getenv("S3_BUCKET", "finserv-insurance-demo-docs-XXXXXXXXXXXX")

OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "output")
SQL_FILE = os.path.join(OUTPUT_DIR, "incremental_changes.sql")
MAX_IDS_FILE = os.path.join(OUTPUT_DIR, "max_ids.json")


def run_local(cmd):
    return subprocess.run(cmd, capture_output=True, text=True)


def run_ssm(commands, timeout_secs=300):
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


def get_counts(label):
    print(f"\n{label}")
    result = run_ssm([
        f"export MYSQL_PASS=\"{MYSQL_PASS}\"",
        f"mysql -u {MYSQL_USER} -p\"$MYSQL_PASS\" insurance_db -e \""
        "SELECT 'customers' as tbl, COUNT(*) as cnt FROM customers "
        "UNION ALL SELECT 'policies', COUNT(*) FROM policies "
        "UNION ALL SELECT 'underwriting_decisions', COUNT(*) FROM underwriting_decisions "
        "UNION ALL SELECT 'claims', COUNT(*) FROM claims "
        "UNION ALL SELECT 'claim_payments', COUNT(*) FROM claim_payments "
        "UNION ALL SELECT 'risk_factors', COUNT(*) FROM risk_factors "
        "UNION ALL SELECT 'underwriters', COUNT(*) FROM underwriters "
        "UNION ALL SELECT 'adjusters', COUNT(*) FROM adjusters "
        "ORDER BY tbl;\" 2>/dev/null"
    ])
    if result and result["status"] == "Success":
        print(result["stdout"])
    else:
        print(f"  Failed to get counts: {result}")


def query_max_ids():
    print("Querying current MAX IDs from MySQL...")
    result = run_ssm([
        f"export MYSQL_PASS=\"{MYSQL_PASS}\"",
        f"mysql -u {MYSQL_USER} -p\"$MYSQL_PASS\" insurance_db -N -B -e \""
        "SELECT 'customer_id', MAX(customer_id) FROM customers "
        "UNION ALL SELECT 'policy_id', MAX(policy_id) FROM policies "
        "UNION ALL SELECT 'claim_id', MAX(claim_id) FROM claims "
        "UNION ALL SELECT 'payment_id', MAX(payment_id) FROM claim_payments "
        "UNION ALL SELECT 'factor_id', MAX(factor_id) FROM risk_factors "
        "UNION ALL SELECT 'decision_id', MAX(decision_id) FROM underwriting_decisions;"
        "\" 2>/dev/null"
    ])
    if not result or result["status"] != "Success":
        print(f"  Failed to query max IDs: {result}")
        return False

    max_ids = {}
    for line in result["stdout"].strip().split("\n"):
        parts = line.split("\t")
        if len(parts) == 2:
            max_ids[parts[0]] = int(parts[1])
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(MAX_IDS_FILE, "w") as f:
        json.dump(max_ids, f, indent=2)
    print(f"  Saved to {MAX_IDS_FILE}: {max_ids}")
    return True


def main():
    if "--query-ids" in sys.argv:
        query_max_ids()
        return

    print("Step 1: Query current MAX IDs from MySQL...")
    if not query_max_ids():
        print("  WARNING: Could not query max IDs, generate_incremental.py will use defaults")

    print("\nStep 2: Regenerating incremental SQL...")
    gen_result = subprocess.run(
        [sys.executable, os.path.join(os.path.dirname(os.path.abspath(__file__)), "generate_incremental.py")],
        capture_output=True, text=True
    )
    print(gen_result.stdout)
    if gen_result.returncode != 0:
        print(f"  ERROR generating SQL: {gen_result.stderr}")
        sys.exit(1)

    if not os.path.exists(SQL_FILE):
        print(f"SQL file not found: {SQL_FILE}")
        sys.exit(1)

    file_size = os.path.getsize(SQL_FILE)
    with open(SQL_FILE) as f:
        line_count = sum(1 for _ in f)
    print(f"Incremental SQL file: {SQL_FILE}")
    print(f"  Size: {file_size:,} bytes, {line_count} lines")
    print(f"  Target: MySQL on EC2 ({INSTANCE_ID})")
    print()

    get_counts("=== BEFORE counts ===")

    print("\nUploading SQL file to S3...")
    s3_key = f"s3://{S3_BUCKET}/incremental/incremental_changes.sql"
    result = run_local(["aws", "s3", "cp", SQL_FILE, s3_key, "--region", REGION])
    if result.returncode != 0:
        print(f"  ERROR uploading to S3: {result.stderr.strip()}")
        sys.exit(1)
    print(f"  Uploaded to {s3_key}")

    print("\nDownloading and executing on EC2...")
    result = run_ssm([
        f"export MYSQL_PASS=\"{MYSQL_PASS}\"",
        f"aws s3 cp {s3_key} /tmp/incremental_changes.sql --region {REGION}",
        f"mysql -u {MYSQL_USER} -p\"$MYSQL_PASS\" --force < /tmp/incremental_changes.sql 2>&1 | tail -20",
        "rm -f /tmp/incremental_changes.sql",
    ], timeout_secs=300)

    if result is None:
        print("  ERROR: No response from SSM")
        sys.exit(1)

    if result["stdout"]:
        print(result["stdout"])
    if result["stderr"]:
        errors = [l for l in result["stderr"].split("\n") if l.strip() and "Warning" not in l]
        if errors:
            print("Errors (non-warning):")
            for e in errors:
                print(f"  {e}")

    if result["status"] == "Success":
        print("SQL execution completed successfully.")
    else:
        print(f"SQL execution status: {result['status']}")

    get_counts("=== AFTER counts ===")

    print("\nCleaning up S3...")
    run_local(["aws", "s3", "rm", s3_key, "--region", REGION])

    print("\nDone! CDC changes should propagate to Snowflake within ~60 seconds.")
    print("Verify in Snowflake:")
    print('  SELECT COUNT(*) FROM INSURANCE_RAW."insurance_db"."customers";')
    print('  SELECT COUNT(*) FROM INSURANCE_RAW."insurance_db"."policies";')
    print('  SELECT * FROM INSURANCE_RAW."insurance_db"."claims" WHERE "claim_number" LIKE \'CLM-%-9%\' ORDER BY "claim_id" DESC LIMIT 10;')


if __name__ == "__main__":
    main()
