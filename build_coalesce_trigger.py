#!/usr/bin/env python3
"""
Build the Coalesce Transform Trigger chain in OpenFlow (NiFi).

This script adds 3 processors to the main canvas after the MySQL CDC module:
1. ReplaceText - Crafts the Coalesce API request body
2. InvokeHTTP - POSTs to Coalesce Scheduler API
3. LogAttribute - Captures API response for auditing

Prerequisites:
- nipyapi installed: pip3 install "nipyapi[cli]>=1.2.0"
- NiFi PAT configured in ~/.nipyapi/profiles.yml
- coalesce_trigger_config.json filled with your values

Usage:
  export NIFI_PAT="your-nifi-personal-access-token"
  python3 build_coalesce_trigger.py
"""

import json
import os
import sys

try:
    import nipyapi
except ImportError:
    print("ERROR: nipyapi not installed. Run: pip3 install 'nipyapi[cli]>=1.2.0'")
    sys.exit(1)

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CONFIG_FILE = os.path.join(SCRIPT_DIR, "coalesce_trigger_config.json")
PROFILE_NAME = "finserv_runtime"
NIFI_URL = "https://of--sfsenorthamerica-demo-vsuru.snowflakecomputing.app/finserv-runtime/nifi-api"

def load_config():
    with open(CONFIG_FILE, "r") as f:
        config = json.load(f)

    for key in ["coalesce_api_token", "environmentID", "jobID"]:
        val = config.get(key, "")
        if not val or "YOUR_" in val or "HERE" in val:
            print(f"ERROR: Please fill in '{key}' in {CONFIG_FILE}")
            sys.exit(1)
    return config


def ensure_profile():
    profiles_file = os.path.expanduser("~/.nipyapi/profiles.yml")
    if os.path.exists(profiles_file):
        with open(profiles_file, "r") as f:
            if PROFILE_NAME + ":" in f.read():
                print(f"[OK] Profile '{PROFILE_NAME}' already exists")
                return

    pat = os.environ.get("NIFI_PAT")
    if not pat:
        print("ERROR: Set NIFI_PAT environment variable with your NiFi Personal Access Token.")
        print("  Then: export NIFI_PAT='eyJ...'")
        sys.exit(1)

    profiles_dir = os.path.expanduser("~/.nipyapi")
    os.makedirs(profiles_dir, exist_ok=True)

    profile_content = f"""{PROFILE_NAME}:
  nifi_url: "{NIFI_URL}"
  nifi_bearer_token: "{pat}"
"""

    if os.path.exists(profiles_file):
        with open(profiles_file, "a") as f:
            f.write("\n" + profile_content)
    else:
        with open(profiles_file, "w") as f:
            f.write(profile_content)

    print(f"[OK] Profile '{PROFILE_NAME}' configured")


def connect():
    nipyapi.profiles.switch(PROFILE_NAME)
    try:
        info = nipyapi.system.get_nifi_version_info()
        print(f"[OK] Connected to NiFi {info.ni_fi_version}")
    except Exception as e:
        print(f"ERROR: Cannot connect to NiFi: {e}")
        sys.exit(1)


def find_mysql_process_group():
    root_pg = nipyapi.canvas.get_process_group("root", "id")
    child_pgs = nipyapi.canvas.list_all_process_groups(root_pg.id)
    
    mysql_pg = None
    for pg in child_pgs:
        name_lower = pg.status.name.lower() if pg.status else ""
        if "mysql" in name_lower or "google" in name_lower or "cdc" in name_lower:
            mysql_pg = pg
            break

    if not mysql_pg:
        print("Available process groups:")
        for pg in child_pgs:
            print(f"  - {pg.status.name} (id: {pg.id})")
        print("\nERROR: Could not auto-detect the MySQL/CDC process group.")
        print("Please set MYSQL_PG_ID environment variable with the correct ID.")
        sys.exit(1)

    print(f"[OK] Found source process group: '{mysql_pg.status.name}' ({mysql_pg.id})")
    return mysql_pg


def get_or_find_mysql_pg():
    pg_id = os.environ.get("MYSQL_PG_ID")
    if pg_id:
        pg = nipyapi.canvas.get_process_group(pg_id, "id")
        print(f"[OK] Using specified process group: {pg.status.name} ({pg.id})")
        return pg
    return find_mysql_process_group()


def create_coalesce_trigger(config):
    root_pg = nipyapi.canvas.get_process_group("root", "id")
    
    mysql_pg = get_or_find_mysql_pg()
    mysql_position = mysql_pg.position

    x_offset = mysql_position.x + 600
    y_offset = mysql_position.y

    replace_text_type = nipyapi.canvas.get_processor_type("ReplaceText", identifier_type="name", greedy=True)
    if isinstance(replace_text_type, list):
        replace_text_type = replace_text_type[0]
    replace_proc = nipyapi.canvas.create_processor(
        parent_pg=root_pg,
        processor=replace_text_type,
        location=(x_offset, y_offset),
        name="Craft Coalesce Request Body"
    )
    print(f"[OK] Created ReplaceText processor: {replace_proc.id}")

    payload = json.dumps({
        "runDetails": {
            "environmentID": config["environmentID"],
            "parallelism": 16,
            "jobID": config["jobID"]
        },
        "userCredentials": {
            "snowflakeAuthType": "KeyPair"
        }
    }, indent=2)

    nipyapi.canvas.update_processor(
        replace_proc,
        nipyapi.nifi.ProcessorConfigDTO(
            properties={
                "Replacement Strategy": "Always Replace",
                "Evaluation Mode": "Entire text",
                "Replacement Value": payload
            },
            auto_terminated_relationships=["failure"]
        )
    )
    print("[OK] Configured ReplaceText with Coalesce payload")

    invoke_http_type = nipyapi.canvas.get_processor_type("InvokeHTTP", identifier_type="name", greedy=True)
    if isinstance(invoke_http_type, list):
        invoke_http_type = invoke_http_type[0]
    invoke_proc = nipyapi.canvas.create_processor(
        parent_pg=root_pg,
        processor=invoke_http_type,
        location=(x_offset + 500, y_offset),
        name="Trigger Coalesce Production Refresh"
    )
    print(f"[OK] Created InvokeHTTP processor: {invoke_proc.id}")

    nipyapi.canvas.update_processor(
        invoke_proc,
        nipyapi.nifi.ProcessorConfigDTO(
            properties={
                "HTTP Method": "POST",
                "HTTP URL": "https://app.coalescesoftware.io/scheduler/startRun",
                "Content-Type": "application/json",
                "Authorization": f"Bearer {config['coalesce_api_token']}"
            },
            auto_terminated_relationships=["Original", "No Retry", "Retry"]
        )
    )
    print("[OK] Configured InvokeHTTP with Coalesce endpoint")

    log_attr_type = nipyapi.canvas.get_processor_type("LogAttribute", identifier_type="name", greedy=True)
    if isinstance(log_attr_type, list):
        log_attr_type = log_attr_type[0]
    log_proc = nipyapi.canvas.create_processor(
        parent_pg=root_pg,
        processor=log_attr_type,
        location=(x_offset + 500, y_offset + 300),
        name="Log Coalesce Response"
    )
    print(f"[OK] Created LogAttribute processor: {log_proc.id}")

    nipyapi.canvas.update_processor(
        log_proc,
        nipyapi.nifi.ProcessorConfigDTO(
            properties={
                "Log Level": "info",
                "Log Payload": "true"
            },
            auto_terminated_relationships=["success"]
        )
    )
    print("[OK] Configured LogAttribute")

    nipyapi.canvas.create_connection(
        replace_proc, invoke_proc, relationships=["success"]
    )
    print("[OK] Connected ReplaceText → InvokeHTTP (success)")

    nipyapi.canvas.create_connection(
        invoke_proc, log_proc, relationships=["Response"]
    )
    print("[OK] Connected InvokeHTTP → LogAttribute (Response)")

    nipyapi.canvas.create_connection(
        invoke_proc, log_proc, relationships=["Failure"]
    )
    print("[OK] Connected InvokeHTTP → LogAttribute (Failure)")

    print("\n" + "=" * 60)
    print("COALESCE TRIGGER CHAIN BUILT SUCCESSFULLY")
    print("=" * 60)
    print(f"\nProcessors created on main canvas:")
    print(f"  1. Craft Coalesce Request Body (ReplaceText) - {replace_proc.id}")
    print(f"  2. Trigger Coalesce Production Refresh (InvokeHTTP) - {invoke_proc.id}")
    print(f"  3. Log Coalesce Response (LogAttribute) - {log_proc.id}")
    print(f"\nMANUAL STEPS REMAINING:")
    print(f"  1. Double-click the MySQL process group to enter it")
    print(f"  2. Add an Output Port named 'to_coalesce_trigger'")
    print(f"  3. Connect the final processor's 'success' relationship to that Output Port")
    print(f"  4. Return to the main canvas (breadcrumb: NiFi Flow)")
    print(f"  5. Draw a connection from the MySQL PG to 'Craft Coalesce Request Body'")
    print(f"  6. Start all 3 trigger processors")


def main():
    print("=" * 60)
    print("Coalesce Transform Trigger Builder for OpenFlow")
    print("=" * 60)
    print()

    config = load_config()
    print(f"[OK] Config loaded: environmentID={config['environmentID']}, jobID={config['jobID']}")

    ensure_profile()
    connect()

    create_coalesce_trigger(config)


if __name__ == "__main__":
    main()
