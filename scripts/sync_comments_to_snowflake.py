"""
sync_comments_to_snowflake.py
------------------------------
Reads dbt's manifest.json and pushes model + column descriptions
to Snowflake as native COMMENT ON TABLE / COMMENT ON COLUMN statements.

Run after every dbt run:
    python scripts/sync_comments_to_snowflake.py

Or wire into CI/CD after the dbt run step.

Requirements:
    pip install snowflake-connector-python python-dotenv

Environment variables (set in .env or CI secrets):
    SNOWFLAKE_ACCOUNT    e.g. abc12345.eu-west-1
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD   (or use SNOWFLAKE_PRIVATE_KEY_PATH for key-pair auth)
    SNOWFLAKE_ROLE
    SNOWFLAKE_WAREHOUSE
    SNOWFLAKE_DATABASE   the target database (e.g. prod_conform)

Optional:
    DBT_MANIFEST_PATH    path to manifest.json (default: target/manifest.json)
    SYNC_TAGS            comma-separated dbt tags to filter (e.g. p1,billing)
                         if not set, all documented models are synced
"""

import json
import os
import sys
from pathlib import Path

import snowflake.connector
from dotenv import load_dotenv

load_dotenv()

# ── Config ────────────────────────────────────────────────────────────────────

MANIFEST_PATH = Path(os.getenv("DBT_MANIFEST_PATH", "target/manifest.json"))
SYNC_TAGS     = [t.strip() for t in os.getenv("SYNC_TAGS", "").split(",") if t.strip()]

SNOWFLAKE_CONFIG = {
    "account":   os.environ["SNOWFLAKE_ACCOUNT"],
    "user":      os.environ["SNOWFLAKE_USER"],
    "password":  os.environ["SNOWFLAKE_PASSWORD"],
    "role":      os.environ["SNOWFLAKE_ROLE"],
    "warehouse": os.environ["SNOWFLAKE_WAREHOUSE"],
    "database":  os.environ["SNOWFLAKE_DATABASE"],
}

# ── Helpers ───────────────────────────────────────────────────────────────────

def clean(text: str) -> str:
    """Normalise whitespace and escape single quotes for SQL."""
    return " ".join(text.split()).replace("'", "''")


def should_sync(node: dict) -> bool:
    """Return True if this node should be synced."""
    if node.get("resource_type") != "model":
        return False
    if node.get("config", {}).get("materialized") == "ephemeral":
        return False
    if not node.get("description", "").strip():
        return False
    if SYNC_TAGS:
        node_tags = set(node.get("tags", []))
        if not node_tags.intersection(SYNC_TAGS):
            return False
    return True


def resolve_relation(node: dict, default_database: str) -> tuple[str, str, str]:
    """
    Return (database, schema, table) for a node.
    dbt stores the resolved relation in node['relation_name'] as
    `database`.`schema`.`table` — parse that when available.
    """
    relation = node.get("relation_name", "")
    if relation:
        parts = [p.strip('"`') for p in relation.split(".")]
        if len(parts) == 3:
            return parts[0], parts[1], parts[2]

    # Fallback: build from config
    database = node.get("database") or default_database
    schema   = node.get("schema") or node.get("config", {}).get("schema", "")
    name     = node.get("name", "")
    return database.upper(), schema.upper(), name.upper()


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    if not MANIFEST_PATH.exists():
        print(f"ERROR: manifest.json not found at {MANIFEST_PATH}")
        print("Run `dbt compile` or `dbt run` first to generate it.")
        sys.exit(1)

    manifest = json.loads(MANIFEST_PATH.read_text())
    nodes    = manifest.get("nodes", {})
    default_db = SNOWFLAKE_CONFIG["database"].upper()

    statements = []
    model_count = 0
    col_count   = 0

    for node_id, node in nodes.items():
        if not should_sync(node):
            continue

        database, schema, table = resolve_relation(node, default_db)
        model_desc = clean(node["description"])
        model_count += 1

        statements.append(
            f"COMMENT ON TABLE {database}.{schema}.{table} IS '{model_desc}';"
        )

        for col_name, col_meta in node.get("columns", {}).items():
            col_desc = col_meta.get("description", "").strip()
            if not col_desc:
                continue
            col_count += 1
            statements.append(
                f"COMMENT ON COLUMN {database}.{schema}.{table}.{col_name.upper()} "
                f"IS '{clean(col_desc)}';"
            )

    if not statements:
        print("No documented models found to sync.")
        if SYNC_TAGS:
            print(f"  (filtering by tags: {SYNC_TAGS})")
        sys.exit(0)

    print(f"Syncing {model_count} models, {col_count} columns to Snowflake...")
    if SYNC_TAGS:
        print(f"  Tags filter: {SYNC_TAGS}")

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    success = 0
    errors  = []

    for stmt in statements:
        try:
            cursor.execute(stmt)
            success += 1
        except snowflake.connector.errors.ProgrammingError as e:
            errors.append({"statement": stmt[:120], "error": str(e)})

    cursor.close()
    conn.close()

    print(f"\nDone. {success} comments synced successfully.")

    if errors:
        print(f"\n{len(errors)} statements failed (non-fatal — table may not exist yet):")
        for e in errors:
            print(f"  - {e['statement']}")
            print(f"    {e['error']}")


if __name__ == "__main__":
    main()
