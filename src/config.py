"""Central configuration for the Schneider Electric energy medallion project."""

import os

from dotenv import load_dotenv

load_dotenv()


def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(
            f"Missing required environment variable '{name}'. "
            "Copy .env.example to .env and fill in the values."
        )
    return value


# Target Fabric workspace that will host the three lakehouses.
WORKSPACE_ID = require_env("FABRIC_WORKSPACE_ID")

# Medallion layers and the lakehouse display name used for each one.
LAYERS = ("bronze", "silver", "gold")
LAKEHOUSE_NAMES = {
    "bronze": "lh_bronze",
    "silver": "lh_silver",
    "gold": "lh_gold",
}

# OneLake DFS endpoint (same for every workspace/lakehouse).
ONELAKE_ACCOUNT = "onelake.dfs.fabric.microsoft.com"


def onelake_table_path(lakehouse_id: str, table_name: str) -> str:
    """ABFS path to a Delta table inside a lakehouse's Tables/ area on OneLake."""
    return (
        f"abfss://{WORKSPACE_ID}@{ONELAKE_ACCOUNT}/"
        f"{lakehouse_id}/Tables/{table_name}"
    )
