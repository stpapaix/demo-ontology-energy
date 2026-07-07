# %% [markdown]
# # Truncate all tables (delete all rows)
# Deletes **every row** from all tables in `lh_bronze`, `lh_silver` and
# `lh_gold`. The tables and their schemas are kept — only the data is removed.
# Run this manually whenever you want to start over with empty tables.

# %%
# --- Configuration: resolve workspace and build OneLake table paths ---
import notebookutils
from pyspark.sql import functions as F

ONELAKE = "onelake.dfs.fabric.microsoft.com"
BRONZE, SILVER, GOLD = "lh_bronze", "lh_silver", "lh_gold"

_ctx = notebookutils.runtime.context
WORKSPACE_ID = _ctx.get("currentWorkspaceId") or "93722789-8888-4BAD-9EF1-5AFA52BA442F"


def tpath(lakehouse_name: str, table: str) -> str:
    return f"abfss://{WORKSPACE_ID}@{ONELAKE}/{lakehouse_name}.Lakehouse/Tables/{table}"

# %%
# --- Delete all rows from every table (schemas preserved) ---
from delta.tables import DeltaTable

TABLES = {
    BRONZE: ["raw_site", "raw_device", "raw_meter_readings", "raw_billing"],
    SILVER: ["dim_site", "dim_device", "fact_energy_consumption", "fact_energy_cost"],
    GOLD: ["agg_daily_consumption_by_site", "kpi_co2_by_region"],
}

for lakehouse_name, tables in TABLES.items():
    for table in tables:
        path = tpath(lakehouse_name, table)
        try:
            DeltaTable.forPath(spark, path).delete()
            print(f"cleared {lakehouse_name}.{table}")
        except Exception as error:
            print(f"skipped {lakehouse_name}.{table}: {error}")

print("\nAll tables truncated.")
