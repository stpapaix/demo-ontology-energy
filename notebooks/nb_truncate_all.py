# %% [markdown]
# # Truncate all tables (delete all rows)
# Deletes **every row** from all tables in `lh_bronze`, `lh_silver` and
# `lh_gold`. The tables and their schemas are kept — only the data is removed.
# Run this manually whenever you want to start over with empty tables.

# %%
# --- Configuration: resolve lakehouse paths on OneLake (GUID form) ---
import notebookutils
from pyspark.sql import functions as F

BRONZE, SILVER, GOLD = "lh_bronze", "lh_silver", "lh_gold"

_LH_BASE = {}


def _get(obj, key):
    return obj[key] if isinstance(obj, dict) else getattr(obj, key)


def _lakehouse_base(name: str) -> str:
    """Canonical OneLake abfss base path for a lakehouse, resolved by name."""
    if name not in _LH_BASE:
        info = notebookutils.lakehouse.get(name)
        try:
            _LH_BASE[name] = _get(_get(info, "properties"), "abfsPath")
        except Exception:
            ws = notebookutils.runtime.context.get("currentWorkspaceId")
            _LH_BASE[name] = f"abfss://{ws}@onelake.dfs.fabric.microsoft.com/{_get(info, 'id')}"
    return _LH_BASE[name]


def tpath(lakehouse_name: str, table: str) -> str:
    return f"{_lakehouse_base(lakehouse_name)}/Tables/{table}"

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
