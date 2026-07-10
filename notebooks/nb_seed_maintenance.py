# %% [markdown]
# # Seed maintenance source into silver (run once)
# Populates the **second source system** in `lh_silver`: `dim_supplier` and
# `maintenance_event`. Maintenance events reference **real** `device_id`/`site_id`
# values read from `dim_device`, so the ontology's cross-source relationships
# (Device -> MaintenanceEvent -> Supplier) resolve correctly.
#
# Run this **after** `nb_bronze_to_silver` has populated `dim_device`. It
# **overwrites** the two tables so re-running gives a clean, consistent set.

# %%
# --- Configuration: resolve lakehouse paths on OneLake (GUID form) ---
import notebookutils
from pyspark.sql import functions as F

SILVER = "lh_silver"

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
# --- Generate suppliers ---
import datetime
import random

random.seed(7)

SUPPLIERS = [
    ("SUP001", "Schneider Field Services", "OEM", "France"),
    ("SUP002", "VoltCare Partners", "Service Partner", "Germany"),
    ("SUP003", "GridWorks Maintenance", "Service Partner", "USA"),
    ("SUP004", "AsiaPower Tech", "Distributor", "China"),
    ("SUP005", "IberElectric Services", "Service Partner", "Spain"),
]

supplier_cols = ["supplier_id", "supplier_name", "supplier_type", "country", "contract_since"]
suppliers = [
    (sid, name, stype, country,
     datetime.date(random.randint(2018, 2023), random.randint(1, 12), random.randint(1, 28)))
    for sid, name, stype, country in SUPPLIERS
]

supplier_df = spark.createDataFrame(suppliers, supplier_cols)
supplier_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(tpath(SILVER, "dim_supplier"))
print(f"dim_supplier written: {supplier_df.count()} rows")

# %%
# --- Generate maintenance events against REAL devices from dim_device ---
device_rows = spark.read.format("delta").load(tpath(SILVER, "dim_device")).select("device_id", "site_id").collect()
if not device_rows:
    raise RuntimeError("dim_device is empty - run nb_bronze_to_silver first.")

EVENT_TYPES = ["preventive", "corrective", "inspection", "calibration"]
supplier_ids = [s[0] for s in SUPPLIERS]

event_cols = ["event_id", "device_id", "site_id", "supplier_id", "event_date",
              "event_type", "downtime_hours", "cost", "currency", "description"]
events = []
n = 1
for row in device_rows:
    for _ in range(random.randint(1, 4)):  # 1-4 events per device
        etype = random.choice(EVENT_TYPES)
        downtime = round(random.uniform(0.0, 8.0), 2) if etype == "corrective" else round(random.uniform(0.0, 2.0), 2)
        events.append((
            f"MEVT{n:05d}", row["device_id"], row["site_id"], random.choice(supplier_ids),
            datetime.datetime(2024, random.randint(1, 12), random.randint(1, 28),
                              random.randint(0, 23), random.randint(0, 59)),
            etype, downtime, round(random.uniform(150, 4000), 2), "EUR",
            f"{etype.capitalize()} maintenance on {row['device_id']}",
        ))
        n += 1

event_df = spark.createDataFrame(events, event_cols)
event_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(tpath(SILVER, "maintenance_event"))
print(f"maintenance_event written: {event_df.count()} rows")
