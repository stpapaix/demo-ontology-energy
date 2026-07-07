# %% [markdown]
# # Seed bronze dimensions (run once)
# Creates **20 sites** and **100 devices** in `lh_bronze` (`raw_site`, `raw_device`).
# Run this notebook **once** before generating facts. It **overwrites** the
# dimension tables so re-running gives a clean, consistent set.

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
# --- Generate dimension rows (string-typed, bronze convention) ---
import random

random.seed(42)

COUNTRIES = [
    ("France", "EMEA"), ("Spain", "EMEA"), ("Germany", "EMEA"), ("Italy", "EMEA"),
    ("USA", "AMER"), ("Brazil", "AMER"), ("India", "APAC"), ("China", "APAC"),
]
SITE_TYPES = ["industrial", "data-center", "commercial", "warehouse"]
DEVICE_TYPES = ["power_meter", "ct_sensor", "breaker"]
MODELS = ["Acti9 PowerTag", "PM8000", "PowerLogic ION9000", "PowerLogic PM5000"]

N_SITES, N_DEVICES = 20, 100

site_cols = ["site_id", "site_name", "country", "region", "latitude", "longitude",
             "site_type", "contracted_power_kw", "commissioned_date"]
sites = []
for i in range(1, N_SITES + 1):
    country, region = random.choice(COUNTRIES)
    sites.append((
        f"SITE{i:03d}",
        f"{country} Plant {i}",
        country, region,
        f"{random.uniform(-55, 60):.4f}",
        f"{random.uniform(-120, 120):.4f}",
        random.choice(SITE_TYPES),
        f"{random.uniform(500, 5000):.1f}",
        f"2020-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
    ))

device_cols = ["device_id", "site_id", "device_type", "model",
               "measurement_unit", "install_location", "is_active"]
devices = []
for i in range(1, N_DEVICES + 1):
    site_id = f"SITE{random.randint(1, N_SITES):03d}"
    dtype = random.choice(DEVICE_TYPES)
    unit = "A" if dtype == "ct_sensor" else "kWh"
    devices.append((
        f"DEV{i:03d}", site_id, dtype, random.choice(MODELS), unit,
        f"feeder-{random.randint(1, 12)}",
        "true" if random.random() > 0.1 else "false",
    ))

# %%
# --- Write dimensions to bronze (overwrite) ---
site_df = (
    spark.createDataFrame(sites, site_cols)
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_file", F.lit("seed_dimensions"))
)
site_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(tpath(BRONZE, "raw_site"))
print(f"raw_site written: {site_df.count()} rows")

device_df = (
    spark.createDataFrame(devices, device_cols)
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_file", F.lit("seed_dimensions"))
)
device_df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(tpath(BRONZE, "raw_device"))
print(f"raw_device written: {device_df.count()} rows")
