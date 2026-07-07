# %% [markdown]
# # Generate bronze facts (run many times)
# Randomly generates **> 1000 meter readings** and **> 1000 billing rows**,
# **appended** to `lh_bronze` (`raw_meter_readings`, `raw_billing`).
# Run this notebook multiple times to accumulate more data. Each row uses a
# unique UUID id, and references existing sites/devices so the data stays
# **coherent** with `nb_seed_dimensions`.

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
# --- Load existing dimensions for coherence ---
import datetime
import random
import uuid

device_rows = spark.read.format("delta").load(tpath(BRONZE, "raw_device")).select("device_id", "site_id").collect()
site_rows = spark.read.format("delta").load(tpath(BRONZE, "raw_site")).select("site_id").collect()

device_list = [(r["device_id"], r["site_id"]) for r in device_rows]
site_list = [r["site_id"] for r in site_rows]
assert device_list and site_list, "No dimensions found - run nb_seed_dimensions first."

N_READINGS = 1200   # > 1000 per run
N_BILLING = 1100    # > 1000 per run
now = datetime.datetime.utcnow()
print(f"{len(site_list)} sites, {len(device_list)} devices available")

# %%
# --- Meter readings (append) ---
reading_cols = ["reading_id", "device_id", "site_id", "timestamp",
                "active_power_kw", "energy_kwh", "voltage_v", "current_a", "power_factor"]
readings = []
for _ in range(N_READINGS):
    device_id, site_id = random.choice(device_list)
    ts = now - datetime.timedelta(minutes=random.randint(0, 60 * 24 * 30))
    readings.append((
        str(uuid.uuid4()), device_id, site_id,
        ts.strftime("%Y-%m-%d %H:%M:%S"),
        f"{random.uniform(5, 400):.2f}",
        f"{random.uniform(1, 80):.2f}",
        f"{random.uniform(220, 410):.1f}",
        f"{random.uniform(5, 120):.1f}",
        f"{random.uniform(0.85, 1.0):.3f}",
    ))
readings_df = (
    spark.createDataFrame(readings, reading_cols)
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_file", F.lit("meter_stream"))
)
readings_df.write.format("delta").mode("append").save(tpath(BRONZE, "raw_meter_readings"))
print(f"raw_meter_readings appended: {readings_df.count()} rows")

# %%
# --- Billing (append) ---
billing_cols = ["cost_id", "site_id", "billing_period", "energy_consumed_kwh", "peak_demand_kw",
                "tariff_rate", "energy_cost", "co2_emissions_kg", "currency"]
billing = []
for _ in range(N_BILLING):
    site_id = random.choice(site_list)
    period = f"2026-{random.randint(1, 12):02d}"
    energy = random.uniform(5000, 30000)
    tariff = random.uniform(0.08, 0.20)
    billing.append((
        str(uuid.uuid4()), site_id, period,
        f"{energy:.1f}",
        f"{random.uniform(100, 400):.1f}",
        f"{tariff:.3f}",
        f"{energy * tariff:.2f}",
        f"{energy * random.uniform(0.1, 0.4):.1f}",
        random.choice(["EUR", "USD"]),
    ))
billing_df = (
    spark.createDataFrame(billing, billing_cols)
    .withColumn("ingested_at", F.current_timestamp())
    .withColumn("source_file", F.lit("erp_billing"))
)
billing_df.write.format("delta").mode("append").save(tpath(BRONZE, "raw_billing"))
print(f"raw_billing appended: {billing_df.count()} rows")
