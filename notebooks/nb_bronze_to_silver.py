# %% [markdown]
# # Pipeline step: Bronze -> Silver
# Cleans, type-casts and deduplicates the bronze raw tables into the conformed
# silver star model. Rebuilt with `overwrite` so it is idempotent.
# This notebook is run by the **pl_bronze_to_silver** data pipeline.

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
# --- dim_site ---
dim_site = (
    spark.read.format("delta").load(tpath(BRONZE, "raw_site"))
    .select(
        "site_id", "site_name", "country", "region",
        F.col("latitude").cast("double"),
        F.col("longitude").cast("double"),
        "site_type",
        F.col("contracted_power_kw").cast("double"),
        F.to_date("commissioned_date").alias("commissioned_date"),
    )
    .filter(F.col("site_id").isNotNull())
    .dropDuplicates(["site_id"])
)
dim_site.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(tpath(SILVER, "dim_site"))
print(f"dim_site: {dim_site.count()} rows")

# %%
# --- dim_device ---
dim_device = (
    spark.read.format("delta").load(tpath(BRONZE, "raw_device"))
    .select(
        "device_id", "site_id", "device_type", "model",
        "measurement_unit", "install_location",
        (F.lower(F.col("is_active")) == "true").alias("is_active"),
    )
    .filter(F.col("device_id").isNotNull())
    .dropDuplicates(["device_id"])
)
dim_device.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(tpath(SILVER, "dim_device"))
print(f"dim_device: {dim_device.count()} rows")

# %%
# --- fact_energy_consumption (partitioned by reading_date) ---
fact_consumption = (
    spark.read.format("delta").load(tpath(BRONZE, "raw_meter_readings"))
    .select(
        "reading_id", "device_id", "site_id",
        F.to_timestamp("timestamp").alias("timestamp"),
        F.col("active_power_kw").cast("double"),
        F.col("energy_kwh").cast("double"),
        F.col("voltage_v").cast("double"),
        F.col("current_a").cast("double"),
        F.col("power_factor").cast("double"),
    )
    .withColumn("reading_date", F.to_date("timestamp"))
    .filter(F.col("reading_id").isNotNull() & F.col("timestamp").isNotNull())
    .dropDuplicates(["reading_id"])
)
(
    fact_consumption.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").partitionBy("reading_date")
    .save(tpath(SILVER, "fact_energy_consumption"))
)
print(f"fact_energy_consumption: {fact_consumption.count()} rows")

# %%
# --- fact_energy_cost ---
fact_cost = (
    spark.read.format("delta").load(tpath(BRONZE, "raw_billing"))
    .select(
        "cost_id", "site_id", "billing_period",
        F.col("energy_consumed_kwh").cast("double"),
        F.col("peak_demand_kw").cast("double"),
        F.col("tariff_rate").cast("double"),
        F.col("energy_cost").cast("double"),
        F.col("co2_emissions_kg").cast("double"),
        "currency",
    )
    .filter(F.col("cost_id").isNotNull())
    .dropDuplicates(["cost_id"])
)
fact_cost.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(tpath(SILVER, "fact_energy_cost"))
print(f"fact_energy_cost: {fact_cost.count()} rows")
