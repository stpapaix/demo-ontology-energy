# %% [markdown]
# # Pipeline step: Bronze -> Silver
# Cleans, type-casts and deduplicates the bronze raw tables into the conformed
# silver star model. Rebuilt with `overwrite` so it is idempotent.
# This notebook is run by the **pl_bronze_to_silver** data pipeline.

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
# --- dim_region (derived from raw_site regions) ---
dim_region = (
    spark.read.format("delta").load(tpath(BRONZE, "raw_site"))
    .select(F.col("region").alias("region_code"))
    .filter(F.col("region_code").isNotNull())
    .distinct()
    .withColumn("region_id", F.md5(F.col("region_code")))
    .withColumn(
        "region_name",
        F.when(F.col("region_code") == "EMEA", "Europe, Middle East & Africa")
        .when(F.col("region_code") == "AMER", "Americas")
        .when(F.col("region_code") == "APAC", "Asia-Pacific")
        .otherwise(F.col("region_code")),
    )
    .select("region_id", "region_code", "region_name")
)
dim_region.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(tpath(SILVER, "dim_region"))
print(f"dim_region: {dim_region.count()} rows")

# %%
# --- dim_site (region_id links to dim_region) ---
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
    .withColumn("region_id", F.md5(F.col("region")))
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

# %%
# --- site_summary (per-site totals for the ontology Site entity) ---
energy_by_site = fact_consumption.groupBy("site_id").agg(F.sum("energy_kwh").alias("total_energy_kwh"))
cost_by_site = fact_cost.groupBy("site_id").agg(
    F.sum("energy_cost").alias("total_energy_cost"),
    F.sum("co2_emissions_kg").alias("total_co2_kg"),
)
site_summary = (
    dim_site.select("site_id")
    .join(energy_by_site, "site_id", "left")
    .join(cost_by_site, "site_id", "left")
    .na.fill(0, ["total_energy_kwh", "total_energy_cost", "total_co2_kg"])
)
site_summary.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(tpath(SILVER, "site_summary"))
print(f"site_summary: {site_summary.count()} rows")
