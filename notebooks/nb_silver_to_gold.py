# %% [markdown]
# # Pipeline step: Silver -> Gold
# Aggregates the conformed silver facts into business-ready gold tables & KPIs.
# Rebuilt with `overwrite` so it is idempotent.
# This notebook is run by the **pl_silver_to_gold** data pipeline.

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
# --- agg_daily_consumption_by_site ---
agg_daily = (
    spark.read.format("delta").load(tpath(SILVER, "fact_energy_consumption"))
    .groupBy("site_id", "reading_date")
    .agg(
        F.sum("energy_kwh").alias("total_energy_kwh"),
        F.avg("active_power_kw").alias("avg_active_power_kw"),
        F.avg("power_factor").alias("avg_power_factor"),
        F.count("*").alias("reading_count"),
    )
)
agg_daily.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(tpath(GOLD, "agg_daily_consumption_by_site"))
print(f"agg_daily_consumption_by_site: {agg_daily.count()} rows")

# %%
# --- kpi_co2_by_region ---
cost = spark.read.format("delta").load(tpath(SILVER, "fact_energy_cost"))
site_region = spark.read.format("delta").load(tpath(SILVER, "dim_site")).select("site_id", "region")
kpi_co2 = (
    cost.join(site_region, on="site_id", how="left")
    .groupBy("region", "billing_period")
    .agg(
        F.sum("co2_emissions_kg").alias("total_co2_kg"),
        F.sum("energy_consumed_kwh").alias("total_energy_kwh"),
        F.sum("energy_cost").alias("total_cost"),
    )
)
kpi_co2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(tpath(GOLD, "kpi_co2_by_region"))
print(f"kpi_co2_by_region: {kpi_co2.count()} rows")
