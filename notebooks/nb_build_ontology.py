# %% [markdown]
# # Build ontology serving layer (business-oriented views)
# Materializes denormalized, business-named entity tables in `lh_gold`
# (`onto_*`), resolving the ontology relationships (Site->Region, Device->Site,
# ...). Consumers get a business-oriented view without needing the star-schema
# joins. Run after `pl_bronze_to_silver` (and `pl_silver_to_gold` for KPIs).
# This notebook is run by the **pl_build_ontology** pipeline.

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


def _save_gold(df, table):
    df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(tpath(GOLD, table))
    print(f"{table}: {df.count()} rows")

# %%
# --- Load silver entities ---
regions = spark.read.format("delta").load(tpath(SILVER, "dim_region"))
sites = spark.read.format("delta").load(tpath(SILVER, "dim_site"))
devices = spark.read.format("delta").load(tpath(SILVER, "dim_device"))
consumption = spark.read.format("delta").load(tpath(SILVER, "fact_energy_consumption"))
billing = spark.read.format("delta").load(tpath(SILVER, "fact_energy_cost"))

# %%
# --- onto_region (Region class) ---
_save_gold(regions.select("region_id", "region_code", "region_name"), "onto_region")

# %%
# --- onto_site (Site + Region) ---
onto_site = (
    sites.join(regions, on="region_id", how="left")
    .select(
        "site_id", "site_name", "country",
        "region_id", "region_code", "region_name",
        "site_type", "contracted_power_kw", "commissioned_date",
    )
)
_save_gold(onto_site, "onto_site")

# %%
# --- onto_device (Device + Site + Region) ---
onto_device = (
    devices.join(onto_site.select("site_id", "site_name", "region_name"), on="site_id", how="left")
    .select(
        "device_id", "device_type", "model", "measurement_unit", "install_location", "is_active",
        "site_id", "site_name", "region_name",
    )
)
_save_gold(onto_device, "onto_device")

# %%
# --- onto_billing (BillingRecord + Site + Region) ---
onto_billing = (
    billing.join(onto_site.select("site_id", "site_name", "region_id", "region_name"), on="site_id", how="left")
    .select(
        "cost_id", "site_id", "site_name", "region_id", "region_name", "billing_period",
        "energy_consumed_kwh", "energy_cost", "co2_emissions_kg", "currency",
    )
)
_save_gold(onto_billing, "onto_billing")

# %%
# --- onto_site_360 (per-site business summary) ---
device_counts = devices.groupBy("site_id").agg(F.count("*").alias("device_count"))
energy = consumption.groupBy("site_id").agg(F.sum("energy_kwh").alias("total_energy_kwh"))
cost_agg = billing.groupBy("site_id").agg(
    F.sum("energy_cost").alias("total_cost"),
    F.sum("co2_emissions_kg").alias("total_co2_kg"),
)
onto_site_360 = (
    onto_site.select("site_id", "site_name", "region_name", "site_type", "contracted_power_kw")
    .join(device_counts, "site_id", "left")
    .join(energy, "site_id", "left")
    .join(cost_agg, "site_id", "left")
    .na.fill(0, ["device_count", "total_energy_kwh", "total_cost", "total_co2_kg"])
)
_save_gold(onto_site_360, "onto_site_360")
