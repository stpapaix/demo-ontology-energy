"""Delta table schemas for the Schneider Electric energy medallion.

Each layer plays a distinct role and has its own set of tables:

  bronze : raw, as-ingested source extracts. Everything string-typed plus
           ingestion metadata (_ingested_at, _source_file). Append/reprocessable.
  silver : cleaned, type-cast, deduplicated, conformed star model
           (dimensions + full-granularity facts).
  gold   : business-ready aggregates and KPIs for reporting/analytics.
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ---------------------------------------------------------------------------
# Bronze: raw landing zone. All business columns are strings + ingest metadata.
# ---------------------------------------------------------------------------

_INGEST_META = [
    StructField("ingested_at", TimestampType()),
    StructField("source_file", StringType()),
]


def _raw(*business_columns: str) -> StructType:
    fields = [StructField(name, StringType()) for name in business_columns]
    return StructType(fields + _INGEST_META)


RAW_SITE = _raw(
    "site_id", "site_name", "country", "region", "latitude", "longitude",
    "site_type", "contracted_power_kw", "commissioned_date",
)
RAW_DEVICE = _raw(
    "device_id", "site_id", "device_type", "model",
    "measurement_unit", "install_location", "is_active",
)
RAW_METER_READINGS = _raw(
    "reading_id", "device_id", "site_id", "timestamp",
    "active_power_kw", "energy_kwh", "voltage_v", "current_a", "power_factor",
)
RAW_BILLING = _raw(
    "cost_id", "site_id", "billing_period", "energy_consumed_kwh", "peak_demand_kw",
    "tariff_rate", "energy_cost", "co2_emissions_kg", "currency",
)

# ---------------------------------------------------------------------------
# Silver: conformed, typed star model.
# ---------------------------------------------------------------------------

DIM_SITE = StructType(
    [
        StructField("site_id", StringType(), nullable=False),
        StructField("site_name", StringType()),
        StructField("country", StringType()),
        StructField("region", StringType()),
        StructField("region_id", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("site_type", StringType()),
        StructField("contracted_power_kw", DoubleType()),
        StructField("commissioned_date", DateType()),
        StructField("total_energy_kwh", DoubleType()),
        StructField("total_energy_cost", DoubleType()),
        StructField("total_co2_kg", DoubleType()),
    ]
)

DIM_REGION = StructType(
    [
        StructField("region_id", StringType(), nullable=False),
        StructField("region_code", StringType()),
        StructField("region_name", StringType()),
    ]
)

DIM_DEVICE = StructType(
    [
        StructField("device_id", StringType(), nullable=False),
        StructField("site_id", StringType(), nullable=False),
        StructField("device_type", StringType()),
        StructField("model", StringType()),
        StructField("measurement_unit", StringType()),
        StructField("install_location", StringType()),
        StructField("is_active", BooleanType()),
    ]
)

FACT_ENERGY_CONSUMPTION = StructType(
    [
        StructField("reading_id", StringType(), nullable=False),
        StructField("device_id", StringType(), nullable=False),
        StructField("site_id", StringType(), nullable=False),
        StructField("timestamp", TimestampType()),
        StructField("active_power_kw", DoubleType()),
        StructField("energy_kwh", DoubleType()),
        StructField("voltage_v", DoubleType()),
        StructField("current_a", DoubleType()),
        StructField("power_factor", DoubleType()),
        StructField("reading_date", DateType()),
    ]
)

FACT_ENERGY_COST = StructType(
    [
        StructField("cost_id", StringType(), nullable=False),
        StructField("site_id", StringType(), nullable=False),
        StructField("billing_period", StringType()),
        StructField("energy_consumed_kwh", DoubleType()),
        StructField("peak_demand_kw", DoubleType()),
        StructField("tariff_rate", DoubleType()),
        StructField("energy_cost", DoubleType()),
        StructField("co2_emissions_kg", DoubleType()),
        StructField("currency", StringType()),
    ]
)

# Second source system (asset maintenance) conformed into silver. The ontology
# unifies these with the energy star schema above via cross-source relationships
# (Device -> MaintenanceEvent -> Supplier), which a single semantic model cannot do.
DIM_SUPPLIER = StructType(
    [
        StructField("supplier_id", StringType(), nullable=False),
        StructField("supplier_name", StringType()),
        StructField("supplier_type", StringType()),
        StructField("country", StringType()),
        StructField("contract_since", DateType()),
    ]
)

MAINTENANCE_EVENT = StructType(
    [
        StructField("event_id", StringType(), nullable=False),
        StructField("device_id", StringType(), nullable=False),
        StructField("site_id", StringType()),
        StructField("supplier_id", StringType(), nullable=False),
        StructField("event_date", TimestampType()),
        StructField("event_type", StringType()),
        StructField("downtime_hours", DoubleType()),
        StructField("cost", DoubleType()),
        StructField("currency", StringType()),
        StructField("description", StringType()),
    ]
)

# ---------------------------------------------------------------------------
# Gold: aggregated, business-ready tables and KPIs.
# ---------------------------------------------------------------------------

AGG_DAILY_CONSUMPTION_BY_SITE = StructType(
    [
        StructField("site_id", StringType(), nullable=False),
        StructField("reading_date", DateType()),
        StructField("total_energy_kwh", DoubleType()),
        StructField("avg_active_power_kw", DoubleType()),
        StructField("avg_power_factor", DoubleType()),
        StructField("reading_count", LongType()),
    ]
)

KPI_CO2_BY_REGION = StructType(
    [
        StructField("region_id", StringType()),
        StructField("region", StringType()),
        StructField("billing_period", StringType()),
        StructField("total_co2_kg", DoubleType()),
        StructField("total_energy_kwh", DoubleType()),
        StructField("total_cost", DoubleType()),
    ]
)


# Per-layer table definitions: layer -> {table_name: (schema, partition_columns)}.
LAYER_TABLES = {
    "bronze": {
        "raw_site": (RAW_SITE, []),
        "raw_device": (RAW_DEVICE, []),
        "raw_meter_readings": (RAW_METER_READINGS, []),
        "raw_billing": (RAW_BILLING, []),
    },
    "silver": {
        "dim_region": (DIM_REGION, []),
        "dim_site": (DIM_SITE, []),
        "dim_device": (DIM_DEVICE, []),
        "fact_energy_consumption": (FACT_ENERGY_CONSUMPTION, ["reading_date"]),
        "fact_energy_cost": (FACT_ENERGY_COST, []),
        "dim_supplier": (DIM_SUPPLIER, []),
        "maintenance_event": (MAINTENANCE_EVENT, []),
    },
    "gold": {
        "agg_daily_consumption_by_site": (AGG_DAILY_CONSUMPTION_BY_SITE, []),
        "kpi_co2_by_region": (KPI_CO2_BY_REGION, []),
    },
}
