"""Delta table schemas for the Schneider Electric energy analytics domain.

Four tables form a small star model:
  - dim_site                 : monitored facilities / locations
  - dim_device               : meters & sensors (e.g. PowerTag, PM8000)
  - fact_energy_consumption  : time-series meter readings
  - fact_energy_cost         : billing / tariff facts for cost & CO2 analytics
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

DIM_SITE = StructType(
    [
        StructField("site_id", StringType(), nullable=False),
        StructField("site_name", StringType()),
        StructField("country", StringType()),
        StructField("region", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("site_type", StringType()),
        StructField("contracted_power_kw", DoubleType()),
        StructField("commissioned_date", DateType()),
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


# Table definition: name -> (schema, partition columns).
TABLES = {
    "dim_site": (DIM_SITE, []),
    "dim_device": (DIM_DEVICE, []),
    "fact_energy_consumption": (FACT_ENERGY_CONSUMPTION, ["reading_date"]),
    "fact_energy_cost": (FACT_ENERGY_COST, []),
}
