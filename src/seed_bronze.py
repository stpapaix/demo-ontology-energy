"""Seed the bronze raw tables with a small string-typed demo dataset.

Mimics raw ingestion: everything is stored as text plus ingestion metadata,
exactly as it would land from source extracts (meter telemetry, ERP billing).
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from config import onelake_table_path

_RAW_SITE_COLS = [
    "site_id", "site_name", "country", "region", "latitude", "longitude",
    "site_type", "contracted_power_kw", "commissioned_date",
]
_RAW_SITE = [
    ("SITE001", "Grenoble Plant", "France", "EMEA", "45.188", "5.724", "industrial", "2500.0", "2019-03-15"),
    ("SITE002", "Barcelona DC", "Spain", "EMEA", "41.390", "2.154", "data-center", "1800.5", "2020-07-01"),
    ("SITE003", "Austin Office", "USA", "AMER", "30.267", "-97.743", "commercial", "950.0", "2021-01-20"),
]

_RAW_DEVICE_COLS = [
    "device_id", "site_id", "device_type", "model",
    "measurement_unit", "install_location", "is_active",
]
_RAW_DEVICE = [
    ("DEV001", "SITE001", "power_meter", "Acti9 PowerTag", "kWh", "feeder-A", "true"),
    ("DEV002", "SITE001", "power_meter", "PM8000", "kWh", "main-panel", "true"),
    ("DEV003", "SITE002", "power_meter", "Acti9 PowerTag", "kWh", "feeder-B", "true"),
    ("DEV004", "SITE003", "ct_sensor", "PowerLogic", "A", "circuit-3", "false"),
]

_RAW_READINGS_COLS = [
    "reading_id", "device_id", "site_id", "timestamp",
    "active_power_kw", "energy_kwh", "voltage_v", "current_a", "power_factor",
]
_RAW_READINGS = [
    ("R0001", "DEV001", "SITE001", "2026-06-01 08:00:00", "120.5", "30.2", "400.1", "52.3", "0.95"),
    ("R0002", "DEV001", "SITE001", "2026-06-01 09:00:00", "135.0", "31.0", "399.8", "54.0", "0.96"),
    ("R0003", "DEV002", "SITE001", "2026-06-01 08:00:00", "80.2", "20.1", "401.0", "35.5", "0.93"),
    ("R0004", "DEV003", "SITE002", "2026-06-01 08:00:00", "210.7", "55.0", "398.5", "90.2", "0.97"),
    ("R0005", "DEV001", "SITE001", "2026-06-02 08:00:00", "118.3", "29.8", "400.5", "51.0", "0.94"),
    ("R0006", "DEV002", "SITE001", "2026-06-02 08:00:00", "82.5", "21.0", "400.9", "36.0", "0.92"),
    ("R0007", "DEV003", "SITE002", "2026-06-02 08:00:00", "205.1", "53.5", "399.0", "88.7", "0.96"),
    ("R0008", "DEV004", "SITE003", "2026-06-02 08:00:00", "40.0", "10.0", "230.0", "17.4", "0.90"),
]

_RAW_BILLING_COLS = [
    "cost_id", "site_id", "billing_period", "energy_consumed_kwh", "peak_demand_kw",
    "tariff_rate", "energy_cost", "co2_emissions_kg", "currency",
]
_RAW_BILLING = [
    ("C0001", "SITE001", "2026-06", "18500.0", "250.0", "0.145", "2682.5", "3700.0", "EUR"),
    ("C0002", "SITE002", "2026-06", "14200.0", "190.5", "0.150", "2130.0", "2840.0", "EUR"),
    ("C0003", "SITE003", "2026-06", "9800.0", "130.0", "0.120", "1176.0", "4900.0", "USD"),
]


def _write_raw(spark, bronze_id, table, rows, cols, source_file):
    df = (
        spark.createDataFrame(rows, cols)
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(source_file))
    )
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(onelake_table_path(bronze_id, table))
    )
    print(f"  seeded bronze.{table} ({len(rows)} rows)")


def seed_bronze(spark: SparkSession, bronze_id: str) -> None:
    print("\nSeeding bronze raw tables with demo data...")
    _write_raw(spark, bronze_id, "raw_site", _RAW_SITE, _RAW_SITE_COLS, "sites.csv")
    _write_raw(spark, bronze_id, "raw_device", _RAW_DEVICE, _RAW_DEVICE_COLS, "devices.csv")
    _write_raw(spark, bronze_id, "raw_meter_readings", _RAW_READINGS, _RAW_READINGS_COLS, "readings.csv")
    _write_raw(spark, bronze_id, "raw_billing", _RAW_BILLING, _RAW_BILLING_COLS, "billing.csv")
