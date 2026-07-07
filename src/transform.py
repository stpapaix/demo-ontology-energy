"""Medallion transformation pipeline: bronze -> silver -> gold.

  bronze (raw, string-typed)  --clean/typecast/dedup-->  silver (conformed)
  silver (conformed facts)    --aggregate/KPIs-------->  gold (business-ready)

All reads/writes are Delta tables on OneLake. Silver/gold tables are rebuilt
with mode('overwrite') so the pipeline is idempotent.
"""

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config import onelake_table_path


def _read(spark: SparkSession, lakehouse_id: str, table: str) -> DataFrame:
    return spark.read.format("delta").load(onelake_table_path(lakehouse_id, table))


def _write(df: DataFrame, lakehouse_id: str, table: str, partition_cols=None) -> None:
    writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.save(onelake_table_path(lakehouse_id, table))
    print(f"  -> {table} ({df.count()} rows)")


def bronze_to_silver(spark: SparkSession, bronze_id: str, silver_id: str) -> None:
    """Clean, type-cast and deduplicate raw bronze data into conformed silver tables."""
    print("\n[bronze -> silver] cleaning & conforming...")

    dim_site = (
        _read(spark, bronze_id, "raw_site")
        .select(
            F.col("site_id"),
            F.col("site_name"),
            F.col("country"),
            F.col("region"),
            F.col("latitude").cast("double"),
            F.col("longitude").cast("double"),
            F.col("site_type"),
            F.col("contracted_power_kw").cast("double"),
            F.to_date("commissioned_date").alias("commissioned_date"),
        )
        .filter(F.col("site_id").isNotNull())
        .dropDuplicates(["site_id"])
    )
    _write(dim_site, silver_id, "dim_site")

    dim_device = (
        _read(spark, bronze_id, "raw_device")
        .select(
            F.col("device_id"),
            F.col("site_id"),
            F.col("device_type"),
            F.col("model"),
            F.col("measurement_unit"),
            F.col("install_location"),
            (F.lower(F.col("is_active")) == "true").alias("is_active"),
        )
        .filter(F.col("device_id").isNotNull())
        .dropDuplicates(["device_id"])
    )
    _write(dim_device, silver_id, "dim_device")

    fact_consumption = (
        _read(spark, bronze_id, "raw_meter_readings")
        .select(
            F.col("reading_id"),
            F.col("device_id"),
            F.col("site_id"),
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
    _write(fact_consumption, silver_id, "fact_energy_consumption", ["reading_date"])

    fact_cost = (
        _read(spark, bronze_id, "raw_billing")
        .select(
            F.col("cost_id"),
            F.col("site_id"),
            F.col("billing_period"),
            F.col("energy_consumed_kwh").cast("double"),
            F.col("peak_demand_kw").cast("double"),
            F.col("tariff_rate").cast("double"),
            F.col("energy_cost").cast("double"),
            F.col("co2_emissions_kg").cast("double"),
            F.col("currency"),
        )
        .filter(F.col("cost_id").isNotNull())
        .dropDuplicates(["cost_id"])
    )
    _write(fact_cost, silver_id, "fact_energy_cost")


def silver_to_gold(spark: SparkSession, silver_id: str, gold_id: str) -> None:
    """Aggregate conformed silver facts into business-ready gold tables & KPIs."""
    print("\n[silver -> gold] aggregating & computing KPIs...")

    agg_daily = (
        _read(spark, silver_id, "fact_energy_consumption")
        .groupBy("site_id", "reading_date")
        .agg(
            F.sum("energy_kwh").alias("total_energy_kwh"),
            F.avg("active_power_kw").alias("avg_active_power_kw"),
            F.avg("power_factor").alias("avg_power_factor"),
            F.count("*").alias("reading_count"),
        )
    )
    _write(agg_daily, gold_id, "agg_daily_consumption_by_site")

    cost = _read(spark, silver_id, "fact_energy_cost")
    site_region = _read(spark, silver_id, "dim_site").select("site_id", "region")
    kpi_co2 = (
        cost.join(site_region, on="site_id", how="left")
        .groupBy("region", "billing_period")
        .agg(
            F.sum("co2_emissions_kg").alias("total_co2_kg"),
            F.sum("energy_consumed_kwh").alias("total_energy_kwh"),
            F.sum("energy_cost").alias("total_cost"),
        )
    )
    _write(kpi_co2, gold_id, "kpi_co2_by_region")


def run_transformations(spark: SparkSession, layer_to_id: dict[str, str]) -> None:
    bronze_to_silver(spark, layer_to_id["bronze"], layer_to_id["silver"])
    silver_to_gold(spark, layer_to_id["silver"], layer_to_id["gold"])


if __name__ == "__main__":
    from provision_lakehouses import provision_lakehouses
    from spark_utils import build_spark

    spark_session = build_spark()
    try:
        run_transformations(spark_session, provision_lakehouses())
    finally:
        spark_session.stop()
