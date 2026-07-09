"""Create the per-layer Delta tables in each lakehouse via Spark on OneLake.

Each medallion layer has its own set of tables (see schemas.LAYER_TABLES).
Empty Delta tables are written so the structure exists before any data lands.
Writing is idempotent (mode='ignore'): existing tables with data are untouched.
"""

from pyspark.sql import SparkSession

from config import onelake_table_path
from schemas import LAYER_TABLES


def create_tables(spark: SparkSession, layer: str, lakehouse_id: str) -> None:
    """Create every Delta table defined for one layer inside its lakehouse."""
    print(f"\nCreating '{layer}' tables ({lakehouse_id})...")
    for table_name, (schema, partition_cols) in LAYER_TABLES[layer].items():
        path = onelake_table_path(lakehouse_id, table_name)
        empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        writer = empty_df.write.format("delta").mode("append").option("mergeSchema", "true")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(path)
        part = f" partitioned by {partition_cols}" if partition_cols else ""
        print(f"  + {table_name}{part}")


def create_all_tables(spark: SparkSession, layer_to_id: dict[str, str]) -> None:
    for layer, lakehouse_id in layer_to_id.items():
        create_tables(spark, layer, lakehouse_id)


if __name__ == "__main__":
    from provision_lakehouses import provision_lakehouses
    from spark_utils import build_spark

    spark_session = build_spark()
    try:
        create_all_tables(spark_session, provision_lakehouses())
    finally:
        spark_session.stop()
