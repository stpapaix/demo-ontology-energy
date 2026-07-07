"""Create the four energy Delta tables in each lakehouse via Spark on OneLake.

An external Spark session is authenticated to OneLake with the same service
principal (client-credentials OAuth), then an empty Delta table is written for
every table in every medallion layer. Writing is idempotent (mode='ignore'):
re-running will not overwrite tables that already contain data.
"""

from pyspark.sql import SparkSession

from auth import _require_env  # reuse env validation
from config import ONELAKE_ACCOUNT, WORKSPACE_ID
from schemas import TABLES

# Delta + Hadoop-Azure packages for a vanilla (non-Fabric) Spark runtime.
_SPARK_PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.hadoop:hadoop-azure:3.3.4"
)


def build_spark() -> SparkSession:
    """Create a Spark session configured to write Delta tables to OneLake."""
    tenant_id = _require_env("AZURE_TENANT_ID")
    client_id = _require_env("AZURE_CLIENT_ID")
    client_secret = _require_env("AZURE_CLIENT_SECRET")

    spark = (
        SparkSession.builder.appName("energy-medallion-deploy")
        .config("spark.jars.packages", _SPARK_PACKAGES)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    # Service-principal OAuth for the OneLake ABFS driver.
    conf = spark._jsc.hadoopConfiguration()
    conf.set(f"fs.azure.account.auth.type.{ONELAKE_ACCOUNT}", "OAuth")
    conf.set(
        f"fs.azure.account.oauth.provider.type.{ONELAKE_ACCOUNT}",
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
    )
    conf.set(f"fs.azure.account.oauth2.client.id.{ONELAKE_ACCOUNT}", client_id)
    conf.set(f"fs.azure.account.oauth2.client.secret.{ONELAKE_ACCOUNT}", client_secret)
    conf.set(
        f"fs.azure.account.oauth2.client.endpoint.{ONELAKE_ACCOUNT}",
        f"https://login.microsoftonline.com/{tenant_id}/oauth2/token",
    )
    return spark


def _table_path(lakehouse_id: str, table_name: str) -> str:
    return (
        f"abfss://{WORKSPACE_ID}@{ONELAKE_ACCOUNT}/"
        f"{lakehouse_id}/Tables/{table_name}"
    )


def create_tables(spark: SparkSession, layer: str, lakehouse_id: str) -> None:
    """Create every defined Delta table (empty) inside one lakehouse."""
    print(f"\nCreating tables in '{layer}' lakehouse ({lakehouse_id})...")
    for table_name, (schema, partition_cols) in TABLES.items():
        path = _table_path(lakehouse_id, table_name)
        empty_df = spark.createDataFrame(spark.sparkContext.emptyRDD(), schema)
        writer = empty_df.write.format("delta").mode("ignore")
        if partition_cols:
            writer = writer.partitionBy(*partition_cols)
        writer.save(path)
        part = f" partitioned by {partition_cols}" if partition_cols else ""
        print(f"  + {table_name}{part}")


def create_all_tables(layer_to_id: dict[str, str]) -> None:
    spark = build_spark()
    try:
        for layer, lakehouse_id in layer_to_id.items():
            create_tables(spark, layer, lakehouse_id)
    finally:
        spark.stop()


if __name__ == "__main__":
    # Allow running standalone by discovering the lakehouses first.
    from provision_lakehouses import provision_lakehouses

    create_all_tables(provision_lakehouses())
