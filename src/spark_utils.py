"""Shared Spark session configured to read/write Delta tables on OneLake.

Uses the same service principal (client-credentials OAuth) as the REST client.
"""

from pyspark.sql import SparkSession

from auth import _require_env
from config import ONELAKE_ACCOUNT

# Delta + Hadoop-Azure packages for a vanilla (non-Fabric) Spark runtime.
_SPARK_PACKAGES = (
    "io.delta:delta-spark_2.12:3.2.0,"
    "org.apache.hadoop:hadoop-azure:3.3.4"
)


def build_spark() -> SparkSession:
    """Create a Spark session authenticated to OneLake with the service principal."""
    tenant_id = _require_env("AZURE_TENANT_ID")
    client_id = _require_env("AZURE_CLIENT_ID")
    client_secret = _require_env("AZURE_CLIENT_SECRET")

    spark = (
        SparkSession.builder.appName("energy-medallion")
        .config("spark.jars.packages", _SPARK_PACKAGES)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

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
