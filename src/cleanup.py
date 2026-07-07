"""Remove existing Delta tables from a lakehouse's Tables/ area on OneLake.

Used to reset the medallion to a clean state so re-deployments don't leave
stale tables behind (the table-creation step is idempotent but never deletes).
"""

from pyspark.sql import SparkSession

from config import ONELAKE_ACCOUNT, WORKSPACE_ID


def _filesystem(spark: SparkSession, path: str):
    jvm = spark._jvm
    hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
    fs = hadoop_path.getFileSystem(spark._jsc.hadoopConfiguration())
    return fs, hadoop_path


def delete_all_tables(spark: SparkSession, lakehouse_id: str) -> None:
    """Delete every table folder under Tables/ for a lakehouse."""
    tables_root = f"abfss://{WORKSPACE_ID}@{ONELAKE_ACCOUNT}/{lakehouse_id}/Tables"
    fs, root = _filesystem(spark, tables_root)
    if not fs.exists(root):
        return
    removed = []
    for status in fs.listStatus(root):
        path = status.getPath()
        fs.delete(path, True)
        removed.append(path.getName())
    if removed:
        print(f"  reset {lakehouse_id}: removed {removed}")


def reset_lakehouses(spark: SparkSession, layer_to_id: dict[str, str]) -> None:
    print("\nResetting lakehouse tables (removing existing)...")
    for lakehouse_id in layer_to_id.values():
        delete_all_tables(spark, lakehouse_id)
