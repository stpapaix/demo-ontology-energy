"""Deploy the energy medallion structure end to end.

Steps:
  1. Provision the bronze / silver / gold lakehouses (Fabric REST API).
  2. Optionally reset existing tables for a clean state (RESET_TABLES=true).
  3. Create the per-layer Delta tables as EMPTY structure.
  4. Deploy the Fabric notebooks and data pipelines (Fabric REST API).

Data loading and transformation are NOT run here: they are performed by the
manually-run Fabric notebooks (seed) and data pipelines (bronze->silver->gold).
"""

import os

from cleanup import reset_lakehouses
from create_delta_tables import create_all_tables
from deploy_items import deploy_items
from provision_lakehouses import provision_lakehouses
from spark_utils import build_spark


def main() -> None:
    print("=== Deploying Schneider Electric energy medallion ===\n")
    layer_to_id = provision_lakehouses()

    spark = build_spark()
    try:
        if os.environ.get("RESET_TABLES", "false").lower() == "true":
            reset_lakehouses(spark, layer_to_id)
        create_all_tables(spark, layer_to_id)
    finally:
        spark.stop()

    deploy_items()

    print("\n=== Deployment complete: empty tables + notebooks + pipelines ===")


if __name__ == "__main__":
    main()
