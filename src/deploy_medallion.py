"""Deploy the full energy medallion end to end.

Steps:
  1. Provision the bronze / silver / gold lakehouses (Fabric REST API).
  2. Optionally reset existing tables for a clean state (RESET_TABLES=true).
  3. Create the per-layer Delta tables (empty structure).
  4. Optionally seed the bronze layer with demo data (SEED_DEMO_DATA=true).
  5. Run the transformation pipeline bronze -> silver -> gold.
"""

import os

from cleanup import reset_lakehouses
from create_delta_tables import create_all_tables
from provision_lakehouses import provision_lakehouses
from seed_bronze import seed_bronze
from spark_utils import build_spark
from transform import run_transformations


def main() -> None:
    print("=== Deploying Schneider Electric energy medallion ===\n")
    layer_to_id = provision_lakehouses()

    spark = build_spark()
    try:
        if os.environ.get("RESET_TABLES", "false").lower() == "true":
            reset_lakehouses(spark, layer_to_id)

        create_all_tables(spark, layer_to_id)

        if os.environ.get("SEED_DEMO_DATA", "true").lower() == "true":
            seed_bronze(spark, layer_to_id["bronze"])

        run_transformations(spark, layer_to_id)
    finally:
        spark.stop()

    print("\n=== Deployment complete ===")


if __name__ == "__main__":
    main()
