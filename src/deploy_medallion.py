"""Deploy the full energy medallion: provision lakehouses, then create Delta tables."""

from create_delta_tables import create_all_tables
from provision_lakehouses import provision_lakehouses


def main() -> None:
    print("=== Deploying Schneider Electric energy medallion ===\n")
    layer_to_id = provision_lakehouses()
    create_all_tables(layer_to_id)
    print("\n=== Deployment complete ===")


if __name__ == "__main__":
    main()
