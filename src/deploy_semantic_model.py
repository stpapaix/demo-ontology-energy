"""Deploy a Direct Lake Power BI semantic model mirroring the ontology design.

Same entities and relationships as energy_ontology, but implemented as a semantic
model over the silver lakehouse (Direct Lake). Tables map 1:1 to silver Delta
tables; relationships follow the ontology (Site->Region, Device->Site, facts->dims).
Built via the /workspaces/{id}/semanticModels REST API using a TMSL model.bim.
"""

import uuid

import requests

from config import LAKEHOUSE_NAMES, WORKSPACE_ID
from deploy_items import FABRIC_API, _TIMEOUT, _b64, _create_or_update, _headers
from provision_lakehouses import list_lakehouses

DISPLAY_NAME = "energy_semantic_model"
SILVER = LAKEHOUSE_NAMES["silver"]
_NS = uuid.NAMESPACE_URL

# table -> [(column, dataType, is_key)]
TABLES = {
    "dim_region": [
        ("region_id", "string", True), ("region_code", "string", False), ("region_name", "string", False),
    ],
    "dim_site": [
        ("site_id", "string", True), ("site_name", "string", False), ("country", "string", False),
        ("region", "string", False), ("region_id", "string", False), ("latitude", "double", False),
        ("longitude", "double", False), ("site_type", "string", False), ("contracted_power_kw", "double", False),
        ("commissioned_date", "dateTime", False), ("total_energy_kwh", "double", False),
        ("total_energy_cost", "double", False), ("total_co2_kg", "double", False),
    ],
    "dim_device": [
        ("device_id", "string", True), ("site_id", "string", False), ("device_type", "string", False),
        ("model", "string", False), ("measurement_unit", "string", False), ("install_location", "string", False),
        ("is_active", "boolean", False),
    ],
    "fact_energy_consumption": [
        ("reading_id", "string", False), ("device_id", "string", False), ("site_id", "string", False),
        ("timestamp", "dateTime", False), ("active_power_kw", "double", False), ("energy_kwh", "double", False),
        ("voltage_v", "double", False), ("current_a", "double", False), ("power_factor", "double", False),
        ("reading_date", "dateTime", False),
    ],
    "fact_energy_cost": [
        ("cost_id", "string", False), ("site_id", "string", False), ("billing_period", "string", False),
        ("energy_consumed_kwh", "double", False), ("peak_demand_kw", "double", False),
        ("tariff_rate", "double", False), ("energy_cost", "double", False),
        ("co2_emissions_kg", "double", False), ("currency", "string", False),
    ],
}

# Columns hidden from report authors: surrogate/foreign keys, the redundant dim_site[region]
# text (use dim_region[region_name] instead), and the pre-aggregated dim_site snapshot columns
# (the measures below are the live source of truth and never drift from the facts).
HIDDEN_COLUMNS = {
    ("dim_region", "region_id"),
    ("dim_site", "site_id"), ("dim_site", "region_id"), ("dim_site", "region"),
    ("dim_site", "total_energy_kwh"), ("dim_site", "total_energy_cost"), ("dim_site", "total_co2_kg"),
    ("dim_device", "device_id"), ("dim_device", "site_id"),
    ("fact_energy_consumption", "reading_id"), ("fact_energy_consumption", "device_id"),
    ("fact_energy_consumption", "site_id"),
    ("fact_energy_cost", "cost_id"), ("fact_energy_cost", "site_id"),
}

# Geo columns tagged so Power BI map visuals bind correctly.
DATA_CATEGORY = {
    ("dim_site", "latitude"): "Latitude",
    ("dim_site", "longitude"): "Longitude",
    ("dim_site", "country"): "Country",
}

# (fromTable[many], fromColumn, toTable[one], toColumn, isActive) - mirrors the ontology relationships.
# dim_device->dim_site is inactive: fact_energy_consumption already reaches dim_site directly, so an
# active snowflake link would create an ambiguous path (fact->device->site vs fact->site).
RELATIONSHIPS = [
    ("dim_site", "region_id", "dim_region", "region_id", True),
    ("dim_device", "site_id", "dim_site", "site_id", False),
    ("fact_energy_consumption", "site_id", "dim_site", "site_id", True),
    ("fact_energy_consumption", "device_id", "dim_device", "device_id", True),
    ("fact_energy_cost", "site_id", "dim_site", "site_id", True),
]

# table -> [(measure name, DAX, format string)]
MEASURES = {
    "fact_energy_cost": [
        ("Total Energy Cost", "SUM('fact_energy_cost'[energy_cost])", "#,##0.00"),
        ("Total CO2 (kg)", "SUM('fact_energy_cost'[co2_emissions_kg])", "#,##0"),
        ("Cost per kWh", "DIVIDE([Total Energy Cost], [Total Energy (kWh)])", "#,##0.000"),
        ("CO2 Intensity (kg/kWh)", "DIVIDE([Total CO2 (kg)], [Total Energy (kWh)])", "#,##0.000"),
    ],
    "fact_energy_consumption": [
        ("Total Energy (kWh)", "SUM('fact_energy_consumption'[energy_kwh])", "#,##0.00"),
        ("Avg Power Factor", "AVERAGE('fact_energy_consumption'[power_factor])", "#,##0.00"),
    ],
    "dim_site": [
        ("Site Count", "COUNTROWS('dim_site')", "#,##0"),
    ],
}


def _lineage(*parts: str) -> str:
    return str(uuid.uuid5(_NS, "::".join(parts)))


def _part(path: str, obj) -> dict:
    return {"path": path, "payload": _b64(obj), "payloadType": "InlineBase64"}


def _silver_sql_endpoint() -> tuple[str, str]:
    """Return (connectionString, databaseName) for the silver lakehouse SQL endpoint."""
    lakehouse = next(l for l in list_lakehouses() if l["displayName"] == SILVER)
    resp = requests.get(
        f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/lakehouses/{lakehouse['id']}",
        headers=_headers(), timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    sql = resp.json().get("properties", {}).get("sqlEndpointProperties", {})
    connection = sql.get("connectionString")
    if not connection:
        raise RuntimeError("Silver lakehouse SQL endpoint is not provisioned yet.")
    return connection, SILVER


def _table(table: str) -> dict:
    columns = []
    for name, dtype, is_key in TABLES[table]:
        column = {
            "name": name, "dataType": dtype, "sourceColumn": name,
            "summarizeBy": "none", "lineageTag": _lineage("col", table, name),
        }
        if is_key:
            column["isKey"] = True
        if (table, name) in HIDDEN_COLUMNS:
            column["isHidden"] = True
        if (table, name) in DATA_CATEGORY:
            column["dataCategory"] = DATA_CATEGORY[(table, name)]
        columns.append(column)

    obj = {
        "name": table,
        "lineageTag": _lineage("table", table),
        "columns": columns,
        "partitions": [{
            "name": f"{table}-partition",
            "mode": "directLake",
            "source": {
                "type": "entity", "entityName": table,
                "schemaName": "dbo", "expressionSource": "DatabaseQuery",
            },
        }],
    }
    if table in MEASURES:
        obj["measures"] = [
            {"name": name, "expression": dax, "formatString": fmt, "lineageTag": _lineage("measure", table, name)}
            for name, dax, fmt in MEASURES[table]
        ]
    return obj


def _model_bim(connection: str, database: str) -> dict:
    return {
        "compatibilityLevel": 1604,
        "model": {
            "culture": "en-US",
            "defaultPowerBIDataSourceVersion": "powerBI_V3",
            "discourageImplicitMeasures": True,
            "expressions": [{
                "name": "DatabaseQuery",
                "kind": "m",
                "expression": [
                    "let",
                    f'    database = Sql.Database("{connection}", "{database}")',
                    "in",
                    "    database",
                ],
                "lineageTag": _lineage("expr", "DatabaseQuery"),
            }],
            "tables": [_table(table) for table in TABLES],
            "relationships": [
                {
                    "name": _lineage("rel", ft, fc, tt, tc),
                    "fromTable": ft, "fromColumn": fc, "toTable": tt, "toColumn": tc,
                    **({} if active else {"isActive": False}),
                }
                for ft, fc, tt, tc, active in RELATIONSHIPS
            ],
        },
    }


def deploy_semantic_model() -> None:
    connection, database = _silver_sql_endpoint()
    definition = {
        "parts": [
            _part(".platform", {"metadata": {"type": "SemanticModel", "displayName": DISPLAY_NAME}}),
            _part("definition.pbism", {
                "$schema": "https://developer.microsoft.com/json-schemas/fabric/item/semanticModel/definitionProperties/1.0.0/schema.json",
                "version": "5.0",
                "settings": {},
            }),
            _part("model.bim", _model_bim(connection, database)),
        ]
    }
    print("\nDeploying Direct Lake semantic model...")
    _create_or_update("semanticModels", DISPLAY_NAME, definition)


if __name__ == "__main__":
    deploy_semantic_model()
