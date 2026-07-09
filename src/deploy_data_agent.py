"""Deploy a Fabric Data Agent that answers business questions over the energy model.

The agent is grounded on the silver lakehouse (the same conformed tables the
ontology binds to). It ships AI instructions describing the entities and joins,
selects the relevant tables/columns, and includes few-shot question -> SQL
examples so business users get accurate answers.
"""

import json
import os
import uuid

from config import LAKEHOUSE_NAMES, WORKSPACE_ID
from deploy_items import _b64, _create_or_update
from deploy_ontology import _silver_lakehouse_id

_SPEC_PATH = os.path.join(os.path.dirname(__file__), "..", "ontology", "energy_ontology.json")

DISPLAY_NAME = "energy_data_agent"
DATASOURCE_NAME = LAKEHOUSE_NAMES["silver"]  # lh_silver

AI_INSTRUCTIONS = (
    "You are an energy analytics assistant for Schneider Electric. Answer business "
    "questions about energy consumption, cost and CO2 emissions using the silver "
    "lakehouse tables.\n"
    "Entities & tables: Region=dim_region, Site=dim_site, Device=dim_device, "
    "EnergyReading=fact_energy_consumption, BillingRecord=fact_energy_cost.\n"
    "Joins: dim_site.region_id = dim_region.region_id; dim_device.site_id = "
    "dim_site.site_id; fact_energy_consumption.site_id/device_id; "
    "fact_energy_cost.site_id = dim_site.site_id.\n"
    "dim_site already carries per-site totals: total_energy_kwh, total_energy_cost, "
    "total_co2_kg - prefer these for site-level questions.\n"
    "Units: energy in kWh, cost in the row currency, CO2 in kg. Prefer site or "
    "region level aggregation and present clear, rounded numbers."
)

DATASOURCE_INSTRUCTIONS = (
    "Conformed silver star model. Use dim_site for site attributes and per-site "
    "totals (total_energy_cost, total_co2_kg, total_energy_kwh). Aggregate facts "
    "by joining to the dimensions on the *_id keys."
)


def _guid(*parts: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "::".join(parts)))


def _part(path: str, obj) -> dict:
    return {"path": path, "payload": _b64(obj), "payloadType": "InlineBase64"}


def _elements(model: dict) -> list:
    tables = []
    for entity in model["entities"]:
        binding = entity["bindings"][0]
        table = binding["table"]
        tables.append({
            "id": _guid("table", table),
            "display_name": table,
            "type": "lakehouse_tables.table",
            "is_selected": True,
            "children": [
                {
                    "id": _guid("col", table, col),
                    "display_name": col,
                    "type": "lakehouse_tables.column",
                    "is_selected": True,
                }
                for col in binding["columns"]
            ],
        })
    return [{
        "id": _guid("schema", "dbo"),
        "display_name": "dbo",
        "type": "lakehouse_tables.schema",
        "is_selected": True,
        "children": tables,
    }]


def _datasource(model: dict, silver_id: str) -> dict:
    return {
        "$schema": "1.0.0",
        "artifactId": silver_id,
        "workspaceId": WORKSPACE_ID.lower(),
        "displayName": DATASOURCE_NAME,
        "type": "lakehouse_tables",
        "userDescription": "Conformed silver model for Schneider Electric energy analytics.",
        "dataSourceInstructions": DATASOURCE_INSTRUCTIONS,
        "elements": _elements(model),
    }


def _fewshots() -> dict:
    examples = [
        ("What is the total energy cost by region?",
         "SELECT r.region_name, SUM(c.energy_cost) AS total_cost "
         "FROM fact_energy_cost c "
         "JOIN dim_site s ON c.site_id = s.site_id "
         "JOIN dim_region r ON s.region_id = r.region_id "
         "GROUP BY r.region_name ORDER BY total_cost DESC"),
        ("Which site has the highest CO2 emissions?",
         "SELECT TOP 1 site_name, total_co2_kg FROM dim_site ORDER BY total_co2_kg DESC"),
        ("Show total energy consumption per site.",
         "SELECT site_name, total_energy_kwh FROM dim_site ORDER BY total_energy_kwh DESC"),
        ("How many active devices does each site have?",
         "SELECT s.site_name, COUNT(*) AS device_count "
         "FROM dim_device d JOIN dim_site s ON d.site_id = s.site_id "
         "WHERE d.is_active = 1 GROUP BY s.site_name ORDER BY device_count DESC"),
        ("What is the total CO2 by region for billing period 2026-06?",
         "SELECT r.region_name, SUM(c.co2_emissions_kg) AS total_co2 "
         "FROM fact_energy_cost c "
         "JOIN dim_site s ON c.site_id = s.site_id "
         "JOIN dim_region r ON s.region_id = r.region_id "
         "WHERE c.billing_period = '2026-06' GROUP BY r.region_name ORDER BY total_co2 DESC"),
    ]
    return {
        "$schema": "1.0.0",
        "fewShots": [
            {"id": _guid("fewshot", question), "question": question, "query": query}
            for question, query in examples
        ],
    }


def _build_definition(model: dict, silver_id: str) -> dict:
    datasource = _datasource(model, silver_id)
    fewshots = _fewshots()
    stage_config = {"$schema": "1.0.0", "aiInstructions": AI_INSTRUCTIONS}
    ds_dir = f"lakehouse-{DATASOURCE_NAME}"

    parts = [
        _part("Files/Config/data_agent.json", {"$schema": "2.1.0"}),
        _part("Files/Config/draft/stage_config.json", stage_config),
        _part(f"Files/Config/draft/{ds_dir}/datasource.json", datasource),
        _part(f"Files/Config/draft/{ds_dir}/fewshots.json", fewshots),
        _part("Files/Config/published/stage_config.json", stage_config),
        _part(f"Files/Config/published/{ds_dir}/datasource.json", datasource),
        _part(f"Files/Config/published/{ds_dir}/fewshots.json", fewshots),
        _part("Files/Config/publish_info.json", {
            "$schema": "1.0.0",
            "description": "Energy data agent grounded on the silver conformed model.",
        }),
    ]
    return {"parts": parts}


def deploy_data_agent() -> None:
    with open(_SPEC_PATH, encoding="utf-8") as handle:
        model = json.load(handle)
    silver_id = _silver_lakehouse_id()
    definition = _build_definition(model, silver_id)
    print("\nDeploying Fabric Data Agent...")
    _create_or_update("dataAgents", DISPLAY_NAME, definition)


if __name__ == "__main__":
    deploy_data_agent()
