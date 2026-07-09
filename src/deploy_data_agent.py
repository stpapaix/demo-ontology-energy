"""Deploy a Fabric Data Agent grounded on the energy_ontology (Fabric IQ).

The agent uses the **ontology** item as its data source, so business users can ask
natural-language questions ("total energy cost by region?", "which site emits the
most CO2?") answered over the ontology's entities and relationships rather than
raw tables. Built via the /workspaces/{id}/dataAgents REST API.
"""

from config import WORKSPACE_ID
from deploy_items import _b64, _create_or_update, _list_items

DISPLAY_NAME = "energy_data_agent"
ONTOLOGY_NAME = "energy_ontology"

AI_INSTRUCTIONS = (
    "You are an energy analytics assistant for Schneider Electric. Answer business "
    "questions about energy consumption, cost and CO2 using the energy_ontology.\n"
    "Entities: Region, Site, Device, EnergyReading, BillingRecord.\n"
    "Relationships: Site locatedIn Region; Site hasDevice Device; Device produces "
    "EnergyReading; Site billedFor BillingRecord.\n"
    "Site carries per-site totals: total_energy_kwh, total_energy_cost, total_co2_kg "
    "- prefer these for site-level questions and aggregate up to Region via locatedIn.\n"
    "Units: energy in kWh, cost in the row currency, CO2 in kg. Give clear, rounded "
    "numbers at site or region level."
)

DATASOURCE_INSTRUCTIONS = (
    "Reason over the ontology entities and their relationships. Use the Site "
    "total_* properties for per-site cost/energy/CO2, and roll up to Region."
)


def _part(path: str, obj) -> dict:
    return {"path": path, "payload": _b64(obj), "payloadType": "InlineBase64"}


def _ontology_id() -> str:
    for item in _list_items("ontologies"):
        if item["displayName"] == ONTOLOGY_NAME:
            return item["id"]
    raise RuntimeError(f"Ontology '{ONTOLOGY_NAME}' not found - deploy it first.")


def _datasource(ontology_id: str) -> dict:
    return {
        "$schema": "1.0.0",
        "artifactId": ontology_id,
        "workspaceId": WORKSPACE_ID.lower(),
        "displayName": ONTOLOGY_NAME,
        "type": "ontology",
        "userDescription": "Schneider Electric energy ontology (Fabric IQ).",
        "dataSourceInstructions": DATASOURCE_INSTRUCTIONS,
    }


def _build_definition(ontology_id: str) -> dict:
    datasource = _datasource(ontology_id)
    stage_config = {"$schema": "1.0.0", "aiInstructions": AI_INSTRUCTIONS}
    ds_dir = f"ontology-{ONTOLOGY_NAME}"
    return {
        "parts": [
            _part("Files/Config/data_agent.json", {"$schema": "2.1.0"}),
            _part("Files/Config/draft/stage_config.json", stage_config),
            _part(f"Files/Config/draft/{ds_dir}/datasource.json", datasource),
            _part("Files/Config/published/stage_config.json", stage_config),
            _part(f"Files/Config/published/{ds_dir}/datasource.json", datasource),
            _part("Files/Config/publish_info.json", {
                "$schema": "1.0.0",
                "description": "Energy data agent grounded on the Fabric IQ energy ontology.",
            }),
        ]
    }


def deploy_data_agent() -> None:
    ontology_id = _ontology_id()
    definition = _build_definition(ontology_id)
    print("\nDeploying Fabric Data Agent (grounded on energy_ontology)...")
    _create_or_update("dataAgents", DISPLAY_NAME, definition)


if __name__ == "__main__":
    deploy_data_agent()
