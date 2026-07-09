"""Deploy a Fabric Data Agent grounded on the energy_semantic_model (Direct Lake).

Unlike the ontology-grounded agent, a semantic model is a first-class Data Agent
data source (type "semantic_model"). Business users can ask natural-language
questions ("total energy cost by region?", "CO2 intensity per site?") that the
agent answers with DAX over the model's tables, relationships and measures.
Built via the /workspaces/{id}/dataAgents REST API.
"""

from config import WORKSPACE_ID
from deploy_items import _b64, _create_or_update, _list_items

DISPLAY_NAME = "energy_semantic_data_agent"
MODEL_NAME = "energy_semantic_model"
DATASOURCE_TYPE = "semantic_model"

AI_INSTRUCTIONS = (
    "You are an energy analytics assistant for Schneider Electric. Answer business "
    "questions about energy consumption, cost and CO2 using the energy_semantic_model.\n"
    "Tables: dim_region, dim_site, dim_device (dimensions); fact_energy_consumption "
    "and fact_energy_cost (facts).\n"
    "Relationships: dim_site -> dim_region (region_id); fact_energy_consumption -> "
    "dim_site and -> dim_device; fact_energy_cost -> dim_site. The dim_device -> "
    "dim_site link is inactive, so facts filter dim_site directly.\n"
    "Prefer the model measures instead of raw sums: 'Total Energy (kWh)', "
    "'Total Energy Cost', 'Total CO2 (kg)', 'Cost per kWh', 'CO2 Intensity (kg/kWh)', "
    "'Avg Power Factor', 'Site Count'.\n"
    "Slice by dim_region[region_name], dim_site[site_name]/[country], "
    "dim_device[device_type]. Units: energy in kWh, cost in the row currency, CO2 in "
    "kg. Give clear, rounded numbers."
)

DATASOURCE_INSTRUCTIONS = (
    "Query the star schema with the predefined measures. Aggregate facts up to site "
    "via the direct relationships, and to region via dim_site -> dim_region. Use "
    "'Cost per kWh' and 'CO2 Intensity (kg/kWh)' for efficiency questions."
)


def _part(path: str, obj) -> dict:
    return {"path": path, "payload": _b64(obj), "payloadType": "InlineBase64"}


def _semantic_model_id() -> str:
    for item in _list_items("semanticModels"):
        if item["displayName"] == MODEL_NAME:
            return item["id"]
    raise RuntimeError(f"Semantic model '{MODEL_NAME}' not found - deploy it first.")


def _datasource(model_id: str) -> dict:
    return {
        "$schema": "1.0.0",
        "artifactId": model_id,
        "workspaceId": WORKSPACE_ID.lower(),
        "displayName": MODEL_NAME,
        "type": DATASOURCE_TYPE,
        "userDescription": "Schneider Electric energy semantic model (Direct Lake over silver).",
        "dataSourceInstructions": DATASOURCE_INSTRUCTIONS,
    }


def _build_definition(model_id: str) -> dict:
    datasource = _datasource(model_id)
    stage_config = {"$schema": "1.0.0", "aiInstructions": AI_INSTRUCTIONS}
    ds_dir = f"{DATASOURCE_TYPE}-{MODEL_NAME}"
    return {
        "parts": [
            _part("Files/Config/data_agent.json", {"$schema": "2.1.0"}),
            _part("Files/Config/draft/stage_config.json", stage_config),
            _part(f"Files/Config/draft/{ds_dir}/datasource.json", datasource),
            _part("Files/Config/published/stage_config.json", stage_config),
            _part(f"Files/Config/published/{ds_dir}/datasource.json", datasource),
            _part("Files/Config/publish_info.json", {
                "$schema": "1.0.0",
                "description": "Energy data agent grounded on the Direct Lake semantic model.",
            }),
        ]
    }


def deploy_data_agent_semantic() -> None:
    model_id = _semantic_model_id()
    definition = _build_definition(model_id)
    print("\nDeploying Fabric Data Agent (grounded on energy_semantic_model)...")
    _create_or_update("dataAgents", DISPLAY_NAME, definition)


if __name__ == "__main__":
    deploy_data_agent_semantic()
