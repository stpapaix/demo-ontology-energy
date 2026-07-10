"""Deploy a Fabric IQ Ontology item bound to the silver lakehouse tables.

Reads ../ontology/energy_ontology.json and builds the multi-part ontology
definition (EntityTypes + DataBindings + RelationshipTypes + Contextualizations),
then creates/updates the Ontology item via the Fabric REST API. No data is copied;
the ontology sits on top of the silver conformed model.
"""

import hashlib
import json
import os
import uuid

import requests

from config import LAKEHOUSE_NAMES, WORKSPACE_ID
from deploy_items import FABRIC_API, _TIMEOUT, _b64, _create_or_update, _headers
from provision_lakehouses import list_lakehouses

_SPEC_PATH = os.path.join(os.path.dirname(__file__), "..", "ontology", "energy_ontology.json")

# Candidate job types for triggering an on-demand graph refresh. The graph-refresh
# job type is not documented in the public REST API, so we try a short ordered list
# and stop at the first that the service accepts. All failures are non-fatal.
_GRAPH_REFRESH_JOB_TYPES = ("Refresh", "GraphRefresh", "RefreshGraph", "DefaultJob")


def _bigint(*parts: str) -> str:
    """Deterministic positive 64-bit id (stable across re-deploys)."""
    digest = hashlib.sha256("::".join(parts).encode()).digest()
    value = int.from_bytes(digest[:8], "big") & 0x7FFFFFFFFFFFFFFF
    return str(value or 1)


def _guid(*parts: str) -> str:
    return str(uuid.uuid5(uuid.NAMESPACE_URL, "::".join(parts)))


def _part(path: str, obj) -> dict:
    return {"path": path, "payload": _b64(obj), "payloadType": "InlineBase64"}


def _silver_lakehouse_id() -> str:
    name = LAKEHOUSE_NAMES["silver"]
    for lakehouse in list_lakehouses():
        if lakehouse["displayName"] == name:
            return lakehouse["id"]
    raise RuntimeError(f"Silver lakehouse '{name}' not found - deploy it first.")


def _build_definition(model: dict, silver_id: str) -> dict:
    workspace = WORKSPACE_ID.lower()
    schema = model.get("sourceSchema", "dbo")

    entities: dict[str, dict] = {}
    for entity in model["entities"]:
        entity_id = _bigint("entity", entity["name"])
        prop_ids: dict[str, str] = {}
        props: list[dict] = []
        for prop in entity["properties"]:
            prop_id = _bigint("prop", entity["name"], prop["name"])
            prop_ids[prop["name"]] = prop_id
            props.append({
                "id": prop_id, "name": prop["name"], "redefines": None,
                "baseTypeNamespaceType": None, "valueType": prop["valueType"],
            })
        display_name = entity.get("displayNameProperty", entity["key"])
        entities[entity["name"]] = {
            "id": entity_id, "prop_ids": prop_ids, "props": props,
            "key_prop": prop_ids[entity["key"]], "display_prop": prop_ids[display_name],
            "entity": entity,
        }

    parts = [
        _part(".platform", {"metadata": {"type": "Ontology", "displayName": model["displayName"]}}),
        _part("definition.json", {}),
    ]

    for entity in model["entities"]:
        info = entities[entity["name"]]
        parts.append(_part(f"EntityTypes/{info['id']}/definition.json", {
            "id": info["id"], "namespace": "usertypes", "baseEntityTypeId": None,
            "name": entity["name"], "entityIdParts": [info["key_prop"]],
            "displayNamePropertyId": info["display_prop"],
            "namespaceType": "Custom", "visibility": "Visible", "properties": info["props"],
        }))
        for binding in entity["bindings"]:
            binding_guid = _guid("binding", entity["name"], binding["table"])
            parts.append(_part(f"EntityTypes/{info['id']}/DataBindings/{binding_guid}.json", {
                "id": binding_guid,
                "dataBindingConfiguration": {
                    "dataBindingType": "NonTimeSeries",
                    "propertyBindings": [
                        {"sourceColumnName": col, "targetPropertyId": info["prop_ids"][col]}
                        for col in binding["columns"]
                    ],
                    "sourceTableProperties": {
                        "sourceType": "LakehouseTable", "workspaceId": workspace, "itemId": silver_id,
                        "sourceTableName": binding["table"], "sourceSchema": schema,
                    },
                },
            }))

    for rel in model["relationships"]:
        rel_id = _bigint("rel", rel["name"])
        source, target = entities[rel["source"]], entities[rel["target"]]
        parts.append(_part(f"RelationshipTypes/{rel_id}/definition.json", {
            "namespace": "usertypes", "id": rel_id, "name": rel["name"], "namespaceType": "Custom",
            "source": {"entityTypeId": source["id"]}, "target": {"entityTypeId": target["id"]},
        }))
        ctx_guid = _guid("ctx", rel["name"])
        parts.append(_part(f"RelationshipTypes/{rel_id}/Contextualizations/{ctx_guid}.json", {
            "id": ctx_guid,
            "dataBindingTable": {
                "workspaceId": workspace, "itemId": silver_id,
                "sourceTableName": rel["table"], "sourceSchema": schema, "sourceType": "LakehouseTable",
            },
            "sourceKeyRefBindings": [{"sourceColumnName": rel["sourceKeyColumn"], "targetPropertyId": source["key_prop"]}],
            "targetKeyRefBindings": [{"sourceColumnName": rel["targetKeyColumn"], "targetPropertyId": target["key_prop"]}],
        }))

    return {"parts": parts}


def deploy_ontology() -> None:
    with open(_SPEC_PATH, encoding="utf-8") as handle:
        model = json.load(handle)
    silver_id = _silver_lakehouse_id()
    definition = _build_definition(model, silver_id)
    print("\nDeploying Fabric IQ ontology...")
    _create_or_update("ontologies", model["displayName"], definition)
    refresh_graph_model(model["displayName"])


def _find_graph_item(ontology_name: str) -> dict | None:
    """Return the Graph child item auto-created for the ontology, if present.

    The ontology creates a companion graph item named like '<ontology>_graph_<guid>'.
    """
    resp = requests.get(f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/items", headers=_headers(), timeout=_TIMEOUT)
    resp.raise_for_status()
    prefix = f"{ontology_name}_graph"
    for item in resp.json().get("value", []):
        name = item.get("displayName", "")
        item_type = (item.get("type") or "").lower()
        if name.startswith(prefix) or ("graph" in item_type and ontology_name in name):
            return item
    return None


def refresh_graph_model(ontology_name: str) -> None:
    """Best-effort trigger of the graph ingestion job (populates nodes/edges).

    Deploying the ontology via REST does not fire the graph refresh that the portal
    schema editor does, so the companion graph stays empty. We try to start an
    on-demand refresh job; any failure is non-fatal and prints manual guidance.
    """
    try:
        graph = _find_graph_item(ontology_name)
    except requests.RequestException as exc:
        print(f"  ! Could not list items to find the graph model: {exc}")
        graph = None

    if not graph:
        print("  ! Graph child item not found yet; refresh it manually once it exists "
              "(workspace -> graph model -> ... -> Schedule -> Refresh now).")
        return

    graph_id = graph["id"]
    for job_type in _GRAPH_REFRESH_JOB_TYPES:
        url = f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/items/{graph_id}/jobs/instances?jobType={job_type}"
        try:
            resp = requests.post(url, headers=_headers(), timeout=_TIMEOUT)
        except requests.RequestException as exc:
            print(f"  ! Graph refresh request failed ({job_type}): {exc}")
            continue
        if resp.status_code in (200, 202):
            print(f"  + Triggered graph refresh job (jobType={job_type}) on '{graph['displayName']}'.")
            return
    print("  ! Automatic graph refresh not accepted by the API. Refresh manually: "
          "workspace -> graph model -> ... -> Schedule -> Refresh now.")


if __name__ == "__main__":
    deploy_ontology()
