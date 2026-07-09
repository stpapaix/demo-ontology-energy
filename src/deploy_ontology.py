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

from config import LAKEHOUSE_NAMES, WORKSPACE_ID
from deploy_items import _b64, _create_or_update
from provision_lakehouses import list_lakehouses

_SPEC_PATH = os.path.join(os.path.dirname(__file__), "..", "ontology", "energy_ontology.json")


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
            "table": entity["table"], "entity": entity,
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
        binding_guid = _guid("binding", entity["name"])
        parts.append(_part(f"EntityTypes/{info['id']}/DataBindings/{binding_guid}.json", {
            "id": binding_guid,
            "dataBindingConfiguration": {
                "dataBindingType": "NonTimeSeries",
                "propertyBindings": [
                    {"sourceColumnName": prop["name"], "targetPropertyId": info["prop_ids"][prop["name"]]}
                    for prop in entity["properties"]
                ],
                "sourceTableProperties": {
                    "sourceType": "LakehouseTable", "workspaceId": workspace, "itemId": silver_id,
                    "sourceTableName": entity["table"], "sourceSchema": schema,
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


if __name__ == "__main__":
    deploy_ontology()
