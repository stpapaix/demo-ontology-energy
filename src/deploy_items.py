"""Deploy Fabric notebooks and data pipelines from local sources via REST.

Notebook sources live in ../notebooks/*.py using `# %%` cell markers; they are
converted to ipynb and uploaded. Two data pipelines each run one notebook via a
TridentNotebook activity. All operations are idempotent (create or update).
"""

import base64
import json
import os
import time
import uuid

import requests

from auth import get_fabric_token
from config import WORKSPACE_ID

FABRIC_API = "https://api.fabric.microsoft.com/v1"
_TIMEOUT = 60
_NB_DIR = os.path.join(os.path.dirname(__file__), "..", "notebooks")


def _headers() -> dict:
    return {"Authorization": f"Bearer {get_fabric_token()}", "Content-Type": "application/json"}


def _wait_for_operation(response: requests.Response) -> dict:
    operation_url = response.headers.get("Location")
    retry_after = int(response.headers.get("Retry-After", "3"))
    while True:
        time.sleep(retry_after)
        status_resp = requests.get(operation_url, headers=_headers(), timeout=_TIMEOUT)
        status_resp.raise_for_status()
        status = status_resp.json().get("status")
        if status == "Succeeded":
            result = requests.get(f"{operation_url}/result", headers=_headers(), timeout=_TIMEOUT)
            return result.json() if result.status_code == 200 else {}
        if status == "Failed":
            raise RuntimeError(f"Operation failed: {status_resp.text}")


# --- ipynb conversion -------------------------------------------------------

def _as_source(lines: list[str]) -> list[str]:
    if not lines:
        return []
    return [line + "\n" for line in lines[:-1]] + [lines[-1]]


def _py_to_ipynb(py_path: str) -> dict:
    """Convert a `# %%`-delimited Python source into a Jupyter notebook dict."""
    with open(py_path, encoding="utf-8") as handle:
        raw_lines = handle.read().splitlines()

    cells: list[dict] = []
    current: list[str] = []
    kind = "code"
    started = False

    def flush() -> None:
        if not started:
            return
        body = current[:]
        while body and body[0].strip() == "":
            body.pop(0)
        while body and body[-1].strip() == "":
            body.pop()
        if not body:
            return
        cell_id = uuid.uuid4().hex[:8]
        if kind == "markdown":
            md = [line[2:] if line.startswith("# ") else ("" if line.strip() == "#" else line) for line in body]
            cells.append({"cell_type": "markdown", "id": cell_id, "metadata": {}, "source": _as_source(md)})
        else:
            cells.append({
                "cell_type": "code", "id": cell_id, "metadata": {},
                "execution_count": None, "outputs": [], "source": _as_source(body),
            })

    for line in raw_lines:
        if line.strip().startswith("# %%"):
            flush()
            current = []
            started = True
            kind = "markdown" if "[markdown]" in line else "code"
            continue
        current.append(line)
    flush()

    return {
        "cells": cells,
        "metadata": {
            "language_info": {"name": "python"},
            "kernelspec": {"name": "synapse_pyspark", "display_name": "Synapse PySpark"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }


def _b64(payload) -> str:
    raw = json.dumps(payload).encode("utf-8") if isinstance(payload, (dict, list)) else payload.encode("utf-8")
    return base64.b64encode(raw).decode("ascii")


def _list_items(kind: str) -> list[dict]:
    resp = requests.get(f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/{kind}", headers=_headers(), timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json().get("value", [])


def _create_or_update(kind: str, display_name: str, definition: dict) -> str:
    """Create the item, or update its definition if it already exists. Returns id."""
    existing = next((i for i in _list_items(kind) if i["displayName"] == display_name), None)
    if existing:
        item_id = existing["id"]
        resp = requests.post(
            f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/{kind}/{item_id}/updateDefinition",
            headers=_headers(), json={"definition": definition}, timeout=_TIMEOUT,
        )
        if resp.status_code == 202:
            _wait_for_operation(resp)
        else:
            resp.raise_for_status()
        print(f"  = updated {display_name} ({item_id})")
        return item_id

    resp = requests.post(
        f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/{kind}",
        headers=_headers(), json={"displayName": display_name, "definition": definition}, timeout=_TIMEOUT,
    )
    if resp.status_code == 202:
        item = _wait_for_operation(resp)
        item_id = item["id"]
    else:
        resp.raise_for_status()
        item_id = resp.json()["id"]
    print(f"  + created {display_name} ({item_id})")
    return item_id


def deploy_notebook(display_name: str, py_filename: str) -> str:
    ipynb = _py_to_ipynb(os.path.join(_NB_DIR, py_filename))
    definition = {
        "format": "ipynb",
        "parts": [{"path": "notebook-content.ipynb", "payload": _b64(ipynb), "payloadType": "InlineBase64"}],
    }
    return _create_or_update("notebooks", display_name, definition)


def deploy_pipeline(display_name: str, notebook_id: str, activity_name: str) -> str:
    content = {
        "properties": {
            "activities": [
                {
                    "name": activity_name,
                    "type": "TridentNotebook",
                    "dependsOn": [],
                    "policy": {
                        "timeout": "0.12:00:00", "retry": 0, "retryIntervalInSeconds": 30,
                        "secureOutput": False, "secureInput": False,
                    },
                    "typeProperties": {"notebookId": notebook_id.lower(), "workspaceId": WORKSPACE_ID.lower()},
                }
            ]
        }
    }
    definition = {"parts": [{"path": "pipeline-content.json", "payload": _b64(content), "payloadType": "InlineBase64"}]}
    return _create_or_update("dataPipelines", display_name, definition)


def deploy_items() -> None:
    """Deploy the notebooks and the data pipelines."""
    print("\nDeploying notebooks...")
    deploy_notebook("nb_seed_dimensions", "nb_seed_dimensions.py")
    deploy_notebook("nb_seed_facts", "nb_seed_facts.py")
    deploy_notebook("nb_truncate_all", "nb_truncate_all.py")
    nb_b2s = deploy_notebook("nb_bronze_to_silver", "nb_bronze_to_silver.py")
    nb_s2g = deploy_notebook("nb_silver_to_gold", "nb_silver_to_gold.py")
    nb_onto = deploy_notebook("nb_build_ontology", "nb_build_ontology.py")

    print("\nDeploying pipelines...")
    deploy_pipeline("pl_bronze_to_silver", nb_b2s, "Run bronze to silver")
    deploy_pipeline("pl_silver_to_gold", nb_s2g, "Run silver to gold")
    deploy_pipeline("pl_build_ontology", nb_onto, "Run build ontology")


if __name__ == "__main__":
    deploy_items()
