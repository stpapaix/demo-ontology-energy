"""Minimal Microsoft Fabric REST API client used to verify service-principal auth."""

import os

import requests
from dotenv import load_dotenv

from auth import get_fabric_token

load_dotenv()

FABRIC_API = "https://api.fabric.microsoft.com/v1"
_TIMEOUT = 30


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {get_fabric_token()}",
        "Content-Type": "application/json",
    }


def list_workspaces() -> list[dict]:
    """List the workspaces the service principal can access."""
    resp = requests.get(f"{FABRIC_API}/workspaces", headers=_headers(), timeout=_TIMEOUT)
    resp.raise_for_status()
    return resp.json().get("value", [])


def list_items(workspace_id: str) -> list[dict]:
    """List all items (lakehouses, notebooks, pipelines...) in a workspace."""
    resp = requests.get(
        f"{FABRIC_API}/workspaces/{workspace_id}/items",
        headers=_headers(),
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json().get("value", [])


def main() -> None:
    print("Authenticating with the Fabric REST API as a service principal...\n")

    workspaces = list_workspaces()
    print(f"Workspaces the service principal can see ({len(workspaces)}):")
    for ws in workspaces:
        print(f"  - {ws['displayName']} ({ws['id']})")

    workspace_id = os.environ.get("FABRIC_WORKSPACE_ID")
    if workspace_id:
        items = list_items(workspace_id)
        print(f"\nItems in workspace {workspace_id} ({len(items)}):")
        for item in items:
            print(f"  - [{item['type']}] {item['displayName']}")
    else:
        print("\nFABRIC_WORKSPACE_ID not set; skipping item listing.")


if __name__ == "__main__":
    main()
