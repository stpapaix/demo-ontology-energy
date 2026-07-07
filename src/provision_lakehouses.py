"""Provision the bronze / silver / gold lakehouses in the Fabric workspace.

Uses the Fabric REST API with the service-principal token from auth.py.
Idempotent: existing lakehouses are reused instead of recreated.
"""

import time

import requests

from auth import get_fabric_token
from config import LAKEHOUSE_NAMES, LAYERS, WORKSPACE_ID

FABRIC_API = "https://api.fabric.microsoft.com/v1"
_TIMEOUT = 60


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {get_fabric_token()}",
        "Content-Type": "application/json",
    }


def _wait_for_operation(response: requests.Response) -> dict:
    """Poll a long-running operation until it completes, then return its result."""
    operation_url = response.headers.get("Location")
    retry_after = int(response.headers.get("Retry-After", "3"))
    while True:
        time.sleep(retry_after)
        status_resp = requests.get(operation_url, headers=_headers(), timeout=_TIMEOUT)
        status_resp.raise_for_status()
        status = status_resp.json().get("status")
        if status == "Succeeded":
            result = requests.get(f"{operation_url}/result", headers=_headers(), timeout=_TIMEOUT)
            result.raise_for_status()
            return result.json()
        if status == "Failed":
            raise RuntimeError(f"Lakehouse creation failed: {status_resp.text}")


def list_lakehouses() -> list[dict]:
    resp = requests.get(
        f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/lakehouses",
        headers=_headers(),
        timeout=_TIMEOUT,
    )
    resp.raise_for_status()
    return resp.json().get("value", [])


def get_or_create_lakehouse(display_name: str) -> dict:
    """Return the lakehouse with this display name, creating it if absent."""
    for lh in list_lakehouses():
        if lh["displayName"] == display_name:
            print(f"  = lakehouse '{display_name}' already exists ({lh['id']})")
            return lh

    resp = requests.post(
        f"{FABRIC_API}/workspaces/{WORKSPACE_ID}/lakehouses",
        headers=_headers(),
        json={"displayName": display_name},
        timeout=_TIMEOUT,
    )
    if resp.status_code == 202:  # long-running operation
        lh = _wait_for_operation(resp)
    else:
        resp.raise_for_status()
        lh = resp.json()
    print(f"  + created lakehouse '{display_name}' ({lh['id']})")
    return lh


def provision_lakehouses() -> dict[str, str]:
    """Ensure a lakehouse exists for every medallion layer.

    Returns a mapping of layer name -> lakehouse id.
    """
    print(f"Provisioning lakehouses in workspace {WORKSPACE_ID}...")
    layer_to_id: dict[str, str] = {}
    for layer in LAYERS:
        lh = get_or_create_lakehouse(LAKEHOUSE_NAMES[layer])
        layer_to_id[layer] = lh["id"]
    return layer_to_id


if __name__ == "__main__":
    mapping = provision_lakehouses()
    print("\nLayer -> lakehouse id:")
    for layer, lh_id in mapping.items():
        print(f"  {layer}: {lh_id}")
