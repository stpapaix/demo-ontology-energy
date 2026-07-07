"""Non-interactive authentication for Microsoft Fabric using an Entra ID service principal."""

import os

from azure.identity import ClientSecretCredential
from dotenv import load_dotenv

load_dotenv()

# Scope required to call the Microsoft Fabric REST API.
FABRIC_SCOPE = "https://api.fabric.microsoft.com/.default"


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise EnvironmentError(
            f"Missing required environment variable '{name}'. "
            "Copy .env.example to .env and fill in the values."
        )
    return value


def get_fabric_credential() -> ClientSecretCredential:
    """Build a service-principal credential from environment variables."""
    return ClientSecretCredential(
        tenant_id=_require_env("AZURE_TENANT_ID"),
        client_id=_require_env("AZURE_CLIENT_ID"),
        client_secret=_require_env("AZURE_CLIENT_SECRET"),
    )


def get_fabric_token() -> str:
    """Acquire a bearer token for the Fabric REST API (non-interactive)."""
    credential = get_fabric_credential()
    token = credential.get_token(FABRIC_SCOPE)
    return token.token
