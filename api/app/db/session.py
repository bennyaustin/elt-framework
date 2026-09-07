"""pyodbc connection factory for ControlDB.

Uses Azure AD token-based authentication (Service Principal or Managed Identity) so no
SQL login/password is ever stored, consistent with the Service Principal `db_owner`
pattern already used by the ControlDB deployment workflow.
"""
import struct
from contextlib import contextmanager
from typing import Iterator

import pyodbc
from azure.identity import ClientSecretCredential, ManagedIdentityCredential

from app.config import Settings, get_settings

# SQL_COPT_SS_ACCESS_TOKEN connection attribute (see Microsoft docs for pyodbc + AAD token auth)
_SQL_COPT_SS_ACCESS_TOKEN = 1256
_AAD_TOKEN_SCOPE = "https://database.windows.net/.default"


def _get_credential(settings: Settings):
    if settings.sql_auth_mode == "ManagedIdentity":
        return ManagedIdentityCredential(client_id=settings.aad_client_id)
    return ClientSecretCredential(
        tenant_id=settings.aad_tenant_id,
        client_id=settings.aad_client_id,
        client_secret=settings.aad_client_secret,
    )


def _encode_token(access_token: str) -> bytes:
    token_bytes = access_token.encode("utf-16-le")
    return struct.pack(f"<I{len(token_bytes)}s", len(token_bytes), token_bytes)


def _build_connection_string(settings: Settings) -> str:
    return (
        "Driver={ODBC Driver 18 for SQL Server};"
        f"Server=tcp:{settings.sql_server},1433;"
        f"Database={settings.sql_database};"
        "Encrypt=yes;TrustServerCertificate=no;"
    )


@contextmanager
def get_connection(settings: Settings | None = None) -> Iterator[pyodbc.Connection]:
    """Yield a pyodbc connection authenticated via Azure AD access token.

    A new token is fetched per-connection; azure-identity credentials cache/refresh
    tokens internally so this is cheap on the common path.
    """
    settings = settings or get_settings()
    credential = _get_credential(settings)
    token = credential.get_token(_AAD_TOKEN_SCOPE)
    token_struct = _encode_token(token.token)

    conn_str = _build_connection_string(settings)
    connection = pyodbc.connect(
        conn_str,
        attrs_before={_SQL_COPT_SS_ACCESS_TOKEN: token_struct},
        timeout=settings.sql_command_timeout_seconds,
    )
    try:
        yield connection
    finally:
        connection.close()
