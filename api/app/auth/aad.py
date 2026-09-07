"""Azure AD (Entra ID) bearer-token validation for incoming API requests.

Validates JWTs issued to calling service principals / managed identities (e.g. Azure
Data Factory, Microsoft Fabric, Azure Databricks) against the tenant's JWKS, checking
issuer, audience, and (optionally) an allow-list of caller application IDs.
"""
from functools import lru_cache

import httpx
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from jose import jwt
from jose.exceptions import JOSEError

from app.config import Settings, get_settings

_bearer_scheme = HTTPBearer(auto_error=False)


@lru_cache
def _get_jwks(jwks_uri: str) -> dict:
    response = httpx.get(jwks_uri, timeout=10)
    response.raise_for_status()
    return response.json()


def _get_signing_key(token: str, jwks_uri: str) -> dict:
    unverified_header = jwt.get_unverified_header(token)
    jwks = _get_jwks(jwks_uri)
    for key in jwks.get("keys", []):
        if key.get("kid") == unverified_header.get("kid"):
            return key
    # JWKS may have rotated; refresh once.
    _get_jwks.cache_clear()
    jwks = _get_jwks(jwks_uri)
    for key in jwks.get("keys", []):
        if key.get("kid") == unverified_header.get("kid"):
            return key
    raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Unable to find matching signing key")


async def verify_token(
    credentials: HTTPAuthorizationCredentials | None = Depends(_bearer_scheme),
    settings: Settings = Depends(get_settings),
) -> dict:
    """FastAPI dependency: validates the bearer token and returns its claims.

    Use as `Depends(verify_token)` on routers/endpoints that require authentication.
    """
    if settings.auth_disabled:
        return {"sub": "local-dev", "appid": "local-dev"}

    if credentials is None:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing bearer token")

    token = credentials.credentials
    try:
        signing_key = _get_signing_key(token, settings.aad_jwks_uri)
        claims = jwt.decode(
            token,
            signing_key,
            algorithms=["RS256"],
            audience=settings.api_aad_audience,
            issuer=f"https://login.microsoftonline.com/{settings.api_aad_tenant_id}/v2.0",
        )
    except JOSEError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED, detail=f"Invalid token: {exc}"
        ) from exc

    allowed_app_ids = settings.allowed_app_ids
    caller_app_id = claims.get("azp") or claims.get("appid")
    if allowed_app_ids and caller_app_id not in allowed_app_ids:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Caller application is not authorized")

    return claims
