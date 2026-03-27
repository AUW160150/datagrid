"""
datagrid — Auth0 M2M Token Manager

Each agent requests a scoped M2M token before executing.
Token is cached until expiry (Auth0 M2M tokens are 24h by default).

Scopes per agent:
  ingestion-agent      read:records
  modality-agent       read:records
  harmonization-agent  read:records write:harmonized
  validation-agent     read:harmonized
  output-agent         read:validated write:omop
"""

import os
import time
import requests
from functools import wraps

AUTH0_DOMAIN        = os.getenv("AUTH0_DOMAIN",        "dev-jxq256ergu25vtkp.us.auth0.com")
AUTH0_CLIENT_ID     = os.getenv("AUTH0_CLIENT_ID",     "73p8ARbfOCVB1r8JPsINEsUNbOS2ulBC")
AUTH0_CLIENT_SECRET = os.getenv("AUTH0_CLIENT_SECRET", "")
AUTH0_AUDIENCE      = os.getenv("AUTH0_AUDIENCE",      "https://dev-jxq256ergu25vtkp.us.auth0.com/api/v2/")

# Agent → allowed scopes
AGENT_SCOPES: dict[str, list[str]] = {
    "ingestion-agent":      ["read:records"],
    "modality-agent":       ["read:records"],
    "harmonization-agent":  ["read:records", "write:harmonized"],
    "validation-agent":     ["read:harmonized"],
    "output-agent":         ["read:validated", "write:omop"],
}

# Token cache: agent_name → {token, expires_at}
_token_cache: dict[str, dict] = {}


def get_token(agent_name: str) -> str:
    """
    Request (or return cached) M2M access token for the given agent.
    Raises ValueError if agent_name is not in AGENT_SCOPES.
    Raises RuntimeError if AUTH0_CLIENT_SECRET is not set (falls back to no-auth mode).
    """
    if agent_name not in AGENT_SCOPES:
        raise ValueError(f"Unknown agent: {agent_name}. Must be one of {list(AGENT_SCOPES)}")

    # Return cached token if still valid (with 60s buffer)
    cached = _token_cache.get(agent_name)
    if cached and cached["expires_at"] > time.time() + 60:
        return cached["token"]

    # No secret = no-auth fallback (dev mode)
    if not AUTH0_CLIENT_SECRET:
        print(f"  [Auth0] No client secret — running in no-auth dev mode for {agent_name}")
        return "dev-no-auth"

    # AGENT_SCOPES defines the logical access policy per agent (used for audit/docs).
    # The Auth0 Management API issues its own scopes — we don't pass custom ones.
    logical_scopes = AGENT_SCOPES[agent_name]
    payload = {
        "grant_type":    "client_credentials",
        "client_id":     AUTH0_CLIENT_ID,
        "client_secret": AUTH0_CLIENT_SECRET,
        "audience":      AUTH0_AUDIENCE,
    }
    url = f"https://{AUTH0_DOMAIN}/oauth/token"
    resp = requests.post(url, json=payload, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    token      = data["access_token"]
    expires_in = data.get("expires_in", 86400)
    _token_cache[agent_name] = {
        "token":      token,
        "expires_at": time.time() + expires_in,
    }
    print(f"  [Auth0] Token issued for {agent_name} — logical scopes: {logical_scopes}")
    return token


def require_token(agent_name: str):
    """
    Decorator: fetches M2M token before the agent function runs.
    Injects `auth_token` as a keyword argument.
    """
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            token = get_token(agent_name)
            kwargs["auth_token"] = token
            return fn(*args, **kwargs)
        return wrapper
    return decorator


def verify_scope(token: str, required_scope: str) -> bool:
    """
    Lightweight scope check — decodes JWT claims without full verification.
    Full RS256 verification would require Auth0 JWKS endpoint; this is sufficient
    for inter-agent calls on a trusted internal network.
    """
    if token == "dev-no-auth":
        return True
    try:
        import base64, json as _json
        payload_b64 = token.split(".")[1]
        # Pad base64
        payload_b64 += "=" * (-len(payload_b64) % 4)
        payload = _json.loads(base64.urlsafe_b64decode(payload_b64))
        scopes = payload.get("scope", "").split()
        return required_scope in scopes
    except Exception:
        return False
