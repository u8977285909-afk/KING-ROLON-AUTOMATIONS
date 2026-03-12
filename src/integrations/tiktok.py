# -*- coding: utf-8 -*-
"""
Integración con TikTok OAuth2 + PKCE para KING ROLON AUTOMATIONS.

Este módulo se encarga de:
- Construir la URL de login (con code_challenge PKCE)
- Intercambiar "code" -> access_token
- Guardar/leer los tokens desde tokens.py
- Comprobar si está configurado / conectado

Requisitos en .env:
    TIKTOK_CLIENT_KEY=...
    TIKTOK_CLIENT_SECRET=...
    TIKTOK_REDIRECT_URI=http://127.0.0.1:5000/auth/tiktok/callback
"""

import os
import base64
import secrets
import hashlib
import urllib.parse
from typing import Dict, Any, Optional

import requests
from dotenv import load_dotenv

# Cargar variables del .env
load_dotenv()

# ========= CONFIG BÁSICA =========
CLIENT_KEY = os.getenv("TIKTOK_CLIENT_KEY") or ""
CLIENT_SECRET = os.getenv("TIKTOK_CLIENT_SECRET") or ""
REDIRECT_URI = os.getenv("TIKTOK_REDIRECT_URI") or "http://127.0.0.1:5000/auth/tiktok/callback"

# Endpoints oficiales (si TikTok cambia, se actualizan aquí)
AUTH_BASE = "https://www.tiktok.com/v2/auth/authorize"
TOKEN_URL = "https://open.tiktokapis.com/v2/oauth/token/"

# Scopes mínimos de ejemplo
SCOPES = [
    "user.info.basic",
    "user.info.profile",
]

# ========= ALMACÉN DE TOKENS (usa tu tokens.py si existe) =========
try:
    from tokens import load_tokens, save_tokens  # helpers genéricos
except Exception:
    # Fallback mínimo si aún no existen
    TOKEN_FILE = "data/tiktok_tokens.json"

    def load_tokens() -> Dict[str, Any]:
        import json, pathlib
        p = pathlib.Path(TOKEN_FILE)
        if not p.exists():
            return {}
        try:
            return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            return {}

    def save_tokens(data: Dict[str, Any]) -> None:
        import json, pathlib
        p = pathlib.Path(TOKEN_FILE)
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


# ========= UTILIDADES PKCE =========
def _generate_code_verifier(length: int = 64) -> str:
    """
    Genera un code_verifier aleatorio para PKCE (43–128 caracteres).
    """
    raw = secrets.token_urlsafe(length)
    return raw[:length]


def _generate_code_challenge(code_verifier: str) -> str:
    """
    A partir de un code_verifier, genera el code_challenge (S256, base64url, sin '=').
    """
    sha = hashlib.sha256(code_verifier.encode("ascii")).digest()
    b64 = base64.urlsafe_b64encode(sha).decode("ascii")
    return b64.rstrip("=")


# ========= ESTADO / SESIÓN PKCE =========
# Para producción debería ir en BD o sesión. Aquí es memoria (demo).
_PKCE_STORE: Dict[str, str] = {}


def _set_pkce_state(state: str, code_verifier: str) -> None:
    _PKCE_STORE[state] = code_verifier


def _pop_pkce_verifier(state: str) -> Optional[str]:
    return _PKCE_STORE.pop(state, None)


# ========= FUNCIONES PRINCIPALES =========
def is_configured() -> bool:
    """True si las variables de entorno básicas están configuradas."""
    return bool(CLIENT_KEY and CLIENT_SECRET and REDIRECT_URI)


def is_connected() -> bool:
    """True si tenemos un access_token almacenado para TikTok."""
    tokens = load_tokens() or {}
    tk = tokens.get("tiktok") or {}
    return bool(tk.get("access_token"))


def get_auth_url() -> str:
    """
    Construye la URL de autorización de TikTok usando PKCE (S256).
    - Genera state aleatorio
    - Genera code_verifier y code_challenge
    - Guarda code_verifier en _PKCE_STORE[state]
    """
    if not is_configured():
        raise RuntimeError(
            "TikTok no está configurado. Revisa TIKTOK_CLIENT_KEY / SECRET / REDIRECT_URI"
        )

    state = secrets.token_urlsafe(16)
    code_verifier = _generate_code_verifier()
    code_challenge = _generate_code_challenge(code_verifier)

    # Guardamos code_verifier para usarlo luego en el callback
    _set_pkce_state(state, code_verifier)

    params = {
        "client_key": CLIENT_KEY,
        "response_type": "code",
        "redirect_uri": REDIRECT_URI,
        "scope": " ".join(SCOPES),
        "state": state,
        # PKCE
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }

    return AUTH_BASE + "?" + urllib.parse.urlencode(params)


def exchange_code_for_token(code: str, state: str) -> Dict[str, Any]:
    """
    Intercambia el "code" recibido por TikTok por un access_token usando PKCE.
    Necesita el code_verifier asociado al state que guardamos antes.
    """
    if not is_configured():
        raise RuntimeError(
            "TikTok no está configurado. Revisa TIKTOK_CLIENT_KEY / SECRET / REDIRECT_URI"
        )

    code_verifier = _pop_pkce_verifier(state)
    if not code_verifier:
        raise RuntimeError(
            "No se encontró code_verifier para este state (PKCE). "
            "Vuelve a iniciar sesión desde el panel."
        )

    data = {
        "client_key": CLIENT_KEY,
        "client_secret": CLIENT_SECRET,
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": REDIRECT_URI,
        # PKCE
        "code_verifier": code_verifier,
    }

    resp = requests.post(TOKEN_URL, data=data, timeout=15)
    try:
        resp.raise_for_status()
    except Exception as e:
        print("[TIKTOK TOKEN ERROR] status:", resp.status_code, "body:", resp.text)
        raise e

    return resp.json()


def save_tokens_from_response(token_data: Dict[str, Any]) -> None:
    """
    Guarda los tokens de TikTok dentro del almacén genérico (tokens.py).
    """
    tokens_all = load_tokens() or {}
    tokens_all["tiktok"] = token_data
    save_tokens(tokens_all)  # type: ignore[arg-type]


# ========= API SENCILLA PARA web_app.py =========
def connect_url() -> str:
    """Alias amigable para web_app.py -> get_auth_url()."""
    return get_auth_url()


def save_tokens(token_data: Dict[str, Any]) -> None:  # type: ignore[override]
    """
    Alias amigable para web_app.py -> save_tokens_from_response().
    Sobrescribe el save_tokens importado arriba para que el flujo sea directo.
    """
    save_tokens_from_response(token_data)
