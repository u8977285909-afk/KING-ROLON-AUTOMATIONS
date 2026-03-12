# -*- coding: utf-8 -*-
"""
KING ROLON AUTOMATIONS — WEB APP (STABLE / CLEAN / COMPATIBLE)

Incluye:
- i18n SAFE con t("key") + set_language (ES base + EN/DE)
- Contexto global: current_user, lang, kr_asset_ver, t, I18N
- Dashboard / Tasks / Core completos
- API Tasks completa: POST, PUT, TOGGLE, DELETE
- Marketplace + Creator: rutas completas y seguras + filtros GET reales
- AUTH: LOGIN + REGISTER real (server-side) + saneo email
- CORE: Chat estilo ChatGPT (LLM real + tools + memoria)
- Settings con PRG (POST->redirect->GET) + avatar + banner + password change
- HOME PRINCIPAL = VIDEOS
- Ranking route: /ranking (endpoint "ranking")
- Ranking usa user_points (tabla real) + followers + likes + collections

- PUBLIC PROFILE PRO (con FLAG):
    - /u/<public_slug>  (principal)
    - /@<username>      (alias)
    - /profile          (mi perfil)
    - /profile/<id>     (por id)

- FOLLOW API:
    - POST /api/follow/<target_id> (toggle)

- REACTIONS API:
    - POST /api/videos/<video_id>/react  (endpoint "api_video_react_toggle")

- BANNER TRANSFORM API:
    - POST /api/profile/banner/transform

MUY IMPORTANTE:
- Si el módulo de videos (web_video_module.py) falla o no existe, este archivo
  crea endpoints mínimos para que NO reviente base.html / videos.html.

FIX PRO:
- Evita AssertionError por endpoints duplicados
- Guard anti-registro doble del módulo de videos
"""

from __future__ import annotations

import os
import re
import sys
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from typing import Any, Dict, List, Optional
from uuid import uuid4

from dotenv import load_dotenv
from flask import (Flask, current_app, flash, g, jsonify, redirect,
                   render_template, request, session, url_for)
from werkzeug.middleware.proxy_fix import ProxyFix
from werkzeug.security import check_password_hash, generate_password_hash

# =========================================================
# PATHS
# =========================================================
BASE_DIR = Path(__file__).resolve().parent
PROJECT_DIR = BASE_DIR.parent
if str(BASE_DIR) not in sys.path:
    sys.path.insert(0, str(BASE_DIR))

load_dotenv()

# =========================================================
# DB IMPORTS (stable)
# =========================================================
from database import (get_app_by_id, get_app_by_slug,  # noqa: E402
                      get_connection, get_products, get_recent_memory, init_db,
                      list_apps, list_apps_by_owner, record_app_sale,
                      rotate_memory, save_memory, update_app_status)

# Points
try:
    from database import get_points  # type: ignore
except Exception:
    get_points = None  # type: ignore

# Public profile helpers
try:
    from database import get_user_by_id  # type: ignore
    from database import get_user_by_public_slug  # type: ignore
    from database import get_user_by_username  # type: ignore
except Exception:
    get_user_by_username = None  # type: ignore
    get_user_by_public_slug = None  # type: ignore
    get_user_by_id = None  # type: ignore

# Social stats + follow helpers
try:
    from database import get_user_social_stats  # type: ignore
except Exception:
    get_user_social_stats = None  # type: ignore

try:
    from database import is_following as db_is_following  # type: ignore
    from database import toggle_follow as db_toggle_follow  # type: ignore
except Exception:
    db_is_following = None  # type: ignore
    db_toggle_follow = None  # type: ignore

try:
    from database import \
        list_public_videos_by_user as \
        db_list_public_videos_by_user  # type: ignore
except Exception:
    db_list_public_videos_by_user = None  # type: ignore

# Reactions helpers
try:
    from database import \
        get_user_video_reaction as db_get_user_video_reaction  # type: ignore
    from database import \
        get_video_reactions_summary as \
        db_get_video_reactions_summary  # type: ignore
    from database import \
        toggle_video_reaction as db_toggle_video_reaction  # type: ignore
except Exception:
    db_toggle_video_reaction = None  # type: ignore
    db_get_video_reactions_summary = None  # type: ignore
    db_get_user_video_reaction = None  # type: ignore

# Banner transform helpers
try:
    from database import \
        get_user_banner_transform as \
        db_get_user_banner_transform  # type: ignore
    from database import \
        update_user_banner_transform as \
        db_update_user_banner_transform  # type: ignore
except Exception:
    db_get_user_banner_transform = None  # type: ignore
    db_update_user_banner_transform = None  # type: ignore

# =========================================================
# MAIN IMPORTS
# =========================================================
from main import get_logs_dir, load_tasks, log_event, save_tasks  # noqa: E402

# Video module import (no rompe la app si falla)
_video_import_error: Optional[str] = None
try:
    from web_video_module import register_video_routes  # noqa: E402
except Exception as e:
    register_video_routes = None  # type: ignore
    _video_import_error = repr(e)

# =========================================================
# OPTIONALS (Scheduler)
# =========================================================
try:
    from automations.scheduler import get_scheduler_status, start_scheduler
except Exception:

    def start_scheduler():
        return None

    def get_scheduler_status():
        return {
            "running": False,
            "last_run_at": None,
            "interval_seconds": None,
            "last_exec_seconds": None,
            "cycles_executed": 0,
            "consecutive_errors": 0,
            "last_error": "Scheduler no disponible",
        }


# =========================================================
# FLASK APP
# =========================================================
app = Flask(
    __name__,
    template_folder=str(BASE_DIR / "templates"),
    static_folder=str(PROJECT_DIR / "static"),
    static_url_path="/static",
)


@app.context_processor
def inject_app():
    return {"app": current_app}


@app.context_processor
def inject_helpers():
    def endpoint_exists(name: str) -> bool:
        try:
            return bool(name) and (name in current_app.view_functions)
        except Exception:
            return False

    return {"endpoint_exists": endpoint_exists}


app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1)
app.secret_key = os.getenv("APP_SECRET") or ("dev-" + uuid4().hex)

# =========================================================
# COOKIE / HTTPS HARDENING
# =========================================================
def _force_https_enabled() -> bool:
    v = (os.getenv("KR_FORCE_HTTPS") or "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _secure_cookie_by_env() -> Optional[bool]:
    v = (os.getenv("SESSION_COOKIE_SECURE") or "").strip().lower()
    if v in ("1", "true", "yes", "on"):
        return True
    if v in ("0", "false", "no", "off"):
        return False
    return None


app.config.update(
    KR_ASSET_VER=(os.getenv("KR_ASSET_VER") or "dev"),
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=timedelta(days=7),
    MAX_CONTENT_LENGTH=600 * 1024 * 1024,
)

_env_secure = _secure_cookie_by_env()
if _env_secure is not None:
    app.config["SESSION_COOKIE_SECURE"] = _env_secure
else:
    app.config["SESSION_COOKIE_SECURE"] = False

# =========================================================
# INIT DB + DIRS
# =========================================================
init_db()

UPLOADS_DIR = PROJECT_DIR / "static" / "uploads"
AVATARS_DIR = UPLOADS_DIR / "avatars"
BANNERS_DIR = UPLOADS_DIR / "banners"
APPFILES_DIR = UPLOADS_DIR / "apps"

for d in (UPLOADS_DIR, AVATARS_DIR, BANNERS_DIR, APPFILES_DIR):
    d.mkdir(parents=True, exist_ok=True)

LOG_PATH = get_logs_dir() / "activity.log"

# =========================================================
# SECURITY HEADERS
# =========================================================
@app.after_request
def _security_headers(resp):
    resp.headers["X-Frame-Options"] = "SAMEORIGIN"
    resp.headers["X-Content-Type-Options"] = "nosniff"
    resp.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    resp.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    return resp


# =========================================================
# AUTH UTILS
# =========================================================
def login_required(fn):
    @wraps(fn)
    def wrapper(*a, **kw):
        if not getattr(g, "user", None):
            return redirect(url_for("auth"))
        return fn(*a, **kw)

    return wrapper


def uid() -> Optional[int]:
    try:
        u = getattr(g, "user", None) or {}
        if u.get("id") is not None:
            return int(u["id"])
    except Exception:
        return None
    return None


def _is_owner(app_row: Dict[str, Any], user_id: Optional[int]) -> bool:
    if not app_row or user_id is None:
        return False
    owner = app_row.get("owner_id") or app_row.get("user_id") or app_row.get("created_by")
    try:
        return int(owner) == int(user_id)
    except Exception:
        return False


def _table_exists(conn, name: str) -> bool:
    try:
        r = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=? LIMIT 1;",
            (name,),
        ).fetchone()
        return bool(r)
    except Exception:
        return False


def _safe_redirect_after_lang_change():
    """
    Evita redirect a endpoint que NO existe.
    Prioridad:
      videos_feed -> vm_videos_feed -> dashboard -> auth
    """
    if not session.get("user_id"):
        return redirect(url_for("auth"))

    if "videos_feed" in current_app.view_functions:
        return redirect(url_for("videos_feed"))

    if "vm_videos_feed" in current_app.view_functions:
        return redirect(url_for("vm_videos_feed"))

    if "dashboard" in current_app.view_functions:
        return redirect(url_for("dashboard"))

    return redirect(url_for("auth"))


def _json_err(msg: str, code: int = 400):
    return jsonify(ok=False, error=str(msg or "error")), int(code)


def _norm_percent(v: Any, default: float = 50.0) -> float:
    try:
        x = float(v)
    except Exception:
        x = float(default)
    return max(-5000.0, min(5000.0, x))


def _norm_scale(v: Any, default: float = 1.0) -> float:
    try:
        x = float(v)
    except Exception:
        x = float(default)
    return max(0.1, min(10.0, x))


# =========================================================
# i18n
# =========================================================
I18N: Dict[str, Dict[str, str]] = {
    "es": {
        "app.name": "KING ROLON AUTOMATIONS",
        "app.meta_desc": "Automatizaciones premium para creadores.",
        "footer.copy": "© KING ROLON AUTOMATIONS",
        "nav.videos": "Videos",
        "nav.library": "Mi biblioteca",
        "nav.ranking": "Ranking",
        "nav.dashboard": "Dashboard",
        "nav.core": "Core",
        "nav.automations": "Automatizaciones",
        "nav.store": "Store",
        "nav.marketplace": "Marketplace",
        "nav.creator": "Creator",
        "nav.account": "Cuenta",
        "nav.logout": "Salir",
        "common.err_connection": "Error de conexión. Intenta de nuevo.",
        "common.save_changes": "Guardar cambios",
        "common.back_dashboard": "Volver al Dashboard",
        "common.clear_fields": "Limpiar campos",
        "common.cancel": "Cancelar",
        "common.save": "Guardar",
        "lang.label": "Idioma",
        "lang.change": "Cambiar idioma",
        "dashboard.title_tab": "Dashboard",
        "dashboard.title": "Dashboard",
        "dashboard.user_default": "KR User",
        "dashboard.plan_default": "FREE",
        "dashboard.subtitle": "Control total del Reino: {user} · Plan {plan}.",
        "tasks.title_tab": "Automatizaciones",
        "tasks.title": "Automatizaciones",
        "tasks.subtitle": "Crea, edita, pausa y administra tus tareas.",
        "tasks.unspecified": "Sin especificar",
        "core.title_tab": "Core",
        "core.title": "Core",
        "core.subtitle": "Núcleo IA del Reino. Comandos, memoria y chat.",
        "core.user_default": "KR User",
        "core.panel_title": "Estado del Core",
        "core.panel_desc": "Motor activo. Responde y registra memoria.",
        "core.state_ready": "READY",
        "core.identity": "Identidad",
        "core.plan": "Plan",
        "core.note_local": "Privado: tus mensajes se procesan dentro del sistema y pueden guardarse como memoria.",
        "core.chat_title": "Chat del Core",
        "core.chat_desc": "Escribe comandos o preguntas. El Core responde y registra.",
        "core.chip_private": "PRIVATE",
        "core.kr_label": "KR",
        "core.you_label": "Tú",
        "core.welcome": "Bienvenido, {user}. Estoy listo.",
        "core.example_label": "Ejemplo",
        "core.example_text": "Prueba: “estado del scheduler” o “muéstrame logs”.",
        "core.input_ph": "Escribe un comando...",
        "core.send_btn": "Enviar",
        "core.hint_1": "Tip: “logs” para ver actividad",
        "core.hint_2": "Tip: “tareas” para resumen",
        "core.hint_3": "Tip: “scheduler” para estado",
        "core.err_empty": "Escribe un mensaje.",
        "market.not_found": "No encontrado.",
        "market.not_allowed": "No autorizado.",
        "market.download_ok": "Descarga registrada (MVP).",
        "market.status_updated": "Estado actualizado.",
        "market.status_update_fail": "No se pudo actualizar el estado.",
        "market.title_tab": "Marketplace",
        "market.title": "Marketplace",
        "market.subtitle": "Apps y automatizaciones listas para usar. Descarga, filtra y publica tus creaciones.",
        "settings.title_tab": "Cuenta",
        "settings.title": "Cuenta",
        "settings.subtitle": "Perfil, idioma, avatar y seguridad.",
        "settings.identity": "Identidad",
        "settings.identity_sub": "Tu información dentro del Reino.",
        "settings.email": "Correo",
        "settings.creator_name": "Nombre",
        "settings.creator_name_ph": "Ej: Edwin Rolon",
        "settings.plan_role": "Plan",
        "settings.creator_data": "Datos del creador",
        "settings.creator_data_sub": "Ajusta tu nombre y preferencias.",
        "settings.lang_system": "Idioma del sistema",
        "settings.lang_note": "Este cambio afecta toda la interfaz.",
        "settings.avatar_title": "Avatar",
        "settings.avatar_desc": "Sube una imagen para tu perfil.",
        "settings.avatar_upload": "Subir avatar",
        "settings.avatar_tip": "Formatos permitidos: PNG, JPG, WEBP.",
        "settings.avatar_save": "Guardar avatar",
        "settings.avatar_reset": "Restablecer",
        "settings.edit_profile": "Editar perfil",
        "settings.public_profile_title": "Perfil público",
        "settings.public_profile_desc": "Activa tu perfil público para que otros usuarios puedan verlo.",
        "settings.public_profile_switch": "Perfil público",
        "settings.public_profile_switch_sub": "OFF = solo tú. ON = visible para usuarios logueados.",
        "settings.public_profile_link": "Tu enlace público",
        "settings.public_profile_hint": "Comparte este enlace.",
        "settings.copy_link": "Copiar",
        "settings.open_profile": "Abrir",
        "settings.security": "Seguridad",
        "settings.security_desc": "Actualiza tu contraseña cuando lo necesites.",
        "settings.pass_current": "Contraseña actual",
        "settings.pass_new": "Nueva contraseña",
        "settings.pass_confirm": "Confirmar contraseña",
        "settings.pass_update": "Actualizar contraseña",
        "settings.name_invalid": "Nombre inválido.",
        "settings.pass_mismatch": "Las contraseñas no coinciden.",
        "settings.pass_invalid": "La contraseña no cumple requisitos.",
        "settings.pass_current_bad": "Contraseña actual incorrecta.",
        "settings.avatar_invalid": "Formato de imagen inválido.",
        "settings.avatar_saved": "Avatar actualizado.",
        "settings.saved": "Cambios guardados.",
        "auth.title_tab": "Acceso",
        "auth.err_email": "El email es obligatorio.",
        "auth.err_email_fmt": "Formato de email inválido.",
        "auth.err_pass": "La contraseña es obligatoria.",
        "auth.err_pass_len": "Debe tener mínimo 8 caracteres.",
        "auth.err_pass_mix": "Debe incluir letras y números.",
        "auth.err_name": "Tu nombre debe tener mínimo 3 caracteres.",
        "auth.err_name_len": "Tu nombre no puede superar 30 caracteres.",
        "auth.err_pass2": "Confirma tu contraseña.",
        "auth.err_pass2_match": "Las contraseñas no coinciden.",
        "auth.err_email_exists": "Ese email ya existe. Inicia sesión.",
        "auth.err_create_fail": "No se pudo crear el usuario.",
        "auth.register_ok": "Cuenta creada. Bienvenido al Reino.",
        "auth.login_bad": "Credenciales incorrectas",
        "auth.badge": "Acceso Premium",
        "auth.pitch": "Entra al sistema, crea tu cuenta y automatiza como un CEO. Control total del Reino.",
        "auth.b1": "Dashboard profesional y control del Reino.",
        "auth.b2": "Automatizaciones, Scheduler y decisiones rápidas.",
        "auth.b3": "Ecosistema premium para crecer y monetizar.",
        "auth.feature_today": "FEATURED · HOY",
        "auth.feature_1_tag": "NIVEL DIOS",
        "auth.feature_1_title": "Control total del Reino",
        "auth.feature_1_sub": "KPIs, automatizaciones, control del scheduler y decisiones rápidas estilo CEO.",
        "auth.feature_1_pill": "KR",
        "auth.feature_1_meta": "Sistema PRO",
        "auth.feature_1_ribbon": "AUTOMATIONS",
        "auth.feature_2_tag": "CORE IA",
        "auth.feature_2_title": "Chat + Memoria + Tools",
        "auth.feature_2_sub": "El Core responde y registra memoria para optimizar el sistema.",
        "auth.feature_2_pill": "READY",
        "auth.feature_2_meta": "Tools + Logs + Tasks",
        "auth.feature_2_ribbon": "CORE",
        "auth.feature_3_tag": "STORE / MARKET",
        "auth.feature_3_title": "Marketplace del Reino",
        "auth.feature_3_sub": "Instala plantillas listas y publica tus creaciones.",
        "auth.feature_3_pill": "PRO",
        "auth.feature_3_meta": "Creator + Ventas",
        "auth.feature_3_ribbon": "MARKET",
        "auth.tabs_aria": "Acceso",
        "auth.tab_login": "Iniciar sesión",
        "auth.tab_register": "Registro",
        "auth.name_label": "Nombre",
        "auth.name_ph": "Tu nombre (Ej: Edwin)",
        "auth.email_label": "Correo",
        "auth.email_ph": "Ej: correo@dominio.com",
        "auth.pass_label": "Contraseña",
        "auth.pass_ph": "Tu contraseña",
        "auth.pass2_label": "Confirmar contraseña",
        "auth.pass2_ph": "Repite tu contraseña",
        "auth.hint": "Ingresa tus datos para acceder al Reino.",
        "auth.btn_enter": "Entrar",
        "auth.btn_register": "Crear cuenta",
        "auth.note": "Privado: tu sesión es segura y tu actividad puede guardarse como memoria del sistema.",
        "ranking.subtitle": "Clasificación global por impacto en la plataforma (seguidores, likes y colecciones).",
        "ranking.global": "Global",
        "ranking.global_hint": "Ordenado por score (puntos + followers + likes*2 + collections*3).",
        "ranking.top": "TOP",
        "ranking.user": "Usuario",
        "ranking.role": "Rol",
        "ranking.followers": "Seguidores",
        "ranking.likes": "Likes",
        "ranking.collections": "Colecciones",
        "ranking.score": "Score",
        "ranking.you": "Tú",
        "ranking.empty": "Aún no hay datos suficientes para mostrar el ranking.",
        "ranking.how": "Cómo se calcula",
        "ranking.how_desc": "Por ahora usamos una fórmula simple y estable para arrancar.",
        "ranking.rule1": "Seguidores",
        "ranking.rule1_desc": "Cada seguidor suma 1 punto.",
        "ranking.rule2": "Likes recibidos",
        "ranking.rule2_desc": "Cada like recibido suma 2 puntos.",
        "ranking.rule3": "Colecciones recibidas",
        "ranking.rule3_desc": "Cada colección recibida suma 3 puntos.",
        "ranking.note": "Nota: luego metemos puntos por vistas y en vivo.",
        "ranking.points": "Puntos",
    },
    "en": {
        "app.name": "KING ROLON AUTOMATIONS",
        "nav.videos": "Videos",
        "nav.library": "My library",
        "nav.ranking": "Ranking",
        "nav.dashboard": "Dashboard",
        "nav.core": "Core",
        "nav.automations": "Automations",
        "nav.store": "Store",
        "nav.marketplace": "Marketplace",
        "nav.creator": "Creator",
        "nav.account": "Account",
        "nav.logout": "Logout",
        "settings.title_tab": "Account",
        "settings.title": "Account",
        "settings.subtitle": "Profile, language, avatar and security.",
        "settings.identity": "Identity",
        "settings.identity_sub": "Your info inside the Kingdom.",
        "settings.email": "Email",
        "settings.creator_name": "Name",
        "settings.creator_name_ph": "e.g. Edwin Rolon",
        "settings.plan_role": "Plan",
        "settings.creator_data": "Creator data",
        "settings.creator_data_sub": "Update your profile preferences.",
        "settings.lang_system": "System language",
        "settings.lang_note": "This changes the whole interface.",
        "settings.avatar_title": "Avatar",
        "settings.avatar_desc": "Upload a profile image.",
        "settings.avatar_upload": "Upload avatar",
        "settings.avatar_tip": "Allowed: PNG, JPG, WEBP.",
        "settings.avatar_save": "Save avatar",
        "settings.avatar_reset": "Reset",
        "settings.public_profile_title": "Public profile",
        "settings.public_profile_desc": "Enable your public profile.",
        "settings.public_profile_switch": "Public profile",
        "settings.public_profile_switch_sub": "OFF = only you. ON = visible to logged-in users.",
        "settings.public_profile_link": "Your public link",
        "settings.copy_link": "Copy",
        "settings.open_profile": "Open",
        "settings.security": "Security",
        "settings.security_desc": "Update your password when needed.",
        "settings.pass_current": "Current password",
        "settings.pass_new": "New password",
        "settings.pass_confirm": "Confirm password",
        "settings.pass_update": "Update password",
    },
    "de": {
        "app.name": "KING ROLON AUTOMATIONS",
        "nav.videos": "Videos",
        "nav.library": "Meine Bibliothek",
        "nav.ranking": "Ranking",
        "nav.dashboard": "Dashboard",
        "nav.core": "Core",
        "nav.automations": "Automationen",
        "nav.store": "Store",
        "nav.marketplace": "Marketplace",
        "nav.creator": "Creator",
        "nav.account": "Konto",
        "nav.logout": "Abmelden",
        "settings.title_tab": "Konto",
        "settings.title": "Konto",
        "settings.subtitle": "Profil, Sprache, Avatar und Sicherheit.",
        "settings.identity": "Identität",
        "settings.identity_sub": "Deine Infos im Königreich.",
        "settings.email": "E-Mail",
        "settings.creator_name": "Name",
        "settings.creator_name_ph": "z.B. Edwin Rolon",
        "settings.plan_role": "Plan",
        "settings.creator_data": "Creator-Daten",
        "settings.creator_data_sub": "Passe dein Profil an.",
        "settings.lang_system": "Systemsprache",
        "settings.lang_note": "Das ändert die gesamte Oberfläche.",
        "settings.avatar_title": "Avatar",
        "settings.avatar_desc": "Lade ein Profilbild hoch.",
        "settings.avatar_upload": "Avatar hochladen",
        "settings.avatar_tip": "Erlaubt: PNG, JPG, WEBP.",
        "settings.avatar_save": "Avatar speichern",
        "settings.avatar_reset": "Zurücksetzen",
        "settings.public_profile_title": "Öffentliches Profil",
        "settings.public_profile_desc": "Aktiviere dein öffentliches Profil.",
        "settings.public_profile_switch": "Öffentliches Profil",
        "settings.public_profile_switch_sub": "OFF = nur du. ON = sichtbar für eingeloggte Nutzer.",
        "settings.public_profile_link": "Dein öffentlicher Link",
        "settings.copy_link": "Kopieren",
        "settings.open_profile": "Öffnen",
        "settings.security": "Sicherheit",
        "settings.security_desc": "Aktualisiere dein Passwort bei Bedarf.",
        "settings.pass_current": "Aktuelles Passwort",
        "settings.pass_new": "Neues Passwort",
        "settings.pass_confirm": "Passwort bestätigen",
        "settings.pass_update": "Passwort aktualisieren",
    },
}


def get_lang() -> str:
    code = (session.get("lang") or os.getenv("KR_LANG") or "es").strip().lower()
    return code if code in ("es", "en", "de") else "es"


def t(key: str, **kwargs) -> str:
    lang = get_lang()
    txt = (I18N.get(lang) or {}).get(key)
    if not txt:
        txt = (I18N.get("es") or {}).get(key)
    if not txt:
        txt = key
    for k, v in kwargs.items():
        txt = txt.replace("{" + str(k) + "}", str(v))
    return txt


@app.get("/set_language/<code>", endpoint="set_language")
def set_language(code: str):
    code = (code or "").strip().lower()
    if code not in ("es", "en", "de"):
        code = "es"
    session["lang"] = code

    ref = request.referrer or ""
    host_url = (request.host_url or "").rstrip("/")
    if ref.startswith(host_url):
        return redirect(ref)

    return _safe_redirect_after_lang_change()


@app.get("/lang/<code>", endpoint="set_language_alias")
def set_language_alias(code: str):
    return set_language(code)


# =========================================================
# TEMPLATE GLOBALS
# =========================================================
@app.context_processor
def inject_globals():
    vm_loaded = bool(globals().get("_video_module_loaded", False))
    return {
        "t": t,
        "lang": get_lang(),
        "kr_asset_ver": app.config.get("KR_ASSET_VER", "dev"),
        "current_user": getattr(g, "user", None),
        "I18N": I18N,
        "video_module_loaded": vm_loaded,
        "video_import_error": _video_import_error or "",
    }


# =========================================================
# USER LOAD + LANGUAGE SYNC + POINTS
# =========================================================
@app.before_request
def load_user():
    if _env_secure is None:
        is_https = bool(request.is_secure) or _force_https_enabled()
        app.config["SESSION_COOKIE_SECURE"] = bool(is_https)

    user_id = session.get("user_id")
    g.user = None

    if user_id:
        try:
            with get_connection() as c:
                r = c.execute("SELECT * FROM users WHERE id=?", (int(user_id),)).fetchone()
                g.user = dict(r) if r else None
        except Exception:
            g.user = None

        if user_id and not g.user:
            session.clear()
            return redirect(url_for("auth"))

        if g.user and isinstance(g.user, dict):
            try:
                if get_points:
                    g.user["points"] = int(get_points(int(user_id)))  # type: ignore
                else:
                    g.user["points"] = int(g.user.get("points") or 0)
            except Exception:
                g.user["points"] = 0

    try:
        if g.user and isinstance(g.user, dict):
            db_lang = (g.user.get("language") or "").strip().lower()
            if db_lang in ("es", "en", "de") and session.get("lang") != db_lang:
                session["lang"] = db_lang
    except Exception:
        pass

    return None


# =========================================================
# AUTH
# =========================================================
_EMAIL_RE = re.compile(r"^[^\s@]+@[^\s@]+\.[^\s@]+$")
_USERNAME_RE = re.compile(r"^[a-z0-9_]{3,20}$")


def _sanitize_email(v: str) -> str:
    if not v:
        return ""
    v = re.sub(r"[\u200B-\u200D\uFEFF]", "", v)
    v = re.sub(r"\s+", "", v)
    return v.strip().lower()


def _sanitize_username(v: str) -> str:
    if not v:
        return ""
    v = (v or "").strip().lower()
    v = v.replace("-", "_")
    v = re.sub(r"[^a-z0-9_]", "", v)
    v = re.sub(r"_{2,}", "_", v).strip("_")
    return v


def _valid_username(v: str) -> bool:
    return bool(_USERNAME_RE.match(v or ""))


def _has_letter_and_number(pw: str) -> bool:
    return bool(re.search(r"[A-Za-z]", pw or "")) and bool(re.search(r"[0-9]", pw or ""))


def _user_plan(user_row: Optional[Dict[str, Any]]) -> str:
    if not user_row:
        return str(t("dashboard.plan_default")).upper()
    plan = user_row.get("plan") or user_row.get("role") or t("dashboard.plan_default") or t("dashboard.plan_default")
    return str(plan).upper()


@app.route("/auth", methods=["GET", "POST"], endpoint="auth")
def auth():
    if request.method == "GET":
        return render_template("auth.html", active_page="auth")

    action = (request.form.get("action") or "login").strip().lower()
    email = _sanitize_email(request.form.get("email", "") or "")
    password = (request.form.get("password", "") or "").strip()

    name = (request.form.get("name", "") or "").strip()
    confirm = (request.form.get("confirm_password") or "").strip()

    if not email:
        flash(t("auth.err_email"), "error")
        return redirect(url_for("auth"))
    if not _EMAIL_RE.match(email):
        flash(t("auth.err_email_fmt"), "error")
        return redirect(url_for("auth"))

    if not password:
        flash(t("auth.err_pass"), "error")
        return redirect(url_for("auth"))
    if len(password) < 8:
        flash(t("auth.err_pass_len"), "error")
        return redirect(url_for("auth"))
    if not _has_letter_and_number(password):
        flash(t("auth.err_pass_mix"), "error")
        return redirect(url_for("auth"))

    if action == "register":
        if not name or len(name) < 3:
            flash(t("auth.err_name"), "error")
            return redirect(url_for("auth"))
        if len(name) > 30:
            flash(t("auth.err_name_len"), "error")
            return redirect(url_for("auth"))
        if not confirm:
            flash(t("auth.err_pass2"), "error")
            return redirect(url_for("auth"))
        if password != confirm:
            flash(t("auth.err_pass2_match"), "error")
            return redirect(url_for("auth"))

        user: Optional[Dict[str, Any]] = None
        with get_connection() as c:
            exists = c.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()
            if exists:
                flash(t("auth.err_email_exists"), "error")
                return redirect(url_for("auth"))

            pw_hash = generate_password_hash(password)
            inserted = False
            try:
                c.execute(
                    """
                    INSERT INTO users (email, password_hash, name, role, language, created_at)
                    VALUES (?, ?, ?, 'FREE', ?, datetime('now'))
                    """,
                    (email, pw_hash, name, get_lang()),
                )
                inserted = True
            except Exception:
                try:
                    c.execute(
                        "INSERT INTO users (email, password_hash, name, created_at) VALUES (?, ?, ?, datetime('now'))",
                        (email, pw_hash, name),
                    )
                    inserted = True
                except Exception:
                    inserted = False

            if inserted:
                c.commit()
                r = c.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
                user = dict(r) if r else None

        if not user:
            flash(t("auth.err_create_fail"), "error")
            return redirect(url_for("auth"))

        session["user_id"] = int(user["id"])
        session.permanent = True

        try:
            db_lang = (user.get("language") or "").strip().lower()
            if db_lang in ("es", "en", "de"):
                session["lang"] = db_lang
        except Exception:
            pass

        log_event("REGISTER", user_id=int(user["id"]), meta={"email": email})
        flash(t("auth.register_ok"), "success")
        return _safe_redirect_after_lang_change()

    with get_connection() as c:
        r = c.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()
        user = dict(r) if r else None

    if (not user) or (not check_password_hash(user.get("password_hash", "") or "", password)):
        flash(t("auth.login_bad"), "error")
        return redirect(url_for("auth"))

    session["user_id"] = int(user["id"])
    session.permanent = True

    try:
        db_lang = (user.get("language") or "").strip().lower()
        if db_lang in ("es", "en", "de"):
            session["lang"] = db_lang
    except Exception:
        pass

    log_event("LOGIN", user_id=int(user["id"]), meta={"email": email})
    return _safe_redirect_after_lang_change()


@app.get("/logout", endpoint="logout")
def logout():
    _uid = uid()
    try:
        if _uid:
            log_event("LOGOUT", user_id=int(_uid), meta={})
    except Exception:
        pass
    session.clear()
    return redirect(url_for("auth"))


# =========================================================
# HELPERS
# =========================================================
def _last_logs(n: int = 8) -> List[str]:
    try:
        if LOG_PATH.exists():
            return LOG_PATH.read_text(encoding="utf-8", errors="ignore").splitlines()[-n:]
    except Exception:
        pass
    return []


# =========================================================
# HOME PRINCIPAL
# =========================================================
@app.get("/", endpoint="home")
@login_required
def home():
    return _safe_redirect_after_lang_change()


# =========================================================
# DASHBOARD
# =========================================================
@app.get("/dashboard", endpoint="dashboard")
@login_required
def dashboard():
    tareas = load_tasks() or []
    activas = [x for x in tareas if bool(x.get("activa"))]
    scheduler_status = get_scheduler_status() or {"running": False}

    user_name = ((g.user or {}).get("name") if getattr(g, "user", None) else "") or t("dashboard.user_default")
    plan = _user_plan(getattr(g, "user", None))

    return render_template(
        "dashboard.html",
        active_page="dashboard",
        tareas=tareas,
        tareas_activas=activas,
        plataformas=[],
        ingresos_mensuales=0,
        scheduler_status=scheduler_status,
        chart_labels=[],
        chart_values=[],
        ultimos_logs=_last_logs(8),
        affiliate_boxes=[],
        user_name=user_name,
        user_plan=plan,
    )


# =========================================================
# RANKING
# =========================================================
@app.get("/ranking", endpoint="ranking")
@login_required
def ranking():
    me = uid()
    if not me:
        return redirect(url_for("auth"))
    me = int(me)

    rows: List[Dict[str, Any]] = []

    q_with_user_points = """
    WITH
      f AS (
        SELECT followed_id AS user_id, COUNT(*) AS followers
        FROM followers
        GROUP BY followed_id
      ),
      vl AS (
        SELECT v.user_id AS user_id, COUNT(*) AS likes_received
        FROM video_likes l
        JOIN videos v ON v.id = l.video_id
        GROUP BY v.user_id
      ),
      vc AS (
        SELECT v.user_id AS user_id, COUNT(*) AS collections_received
        FROM video_collections c
        JOIN videos v ON v.id = c.video_id
        GROUP BY v.user_id
      )
    SELECT
      u.id AS user_id,
      COALESCE(u.name, u.email) AS display_name,
      u.role AS role,
      COALESCE(p.points, 0) AS points,
      COALESCE(f.followers, 0) AS followers,
      COALESCE(vl.likes_received, 0) AS likes_received,
      COALESCE(vc.collections_received, 0) AS collections_received,
      (
        COALESCE(p.points, 0)
        + COALESCE(f.followers, 0)
        + (COALESCE(vl.likes_received, 0) * 2)
        + (COALESCE(vc.collections_received, 0) * 3)
      ) AS score
    FROM users u
    LEFT JOIN user_points p ON p.user_id = u.id
    LEFT JOIN f  ON f.user_id  = u.id
    LEFT JOIN vl ON vl.user_id = u.id
    LEFT JOIN vc ON vc.user_id = u.id
    ORDER BY score DESC, followers DESC, likes_received DESC, collections_received DESC, u.id ASC
    LIMIT 200;
    """

    q_no_points_table = """
    WITH
      f AS (
        SELECT followed_id AS user_id, COUNT(*) AS followers
        FROM followers
        GROUP BY followed_id
      ),
      vl AS (
        SELECT v.user_id AS user_id, COUNT(*) AS likes_received
        FROM video_likes l
        JOIN videos v ON v.id = l.video_id
        GROUP BY v.user_id
      ),
      vc AS (
        SELECT v.user_id AS user_id, COUNT(*) AS collections_received
        FROM video_collections c
        JOIN videos v ON v.id = c.video_id
        GROUP BY v.user_id
      )
    SELECT
      u.id AS user_id,
      COALESCE(u.name, u.email) AS display_name,
      u.role AS role,
      0 AS points,
      COALESCE(f.followers, 0) AS followers,
      COALESCE(vl.likes_received, 0) AS likes_received,
      COALESCE(vc.collections_received, 0) AS collections_received,
      (
        COALESCE(f.followers, 0)
        + (COALESCE(vl.likes_received, 0) * 2)
        + (COALESCE(vc.collections_received, 0) * 3)
      ) AS score
    FROM users u
    LEFT JOIN f  ON f.user_id  = u.id
    LEFT JOIN vl ON vl.user_id = u.id
    LEFT JOIN vc ON vc.user_id = u.id
    ORDER BY score DESC, followers DESC, likes_received DESC, collections_received DESC, u.id ASC
    LIMIT 200;
    """

    try:
        with get_connection() as conn:
            if _table_exists(conn, "user_points"):
                rows = [dict(r) for r in conn.execute(q_with_user_points).fetchall()]
            else:
                rows = [dict(r) for r in conn.execute(q_no_points_table).fetchall()]
    except Exception:
        rows = []

    return render_template("ranking.html", active_page="ranking", me=me, rows=rows)


# =========================================================
# PUBLIC PROFILE + FOLLOW
# =========================================================
def _ensure_followers_table():
    try:
        with get_connection() as c:
            if not _table_exists(c, "followers"):
                c.execute(
                    """
                    CREATE TABLE IF NOT EXISTS followers (
                      follower_id INTEGER NOT NULL,
                      followed_id INTEGER NOT NULL,
                      created_at TEXT DEFAULT (datetime('now')),
                      UNIQUE(follower_id, followed_id)
                    );
                    """
                )
                c.commit()
    except Exception:
        pass


def _is_following(me_id: int, target_id: int) -> bool:
    if me_id <= 0 or target_id <= 0 or me_id == target_id:
        return False

    if db_is_following:
        try:
            return bool(db_is_following(int(me_id), int(target_id)))  # type: ignore
        except Exception:
            pass

    _ensure_followers_table()
    try:
        with get_connection() as c:
            r = c.execute(
                "SELECT 1 FROM followers WHERE follower_id=? AND followed_id=? LIMIT 1",
                (int(me_id), int(target_id)),
            ).fetchone()
            return bool(r)
    except Exception:
        return False


def _toggle_follow(me_id: int, target_id: int) -> bool:
    if me_id <= 0 or target_id <= 0 or me_id == target_id:
        return False

    if db_toggle_follow:
        try:
            return bool(db_toggle_follow(int(me_id), int(target_id)))  # type: ignore
        except Exception:
            pass

    _ensure_followers_table()
    try:
        with get_connection() as c:
            r = c.execute(
                "SELECT 1 FROM followers WHERE follower_id=? AND followed_id=? LIMIT 1",
                (int(me_id), int(target_id)),
            ).fetchone()

            if r:
                c.execute(
                    "DELETE FROM followers WHERE follower_id=? AND followed_id=?",
                    (int(me_id), int(target_id)),
                )
                c.commit()
                return False

            c.execute(
                "INSERT OR IGNORE INTO followers (follower_id, followed_id, created_at) VALUES (?, ?, datetime('now'))",
                (int(me_id), int(target_id)),
            )
            c.commit()
            return True
    except Exception:
        return False


def _profile_stats(user_id: int) -> Dict[str, int]:
    if get_user_social_stats:
        try:
            st = get_user_social_stats(int(user_id))  # type: ignore
            if isinstance(st, dict):
                return {
                    "followers": int(st.get("followers") or 0),
                    "following": int(st.get("following") or 0),
                    "videos_public": int(st.get("videos_public") or 0),
                    "likes_received": int(st.get("likes_received") or 0),
                    "collections_received": int(st.get("collections_received") or 0),
                    "points": int(st.get("points") or 0),
                }
        except Exception:
            pass

    out = {
        "followers": 0,
        "following": 0,
        "videos_public": 0,
        "likes_received": 0,
        "collections_received": 0,
        "points": 0,
    }

    try:
        if get_points:
            out["points"] = int(get_points(int(user_id)))  # type: ignore
    except Exception:
        out["points"] = 0

    try:
        with get_connection() as c:
            if _table_exists(c, "followers"):
                r1 = c.execute("SELECT COUNT(*) AS n FROM followers WHERE followed_id=?", (int(user_id),)).fetchone()
                r2 = c.execute("SELECT COUNT(*) AS n FROM followers WHERE follower_id=?", (int(user_id),)).fetchone()
                out["followers"] = int((r1["n"] if r1 else 0) or 0)
                out["following"] = int((r2["n"] if r2 else 0) or 0)

            if _table_exists(c, "videos"):
                rv = c.execute(
                    "SELECT COUNT(*) AS n FROM videos WHERE user_id=? AND COALESCE(visibility,'public')='public'",
                    (int(user_id),),
                ).fetchone()
                out["videos_public"] = int((rv["n"] if rv else 0) or 0)

            if _table_exists(c, "video_likes") and _table_exists(c, "videos"):
                rl = c.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM video_likes l
                    JOIN videos v ON v.id = l.video_id
                    WHERE v.user_id=?
                    """,
                    (int(user_id),),
                ).fetchone()
                out["likes_received"] = int((rl["n"] if rl else 0) or 0)

            if _table_exists(c, "video_collections") and _table_exists(c, "videos"):
                rc = c.execute(
                    """
                    SELECT COUNT(*) AS n
                    FROM video_collections col
                    JOIN videos v ON v.id = col.video_id
                    WHERE v.user_id=?
                    """,
                    (int(user_id),),
                ).fetchone()
                out["collections_received"] = int((rc["n"] if rc else 0) or 0)
    except Exception:
        pass

    return out


def _profile_banner_transform(profile_user: Dict[str, Any]) -> Dict[str, float]:
    try:
        user_id = int(profile_user.get("id") or 0)
    except Exception:
        user_id = 0

    default = {"x": 50.0, "y": 50.0, "scale": 1.0}
    if user_id <= 0:
        return default

    if db_get_user_banner_transform:
        try:
            out = db_get_user_banner_transform(int(user_id))  # type: ignore
            if isinstance(out, dict):
                return {
                    "x": _norm_percent(out.get("x"), 50.0),
                    "y": _norm_percent(out.get("y"), 50.0),
                    "scale": _norm_scale(out.get("scale"), 1.0),
                }
        except Exception:
            pass

    try:
        return {
            "x": _norm_percent(profile_user.get("banner_pos_x"), 50.0),
            "y": _norm_percent(profile_user.get("banner_pos_y"), 50.0),
            "scale": _norm_scale(profile_user.get("banner_scale"), 1.0),
        }
    except Exception:
        return default


def _public_videos_for_user(user_id: int, limit: int = 60) -> List[Dict[str, Any]]:
    if db_list_public_videos_by_user:
        try:
            return list(
                db_list_public_videos_by_user(int(user_id), limit=int(limit))
            )  # type: ignore
        except Exception:
            pass

    try:
        with get_connection() as c:
            if not _table_exists(c, "videos"):
                return []
            rows = c.execute(
                """
                SELECT id, user_id, title, description, visibility, size_bytes, created_at, thumbnail_filename
                FROM videos
                WHERE user_id=? AND COALESCE(visibility,'public')='public'
                ORDER BY datetime(created_at) DESC, id DESC
                LIMIT ?
                """,
                (int(user_id), int(limit)),
            ).fetchall()
            return [dict(r) for r in rows] if rows else []
    except Exception:
        return []


def _can_view_public_profile(viewer_id: int, profile_user: Dict[str, Any]) -> bool:
    try:
        owner_id = int(profile_user.get("id") or 0)
        if owner_id <= 0:
            return False
        is_public = int(profile_user.get("is_public") or 0)
        return bool(is_public == 1 or int(viewer_id) == int(owner_id))
    except Exception:
        return False


def _profile_not_found_redirect():
    flash("Perfil no encontrado.", "error")
    if "videos_feed" in current_app.view_functions:
        return redirect(url_for("videos_feed"))
    if "vm_videos_feed" in current_app.view_functions:
        return redirect(url_for("vm_videos_feed"))
    return redirect(url_for("dashboard"))


@app.post("/api/follow/<int:target_id>", endpoint="api_follow_toggle_v1")
@login_required
def api_follow_toggle_v1(target_id: int):
    me_id = uid()
    if not me_id:
        return jsonify(ok=False, error="auth"), 401

    target_id = int(target_id or 0)
    if target_id <= 0 or int(me_id) == int(target_id):
        return jsonify(ok=False, error="invalid"), 400

    following = _toggle_follow(int(me_id), int(target_id))
    try:
        log_event(
            "FOLLOW_TOGGLE",
            user_id=int(me_id),
            meta={"target_id": int(target_id), "following": bool(following)},
        )
    except Exception:
        pass

    return jsonify(ok=True, following=bool(following))


@app.post("/api/profile/banner/transform", endpoint="api_profile_banner_transform")
@login_required
def api_profile_banner_transform():
    me_id = uid()
    if not me_id:
        return jsonify(ok=False, error="auth"), 401

    data = request.get_json(silent=True) or {}
    pos_x = _norm_percent(data.get("x"), 50.0)
    pos_y = _norm_percent(data.get("y"), 50.0)
    scale = _norm_scale(data.get("scale"), 1.0)

    ok = False
    if db_update_user_banner_transform:
        try:
            ok = bool(
                db_update_user_banner_transform(
                    int(me_id),
                    banner_pos_x=pos_x,
                    banner_pos_y=pos_y,
                    banner_scale=scale,
                )
            )
        except Exception:
            ok = False

    if not ok:
        try:
            _safe_update_user(
                int(me_id),
                banner_pos_x=pos_x,
                banner_pos_y=pos_y,
                banner_scale=scale,
            )
            ok = True
        except Exception:
            ok = False

    if not ok:
        return jsonify(ok=False, error="save_failed"), 500

    try:
        log_event(
            "PROFILE_BANNER_TRANSFORM_UPDATED",
            user_id=int(me_id),
            meta={"x": pos_x, "y": pos_y, "scale": scale},
        )
    except Exception:
        pass

    return jsonify(ok=True, x=pos_x, y=pos_y, scale=scale)


@app.post("/api/videos/<video_id>/react", endpoint="api_video_react_toggle")
@login_required
def api_video_react_toggle(video_id: str):
    me_id = uid()
    if not me_id:
        return _json_err("auth", 401)

    vid = (video_id or "").strip()
    if not vid:
        return _json_err("video_id inválido", 400)

    data = request.get_json(silent=True) or {}
    emoji = (data.get("emoji") or data.get("reaction") or "").strip()

    if not emoji:
        return _json_err("emoji requerido", 400)

    if not db_toggle_video_reaction:
        return _json_err("reactions no disponibles (DB)", 501)

    try:
        out = db_toggle_video_reaction(vid, int(me_id), emoji)  # type: ignore
        if not isinstance(out, dict):
            out = {"ok": True}
    except Exception as e:
        return _json_err(f"DB error: {e}", 500)

    try:
        log_event(
            "VIDEO_REACT_TOGGLE",
            user_id=int(me_id),
            meta={"video_id": vid, "emoji": emoji},
        )
    except Exception:
        pass

    selected = None
    counts = {}
    total = 0
    try:
        selected = out.get("selected")
        counts = out.get("counts") or {}
        total = int(out.get("total") or 0)
    except Exception:
        selected = None
        counts = {}
        total = 0

    return jsonify(ok=True, video_id=vid, selected=selected, counts=counts, total=total)


@app.get("/u/<slug>", endpoint="public_profile_slug")
@login_required
def public_profile_slug(slug: str):
    me_id = uid()
    if not me_id:
        return redirect(url_for("auth"))

    s = (slug or "").strip()
    if not s:
        return _profile_not_found_redirect()

    u: Optional[Dict[str, Any]] = None
    try:
        if get_user_by_public_slug:
            u = get_user_by_public_slug(s)  # type: ignore
        else:
            with get_connection() as c:
                r = c.execute("SELECT * FROM users WHERE public_slug=? LIMIT 1", (s,)).fetchone()
                u = dict(r) if r else None
    except Exception:
        u = None

    if not u:
        return _profile_not_found_redirect()

    if not _can_view_public_profile(int(me_id), u):
        flash("Este perfil es privado.", "error")
        return redirect(url_for("profile_me"))

    user_id = int(u.get("id") or 0)
    stats = _profile_stats(user_id)
    videos = _public_videos_for_user(user_id, limit=60)
    banner_transform = _profile_banner_transform(u)

    can_follow = bool(int(me_id) != int(user_id))
    following = False
    if can_follow:
        following = _is_following(int(me_id), int(user_id))

    return render_template(
        "profile.html",
        active_page="profile",
        profile_user=u,
        stats=stats,
        videos=videos,
        me=int(me_id),
        can_follow=can_follow,
        following=following,
        banner_transform=banner_transform,
    )


@app.get("/@<username>", endpoint="public_profile_handle")
@login_required
def public_profile_handle(username: str):
    me_id = uid()
    if not me_id:
        return redirect(url_for("auth"))

    handle = (username or "").strip().lower()
    if not handle:
        return _profile_not_found_redirect()

    u: Optional[Dict[str, Any]] = None
    try:
        if get_user_by_username:
            u = get_user_by_username(handle)  # type: ignore
        else:
            with get_connection() as c:
                r = c.execute("SELECT * FROM users WHERE username=? LIMIT 1", (handle,)).fetchone()
                u = dict(r) if r else None
    except Exception:
        u = None

    if not u:
        return _profile_not_found_redirect()

    try:
        ps = (u.get("public_slug") or "").strip()
        if ps:
            return redirect(url_for("public_profile_slug", slug=ps))
    except Exception:
        pass

    if not _can_view_public_profile(int(me_id), u):
        flash("Este perfil es privado.", "error")
        return redirect(url_for("profile_me"))

    user_id = int(u.get("id") or 0)
    stats = _profile_stats(user_id)
    videos = _public_videos_for_user(user_id, limit=60)
    banner_transform = _profile_banner_transform(u)

    can_follow = bool(int(me_id) != int(user_id))
    following = False
    if can_follow:
        following = _is_following(int(me_id), int(user_id))

    return render_template(
        "profile.html",
        active_page="profile",
        profile_user=u,
        stats=stats,
        videos=videos,
        me=int(me_id),
        can_follow=can_follow,
        following=following,
        banner_transform=banner_transform,
    )


@app.get("/profile", endpoint="profile_me")
@login_required
def profile_me():
    me_id = uid()
    if not me_id:
        return redirect(url_for("auth"))

    u = getattr(g, "user", None) or {}
    ps = (u.get("public_slug") or "").strip()
    if ps:
        return redirect(url_for("public_profile_slug", slug=ps))

    uname = (u.get("username") or "").strip()
    if uname:
        return redirect(url_for("public_profile_handle", username=uname))

    return redirect(url_for("public_profile_id", user_id=int(me_id)))


@app.get("/profile/<int:user_id>", endpoint="public_profile_id")
@login_required
def public_profile_id(user_id: int):
    me_id = uid()
    if not me_id:
        return redirect(url_for("auth"))

    user_id = int(user_id or 0)
    if user_id <= 0:
        return _profile_not_found_redirect()

    u: Optional[Dict[str, Any]] = None
    try:
        if get_user_by_id:
            u = get_user_by_id(user_id)  # type: ignore
        else:
            with get_connection() as c:
                r = c.execute("SELECT * FROM users WHERE id=? LIMIT 1", (int(user_id),)).fetchone()
                u = dict(r) if r else None
    except Exception:
        u = None

    if not u:
        return _profile_not_found_redirect()

    ps = (u.get("public_slug") or "").strip()
    if ps:
        return redirect(url_for("public_profile_slug", slug=ps))

    if not _can_view_public_profile(int(me_id), u):
        flash("Este perfil es privado.", "error")
        return redirect(url_for("profile_me"))

    stats = _profile_stats(int(user_id))
    videos = _public_videos_for_user(int(user_id), limit=60)
    banner_transform = _profile_banner_transform(u)

    can_follow = bool(int(me_id) != int(user_id))
    following = False
    if can_follow:
        following = _is_following(int(me_id), int(user_id))

    return render_template(
        "profile.html",
        active_page="profile",
        profile_user=u,
        stats=stats,
        videos=videos,
        me=int(me_id),
        can_follow=can_follow,
        following=following,
        banner_transform=banner_transform,
    )

# =========================================================
# CORE
# =========================================================
def _tool_scheduler_status() -> str:
    st = get_scheduler_status() or {}
    running = bool(st.get("running"))
    last_run_at = st.get("last_run_at") or "—"
    interval_seconds = st.get("interval_seconds") or "—"
    cycles = st.get("cycles_executed") or 0
    last_err = st.get("last_error") or "—"

    if get_lang() == "de":
        return (
            "**Scheduler Status**\n"
            f"- Läuft: {running}\n"
            f"- Letzter Lauf: {last_run_at}\n"
            f"- Intervall (s): {interval_seconds}\n"
            f"- Zyklen: {cycles}\n"
            f"- Letzter Fehler: {last_err}"
        )
    if get_lang() == "en":
        return (
            "**Scheduler Status**\n"
            f"- Running: {running}\n"
            f"- Last run: {last_run_at}\n"
            f"- Interval (s): {interval_seconds}\n"
            f"- Cycles: {cycles}\n"
            f"- Last error: {last_err}"
        )
    return (
        "**Estado del Scheduler**\n"
        f"- Activo: {running}\n"
        f"- Última ejecución: {last_run_at}\n"
        f"- Intervalo (s): {interval_seconds}\n"
        f"- Ciclos: {cycles}\n"
        f"- Último error: {last_err}"
    )


def _tool_last_logs(n: int = 8) -> str:
    logs = _last_logs(n)
    if not logs:
        return "—"
    return "\n".join(logs)


def _tool_tasks_summary() -> str:
    tareas = load_tasks() or []
    activas = [tsk for tsk in tareas if bool(tsk.get("activa"))]
    paused = len(tareas) - len(activas)

    if get_lang() == "de":
        return f"Aufgaben: **{len(tareas)}** gesamt · **{len(activas)}** aktiv · **{paused}** pausiert."
    if get_lang() == "en":
        return f"Tasks: **{len(tareas)}** total · **{len(activas)}** active · **{paused}** paused."
    return f"Tareas: **{len(tareas)}** total · **{len(activas)}** activas · **{paused}** pausadas."


def _route_tools(user_text: str) -> Optional[str]:
    text = (user_text or "").strip().lower()
    if not text:
        return None

    if "scheduler" in text:
        return _tool_scheduler_status()

    if "logs" in text or "log" in text or "actividad" in text:
        head = "Últimos logs:\n" if get_lang() == "es" else ("Latest logs:\n" if get_lang() == "en" else "Letzte Logs:\n")
        return head + (_tool_last_logs(10) or "—")

    if "tareas" in text or "tasks" in text or "automatizaciones" in text:
        return _tool_tasks_summary()

    return None


def _build_system_prompt() -> str:
    if get_lang() == "de":
        return (
            "Du bist **KING ROLON CORE**, eine Premium-KI für Automationen.\n"
            "Antworte präzise, hilfreich und intelligent.\n"
            "Wenn der Nutzer etwas aus dem System will (Scheduler/Logs/Tasks), nutze die Tools.\n"
            "Wenn etwas unklar ist, stelle höchstens 1 kurze Rückfrage – sonst liefere einen bestmöglichen Vorschlag.\n"
        )
    if get_lang() == "en":
        return (
            "You are **KING ROLON CORE**, a premium AI for automations.\n"
            "Answer accurately, helpfully, with strong reasoning.\n"
            "When the user asks about system state (scheduler/logs/tasks), use tools.\n"
            "If something is unclear, ask at most one short question—otherwise provide your best answer.\n"
        )
    return (
        "Eres **KING ROLON CORE**, una IA premium para automatizaciones.\n"
        "Responde con precisión, inteligencia y razonamiento.\n"
        "Si el usuario pide estado del sistema (scheduler/logs/tareas), usa herramientas internas.\n"
        "Si falta algo, haz como máximo 1 pregunta corta; si no, entrega el mejor resultado posible.\n"
    )


def _load_recent_chat(user_id: Optional[int], limit: int = 18) -> List[Dict[str, str]]:
    out: List[Dict[str, str]] = []
    if not user_id:
        return out

    try:
        rows = get_recent_memory(user_id, limit=limit) or []
    except Exception:
        rows = []

    for r in rows:
        if not isinstance(r, dict):
            continue
        role = str(r.get("role") or "user").strip().lower()
        content = str(r.get("content") or r.get("text") or "").strip()
        if not content:
            continue

        if role in ("ai", "assistant"):
            role = "assistant"
        elif role == "system":
            role = "system"
        else:
            role = "user"

        out.append({"role": role, "content": content})

    return out


def _openai_chat(system_prompt: str, messages: List[Dict[str, str]]) -> str:
    api_key = (os.getenv("OPENAI_API_KEY") or "").strip()
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY missing")

    model = (os.getenv("OPENAI_MODEL") or "gpt-4.1-mini").strip()

    try:
        from openai import OpenAI  # type: ignore

        client = OpenAI(api_key=api_key)
        chat_msgs = [{"role": "system", "content": system_prompt}] + messages
        resp = client.chat.completions.create(
            model=model,
            messages=chat_msgs,
            temperature=0.6,
            max_tokens=700,
        )
        return (resp.choices[0].message.content or "").strip() or "OK"
    except Exception:
        raise RuntimeError("OpenAI SDK not available or request failed")


def _local_fallback_answer(_: str) -> str:
    lang = get_lang()
    if lang == "de":
        return (
            "Ich kann antworten, aber der **LLM ist nicht konfiguriert**.\n\n"
            "Du kannst jetzt:\n"
            "- `scheduler` (Status)\n"
            "- `logs` (Aktivität)\n"
            "- `tareas` / `tasks` (Zusammenfassung)\n\n"
            "Für echte ChatGPT-Antworten setze `OPENAI_API_KEY` (und optional `OPENAI_MODEL`)."
        )
    if lang == "en":
        return (
            "I can reply, but the **LLM is not configured**.\n\n"
            "You can use:\n"
            "- `scheduler` (status)\n"
            "- `logs` (activity)\n"
            "- `tasks` (summary)\n\n"
            "For real ChatGPT-like answers, set `OPENAI_API_KEY` (and optionally `OPENAI_MODEL`)."
        )
    return (
        "Puedo responder, pero el **LLM no está configurado**.\n\n"
        "Puedes usar ahora:\n"
        "- `scheduler` (estado)\n"
        "- `logs` (actividad)\n"
        "- `tareas` (resumen)\n\n"
        "Para respuestas tipo ChatGPT real: configura `OPENAI_API_KEY` (y opcional `OPENAI_MODEL`)."
    )


def _core_reply_for(msg: str) -> str:
    text = (msg or "").strip()
    if not text:
        return t("core.err_empty")

    tool_out = _route_tools(text)
    if tool_out:
        return tool_out

    system_prompt = _build_system_prompt()
    history = _load_recent_chat(uid(), limit=18)
    history.append({"role": "user", "content": text})

    try:
        return _openai_chat(system_prompt, history)
    except Exception:
        return _local_fallback_answer(text)


@app.get("/core", endpoint="core")
@login_required
def core():
    try:
        memories = get_recent_memory(uid(), limit=16) or []
    except Exception:
        memories = []

    core_messages: List[Dict[str, str]] = []
    for m in memories:
        if not isinstance(m, dict):
            continue
        role = str(m.get("role") or "user").strip().lower()
        who = "ai" if role in ("assistant", "ai") else "user"
        text = str(m.get("content") or m.get("text") or "").strip()
        if text:
            core_messages.append({"role": who, "text": text})

    return render_template(
        "core.html",
        active_page="core",
        core_messages=core_messages,
        scheduler_status=get_scheduler_status() or {"running": False},
        ultimos_logs=_last_logs(6),
    )


@app.post("/api/core/message", endpoint="api_core_message")
@login_required
def api_core_message():
    data = request.get_json(silent=True) or {}
    msg = (data.get("message") or data.get("text") or "").strip()
    if not msg:
        return jsonify(ok=False, error="empty"), 400

    user_id = uid()

    try:
        save_memory(user_id, msg, role="user")
        rotate_memory(user_id)
    except Exception:
        pass

    reply = _core_reply_for(msg)

    try:
        save_memory(user_id, reply, role="assistant")
        rotate_memory(user_id)
    except Exception:
        pass

    return jsonify(ok=True, reply=reply)


@app.post("/api/core/chat", endpoint="api_core_chat_alias")
@login_required
def api_core_chat_alias():
    return api_core_message()


# =========================================================
# TASKS UI + APIs
# =========================================================
@app.get("/automatizaciones", endpoint="automatizaciones")
@login_required
def automatizaciones():
    tareas = load_tasks() or []
    tareas_activas = [tsk for tsk in tareas if bool(tsk.get("activa"))]

    plataformas_unicas = sorted(
        {
            ((tsk.get("plataforma") or t("tasks.unspecified")).strip() or t("tasks.unspecified"))
            for tsk in tareas
        }
    )

    return render_template(
        "tasks.html",
        active_page="automatizaciones",
        tareas=tareas,
        tareas_activas=tareas_activas,
        plataformas_unicas=plataformas_unicas,
    )


@app.post("/api/tasks", endpoint="api_create_task")
@login_required
def api_create_task():
    data = request.get_json(silent=True) or {}
    nombre = (data.get("nombre") or "").strip()
    if not nombre:
        return jsonify(ok=False, error="Nombre requerido"), 400

    tarea = {
        "id": uuid4().hex,
        "nombre": nombre,
        "plataforma": (data.get("plataforma") or t("tasks.unspecified")).strip() or t("tasks.unspecified"),
        "frecuencia": (data.get("frecuencia") or "").strip(),
        "activa": True,
        "creada_en": datetime.now().isoformat(timespec="seconds"),
    }

    tareas = load_tasks() or []
    tareas.append(tarea)
    save_tasks(tareas)
    log_event("TASK_CREATED", user_id=uid(), meta={"task_id": tarea["id"]})
    return jsonify(ok=True, task=tarea)


@app.put("/api/tasks/<task_id>", endpoint="api_update_task")
@login_required
def api_update_task(task_id: str):
    data = request.get_json(silent=True) or {}
    nombre = (data.get("nombre") or "").strip()
    if not nombre:
        return jsonify(ok=False, error="Nombre requerido"), 400

    tareas = load_tasks() or []
    for tsk in tareas:
        if str(tsk.get("id")) == str(task_id):
            tsk["nombre"] = nombre
            tsk["plataforma"] = (data.get("plataforma") or t("tasks.unspecified")).strip() or t("tasks.unspecified")
            tsk["frecuencia"] = (data.get("frecuencia") or "").strip()
            save_tasks(tareas)
            log_event("TASK_UPDATED", user_id=uid(), meta={"task_id": task_id})
            return jsonify(ok=True)

    return jsonify(ok=False, error="Not found"), 404


@app.post("/api/tasks/<task_id>/toggle", endpoint="api_toggle_task")
@login_required
def api_toggle_task(task_id: str):
    tareas = load_tasks() or []
    for tsk in tareas:
        if str(tsk.get("id")) == str(task_id):
            tsk["activa"] = not bool(tsk.get("activa", True))
            save_tasks(tareas)
            log_event("TASK_TOGGLED", user_id=uid(), meta={"task_id": task_id})
            return jsonify(ok=True)
    return jsonify(ok=False, error="Not found"), 404


@app.delete("/api/tasks/<task_id>", endpoint="api_delete_task")
@login_required
def api_delete_task(task_id: str):
    tareas = load_tasks() or []
    nuevas = [tsk for tsk in tareas if str(tsk.get("id")) != str(task_id)]
    if len(nuevas) == len(tareas):
        return jsonify(ok=False, error="Not found"), 404
    save_tasks(nuevas)
    log_event("TASK_DELETED", user_id=uid(), meta={"task_id": task_id})
    return jsonify(ok=True)


# =========================================================
# STORE
# =========================================================
@app.get("/store", endpoint="store")
@login_required
def store():
    try:
        products = get_products() or []
    except Exception:
        products = []
    return render_template("store.html", active_page="store", products=products)


# =========================================================
# MARKETPLACE
# =========================================================
@app.get("/marketplace", endpoint="marketplace")
@login_required
def marketplace():
    filter_platform = (request.args.get("platform") or "").strip()
    filter_category = (request.args.get("category") or "").strip()
    filter_max_price = (request.args.get("max_price") or "").strip()

    try:
        apps_public_all = [a for a in (list_apps() or []) if str(a.get("status") or "").lower() == "published"]
    except Exception:
        apps_public_all = []

    apps_public = apps_public_all

    if filter_platform:
        fp = filter_platform.lower()
        apps_public = [a for a in apps_public if fp in str(a.get("platform") or "").lower()]

    if filter_category:
        fc = filter_category.lower()
        apps_public = [a for a in apps_public if fc in str(a.get("category") or "").lower()]

    if filter_max_price:
        try:
            max_price = float(filter_max_price.replace(",", "."))
            max_cents = int(max_price * 100)
            apps_public = [a for a in apps_public if int(a.get("price_cents") or 0) <= max_cents]
        except Exception:
            pass

    try:
        my_apps = list_apps_by_owner(uid()) or []
    except Exception:
        my_apps = []

    return render_template(
        "marketplace.html",
        active_page="marketplace",
        apps_public=apps_public,
        my_apps=my_apps,
        apps_featured=apps_public[:6],
        apps_count=len(apps_public),
        filter_platform=filter_platform,
        filter_category=filter_category,
        filter_max_price=filter_max_price,
    )


@app.get("/marketplace/download/<slug>", endpoint="marketplace_download")
@login_required
def marketplace_download(slug: str):
    try:
        app_row = get_app_by_slug((slug or "").strip())
    except Exception:
        app_row = None

    if not app_row or str(app_row.get("status") or "").lower() != "published":
        flash(t("market.not_found"), "error")
        return redirect(url_for("marketplace"))

    price_cents = int(app_row.get("price_cents") or 0)

    try:
        record_app_sale(app_row.get("id"), uid(), price_cents=price_cents)
        log_event(
            "MARKETPLACE_DOWNLOAD",
            user_id=uid(),
            meta={"app_id": app_row.get("id"), "slug": slug, "price_cents": price_cents},
        )
    except Exception:
        pass

    flash(t("market.download_ok"), "success")
    return redirect(url_for("marketplace"))


@app.post("/marketplace/app/<app_id>/status", endpoint="marketplace_update_status")
@login_required
def marketplace_update_status(app_id: str):
    new_status = (request.form.get("status") or "").strip().lower()
    if new_status not in ("published", "draft"):
        flash(t("market.status_update_fail"), "error")
        return redirect(url_for("marketplace"))

    try:
        app_row = get_app_by_id(app_id)
    except Exception:
        app_row = None

    if not app_row:
        flash(t("market.not_found"), "error")
        return redirect(url_for("marketplace"))
    if not _is_owner(app_row, uid()):
        flash(t("market.not_allowed"), "error")
        return redirect(url_for("marketplace"))

    try:
        update_app_status(app_id, new_status)
        log_event("APP_STATUS_UPDATED", user_id=uid(), meta={"app_id": app_id, "status": new_status})
        flash(t("market.status_updated"), "success")
    except Exception:
        flash(t("market.status_update_fail"), "error")

    return redirect(url_for("marketplace"))


# =========================================================
# CREATOR
# =========================================================
@app.get("/creator", endpoint="creator_panel")
@login_required
def creator_panel():
    try:
        my_apps = list_apps_by_owner(uid()) or []
    except Exception:
        my_apps = []
    return render_template("creator_panel.html", active_page="creator", my_apps=my_apps)


# =========================================================
# SETTINGS
# =========================================================
_ALLOWED_AVATAR_MIME = {"image/png", "image/jpeg", "image/jpg", "image/webp"}
_ALLOWED_AVATAR_EXT = {".png", ".jpg", ".jpeg", ".webp"}

_ALLOWED_BANNER_MIME = {"image/png", "image/jpeg", "image/jpg", "image/webp"}
_ALLOWED_BANNER_EXT = {".png", ".jpg", ".jpeg", ".webp"}


def _safe_update_user(user_id: int, **fields):
    if not fields:
        return
    cols: List[str] = []
    vals: List[Any] = []
    for k, v in fields.items():
        cols.append(f"{k}=?")
        vals.append(v)
    vals.append(int(user_id))
    sql = f"UPDATE users SET {', '.join(cols)} WHERE id=?"
    with get_connection() as c:
        try:
            c.execute(sql, tuple(vals))
            c.commit()
        except Exception:
            pass


def _username_to_public_slug(username: str) -> str:
    slug = (username or "").strip().lower()
    slug = slug.replace("_", "-")
    slug = re.sub(r"[^a-z0-9-]", "", slug)
    slug = re.sub(r"-{2,}", "-", slug).strip("-")
    return slug[:40]


def _slug_available_for_user(slug: str, user_id: int) -> bool:
    if not slug:
        return False
    try:
        with get_connection() as c:
            r = c.execute(
                "SELECT id FROM users WHERE public_slug=? AND id<>? LIMIT 1",
                (slug, int(user_id)),
            ).fetchone()
            return not bool(r)
    except Exception:
        return False


def _unique_public_slug_for_user(username: str, user_id: int) -> Optional[str]:
    base = _username_to_public_slug(username)
    if not base:
        return None

    if _slug_available_for_user(base, int(user_id)):
        return base

    for i in range(2, 1000):
        candidate = f"{base}-{i}"
        if _slug_available_for_user(candidate, int(user_id)):
            return candidate

    return None


def _render_settings(error: str = "", success: str = ""):
    banner_transform = {"x": 50.0, "y": 50.0, "scale": 1.0}

    try:
        current_uid = uid()
    except Exception:
        current_uid = None

    if current_uid:
        if db_get_user_banner_transform:
            try:
                out = db_get_user_banner_transform(int(current_uid))  # type: ignore
                if isinstance(out, dict):
                    banner_transform = {
                        "x": _norm_percent(out.get("x"), 50.0),
                        "y": _norm_percent(out.get("y"), 50.0),
                        "scale": _norm_scale(out.get("scale"), 1.0),
                    }
            except Exception:
                banner_transform = {"x": 50.0, "y": 50.0, "scale": 1.0}
        else:
            try:
                with get_connection() as c:
                    r = c.execute(
                        """
                        SELECT
                          COALESCE(banner_pos_x, 50) AS banner_pos_x,
                          COALESCE(banner_pos_y, 50) AS banner_pos_y,
                          COALESCE(banner_scale, 1) AS banner_scale
                        FROM users
                        WHERE id=?
                        LIMIT 1
                        """,
                        (int(current_uid),),
                    ).fetchone()

                if r:
                    banner_transform = {
                        "x": _norm_percent(r["banner_pos_x"], 50.0),
                        "y": _norm_percent(r["banner_pos_y"], 50.0),
                        "scale": _norm_scale(r["banner_scale"], 1.0),
                    }
            except Exception:
                banner_transform = {"x": 50.0, "y": 50.0, "scale": 1.0}

    if error:
        flash(error, "error")
    if success:
        flash(success, "success")

    return render_template(
        "settings.html",
        active_page="settings",
        banner_transform=banner_transform,
    )


@app.route("/settings", methods=["GET", "POST"], endpoint="settings")
@login_required
def settings():
    user_id = uid()
    if not user_id:
        session.clear()
        return redirect(url_for("auth"))

    if request.method == "GET":
        success = session.pop("settings_success", "")
        return _render_settings(success=success)

    action = (request.form.get("action") or "").strip().lower()
    if action not in ("update_profile", "update_avatar", "update_banner"):
        action = "update_profile"

    # -----------------------------------------------------
    # AVATAR
    # -----------------------------------------------------
    if action == "update_avatar":
        f = request.files.get("avatar_file")
        if not f or not getattr(f, "filename", ""):
            return _render_settings(error=t("settings.avatar_invalid"))

        mime = (getattr(f, "mimetype", "") or "").lower()
        ext = Path(f.filename).suffix.lower()

        if mime not in _ALLOWED_AVATAR_MIME or ext not in _ALLOWED_AVATAR_EXT:
            return _render_settings(error=t("settings.avatar_invalid"))

        out_name = f"{int(user_id)}_{uuid4().hex}{ext}"
        out_path = AVATARS_DIR / out_name

        try:
            f.save(str(out_path))
        except Exception:
            return _render_settings(error=t("settings.avatar_invalid"))

        avatar_url = f"/static/uploads/avatars/{out_name}"

        try:
            _safe_update_user(int(user_id), avatar_url=avatar_url)
            log_event("AVATAR_UPDATED", user_id=int(user_id), meta={"avatar_url": avatar_url})
        except Exception:
            return _render_settings(error=t("settings.avatar_invalid"))

        session["settings_success"] = t("settings.avatar_saved")
        return redirect(url_for("settings"))

    # -----------------------------------------------------
    # BANNER / COVER
    # -----------------------------------------------------
    if action == "update_banner":
        f = request.files.get("banner_file")
        if not f or not getattr(f, "filename", ""):
            return _render_settings(error="Formato de portada inválido.")

        mime = (getattr(f, "mimetype", "") or "").lower()
        ext = Path(f.filename).suffix.lower()

        if mime not in _ALLOWED_BANNER_MIME or ext not in _ALLOWED_BANNER_EXT:
            return _render_settings(error="Formato de portada inválido.")

        out_name = f"{int(user_id)}_{uuid4().hex}{ext}"
        out_path = BANNERS_DIR / out_name

        try:
            f.save(str(out_path))
        except Exception:
            return _render_settings(error="No se pudo guardar la portada.")

        banner_url = f"/static/uploads/banners/{out_name}"

        try:
            _safe_update_user(
                int(user_id),
                banner_url=banner_url,
                banner_pos_x=50.0,
                banner_pos_y=50.0,
                banner_scale=1.0,
            )
            log_event(
                "BANNER_UPDATED",
                user_id=int(user_id),
                meta={"banner_url": banner_url, "x": 50.0, "y": 50.0, "scale": 1.0},
            )
        except Exception:
            return _render_settings(error="No se pudo actualizar la portada.")

        session["settings_success"] = "Portada actualizada."
        return redirect(url_for("settings"))

    # -----------------------------------------------------
    # PROFILE
    # -----------------------------------------------------
    new_name = (request.form.get("new_name") or "").strip()
    username = _sanitize_username(request.form.get("username", "") or "")
    bio = (request.form.get("bio") or "").strip()
    website = (request.form.get("website") or "").strip()
    banner_url = (request.form.get("banner_url") or "").strip()
    language = (request.form.get("language") or "").strip().lower()
    is_public_raw = (request.form.get("is_public") or "").strip()
    is_public = 1 if is_public_raw in ("1", "true", "on", "yes") else 0

    if new_name and (len(new_name) < 3 or len(new_name) > 30):
        return _render_settings(error=t("settings.name_invalid"))

    if username and not _valid_username(username):
        return _render_settings(
            error="Username inválido. Usa 3-20 caracteres: letras, números y _"
        )

    if len(bio) > 280:
        bio = bio[:280]

    if len(website) > 200:
        website = website[:200]

    if len(banner_url) > 500:
        banner_url = banner_url[:500]

    if language not in ("es", "en", "de"):
        language = get_lang()

    try:
        with get_connection() as c:
            if username:
                r = c.execute(
                    "SELECT id FROM users WHERE username=? AND id<>? LIMIT 1",
                    (username, int(user_id)),
                ).fetchone()
                if r:
                    return _render_settings(error="Ese username ya está en uso.")
    except Exception:
        pass

    public_slug = None
    if username:
        public_slug = _unique_public_slug_for_user(username, int(user_id))

    try:
        updates: Dict[str, Any] = {
            "language": language,
            "is_public": int(is_public),
            "username": (username or None),
            "bio": (bio or None),
            "website": (website or None),
            "banner_url": (banner_url or None),
        }

        if public_slug:
            updates["public_slug"] = public_slug

        if new_name:
            updates["name"] = new_name

        _safe_update_user(int(user_id), **updates)
        session["lang"] = language

        try:
            log_event(
                "PROFILE_UPDATED",
                user_id=int(user_id),
                meta={
                    "username": username or "",
                    "public_slug": public_slug or "",
                    "is_public": int(is_public),
                    "has_website": bool(website),
                    "has_banner": bool(banner_url),
                },
            )
        except Exception:
            pass
    except Exception:
        return _render_settings(error="No se pudieron guardar los cambios del perfil.")

    # -----------------------------------------------------
    # PASSWORD
    # -----------------------------------------------------
    current_pw = (request.form.get("current_password") or "").strip()
    new_pw = (request.form.get("new_password") or "").strip()
    confirm_pw = (request.form.get("confirm_password") or "").strip()

    if current_pw or new_pw or confirm_pw:
        if not (current_pw and new_pw and confirm_pw):
            return _render_settings(error=t("settings.pass_invalid"))

        if new_pw != confirm_pw:
            return _render_settings(error=t("settings.pass_mismatch"))

        if len(new_pw) < 8 or (not _has_letter_and_number(new_pw)):
            return _render_settings(error=t("settings.pass_invalid"))

        try:
            with get_connection() as c:
                r = c.execute("SELECT password_hash FROM users WHERE id=?", (int(user_id),)).fetchone()
                ph = (r["password_hash"] if r else "") or ""

            if (not ph) or (not check_password_hash(ph, current_pw)):
                return _render_settings(error=t("settings.pass_current_bad"))

            new_hash = generate_password_hash(new_pw)
            _safe_update_user(int(user_id), password_hash=new_hash)
            log_event("PASSWORD_UPDATED", user_id=int(user_id), meta={})
        except Exception:
            return _render_settings(error=t("settings.pass_invalid"))

    session["settings_success"] = t("settings.saved")
    return redirect(url_for("settings"))


# =========================================================
# REGISTER VIDEO MODULE
# =========================================================
_video_module_loaded = False
_video_register_error: Optional[str] = None

if register_video_routes and not app.config.get("KR_VIDEO_ROUTES_REGISTERED", False):
    try:
        register_video_routes(
            app,
            login_required,
            uid,
            log_event,
            PROJECT_DIR,
        )
        app.config["KR_VIDEO_ROUTES_REGISTERED"] = True
        _video_module_loaded = True
        try:
            log_event("VIDEO_MODULE_REGISTERED", meta={"ok": True})
        except Exception:
            pass
    except Exception as e:
        app.config["KR_VIDEO_ROUTES_REGISTERED"] = False
        _video_module_loaded = False
        _video_register_error = repr(e)
        try:
            log_event("VIDEO_MODULE_REGISTER_FAILED", meta={"err": _video_register_error})
        except Exception:
            pass


@app.context_processor
def _inject_video_state():
    return {
        "video_module_loaded": bool(_video_module_loaded),
        "video_import_error": _video_import_error or "",
        "video_register_error": _video_register_error or "",
    }


# =========================================================
# FALLBACKS MÍNIMOS
# =========================================================
def _video_module_message() -> str:
    if _video_register_error:
        return f"Video module falló al registrarse: {_video_register_error}"
    if _video_import_error:
        return f"Video module no pudo importarse: {_video_import_error}"
    return "Video module no disponible."


if "videos_feed" not in app.view_functions:

    @app.get("/videos", endpoint="videos_feed")
    @login_required
    def videos_feed():
        flash(_video_module_message(), "error")
        return render_template(
            "videos.html",
            active_page="videos",
            videos=[],
            me=(uid() or 0),
            top_rank=[],
            ad_card={},
            search_query="",
            sort_mode="viral",
            search_kind="general",
            result_count=0,
            did_you_mean="",
            recent_searches=[],
            trending_searches=[],
            creator_result_count=0,
            hashtag_result_count=0,
            semantic_result_count=0,
        )


if "videos_library" not in app.view_functions:

    @app.get("/videos/library", endpoint="videos_library")
    @login_required
    def videos_library():
        flash(_video_module_message(), "error")
        return render_template(
            "videos.html",
            active_page="library",
            videos=[],
            me=(uid() or 0),
            top_rank=[],
            ad_card={},
            search_query="",
            sort_mode="viral",
            search_kind="general",
            result_count=0,
            did_you_mean="",
            recent_searches=[],
            trending_searches=[],
            creator_result_count=0,
            hashtag_result_count=0,
            semantic_result_count=0,
        )


if "upload_video_page" not in app.view_functions:

    @app.get("/videos/upload", endpoint="upload_video_page")
    @login_required
    def upload_video_page():
        flash(_video_module_message(), "error")
        return render_template("upload_video.html", active_page="videos")


if "watch_video" not in app.view_functions:

    @app.get("/videos/watch/<video_id>", endpoint="watch_video")
    @login_required
    def watch_video(video_id: str):
        flash(_video_module_message(), "error")
        if "videos_feed" in app.view_functions:
            return redirect(url_for("videos_feed"))
        if "vm_videos_feed" in app.view_functions:
            return redirect(url_for("vm_videos_feed"))
        return redirect(url_for("dashboard"))


if "delete_video" not in app.view_functions:

    @app.post("/videos/delete/<video_id>", endpoint="delete_video")
    @login_required
    def delete_video(video_id: str):
        flash(_video_module_message(), "error")
        if "videos_feed" in app.view_functions:
            return redirect(url_for("videos_feed"))
        if "vm_videos_feed" in app.view_functions:
            return redirect(url_for("vm_videos_feed"))
        return redirect(url_for("dashboard"))


if "api_video_upload" not in app.view_functions:

    @app.post("/api/videos/upload", endpoint="api_video_upload")
    @login_required
    def api_video_upload():
        return jsonify(ok=False, error=_video_module_message()), 400


if "api_video_suggest" not in app.view_functions:

    @app.get("/api/videos/suggest", endpoint="api_video_suggest")
    @login_required
    def api_video_suggest():
        return jsonify(
            ok=True,
            items=[],
            suggestions=[],
            recent=[],
            trending=[],
            did_you_mean="",
        )


if "stream_video" not in app.view_functions:

    @app.get("/videos/stream/<video_id>", endpoint="stream_video")
    @login_required
    def stream_video(video_id: str):
        return "Video module no disponible", 404


# =========================================================
# DEBUG CHECK
# =========================================================
try:
    print("---- REGISTERED ROUTES (KEY) ----")
    for r in sorted(app.view_functions.keys()):
        if r in (
            "videos_feed",
            "videos_library",
            "upload_video_page",
            "watch_video",
            "delete_video",
            "api_video_upload",
            "api_video_suggest",
            "stream_video",
        ):
            print("•", r)
    print("---------------------------------")
except Exception:
    pass


# =========================================================
# RUN
# =========================================================
if __name__ == "__main__":
    try:
        start_scheduler()
    except Exception:
        pass

    app.run(host="0.0.0.0", port=5000, debug=True)