# -*- coding: utf-8 -*-
"""
core_engine.py — KING ROLON AUTOMATIONS
MODO DIOS • CORE LOCAL (SIN OPENAI POR AHORA)

Objetivo:
- Responder "tipo IA" (no respuestas ultra predeterminadas)
- Usar memoria (eye_memory) como contexto
- Tools locales: scheduler / logs / tareas
- Responder en el idioma actual del usuario (es/en/de) cuando sea posible

NOTA:
- web.py ya guarda memoria (user y assistant). Aquí solo generamos respuesta.
"""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

# Dependencias internas
from database import get_recent_memory  # memoria
from main import get_logs_dir, load_tasks

# Scheduler opcional
try:
    from automations.scheduler import get_scheduler_status
except Exception:
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
# Helpers base
# =========================================================
def _lang_from_env(default: str = "es") -> str:
    v = (os.getenv("KR_LANG") or default).strip().lower()
    return v if v in ("es", "en", "de") else "es"


def _norm(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"\s+", " ", s)
    return s


def _tail_logs(n: int = 10) -> List[str]:
    try:
        log_path = get_logs_dir() / "activity.log"
        if log_path.exists():
            return log_path.read_text(encoding="utf-8", errors="ignore").splitlines()[-n:]
    except Exception:
        pass
    return []


# =========================================================
# TOOLS
# =========================================================
def _tool_scheduler(lang: str) -> str:
    st = get_scheduler_status() or {}
    running = bool(st.get("running"))
    last_run_at = st.get("last_run_at") or "—"
    interval_seconds = st.get("interval_seconds") or "—"
    cycles = st.get("cycles_executed") or 0
    last_err = st.get("last_error") or "—"

    if lang == "de":
        return (
            f"**Scheduler Status**\n"
            f"- Läuft: {running}\n"
            f"- Letzter Lauf: {last_run_at}\n"
            f"- Intervall (s): {interval_seconds}\n"
            f"- Zyklen: {cycles}\n"
            f"- Letzter Fehler: {last_err}"
        )
    if lang == "en":
        return (
            f"**Scheduler Status**\n"
            f"- Running: {running}\n"
            f"- Last run: {last_run_at}\n"
            f"- Interval (s): {interval_seconds}\n"
            f"- Cycles: {cycles}\n"
            f"- Last error: {last_err}"
        )
    return (
        f"**Estado del Scheduler**\n"
        f"- Activo: {running}\n"
        f"- Última ejecución: {last_run_at}\n"
        f"- Intervalo (s): {interval_seconds}\n"
        f"- Ciclos: {cycles}\n"
        f"- Último error: {last_err}"
    )


def _tool_tasks(lang: str) -> str:
    tareas = load_tasks() or []
    activas = [t for t in tareas if bool(t.get("activa"))]
    pausadas = len(tareas) - len(activas)

    if lang == "de":
        return f"Aufgaben: **{len(tareas)}** gesamt · **{len(activas)}** aktiv · **{pausadas}** pausiert."
    if lang == "en":
        return f"Tasks: **{len(tareas)}** total · **{len(activas)}** active · **{pausadas}** paused."
    return f"Tareas: **{len(tareas)}** total · **{len(activas)}** activas · **{pausadas}** pausadas."


def _tool_logs(lang: str, n: int = 12) -> str:
    logs = _tail_logs(n)
    if not logs:
        return "—"
    head = "Últimos logs:\n" if lang == "es" else ("Latest logs:\n" if lang == "en" else "Letzte Logs:\n")
    return head + "\n".join(logs)


def _router_tools(text: str) -> Optional[str]:
    lang = _lang_from_env("es")
    t = _norm(text)

    if not t:
        return None

    if "scheduler" in t or "estado del scheduler" in t or "status scheduler" in t:
        return _tool_scheduler(lang)

    if "logs" in t or "log" in t or "actividad" in t:
        return _tool_logs(lang)

    if "tareas" in t or "tasks" in t or "automatizaciones" in t:
        return _tool_tasks(lang)

    return None


# =========================================================
# CONTEXTO (memoria inteligente)
# =========================================================
def _get_context(user_id: Optional[int], limit: int = 10) -> str:
    if not user_id:
        return ""

    try:
        mem = get_recent_memory(user_id, limit=limit) or []
    except Exception:
        return ""

    lines: List[str] = []
    for m in mem[-limit:]:
        if not isinstance(m, dict):
            continue
        role = str(m.get("role") or "user").strip().lower()
        content = str(m.get("content") or "").strip()
        if not content:
            continue
        prefix = "AI" if role in ("assistant", "ai") else "USER"
        lines.append(f"{prefix}: {content}")

    return "\n".join(lines)


# =========================================================
# INTENCIÓN
# =========================================================
def _detect_intent(text: str) -> str:
    t = _norm(text)

    if not t:
        return "empty"

    if re.search(r"\b(hola|buenas|hey|hi|hello|hallo|qué tal|que tal)\b", t):
        return "greeting"

    if any(k in t for k in ("scheduler", "logs", "tareas", "tasks", "automatizaciones")):
        return "tool"

    if any(k in t for k in ("cómo", "como", "plan", "estrategia", "idea", "mejor")):
        return "strategy"

    return "general"


# =========================================================
# CORE RESPONSE ENGINE
# =========================================================
def _local_reasoned_reply(user_text: str, user_id: Optional[int]) -> str:
    lang = _lang_from_env("es")
    intent = _detect_intent(user_text)
    t = user_text.strip()

    use_context = intent not in ("greeting", "tool", "empty") and len(t) > 15
    ctx = _get_context(user_id) if use_context else ""

    # Saludo
    if intent == "greeting":
        if lang == "de":
            return "CORE online. 🧠\n\nWas möchtest du heute bauen oder verbessern?"
        if lang == "en":
            return "CORE online. 🧠\n\nWhat do you want to build or improve today?"
        return "CORE online. 🧠\n\n¿Qué quieres construir o mejorar hoy?"

    # Estrategia
    if intent == "strategy":
        if lang == "de":
            return "Verstanden. Sag mir das Ziel in **einem Satz**."
        if lang == "en":
            return "Understood. Tell me the goal in **one sentence**."
        return "Entendido. Dime el objetivo en **una sola frase**."

    # Respuesta general
    if lang == "de":
        return (
            "Ich habe dich verstanden.\n\n"
            f"**Deine Anfrage:** {t}\n"
            + (f"\n**Kontext:**\n{ctx}\n" if ctx else "\n")
            + "\nWas ist das **konkrete Ergebnis**, das du erreichen willst?"
        )

    if lang == "en":
        return (
            "I understand you.\n\n"
            f"**Your request:** {t}\n"
            + (f"\n**Context:**\n{ctx}\n" if ctx else "\n")
            + "\nWhat is the **exact outcome** you want?"
        )

    return (
        "Te entendí.\n\n"
        f"**Tu pedido:** {t}\n"
        + (f"\n**Contexto:**\n{ctx}\n" if ctx else "\n")
        + "\n¿Cuál es el **resultado exacto** que quieres obtener?"
    )


# =========================================================
# Public API (NO TOCAR)
# =========================================================
def process_message(msg: str, user_id: Optional[int] = None) -> str:
    msg = (msg or "").strip()
    if not msg:
        return "No recibí mensaje."

    # 1) Tools
    tool = _router_tools(msg)
    if tool:
        return tool

    # 2) IA local razonada
    return _local_reasoned_reply(msg, user_id)