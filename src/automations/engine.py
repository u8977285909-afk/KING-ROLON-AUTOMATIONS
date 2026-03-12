# -*- coding: utf-8 -*-
"""
KING ROLON AUTOMATIONS — ENGINE (PRODUCTION-HARDENED ✅ | GOD MODE+)

OBJETIVO:
- Lee tareas desde main.py (tasks.json)
- Ejecuta SOLO tareas activas
- Aísla fallos por tarea (una rota NO tumba el ciclo)
- Devuelve cuántas tareas procesó en este ciclo (int)
- 100% compatible con:
    from automations.engine import run_all_active_tasks_once

MEJORAS PRO (sin romper tu ecosistema):
- Imports robustos (funciona si ejecutas desde /src o como paquete)
- Telemetría mínima pero útil: duración por ciclo y por tarea
- Throttling opcional: max_tasks / max_seconds (sin bloquear scheduler)
- Logs con meta consistente (para analytics futuro)
- Dispatch por plataforma extensible (TikTok / genérico)
"""

from __future__ import annotations

import time
import importlib
from datetime import datetime
from typing import Dict, Any, List, Optional, Callable

Task = Dict[str, Any]


# =========================================================
# IMPORTS DEL CORE (main.py) — ROBUST ✅
# =========================================================
def _import_core() -> tuple:
    """
    Importa (load_tasks, log_event) desde main.py
    Soporta:
    - ejecución normal (src/main.py en sys.path)
    - import como paquete (src. / automations. / etc)
    """
    last_err: Optional[Exception] = None

    # Intentos comunes en proyectos como el tuyo
    candidates = ("main", "src.main")

    for mod_name in candidates:
        try:
            mod = importlib.import_module(mod_name)
            load_tasks = getattr(mod, "load_tasks", None)
            log_event = getattr(mod, "log_event", None)
            if callable(load_tasks) and callable(log_event):
                return load_tasks, log_event
        except Exception as e:
            last_err = e

    # Fallback duro: no romper el engine
    def _fallback_load_tasks() -> List[Task]:
        return []

    def _fallback_log_event(message: str, level: str = "INFO", user_id: Optional[int] = None, meta: Optional[Dict[str, Any]] = None, **kwargs: Any) -> None:
        # último recurso
        try:
            meta_s = f" meta={meta}" if meta else ""
            print(f"[ENGINE-FALLBACK][{level}] {message}{meta_s}")
        except Exception:
            pass

    _fallback_log_event("ENGINE: No se pudo importar main.load_tasks/log_event. Modo degradado activado.", level="WARN", meta={"err": repr(last_err)})
    return _fallback_load_tasks, _fallback_log_event


load_tasks, log_event = _import_core()


# =========================================================
# INTEGRACIONES OPCIONALES (DEGRADADO SI FALTAN) ✅
# =========================================================
def _import_tiktok():
    """
    Importa integraciones.tiktok si existe.
    No rompe si no está.
    """
    for mod_name in ("integrations.tiktok", "src.integrations.tiktok"):
        try:
            return importlib.import_module(mod_name)
        except Exception:
            continue
    return None


tiktok_integration = _import_tiktok()


# =========================================================
# UTILS
# =========================================================
def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _s(v: Any, default: str = "") -> str:
    s = str(v or "").strip()
    return s if s else default


def _safe_task_name(task: Task) -> str:
    return _s(task.get("nombre"), "Tarea sin nombre")


def _safe_platform(task: Task, default: str = "Sin plataforma") -> str:
    return _s(task.get("plataforma"), default)


def _task_id(task: Task) -> str:
    return _s(task.get("id"), "no-id")


def _is_active(task: Task) -> bool:
    try:
        return bool(task.get("activa"))
    except Exception:
        return False


def _log(level: str, msg: str, meta: Optional[Dict[str, Any]] = None) -> None:
    try:
        log_event(msg, level=level, meta=meta or {})
    except TypeError:
        # compat por si tu log_event antiguo no acepta meta/level
        try:
            log_event(msg)
        except Exception:
            pass
    except Exception:
        pass


# =========================================================
# EJECUTORES POR PLATAFORMA (EXTENSIBLE) ✅
# =========================================================
def _run_tiktok_task(task: Task) -> None:
    nombre = _safe_task_name(task)
    tid = _task_id(task)

    # Sin módulo → simulación segura
    if not tiktok_integration:
        _log("INFO", "ENGINE/TIKTOK (SIM): ejecución simulada.", meta={"task_id": tid, "task": nombre})
        return

    try:
        # Si el módulo tiene check de conexión
        if hasattr(tiktok_integration, "is_connected") and callable(getattr(tiktok_integration, "is_connected")):
            if not tiktok_integration.is_connected():
                _log("WARN", "ENGINE/TIKTOK: módulo presente pero NO conectado. SIM.", meta={"task_id": tid, "task": nombre})
                return

        # Método recomendado (demo) — si existe
        if hasattr(tiktok_integration, "publish_demo_clip") and callable(getattr(tiktok_integration, "publish_demo_clip")):
            tiktok_integration.publish_demo_clip(task)
            _log("INFO", "ENGINE/TIKTOK: publish_demo_clip OK.", meta={"task_id": tid, "task": nombre})
            return

        # Si no existe método, registramos ejecución
        _log("INFO", "ENGINE/TIKTOK: sin método publish_demo_clip(). Marcando como ejecutada.", meta={"task_id": tid, "task": nombre})
    except Exception as e:
        _log("ERROR", "ENGINE/TIKTOK ERROR ejecutando tarea.", meta={"task_id": tid, "task": nombre, "err": repr(e)})


def _run_generic_task(task: Task) -> None:
    nombre = _safe_task_name(task)
    plataforma = _safe_platform(task)
    frecuencia = _s(task.get("frecuencia"), "Sin frecuencia")
    tid = _task_id(task)

    _log(
        "INFO",
        "ENGINE/GEN: ejecución genérica.",
        meta={
            "task_id": tid,
            "task": nombre,
            "platform": plataforma,
            "freq": frecuencia,
            "at": _now_iso(),
        },
    )


def _dispatch_task(task: Task) -> None:
    plataforma = _safe_platform(task, "").lower()

    if "tiktok" in plataforma:
        _run_tiktok_task(task)
        return

    # EXTENDER AQUÍ:
    # if "youtube" in plataforma: _run_youtube_task(task); return
    # if "twitch" in plataforma: _run_twitch_task(task); return

    _run_generic_task(task)


# =========================================================
# MOTOR PRINCIPAL ✅
# =========================================================
def run_all_active_tasks_once(
    max_tasks: Optional[int] = None,
    max_seconds: Optional[float] = None,
) -> int:
    """
    Ejecuta todas las tareas activas UNA VEZ.

    Params:
    - max_tasks: limita cuántas tareas procesa en este ciclo (throttle)
    - max_seconds: corta el ciclo si excede tiempo (evita freezes)

    Return:
    - int: cuántas tareas se intentaron ejecutar (procesadas)
    """
    cycle_t0 = time.time()

    # 1) Cargar tareas
    try:
        tasks: List[Task] = load_tasks() or []
    except Exception as e:
        _log("ERROR", "ENGINE: Error cargando tareas.", meta={"err": repr(e)})
        return 0

    if not tasks:
        _log("DEBUG", "ENGINE: No hay tareas registradas.")
        return 0

    # 2) Filtrar activas
    active_tasks: List[Task] = [t for t in tasks if _is_active(t)]
    total_active = len(active_tasks)

    if total_active <= 0:
        _log("DEBUG", "ENGINE: No hay tareas activas.")
        return 0

    # 3) Throttle por cantidad
    if max_tasks is not None:
        try:
            mt = int(max_tasks)
            if mt > 0:
                active_tasks = active_tasks[:mt]
        except Exception:
            pass

    _log(
        "INFO",
        "ENGINE: ciclo iniciado.",
        meta={
            "active_total": total_active,
            "to_process": len(active_tasks),
            "max_tasks": max_tasks,
            "max_seconds": max_seconds,
            "at": _now_iso(),
        },
    )

    processed = 0

    # 4) Loop por tarea (aislado)
    for task in active_tasks:
        # corte por tiempo (anti-freeze)
        if max_seconds is not None:
            try:
                if (time.time() - cycle_t0) >= float(max_seconds):
                    _log("WARN", "ENGINE: corte por max_seconds.", meta={"processed": processed, "max_seconds": float(max_seconds)})
                    break
            except Exception:
                pass

        tid = _task_id(task)
        nombre = _safe_task_name(task)
        plataforma = _safe_platform(task)
        t0 = time.time()

        try:
            _log("DEBUG", "ENGINE: start task.", meta={"task_id": tid, "task": nombre, "platform": plataforma})
            _dispatch_task(task)
            processed += 1
            _log(
                "DEBUG",
                "ENGINE: task OK.",
                meta={"task_id": tid, "task": nombre, "platform": plataforma, "exec_s": round(time.time() - t0, 4)},
            )
        except Exception as e:
            _log(
                "ERROR",
                "ENGINE: task FAILED.",
                meta={"task_id": tid, "task": nombre, "platform": plataforma, "err": repr(e), "exec_s": round(time.time() - t0, 4)},
            )
            # nunca rompe el ciclo

    cycle_s = time.time() - cycle_t0
    _log(
        "INFO",
        "ENGINE: ciclo completado.",
        meta={
            "active_total": total_active,
            "processed": processed,
            "cycle_s": round(cycle_s, 4),
            "at": _now_iso(),
        },
    )
    return processed


# =========================================================
# CLI (SMOKE TEST) ✅
# =========================================================
if __name__ == "__main__":
    total = run_all_active_tasks_once()
    print(f"ENGINE KR ejecutado directamente. Tareas procesadas: {total}")