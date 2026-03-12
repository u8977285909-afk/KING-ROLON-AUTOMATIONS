# -*- coding: utf-8 -*-
"""
KING ROLON AUTOMATIONS — Scheduler Inteligente PRO (GOD MODE ✅)

✔ Seguro para producción
✔ Thread-safe (sin race conditions)
✔ Anti-freeze + watchdog
✔ Intervalo dinámico (auto-tuning)
✔ Sleep interruptible (stop inmediato)
✔ Status rico para dashboard (web_app.py)
✔ Compatible 100% con engine.run_all_active_tasks_once()
"""

from __future__ import annotations

import threading
import time
from datetime import datetime
from typing import Optional, Dict, Any

# ==========================================================
# ENGINE
# ==========================================================
try:
    from automations.engine import run_all_active_tasks_once
except Exception:
    run_all_active_tasks_once = None  # type: ignore


# ==========================================================
# LOGGING
# ==========================================================
try:
    from main import log_event
except Exception:
    def log_event(msg: str, level: str = "INFO", user_id=None, meta=None) -> None:  # type: ignore[override]
        print(f"[SCHEDULER FALLBACK][{level}] {msg}")


# ==========================================================
# GLOBAL STATE (THREAD-SAFE)
# ==========================================================
_LOCK = threading.RLock()
_STOP_EVENT = threading.Event()
_THREAD: Optional[threading.Thread] = None

_STATE: Dict[str, Any] = {
    "running": False,
    "engine_available": False,

    "thread_name": None,
    "thread_alive": False,

    "started_at": None,
    "stopped_at": None,

    "last_run_at": None,
    "last_error": None,

    "interval_seconds": 30,
    "next_run_in": None,          # segundos (aprox)
    "last_exec_seconds": 0.0,
    "last_count": 0,

    "consecutive_errors": 0,
    "cycles_executed": 0,
}

# Límites
_DYNAMIC_MIN = 15
_DYNAMIC_MAX = 60
_HARD_MIN_SLEEP = 5
_FREEZE_THRESHOLD = 10.0  # segundos

# Anti-spam logs
_LOG_NO_TASKS_EVERY = 10  # log “0 tareas” cada 10 ciclos como máximo


# ==========================================================
# HELPERS
# ==========================================================
def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _engine_ok() -> bool:
    return callable(run_all_active_tasks_once)


# ==========================================================
# STATUS PARA DASHBOARD
# ==========================================================
def get_scheduler_status() -> Dict[str, Any]:
    with _LOCK:
        # snapshot seguro
        return dict(_STATE)


# ==========================================================
# INTERVALO DINÁMICO
# ==========================================================
def _adjust_interval(exec_time: float) -> None:
    """
    Ajuste automático:
    - Carga alta -> sube
    - Carga baja -> baja (pero no si vienes en racha de errores)
    """
    with _LOCK:
        interval = int(_STATE.get("interval_seconds", 30) or 30)
        consec_err = int(_STATE.get("consecutive_errors", 0) or 0)

        if exec_time > 1.5:
            interval = min(interval + 5, _DYNAMIC_MAX)
        elif exec_time < 0.5:
            # si viene fallando, NO bajamos agresivo
            if consec_err == 0:
                interval = max(interval - 5, _DYNAMIC_MIN)

        _STATE["interval_seconds"] = int(interval)


# ==========================================================
# LOOP PRINCIPAL
# ==========================================================
def _scheduler_loop() -> None:
    log_event("Scheduler GOD MODE iniciado.", level="INFO")

    with _LOCK:
        _STATE["running"] = True
        _STATE["engine_available"] = _engine_ok()
        _STATE["started_at"] = _now_iso()
        _STATE["stopped_at"] = None

    no_tasks_counter = 0

    while not _STOP_EVENT.is_set():
        start_ts = time.time()

        try:
            if not _engine_ok():
                raise RuntimeError("ENGINE no disponible (run_all_active_tasks_once no importable)")

            # Run
            count = int(run_all_active_tasks_once() or 0)
            exec_time = time.time() - start_ts

            with _LOCK:
                _STATE["engine_available"] = True
                _STATE["last_run_at"] = _now_iso()
                _STATE["last_exec_seconds"] = round(exec_time, 4)
                _STATE["last_count"] = count
                _STATE["cycles_executed"] = int(_STATE.get("cycles_executed", 0) or 0) + 1
                _STATE["consecutive_errors"] = 0
                _STATE["last_error"] = None

            # Logs (telemetría reducida)
            if count > 0:
                log_event(f"[Scheduler] OK · {count} tareas · {exec_time:.3f}s", level="INFO")
                no_tasks_counter = 0
            else:
                no_tasks_counter += 1
                # No spamear “0 tareas”
                if no_tasks_counter % _LOG_NO_TASKS_EVERY == 0:
                    log_event("[Scheduler] Ciclo OK · 0 tareas (silencioso).", level="DEBUG")

            # Ajuste intervalo
            _adjust_interval(exec_time)

            # Watchdog anti-freeze
            if exec_time > _FREEZE_THRESHOLD:
                log_event(f"[Scheduler WARNING] Ciclo excesivo: {exec_time:.2f}s", level="WARN")

        except Exception as e:
            with _LOCK:
                _STATE["engine_available"] = _engine_ok()
                _STATE["last_error"] = repr(e)
                _STATE["consecutive_errors"] = int(_STATE.get("consecutive_errors", 0) or 0) + 1

                # Si hay muchos errores, subir intervalo
                if _STATE["consecutive_errors"] >= 3:
                    _STATE["interval_seconds"] = min(
                        int(_STATE.get("interval_seconds", 30) or 30) + 10,
                        _DYNAMIC_MAX,
                    )

            log_event(f"[Scheduler ERROR] {e!r}", level="ERROR")

        # Sleep interruptible + next_run_in
        with _LOCK:
            sleep_for = max(int(_STATE.get("interval_seconds", 30) or 30), _HARD_MIN_SLEEP)
            _STATE["next_run_in"] = int(sleep_for)

        # Espera “interruptible”
        # (si stop se dispara, sale inmediatamente)
        t0 = time.time()
        while True:
            if _STOP_EVENT.wait(timeout=0.5):
                break
            elapsed = time.time() - t0
            remaining = max(0.0, sleep_for - elapsed)
            with _LOCK:
                _STATE["next_run_in"] = int(round(remaining))
            if elapsed >= sleep_for:
                break

    with _LOCK:
        _STATE["running"] = False
        _STATE["thread_alive"] = False
        _STATE["next_run_in"] = None
        _STATE["stopped_at"] = _now_iso()

    log_event("Scheduler detenido correctamente.", level="INFO")


# ==========================================================
# START / STOP
# ==========================================================
def start_scheduler() -> None:
    """
    Arranca el scheduler si no está corriendo.
    Thread-safe: evita doble arranque incluso si llaman dos veces rápido.
    """
    global _THREAD

    with _LOCK:
        # Si ya existe thread viva, no arranca otro
        if _THREAD is not None and _THREAD.is_alive():
            _STATE["running"] = True
            _STATE["thread_alive"] = True
            _STATE["thread_name"] = _THREAD.name
            log_event("Scheduler ya activo. Start ignorado.", level="DEBUG")
            return

        # Reset stop
        _STOP_EVENT.clear()

        # Crear thread
        _THREAD = threading.Thread(
            target=_scheduler_loop,
            name="KR-Scheduler",
            daemon=True,
        )
        _STATE["thread_name"] = _THREAD.name
        _STATE["thread_alive"] = True

    _THREAD.start()
    log_event("Scheduler lanzado correctamente.", level="INFO")


def stop_scheduler(wait: bool = False, timeout: float = 5.0) -> bool:
    """
    Señala stop. Opcionalmente espera (join) hasta `timeout`.

    Retorna:
      - True si ya está detenido (o se detuvo dentro del timeout si wait=True)
      - False si sigue vivo tras el timeout
    """
    global _THREAD

    _STOP_EVENT.set()
    log_event("stop_scheduler() solicitado.", level="INFO")

    if not wait:
        return True

    th = _THREAD
    if th is None:
        return True

    try:
        th.join(timeout=max(0.0, float(timeout)))
    except Exception:
        pass

    alive = th.is_alive()
    with _LOCK:
        _STATE["thread_alive"] = bool(alive)
        if not alive:
            _STATE["running"] = False
            _STATE["next_run_in"] = None

    return not alive