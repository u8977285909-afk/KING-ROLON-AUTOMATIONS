# -*- coding: utf-8 -*-
"""
KING ROLON AUTOMATIONS — MAIN / CORE (PRODUCTION-HARDENED ✅)
Compat directo con web.py:
- load_tasks()
- save_tasks(tasks)
- get_logs_dir()
- get_logs_dir() / "activity.log"
- log_event(message, level="INFO", user_id=None, meta=None, **kwargs)

✅ Fix clave (SIN ROMPER):
- Bootstrap seguro: crea /data /logs /backups al importar el módulo (no afecta tu web.py).
- log_event compat legacy: log_event("X", user_id=..., extra="...") y cualquier kw -> META (sin pisar meta explícita).
- Project root detectado automáticamente (busca /static primero; luego /templates o /data).
- tasks.json atómico (tmp + os.replace)
- backups de JSON corrupto / gigante
- normalización fuerte + dedupe por id
- límites de tamaño/volumen
- logging robusto + rotación
"""

from __future__ import annotations

import json
import os
import shutil
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

__all__ = [
    "PROJECT_NAME",
    "CORE_VERSION",
    "get_project_dir",
    "get_data_dir",
    "get_logs_dir",
    "get_backups_dir",
    "get_tasks_path",
    "get_log_file",
    "log_event",
    "load_tasks",
    "save_tasks",
    "tasks_summary",
    "tasks_stats_by_platform",
]

PROJECT_NAME: str = "KING ROLON AUTOMATIONS"
CORE_VERSION: str = "6.1.3-GODMODE"

# ==== límites saneados ====
MAX_LOG_SIZE_BYTES: int = 1_500_000
MAX_TASKS_ALLOWED: int = 5_000
MAX_STR_LEN: int = 512
MAX_TASKS_JSON_BYTES: int = 5_000_000
MAX_TASK_NAME_LEN: int = 200
MAX_PLATFORM_LEN: int = 100
MAX_FREQ_LEN: int = 120
MAX_ID_LEN: int = 64

Task = Dict[str, Any]


# =========================================================
# Project root detection (ROBUST) ✅
# =========================================================
def get_project_dir() -> Path:
    """
    Detecta el root del proyecto subiendo carpetas.

    PRIORIDAD:
      1) carpeta que contiene /static
      2) carpeta que contiene /templates o /data

    Si no encuentra, usa parent del archivo.
    """
    here = Path(__file__).resolve()

    candidates = [
        here.parent,
        here.parent.parent,
        here.parent.parent.parent,
        here.parent.parent.parent.parent,
        here.parent.parent.parent.parent.parent,
    ]

    for base in candidates:
        try:
            if (base / "static").is_dir():
                return base
        except Exception:
            continue

    for base in candidates:
        try:
            if (base / "templates").is_dir() or (base / "data").is_dir():
                return base
        except Exception:
            continue

    return here.parent


def get_data_dir() -> Path:
    return get_project_dir() / "data"


def get_logs_dir() -> Path:
    return get_project_dir() / "logs"


def get_backups_dir() -> Path:
    return get_data_dir() / "backups"


def get_tasks_path() -> Path:
    return get_data_dir() / "tasks.json"


def get_log_file() -> Path:
    return get_logs_dir() / "activity.log"


# =========================================================
# Internal utils
# =========================================================
def _now(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return datetime.now().strftime(fmt)


def _timestamp_compact() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


def _ensure_directories() -> None:
    for d in (get_data_dir(), get_logs_dir(), get_backups_dir()):
        try:
            d.mkdir(parents=True, exist_ok=True)
        except Exception:
            pass


try:
    _ensure_directories()
except Exception:
    pass


def _sanitize_str(value: Any, max_len: int) -> str:
    s = str(value or "").replace("\r", " ").replace("\n", " ").strip()
    if len(s) > max_len:
        s = s[: max_len - 3] + "..."
    return s


def _safe_json_dumps(obj: Any, *, indent: Optional[int] = 2) -> str:
    try:
        return json.dumps(obj, ensure_ascii=False, indent=indent)
    except Exception:
        try:
            return json.dumps(str(obj), ensure_ascii=False, indent=indent)
        except Exception:
            return "[]"


# =========================================================
# Logging
# =========================================================
def _rotate_log_if_needed(max_bytes: int = MAX_LOG_SIZE_BYTES) -> None:
    log_path = get_log_file()
    if not log_path.exists():
        return
    try:
        size = log_path.stat().st_size
    except Exception:
        return
    if size < max_bytes:
        return

    stamp = _timestamp_compact()
    backup_path = log_path.with_name(f"activity_{stamp}.log")
    try:
        shutil.move(str(log_path), str(backup_path))
    except Exception:
        pass


def log_event(
    message: str,
    level: str = "INFO",
    user_id: Optional[int] = None,
    meta: Optional[Dict[str, Any]] = None,
    **kwargs: Any,
) -> None:
    """
    Safe logger: nunca rompe la app.
    Compat con web.py aunque llame: log_event("X", user_id=..., extra="...")

    kwargs soportados:
      - extra: se guarda dentro de META como {"extra": "..."}
      - cualquier otro kw también entra a META.
    """
    try:
        _ensure_directories()

        lvl = (level or "INFO").upper().strip()
        if lvl not in {"INFO", "WARN", "ERROR", "SECURITY", "DEBUG"}:
            lvl = "INFO"

        msg = _sanitize_str(message, MAX_STR_LEN) or "(empty)"

        uid_part = ""
        if user_id is not None:
            try:
                uid_part = f" [UID={int(user_id)}]"
            except Exception:
                uid_part = f" [UID={_sanitize_str(user_id, 32)}]"

        merged_meta: Dict[str, Any] = {}
        if isinstance(meta, dict):
            merged_meta.update(meta)

        if kwargs:
            if "extra" in kwargs and "extra" not in merged_meta:
                merged_meta["extra"] = kwargs.get("extra")

            for k, v in kwargs.items():
                if k == "extra":
                    continue
                if k not in merged_meta:
                    merged_meta[k] = v

        meta_str = ""
        if merged_meta:
            try:
                meta_str = " | META=" + json.dumps(
                    merged_meta,
                    ensure_ascii=False,
                    separators=(",", ":"),
                )
            except Exception:
                meta_str = " | META_INVALID"

        line = f"[{_now()}] [{lvl}]{uid_part} {msg}{meta_str}"

        _rotate_log_if_needed()
        with get_log_file().open("a", encoding="utf-8") as f:
            f.write(line + "\n")
    except Exception:
        pass


# =========================================================
# Tasks normalization
# =========================================================
def _ensure_task_id() -> str:
    return datetime.now().strftime("T%Y%m%d_%H%M%S_%f")


def _normalize_task(raw: Dict[str, Any]) -> Optional[Task]:
    if not isinstance(raw, dict):
        return None

    t: Task = {}

    t_id = raw.get("id") or _ensure_task_id()
    t["id"] = _sanitize_str(t_id, MAX_ID_LEN) or _ensure_task_id()

    nombre = raw.get("nombre") or "Tarea sin nombre"
    t["nombre"] = _sanitize_str(nombre, MAX_TASK_NAME_LEN) or "Tarea sin nombre"

    plataforma = raw.get("plataforma") or "Sin especificar"
    t["plataforma"] = _sanitize_str(plataforma, MAX_PLATFORM_LEN) or "Sin especificar"

    frecuencia = raw.get("frecuencia") or ""
    t["frecuencia"] = _sanitize_str(frecuencia, MAX_FREQ_LEN)

    t["activa"] = bool(raw.get("activa", True))

    creada_en = raw.get("creada_en") or _now()
    t["creada_en"] = _sanitize_str(creada_en, 64) or _now()

    if raw.get("plantilla_id") is not None:
        t["plantilla_id"] = _sanitize_str(raw.get("plantilla_id"), 128)

    if raw.get("meta") is not None:
        m = raw.get("meta")
        if isinstance(m, dict):
            t["meta"] = m
        else:
            t["meta"] = {"value": _sanitize_str(m, 256)}

    return t


def _dedupe_by_id(tasks: List[Task]) -> List[Task]:
    seen = set()
    out: List[Task] = []
    for t in tasks:
        tid = str(t.get("id") or "")
        if not tid or tid in seen:
            continue
        seen.add(tid)
        out.append(t)
    return out


def _normalize_tasks(tasks_in: Iterable[Dict[str, Any]]) -> List[Task]:
    normalized: List[Task] = []
    for raw in tasks_in:
        if len(normalized) >= MAX_TASKS_ALLOWED:
            log_event("Límite máximo de tareas alcanzado; se ignoran las demás.", level="WARN")
            break
        t = _normalize_task(raw)
        if t:
            normalized.append(t)
    return _dedupe_by_id(normalized)


# =========================================================
# Tasks file handling
# =========================================================
def _backup_corrupted_tasks_file(original: Path, reason: str) -> None:
    try:
        if not original.exists():
            return
        _ensure_directories()
        stamp = _timestamp_compact()
        backup_path = get_backups_dir() / f"tasks_corrupted_{stamp}.json"
        shutil.move(str(original), str(backup_path))
        log_event(
            "BACKUP tasks.json corrupto",
            level="ERROR",
            meta={"backup": backup_path.name, "reason": reason},
        )
    except Exception:
        pass


def _save_tasks_raw(tasks: List[Task]) -> None:
    _ensure_directories()

    tasks_path = get_tasks_path()
    tmp_path = tasks_path.with_suffix(".json.tmp")

    payload = _safe_json_dumps(tasks, indent=2)
    if len(payload.encode("utf-8")) > MAX_TASKS_JSON_BYTES:
        log_event(
            "Bloqueado guardado tasks.json: excede tamaño máximo permitido.",
            level="SECURITY",
            meta={"max_bytes": MAX_TASKS_JSON_BYTES},
        )
        return

    try:
        with tmp_path.open("w", encoding="utf-8") as f:
            f.write(payload)
        os.replace(tmp_path, tasks_path)
    except Exception as e:
        log_event("Error guardando tasks.json", level="ERROR", meta={"err": repr(e)})
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except Exception:
            pass


def load_tasks() -> List[Task]:
    _ensure_directories()
    tasks_path = get_tasks_path()

    if tasks_path.exists():
        try:
            if tasks_path.stat().st_size > MAX_TASKS_JSON_BYTES:
                _backup_corrupted_tasks_file(tasks_path, "Archivo excede tamaño máximo permitido")
                _save_tasks_raw([])
                return []
        except Exception:
            pass

    if not tasks_path.exists():
        _save_tasks_raw([])
        return []

    try:
        with tasks_path.open("r", encoding="utf-8") as f:
            raw_data = json.load(f)
    except json.JSONDecodeError as e:
        _backup_corrupted_tasks_file(tasks_path, f"JSONDecodeError: {e}")
        _save_tasks_raw([])
        return []
    except Exception as e:
        log_event("Error leyendo tasks.json", level="ERROR", meta={"err": repr(e)})
        return []

    if not isinstance(raw_data, list):
        _backup_corrupted_tasks_file(tasks_path, "Formato no es lista")
        _save_tasks_raw([])
        return []

    normalized = _normalize_tasks(raw_data)

    try:
        if normalized != raw_data:
            log_event("tasks.json normalizado al cargar (reescritura limpia).", level="WARN")
            _save_tasks_raw(normalized)
    except Exception:
        pass

    return normalized


def save_tasks(tasks: List[Task]) -> None:
    normalized = _normalize_tasks(tasks or [])
    _save_tasks_raw(normalized)


# =========================================================
# Helpers de stats
# =========================================================
def tasks_stats_by_platform(tasks: Optional[List[Task]] = None) -> Dict[str, int]:
    if tasks is None:
        tasks = load_tasks()
    stats: Dict[str, int] = {}
    for t in tasks:
        if not t.get("activa"):
            continue
        plat = str(t.get("plataforma") or "Sin especificar")
        stats[plat] = stats.get(plat, 0) + 1
    return stats


def tasks_summary(tasks: Optional[List[Task]] = None) -> Dict[str, int]:
    if tasks is None:
        tasks = load_tasks()
    total = len(tasks)
    activas = sum(1 for t in tasks if t.get("activa"))
    return {"total": total, "activas": activas, "pausadas": max(0, total - activas)}


# =========================================================
# CLI smoke test
# =========================================================
if __name__ == "__main__":
    _ensure_directories()
    log_event(f"CORE standalone iniciado · v{CORE_VERSION}")
    print(f"{PROJECT_NAME} · CORE {CORE_VERSION}")
    print("project_dir:", get_project_dir())
    print("tasks.json:", get_tasks_path())
    print("logs:", get_log_file())