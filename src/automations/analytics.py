# -*- coding: utf-8 -*-
"""
======================================================================
  KING ROLON AUTOMATIONS — ANALYTICS v1.0 (MODO DIOS)

  Módulo PRO de analíticas internas del Reino.

  - Lee logs SaaS (activity.log) y genera métricas:
      • actividad diaria últimos N días
      • uso del motor (runs, errores)
      • eventos relacionados con TikTok y SECURITY
  - Lee la base de datos (SQLite) para métricas de negocio:
      • ventas totales
      • ingresos creador vs. KR
      • últimos 30 días
  - Analíticas de tareas:
      • resumen total / activas / pausadas
      • distribución por plataforma

  TODO está diseñado para:
      - NO reventar si faltan logs o tablas
      - Ser usado desde web_app.py o scripts internos

  Función pensada para el dashboard:
      get_dashboard_analytics(user_id: Optional[int]) -> Dict[str, Any]
======================================================================
"""

from __future__ import annotations

import sqlite3
from datetime import datetime, timedelta, date
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from main import (
    get_logs_dir,
    load_tasks,
    tasks_summary,
    tasks_stats_by_platform,
)

from database import get_connection

# Ruta al log principal
LOG_PATH: Path = get_logs_dir() / "activity.log"


# ====================================================
#   HELPERS PARA LOGS
# ====================================================
def _parse_log_line(line: str) -> Optional[Dict[str, Any]]:
    """
    Parsea una línea de activity.log producida por main.log_event():

    Formato esperado aproximado:
      [YYYY-mm-dd HH:MM:SS] [LEVEL] [UID=7] Mensaje... | META=...

    Devuelve:
      {
        "timestamp": datetime,
        "date": date,
        "level": "INFO"/"ERROR"/...,
        "uid": Optional[int],
        "message": str,
        "raw": str
      }
    o None si no se puede parsear.
    """
    try:
        line = line.strip()
        if not line.startswith("["):
            return None

        # [time]
        close_1 = line.find("]")
        if close_1 <= 0:
            return None
        ts_str = line[1:close_1]
        ts = datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S")

        # [LEVEL]
        rest = line[close_1 + 1 :].lstrip()
        if not rest.startswith("["):
            return None
        close_2 = rest.find("]")
        if close_2 <= 0:
            return None
        level = rest[1:close_2].strip().upper()
        rest = rest[close_2 + 1 :].lstrip()

        uid: Optional[int] = None
        if rest.startswith("[UID="):
            close_3 = rest.find("]")
            if close_3 > 0:
                uid_str = rest[5:close_3]
                try:
                    uid = int(uid_str)
                except Exception:
                    uid = None
                rest = rest[close_3 + 1 :].lstrip()

        message = rest.strip()

        return {
            "timestamp": ts,
            "date": ts.date(),
            "level": level,
            "uid": uid,
            "message": message,
            "raw": line,
        }
    except Exception:
        return None


def _iter_recent_logs(days: int = 7) -> List[Dict[str, Any]]:
    """
    Devuelve lista de logs parseados de los últimos N días.
    Si el archivo no existe, devuelve lista vacía.
    """
    if not LOG_PATH.exists():
        return []

    limit_date = datetime.now().date() - timedelta(days=days - 1)
    result: List[Dict[str, Any]] = []

    try:
        with LOG_PATH.open("r", encoding="utf-8") as f:
            for raw in f:
                parsed = _parse_log_line(raw)
                if not parsed:
                    continue
                if parsed["date"] < limit_date:
                    # muy antiguo
                    continue
                result.append(parsed)
    except Exception:
        return []

    return result


def get_log_activity_last_days(days: int = 7) -> Dict[str, Any]:
    """
    Métrica para gráfico de barras: actividad total por día (contando logs).

    Devuelve:
      {
        "labels": ["01/12", "02/12", ...],
        "values": [10, 5, ...],
        "total_events": int
      }
    """
    today = datetime.now().date()
    days = max(1, days)

    # Inicializar contadores por fecha
    counts: Dict[date, int] = {
        today - timedelta(days=i): 0 for i in range(days)
    }

    logs = _iter_recent_logs(days=days)
    for entry in logs:
        d: date = entry["date"]
        if d in counts:
            counts[d] += 1

    labels: List[str] = []
    values: List[int] = []
    for i in reversed(range(days)):
        d = today - timedelta(days=i)
        labels.append(d.strftime("%d/%m"))
        values.append(counts[d])

    return {
        "labels": labels,
        "values": values,
        "total_events": sum(values),
    }


def get_engine_metrics(days: int = 7) -> Dict[str, Any]:
    """
    Métricas del motor de automatizaciones, basadas en activity.log:

      - runs_total      → ENGINE_RUN_REAL / ENGINE_RUN_SIMULATION
      - runs_error      → ENGINE_RUN_ERROR
      - tiktok_events   → líneas con 'ENGINE/TIKTOK'
      - security_events → líneas con nivel SECURITY
    """
    logs = _iter_recent_logs(days=days)

    runs_total = 0
    runs_error = 0
    tiktok_events = 0
    security_events = 0

    for entry in logs:
        msg = entry["message"]
        level = entry["level"]

        # Motor
        if "ENGINE_RUN_REAL" in msg or "ENGINE_RUN_SIMULATION" in msg:
            runs_total += 1
        if "ENGINE_RUN_ERROR" in msg:
            runs_error += 1

        # TikTok
        if "ENGINE/TIKTOK" in msg:
            tiktok_events += 1

        # SECURITY level
        if level == "SECURITY":
            security_events += 1

    return {
        "runs_total": runs_total,
        "runs_error": runs_error,
        "tiktok_events": tiktok_events,
        "security_events": security_events,
    }


# ====================================================
#   MÉTRICAS DE NEGOCIO (VENTAS / INGRESOS)
# ====================================================
def get_revenue_metrics(days: int = 30) -> Dict[str, Any]:
    """
    Lee la tabla app_sales para sacar métricas globales.

    Devuelve todo en centavos e incluye versión en EUR para usar directo
    en templates si quieres.
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        # Global
        cur.execute(
            """
            SELECT
                COUNT(id) AS total_sales,
                COALESCE(SUM(price_cents), 0) AS gross_cents,
                COALESCE(SUM(creator_amount_cents), 0) AS creator_cents,
                COALESCE(SUM(kr_fee_cents), 0) AS kr_fee_cents
            FROM app_sales;
            """
        )
        row_all = cur.fetchone() or {
            "total_sales": 0,
            "gross_cents": 0,
            "creator_cents": 0,
            "kr_fee_cents": 0,
        }

        # Últimos N días
        cur.execute(
            """
            SELECT
                COUNT(id) AS total_sales,
                COALESCE(SUM(price_cents), 0) AS gross_cents,
                COALESCE(SUM(creator_amount_cents), 0) AS creator_cents,
                COALESCE(SUM(kr_fee_cents), 0) AS kr_fee_cents
            FROM app_sales
            WHERE created_at >= datetime('now', ?);
            """,
            (f"-{int(days)} days",),
        )
        row_recent = cur.fetchone() or {
            "total_sales": 0,
            "gross_cents": 0,
            "creator_cents": 0,
            "kr_fee_cents": 0,
        }

        conn.close()
        conn = None

        def _to_eur(cents: int) -> float:
            return round((cents or 0) / 100.0, 2)

        return {
            "all": {
                "total_sales": int(row_all["total_sales"]),
                "gross_cents": int(row_all["gross_cents"]),
                "creator_cents": int(row_all["creator_cents"]),
                "kr_fee_cents": int(row_all["kr_fee_cents"]),
                "gross_eur": _to_eur(row_all["gross_cents"]),
                "creator_eur": _to_eur(row_all["creator_cents"]),
                "kr_fee_eur": _to_eur(row_all["kr_fee_cents"]),
            },
            "recent": {
                "days": days,
                "total_sales": int(row_recent["total_sales"]),
                "gross_cents": int(row_recent["gross_cents"]),
                "creator_cents": int(row_recent["creator_cents"]),
                "kr_fee_cents": int(row_recent["kr_fee_cents"]),
                "gross_eur": _to_eur(row_recent["gross_cents"]),
                "creator_eur": _to_eur(row_recent["creator_cents"]),
                "kr_fee_eur": _to_eur(row_recent["kr_fee_cents"]),
            },
        }
    except Exception:
        if conn:
            conn.close()
        # Si algo falla, devolvemos ceros para no romper el dashboard
        return {
            "all": {
                "total_sales": 0,
                "gross_cents": 0,
                "creator_cents": 0,
                "kr_fee_cents": 0,
                "gross_eur": 0.0,
                "creator_eur": 0.0,
                "kr_fee_eur": 0.0,
            },
            "recent": {
                "days": days,
                "total_sales": 0,
                "gross_cents": 0,
                "creator_cents": 0,
                "kr_fee_cents": 0,
                "gross_eur": 0.0,
                "creator_eur": 0.0,
                "kr_fee_eur": 0.0,
            },
        }


def get_user_business_metrics(user_id: int) -> Dict[str, Any]:
    """
    Métricas de negocio por usuario:
      - apps publicadas
      - ventas de sus apps
      - ingresos totales del creador
    """
    conn = None
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                COUNT(*) AS apps_total,
                SUM(CASE WHEN status = 'published' THEN 1 ELSE 0 END) AS apps_published
            FROM apps
            WHERE owner_id = ?;
            """,
            (user_id,),
        )
        apps_row = cur.fetchone() or {
            "apps_total": 0,
            "apps_published": 0,
        }

        cur.execute(
            """
            SELECT
                COUNT(app_sales.id) AS sales_count,
                COALESCE(SUM(app_sales.creator_amount_cents), 0) AS creator_cents
            FROM app_sales
            JOIN apps ON apps.id = app_sales.app_id
            WHERE apps.owner_id = ?;
            """,
            (user_id,),
        )
        sales_row = cur.fetchone() or {
            "sales_count": 0,
            "creator_cents": 0,
        }

        conn.close()
        conn = None

        creator_cents = int(sales_row["creator_cents"] or 0)
        creator_eur = round(creator_cents / 100.0, 2)

        return {
            "apps_total": int(apps_row["apps_total"] or 0),
            "apps_published": int(apps_row["apps_published"] or 0),
            "sales_count": int(sales_row["sales_count"] or 0),
            "creator_cents": creator_cents,
            "creator_eur": creator_eur,
        }
    except Exception:
        if conn:
            conn.close()
        return {
            "apps_total": 0,
            "apps_published": 0,
            "sales_count": 0,
            "creator_cents": 0,
            "creator_eur": 0.0,
        }


# ====================================================
#   FUNCIÓN PRINCIPAL PARA EL DASHBOARD
# ====================================================
def get_dashboard_analytics(user_id: Optional[int] = None) -> Dict[str, Any]:
    """
    Paquete completo de analíticas para usar en el dashboard KR.

    Lo que devuelve:
    {
      "tasks_summary": {...},
      "tasks_by_platform": {...},
      "logs_activity": {...},
      "engine": {...},
      "revenue": {...},
      "user_business": {...}  # solo si user_id
    }
    """
    # Tareas
    tasks = load_tasks()
    tasks_sum = tasks_summary(tasks)
    tasks_by_plat = tasks_stats_by_platform(tasks)

    # Logs
    logs_activity = get_log_activity_last_days(days=7)
    engine = get_engine_metrics(days=7)

    # Negocio global
    revenue = get_revenue_metrics(days=30)

    # Negocio por usuario (creador)
    user_business: Optional[Dict[str, Any]] = None
    if user_id is not None:
        user_business = get_user_business_metrics(user_id)

    return {
        "tasks_summary": tasks_sum,
        "tasks_by_platform": tasks_by_plat,
        "logs_activity": logs_activity,
        "engine": engine,
        "revenue": revenue,
        "user_business": user_business,
    }


# ====================================================
#   TEST MANUAL
# ====================================================
if __name__ == "__main__":
    # Pequeña prueba rápida en consola:
    data = get_dashboard_analytics(user_id=None)
    print("=== ANALYTICS (MODO DIOS) ===")
    for k, v in data.items():
        print(f"\n{k}:")
        print(v)