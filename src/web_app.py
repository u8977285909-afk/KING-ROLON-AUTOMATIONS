# -*- coding: utf-8 -*-
import os
from uuid import uuid4
from pathlib import Path
from typing import List, Dict
from flask import Flask, render_template, request, jsonify
from dotenv import load_dotenv  # lee variables del .env

# Utilidades del core (de tu main.py)
from main import load_tasks, save_tasks, get_logs_dir

# Cargar variables de entorno desde .env
load_dotenv()

# Nota: este archivo está en src/, las plantillas en src/templates y estáticos en /static
app = Flask(__name__, template_folder="templates", static_folder="../static")

LOG_PATH: Path = get_logs_dir() / "activity.log"


def _last_logs(n: int = 8) -> List[str]:
    if LOG_PATH.exists():
        with LOG_PATH.open("r", encoding="utf-8") as f:
            return [ln.strip() for ln in f.readlines()[-n:]]
    return []


def token_status(var_name: str) -> str:
    """
    Devuelve el estado según el valor en .env:
    - 'Conectado'  si hay un token con longitud razonable
    - 'Pendiente'  si hay algo escrito pero corto (placeholder)
    - 'No conectado' si está vacío
    """
    val = (os.getenv(var_name) or "").strip()
    if len(val) >= 10:
        return "Conectado"
    if len(val) > 0:
        return "Pendiente"
    return "No conectado"


@app.route("/")
def dashboard():
    # Carga tareas (si no hay, crea demo)
    tareas = load_tasks()
    if not tareas:
        tareas = [
            {
                "id": str(uuid4()),
                "nombre": "Subir clip diario a TikTok",
                "plataforma": "TikTok",
                "frecuencia": "Cada 24h",
                "activa": True,
            },
            {
                "id": str(uuid4()),
                "nombre": "Repost en Facebook Gaming",
                "plataforma": "Facebook",
                "frecuencia": "Después de cada stream",
                "activa": True,
            },
            {
                "id": str(uuid4()),
                "nombre": "Guardar mejores jugadas",
                "plataforma": "Global",
                "frecuencia": "Automático",
                "activa": True,
            },
        ]
        save_tasks(tareas)

    # Estado dinámico por tokens del .env
    plataformas = [
        {"nombre": "TikTok",    "estado": token_status("TIKTOK_TOKEN")},
        {"nombre": "Facebook",  "estado": token_status("FACEBOOK_TOKEN")},
        {"nombre": "YouTube",   "estado": token_status("YOUTUBE_TOKEN")},
        {"nombre": "Twitch",    "estado": token_status("TWITCH_TOKEN")},
        {"nombre": "Kik",       "estado": token_status("KIK_TOKEN")},
        {"nombre": "Instagram", "estado": token_status("INSTAGRAM_TOKEN")},
        {"nombre": "Twitter/X", "estado": token_status("TWITTER_TOKEN")},
    ]

    ingresos_mensuales = 1450 + len([t for t in tareas if t.get("activa")]) * 75

    return render_template(
        "dashboard.html",
        ingresos_mensuales=ingresos_mensuales,
        tareas_activas=tareas,
        plataformas=plataformas,
        ultimos_logs=_last_logs(8),
    )


# ============== API (AJAX) ==============

@app.get("/api/tasks")
def api_list_tasks():
    return jsonify({"ok": True, "tasks": load_tasks()})


@app.post("/api/tasks")
def api_create_task():
    """Crea una tarea desde el modal (JSON)."""
    data = request.get_json(force=True) or {}
    nombre = (data.get("nombre") or "").strip()
    plataforma = (data.get("plataforma") or "").strip() or "Sin especificar"
    frecuencia = (data.get("frecuencia") or "").strip() or "Sin especificar"

    if not nombre:
        return jsonify({"ok": False, "error": "El nombre es obligatorio."}), 400

    tasks = load_tasks()
    task: Dict = {
        "id": str(uuid4()),
        "nombre": nombre,
        "plataforma": plataforma,
        "frecuencia": frecuencia,
        "activa": True,
    }
    tasks.append(task)
    save_tasks(tasks)
    return jsonify({"ok": True, "task": task}), 201


@app.get("/api/logs")
def api_logs():
    return jsonify({"ok": True, "logs": _last_logs(12)})


if __name__ == "__main__":
    # Ejecuta solo este archivo si quieres levantar el panel directamente
    app.run(host="127.0.0.1", port=5000, debug=False)
