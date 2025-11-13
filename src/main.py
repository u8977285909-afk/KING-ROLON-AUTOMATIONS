"""
======================================================================
  👑 KING ROLON AUTOMATIONS — CORE v4.0 (Ultra)
  Consola + Web (Flask) en un solo archivo, sin dependencias extra.
  - Gestión de tareas (crear, listar, activar/desactivar, borrar)
  - Reportes diarios (append)
  - Logging con rotación automática
  - Exportación CSV
  - Dashboard compatible (templates/dashboard.html + base.html)
======================================================================
"""
from __future__ import annotations

import csv
import json
import os
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

from flask import Flask, render_template

# ========= IDENTIDAD =========
PROJECT_NAME = "KING ROLON AUTOMATIONS"
VERSION = "4.0 Ultra"

# ========= RUTAS =========
def get_project_dir() -> Path:
    return Path(__file__).resolve().parent.parent  # /KING ROLON AUTOMATIONS

def get_data_dir() -> Path:
    return get_project_dir() / "data"

def get_logs_dir() -> Path:
    return get_project_dir() / "logs"

def get_reports_dir() -> Path:
    return get_data_dir() / "reports"

def get_tasks_path() -> Path:
    return get_data_dir() / "tasks.json"

def get_tasks_csv_path() -> Path:
    return get_data_dir() / "tasks_export.csv"

def get_log_file() -> Path:
    return get_logs_dir() / "activity.log"

# ========= ENTORNO =========
def init_environment() -> None:
    get_logs_dir().mkdir(parents=True, exist_ok=True)
    get_reports_dir().mkdir(parents=True, exist_ok=True)
    get_data_dir().mkdir(parents=True, exist_ok=True)
    if not get_tasks_path().exists():
        save_tasks([])

# ========= UTILIDADES =========
def now_str(fmt: str = "%Y-%m-%d %H:%M:%S") -> str:
    return datetime.now().strftime(fmt)

def gen_task_id() -> str:
    return datetime.now().strftime("T%Y%m%d%H%M%S%f")

# ========= LOGGING =========
def rotate_log_if_needed(max_bytes: int = 512_000) -> None:
    log_path = get_log_file()
    if log_path.exists() and log_path.stat().st_size >= max_bytes:
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup = log_path.with_name(f"activity_{stamp}.log")
        log_path.rename(backup)

def log_event(message: str) -> None:
    rotate_log_if_needed()
    with get_log_file().open("a", encoding="utf-8") as f:
        f.write(f"[{now_str()}] {message}\n")

def show_last_logs(lines: int = 12) -> None:
    p = get_log_file()
    if not p.exists():
        print("📂 Aún no hay logs.")
        return
    with p.open("r", encoding="utf-8") as f:
        all_lines = f.readlines()
    print(f"\n📜 Últimos {lines} eventos:")
    for line in all_lines[-lines:]:
        print("  " + line.rstrip())

# ========= TAREAS =========
Task = Dict[str, object]

def load_tasks() -> List[Task]:
    if not get_tasks_path().exists():
        save_tasks([])
    with get_tasks_path().open("r", encoding="utf-8") as f:
        return json.load(f)

def save_tasks(tasks: List[Task]) -> None:
    get_tasks_path().parent.mkdir(parents=True, exist_ok=True)
    with get_tasks_path().open("w", encoding="utf-8") as f:
        json.dump(tasks, f, ensure_ascii=False, indent=2)

def create_task_interactive() -> None:
    print("\n⚙️  Nueva tarea automática")
    nombre = input("   Nombre (p.ej. Subir clip diario a TikTok): ").strip()
    plataforma = input("   Plataforma (TikTok / Facebook / YouTube / Twitch): ").strip()
    frecuencia = input("   Frecuencia (Cada 24h / después de cada stream / …): ").strip()

    if not nombre:
        print("⚠️  Falta el nombre. No se creó la tarea.")
        return

    tasks = load_tasks()
    task: Task = {
        "id": gen_task_id(),
        "nombre": nombre,
        "plataforma": plataforma or "Sin especificar",
        "frecuencia": frecuencia or "Sin especificar",
        "activa": True,
        "creada_en": now_str(),
    }
    tasks.append(task)
    save_tasks(tasks)
    log_event(f"Tarea creada: {task['nombre']} [{task['plataforma']}] ({task['frecuencia']})")
    print("✅ Tarea creada.")

def list_tasks(show_index: bool = True) -> List[Task]:
    tasks = load_tasks()
    if not tasks:
        print("\n( No hay tareas aún )")
        return []
    print("\n📋 Tareas:")
    for i, t in enumerate(tasks, 1):
        estado = "🟢 Activa" if t.get("activa") else "🔴 Pausada"
        prefix = f"{i:02d}. " if show_index else ""
        print(f"{prefix}{t['nombre']}  ·  {t['plataforma']}  ·  {t['frecuencia']}  ·  {estado}  · id={t['id']}")
    return tasks

def toggle_task() -> None:
    tasks = list_tasks()
    if not tasks:
        return
    sel = input("\nIndica # o id para activar/pausar: ").strip()
    if not sel:
        return
    chosen: Optional[Task] = None
    if sel.isdigit():
        idx = int(sel) - 1
        if 0 <= idx < len(tasks):
            chosen = tasks[idx]
    if chosen is None:
        for t in tasks:
            if str(t.get("id")) == sel:
                chosen = t
                break
    if chosen is None:
        print("❌ No encontrado.")
        return
    chosen["activa"] = not bool(chosen.get("activa"))
    save_tasks(tasks)
    state = "activada" if chosen["activa"] else "pausada"
    log_event(f"Tarea {state}: {chosen['nombre']}")
    print(f"✅ Tarea {state}.")

def delete_task() -> None:
    tasks = list_tasks()
    if not tasks:
        return
    sel = input("\nIndica # o id para eliminar: ").strip()
    if not sel:
        return
    target_idx: Optional[int] = None
    if sel.isdigit():
        idx = int(sel) - 1
        if 0 <= idx < len(tasks):
            target_idx = idx
    if target_idx is None:
        for i, t in enumerate(tasks):
            if str(t.get("id")) == sel:
                target_idx = i
                break
    if target_idx is None:
        print("❌ No encontrado.")
        return
    removed = tasks.pop(target_idx)
    save_tasks(tasks)
    log_event(f"Tarea eliminada: {removed['nombre']}")
    print("🗑️  Tarea eliminada.")

def export_tasks_csv() -> None:
    tasks = load_tasks()
    if not tasks:
        print("No hay tareas para exportar.")
        return
    csv_path = get_tasks_csv_path()
    with csv_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["id", "nombre", "plataforma", "frecuencia", "activa", "creada_en"])
        for t in tasks:
            w.writerow([
                t.get("id", ""),
                t.get("nombre", ""),
                t.get("plataforma", ""),
                t.get("frecuencia", ""),
                "1" if t.get("activa") else "0",
                t.get("creada_en", ""),
            ])
    print(f"📦 Exportado: {csv_path}")
    log_event("Export CSV de tareas generado")

# ========= REPORTES =========
def append_daily_report(note: str) -> None:
    report_file = get_reports_dir() / f"reporte_{datetime.now().strftime('%Y-%m-%d')}.txt"
    with report_file.open("a", encoding="utf-8") as f:
        f.write(f"[{now_str()}] {note}\n")

# ========= INTERFAZ DE CONSOLA =========
def header() -> None:
    os.system("cls" if os.name == "nt" else "clear")
    print("=" * 66)
    print(f"👑  {PROJECT_NAME}  ·  v{VERSION}".center(66))
    print("=" * 66)

def menu_console() -> None:
    init_environment()
    log_event("Plataforma iniciada (modo consola)")
    while True:
        header()
        print("1) Añadir nota a reporte de hoy")
        print("2) Ver últimos logs")
        print("3) Crear tarea automática")
        print("4) Listar tareas")
        print("5) Activar/Pausar tarea")
        print("6) Eliminar tarea")
        print("7) Exportar tareas a CSV")
        print("8) Abrir panel web (Flask)")
        print("0) Salir")
        choice = input("\nElige: ").strip()

        if choice == "1":
            note = input("\n📝 Nota: ").strip()
            if note:
                append_daily_report(note)
                log_event(f"Nota añadida al reporte: {note}")
                print("✅ Guardada.")
            else:
                print("⚠️ Vacía, no se guardó.")
            input("\nEnter para continuar...")

        elif choice == "2":
            show_last_logs()
            input("\nEnter para continuar...")

        elif choice == "3":
            create_task_interactive()
            input("\nEnter para continuar...")

        elif choice == "4":
            list_tasks()
            input("\nEnter para continuar...")

        elif choice == "5":
            toggle_task()
            input("\nEnter para continuar...")

        elif choice == "6":
            delete_task()
            input("\nEnter para continuar...")

        elif choice == "7":
            export_tasks_csv()
            input("\nEnter para continuar...")

        elif choice == "8":
            log_event("Dashboard iniciado (Flask)")
            print("\n🌐 Abriendo el panel web KING ROLON AUTOMATIONS...")
            run_dashboard()
            input("\n(Panel web detenido) Enter para volver al menú...")

        elif choice == "0":
            log_event("Plataforma cerrada por el usuario")
            print("\n👋 Hasta la próxima, KING.")
            time.sleep(0.6)
            break

        else:
            print("❌ Opción no válida.")
            time.sleep(0.6)

# ========= FLASK (DASHBOARD) =========
app = Flask(__name__)
LOG_PATH = get_log_file()

@app.route("/")
def dashboard():
    ingresos_mensuales = 1450
    tareas = load_tasks() or [
        {"id": "demo1", "nombre": "Subir clip diario a TikTok", "plataforma": "TikTok", "frecuencia": "Cada 24h", "activa": True},
        {"id": "demo2", "nombre": "Repost en Facebook Gaming", "plataforma": "Facebook", "frecuencia": "Después de cada stream", "activa": True},
        {"id": "demo3", "nombre": "Guardar mejores jugadas", "plataforma": "Global", "frecuencia": "Automático", "activa": True},
    ]
    plataformas = [
        {"nombre": "TikTok", "estado": "Conectado"},
        {"nombre": "Facebook", "estado": "Conectado"},
        {"nombre": "YouTube", "estado": "Pendiente"},
        {"nombre": "Twitch", "estado": "No conectado"},
    ]
    ultimos_logs: List[str] = []
    if LOG_PATH.exists():
        with LOG_PATH.open("r", encoding="utf-8") as f:
            ultimos_logs = [l.strip() for l in f.readlines()[-8:]]

    return render_template(
        "dashboard.html",
        ingresos_mensuales=ingresos_mensuales,
        tareas_activas=tareas,
        plataformas=plataformas,
        ultimos_logs=ultimos_logs,
    )

def run_dashboard() -> None:
    log_event("Dashboard iniciado (Flask)")
    app.run(host="127.0.0.1", port=5000, debug=False)

# ========= ENTRY =========
if __name__ == "__main__":
    header()
    print("1) Menú de consola")
    print("2) Panel web (Flask)")
    choice = input("\nSelecciona (1/2): ").strip()
    if choice == "1":
        menu_console()
    elif choice == "2":
        init_environment()
        print("\nAbre tu navegador en http://127.0.0.1:5000")
        run_dashboard()
    else:
        print("Cerrando…")
