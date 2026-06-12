import requests
import threading
import time
import os
import json
import argparse
from flask import Flask, jsonify, request as flask_request
from flask_cors import CORS
# d1f046b4561bf94314e5a30c6c6bac3c
# =========================================================
# CONFIGURACIÓN (parametrizable, sin hardcodear)
# =========================================================

UNITS = "metric"        # Celsius
CHECK_INTERVAL = 4      # segundos entre consultas
DB_FILE = "db.json"

# Se rellenan en main() con los argumentos de línea de comandos
OPENWEATHER_API_KEY = ""
API_CENTRAL_URL = ""
EVW_PORT = 6060

# =========================================================
# ESTADO GLOBAL
# =========================================================

# ciudad -> {"estado": "OK"/"KO"/"ERROR", "temperatura": float}
LOCATIONS = {}
RUNNING = True

# =========================================================
# OPENWEATHER
# =========================================================

def consultar_tiempo(ciudad):
    try:
        url = (
            "https://api.openweathermap.org/data/2.5/weather"
            f"?q={ciudad}&appid={OPENWEATHER_API_KEY}&units={UNITS}"
        )
        response = requests.get(url, timeout=5)
        data = response.json()

        if "main" not in data:
            print(f"[EV_W][ERROR] No se pudo consultar {ciudad}: {data.get('message', 'sin datos')}")
            return "ERROR", None

        temp = data["main"]["temp"]
        estado = "KO" if temp < 0 else "OK"
        return estado, temp

    except requests.exceptions.ConnectionError:
        print(f"[EV_W][ERROR] Sin conexión a OpenWeather al consultar {ciudad}")
        return "ERROR", None
    except Exception as e:
        print(f"[EV_W][ERROR] Error inesperado consultando {ciudad}: {e}")
        return "ERROR", None

# =========================================================
# NOTIFICACIÓN A EV_CENTRAL
# =========================================================

def notificar_central(ciudad, estado, temperatura):
    payload = {
        "localizacion": ciudad,
        "estado": estado,
        "temperatura": temperatura
    }
    try:
        response = requests.post(API_CENTRAL_URL, json=payload, timeout=3)
        if response.status_code == 200:
            print(f"[EV_W] Notificado a Central: {ciudad} -> {estado} ({temperatura}°C)")
        else:
            print(f"[EV_W][ERROR] Central respondió {response.status_code} al notificar {ciudad}")
    except requests.exceptions.ConnectionError:
        print(f"[EV_W][ERROR] Imposible conectar con Central. ¿Está arrancada?")
    except Exception as e:
        print(f"[EV_W][ERROR] Error notificando a Central: {e}")

# =========================================================
# CICLO PRINCIPAL DE CONTROL CLIMÁTICO
# =========================================================

def ciclo_clima():
    primer_ciclo = True
    while RUNNING:
        for ciudad in list(LOCATIONS.keys()):
            estado_nuevo, temp = consultar_tiempo(ciudad)

            # Solo actuar si obtuvimos datos válidos
            if estado_nuevo == "ERROR":
                continue

            estado_anterior = LOCATIONS[ciudad]["estado"]
            temp_anterior   = LOCATIONS[ciudad]["temperatura"]

            LOCATIONS[ciudad]["temperatura"] = temp
            LOCATIONS[ciudad]["estado"]      = estado_nuevo

            # Guardar en BD solo si algo cambió
            if estado_nuevo != estado_anterior or temp != temp_anterior:
                guardar_clima(ciudad, estado_nuevo, temp)

            # Notificar a Central: siempre en el primer ciclo, o si cambia el estado
            if primer_ciclo or estado_nuevo != estado_anterior:
                if not primer_ciclo:
                    print(f"[EV_W] Cambio de estado en {ciudad}: {estado_anterior} -> {estado_nuevo} ({temp}°C)")
                notificar_central(ciudad, estado_nuevo, temp)

        primer_ciclo = False
        time.sleep(CHECK_INTERVAL)

# =========================================================
# PERSISTENCIA EN BD
# =========================================================

def guardar_clima(ciudad, estado, temperatura):
    try:
        if os.path.exists(DB_FILE):
            with open(DB_FILE, "r", encoding="utf-8") as f:
                db = json.load(f)
        else:
            db = {"clientes": [], "cps": [], "climas": []}

        if "climas" not in db:
            db["climas"] = []

        for c in db["climas"]:
            if c["ciudad"].lower() == ciudad.lower():
                c["estado"]      = estado
                c["temperatura"] = temperatura
                break
        else:
            db["climas"].append({
                "ciudad":      ciudad,
                "estado":      estado,
                "temperatura": temperatura
            })

        with open(DB_FILE, "w", encoding="utf-8") as f:
            json.dump(db, f, indent=4, ensure_ascii=False)

    except Exception as e:
        print(f"[EV_W][ERROR] No se pudo guardar en BD: {e}")

# =========================================================
# CARGA INICIAL DE CIUDADES DESDE LA BD
# =========================================================

def cargar_ciudades_desde_db():
    """Lee db.json y añade a LOCATIONS todas las ciudades de los CPs registrados."""
    try:
        if not os.path.exists(DB_FILE):
            return
        with open(DB_FILE, "r", encoding="utf-8") as f:
            db = json.load(f)
        for cp in db.get("cps", []):
            loc = cp.get("location", "").strip()
            if loc and loc.lower() != "desconocida":
                ciudad = loc.title()
                if ciudad not in LOCATIONS:
                    LOCATIONS[ciudad] = {"estado": "OK", "temperatura": None}
                    print(f"[EV_W] Ciudad cargada desde BD: {ciudad}")
    except Exception as e:
        print(f"[EV_W][ERROR] No se pudieron cargar ciudades desde BD: {e}")

# =========================================================
# SERVIDOR FLASK — recibe notificaciones de EV_Central
# =========================================================

app_evw = Flask(__name__)
CORS(app_evw)

@app_evw.route("/ciudad", methods=["POST"])
def recibir_ciudad():
    data = flask_request.json
    ciudad_raw = data.get("ciudad", "").strip() if data else ""
    if not ciudad_raw:
        return jsonify({"error": "ciudad no especificada"}), 400

    ciudad = ciudad_raw.title()
    if ciudad.lower() == "desconocida":
        return jsonify({"error": "ciudad no válida"}), 400

    if ciudad not in LOCATIONS:
        LOCATIONS[ciudad] = {"estado": "OK", "temperatura": None}
        print(f"[EV_W] Nueva ciudad recibida de Central: {ciudad}")
        return jsonify({"resultado": "OK", "ciudad": ciudad}), 200
    else:
        return jsonify({"resultado": "YA_EXISTE", "ciudad": ciudad}), 200

def run_flask_evw():
    app_evw.run(host="0.0.0.0", port=EVW_PORT, debug=False, use_reloader=False)

# =========================================================
# MENÚ DE CONTROL (EN CALIENTE)
# =========================================================

def menu():
    global RUNNING
    print("\n[EV_W] Weather Control Office iniciado")
    print(f"  Central : {API_CENTRAL_URL}")
    print("Comandos:")
    print("  add <ciudad>  -> Añadir localización")
    print("  del <ciudad>  -> Eliminar localización")
    print("  list          -> Ver estado actual")
    print("  exit          -> Salir\n")

    while RUNNING:
        try:
            cmd = input("EV_W> ").strip()
        except EOFError:
            break

        if not cmd:
            continue

        parts = cmd.split()

        if parts[0].lower() == "add" and len(parts) >= 2:
            # Permite ciudades con espacios: add San Sebastian
            ciudad = " ".join(parts[1:]).title()
            if ciudad not in LOCATIONS:
                LOCATIONS[ciudad] = {"estado": "OK", "temperatura": None}
                print(f"[EV_W] Localización añadida: {ciudad}")
            else:
                print(f"[EV_W] {ciudad} ya estaba en la lista")

        elif parts[0].lower() == "del" and len(parts) >= 2:
            ciudad = " ".join(parts[1:]).title()
            if ciudad in LOCATIONS:
                del LOCATIONS[ciudad]
                print(f"[EV_W] Localización eliminada: {ciudad}")
            else:
                print(f"[EV_W] Ciudad no encontrada: {ciudad}")

        elif parts[0].lower() == "list":
            if not LOCATIONS:
                print("[EV_W] No hay localizaciones registradas")
            else:
                print(f"{'Ciudad':<20} {'Estado':<8} {'Temperatura'}")
                print("-" * 40)
                for c, info in LOCATIONS.items():
                    temp = f"{info['temperatura']}°C" if info['temperatura'] is not None else "---"
                    print(f"  {c:<18} {info['estado']:<8} {temp}")

        elif parts[0].lower() == "exit":
            RUNNING = False
            print("[EV_W] Cerrando Weather Control Office...")
            break

        else:
            print("[EV_W] Comando no válido. Usa: add <ciudad> | del <ciudad> | list | exit")

# =========================================================
# MAIN
# =========================================================

def main():
    global OPENWEATHER_API_KEY, API_CENTRAL_URL, EVW_PORT

    parser = argparse.ArgumentParser(description="EV_W - Weather Control Office")
    parser.add_argument(
        "--apikey",
        type=str,
        required=True,
        help="API key de OpenWeather"
    )
    parser.add_argument(
        "--central",
        type=str,
        default="http://127.0.0.1:8000",
        help="URL base de EV_Central (ej. http://192.168.1.10:8000)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=6060,
        help="Puerto en el que escucha EV_W (default: 6060)"
    )
    args = parser.parse_args()

    OPENWEATHER_API_KEY = args.apikey
    API_CENTRAL_URL     = f"{args.central.rstrip('/')}/clima"
    EVW_PORT            = args.port

    cargar_ciudades_desde_db()
    threading.Thread(target=run_flask_evw, daemon=True).start()
    threading.Thread(target=ciclo_clima, daemon=True).start()
    menu()

if __name__ == "__main__":
    main()