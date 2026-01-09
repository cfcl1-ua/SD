import requests
import threading
import time
import os
import json

# =========================================================
# CONFIGURACIÓN
# =========================================================

OPENWEATHER_API_KEY = "d1f046b4561bf94314e5a30c6c6bac3c"
UNITS = "metric"                     # Celsius
CHECK_INTERVAL = 4                   # segundos

DB_FILE = "db.json"

API_CENTRAL_URL = "http://127.0.0.1:8000/clima"

# =========================================================
# ESTADO GLOBAL
# =========================================================

LOCATIONS = {}       # ciudad -> {"estado": OK/KO, "temperatura": float}

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
        
        response = requests.get(url, timeout=3)
        data = response.json()
        
        if "main" not in data:
            print(f"[ERROR] No se pudo consultar {ciudad}: {data.get('message', 'sin datos')}")
            return "ERROR", None

        temp = data["main"]["temp"]
        estado = "KO" if temp < 0 else "OK"

        return estado, temp

    except Exception as e:
        print(f"[EV_W][ERROR] No se pudo consultar {ciudad}: {e}")
        return "KO", None

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
        requests.put(API_CENTRAL_URL, json=payload, timeout=3)
        print(f"[EV_W] Notificado a Central: {ciudad} -> {estado} ({temperatura}°C)")
    except Exception as e:
        print(f"[EV_W][ERROR] No se pudo notificar a Central: {e}")

# =========================================================
# CICLO PRINCIPAL DE CONTROL CLIMÁTICO
# =========================================================

def ciclo_clima():
    while RUNNING:
        for ciudad in list(LOCATIONS.keys()):
            estado_nuevo, temp = consultar_tiempo(ciudad)
            estado_anterior = LOCATIONS[ciudad]["estado"]

            LOCATIONS[ciudad]["temperatura"] = temp
            LOCATIONS[ciudad]["estado"] = estado_nuevo
            
            guardar_clima(ciudad, estado_nuevo, temp)

            # Notificar solo si cambia o para refrescar estado
            if estado_nuevo != estado_anterior:
                notificar_central(ciudad, estado_nuevo, temp)

        time.sleep(CHECK_INTERVAL)

def guardar_clima(ciudad, estado, temperatura):
    # 1️⃣ Cargar DB
    if os.path.exists(DB_FILE):
        with open(DB_FILE, "r") as f:
            db = json.load(f)
    else:
        db = {"clientes": [], "cps": [], "climas": []}

    # 2️⃣ Crear la tabla "climas" si no existe
    if "climas" not in db:
        db["climas"] = []

    # 3️⃣ Buscar si ya existe la ciudad
    for c in db["climas"]:
        if c["ciudad"].lower() == ciudad.lower():
            # Actualizar
            c["estado"] = estado
            c["temperatura"] = temperatura
            break
    else:
        # Si no existe, agregar nuevo
        db["climas"].append({
            "ciudad": ciudad,
            "estado": estado,
            "temperatura": temperatura,
        })

    # 4️⃣ Guardar DB
    with open(DB_FILE, "w") as f:
        json.dump(db, f, indent=4)

    print(f"[EV_W] Clima guardado: {ciudad} -> {estado} ({temperatura}°C)")
# =========================================================
# MENÚ DE CONTROL (EN CALIENTE)
# =========================================================

def menu():
    global RUNNING
    print("\n[EV_W] Weather Control Office iniciado")
    print("Comandos:")
    print("  add <ciudad>     -> Añadir localización")
    print("  del <ciudad>     -> Eliminar localización")
    print("  list             -> Ver estado")
    print("  exit             -> Salir\n")

    while RUNNING:
        cmd = input("EV_W> ").strip().lower()
        if not cmd:
            continue

        parts = cmd.split()

        if parts[0] == "add" and len(parts) == 2:
            ciudad = parts[1].capitalize()
            if ciudad not in LOCATIONS:
                LOCATIONS[ciudad] = {"estado": "OK", "temperatura": None}
                print(f"[EV_W] Localización añadida: {ciudad}")
            else:
                print("[EV_W] La ciudad ya existe")

        elif parts[0] == "del" and len(parts) == 2:
            ciudad = parts[1].capitalize()
            if ciudad in LOCATIONS:
                del LOCATIONS[ciudad]
                print(f"[EV_W] Localización eliminada: {ciudad}")
            else:
                print("[EV_W] Ciudad no encontrada")

        elif parts[0] == "list":
            for ciudad, info in LOCATIONS.items():
                print(f"- {ciudad}: {info['estado']} ({info['temperatura']}°C)")

        elif parts[0] == "exit":
            RUNNING = False
            break

        else:
            print("[EV_W] Comando no válido")

# =========================================================
# MAIN
# =========================================================

def main():
    threading.Thread(target=ciclo_clima, daemon=True).start()
    menu()

if __name__ == "__main__":
    main()
