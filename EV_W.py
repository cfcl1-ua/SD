import requests
import threading
import time

# =========================================================
# CONFIGURACIÓN
# =========================================================

OPENWEATHER_API_KEY = "TU_API_KEY_AQUI"
UNITS = "metric"                     # Celsius
CHECK_INTERVAL = 4                   # segundos

API_CENTRAL_URL = "http://localhost:8000/clima"

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

            # Notificar solo si cambia o para refrescar estado
            if estado_nuevo != estado_anterior:
                notificar_central(ciudad, estado_nuevo, temp)

        time.sleep(CHECK_INTERVAL)

# =========================================================
# MENÚ DE CONTROL (EN CALIENTE)
# =========================================================

def menu():
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
            global RUNNING
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
