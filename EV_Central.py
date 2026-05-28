import socket
import threading
import time
import json
import jwt
from kafka import KafkaProducer, KafkaConsumer
from flask import Flask, jsonify, request
from flask_cors import CORS
from cryptography.fernet import Fernet
import requests

# =========================================================
# CONFIGURACIÓN GENERAL
# =========================================================

HEADER = 64
PORT = 5050
FORMAT = "utf-8"
FIN = "FIN"

TOPIC_DTC = "driver-to-central"
TOPIC_ETC = "engine-to-central"

DB_FILE = "db.json"

REGISTRY_HOST = "localhost"
REGISTRY_PORT = 9100

SECRET_KEY = "evregistry2425"

# =========================================================
# ESTADO GLOBAL
# =========================================================

SERVER = "localhost"
ADDR = (SERVER, PORT)

CPS = []
CPS_IDX = []
CP_KEYS = {}        # { id_cp: clave_aes_str }
CP_SOCKETS = {}     # { id_cp: conn }

CLIMATE_ALERTS = {}
AUDIT_LOG = []

CONEX_ACTIVAS = 0

# =========================================================
# FLASK API — puerto 8000
# =========================================================

app = Flask(__name__)
CORS(app)

# ==============================
# AUDITORÍA
# ==============================

def audit(action, description, origin=None):
    evento = {
        "fecha_hora": time.strftime("%Y-%m-%d %H:%M:%S"),
        "origen": origin or SERVER,
        "accion": action,
        "descripcion": description
    }
    AUDIT_LOG.append(evento)
    print("[AUDITORIA]", evento)

# ==============================
# CLIMA
# ==============================

@app.route("/clima", methods=["POST"])
def recibir_clima():
    data = request.json
    loc = data.get("localizacion")
    estado = data.get("estado")
    temperatura = data.get("temperatura")

    if not loc or not estado:
        return jsonify({"error": "datos incompletos"}), 400

    CLIMATE_ALERTS[loc] = estado
    audit("ALERTA_CLIMATOLOGICA", f"{loc} -> {estado} ({temperatura}°C)", request.remote_addr)

    db = loadDB()
    climas = db.get("climas", [])
    entrada = next((c for c in climas if c["ciudad"].lower() == loc.lower()), None)
    if entrada:
        entrada["estado"] = estado
        if temperatura is not None:
            entrada["temperatura"] = temperatura
    else:
        climas.append({
            "ciudad": loc,
            "estado": estado,
            "temperatura": temperatura if temperatura is not None else "-"
        })
    db["climas"] = climas

    cps_afectados = []
    for cp in db.get("cps", []):
        if cp.get("location", "").lower() == loc.lower():
            if estado == "KO":
                if cp["estado"] not in ("OFFLINE", "CHARGING"):
                    cp["estado"] = "ERROR"
                    audit("ESTADO_CP", f"CP {cp['id']} -> ERROR por alerta climática en {loc}")
            else:
                if cp["estado"] == "ERROR":
                    cp["estado"] = "AVAILABLE"
                    audit("ESTADO_CP", f"CP {cp['id']} -> AVAILABLE, clima restaurado en {loc}")
            cps_afectados.append(cp["id"])
        for cp_mem in CPS:
            if cp_mem["id"] == cp["id"]:
                cp_mem["estado"] = cp["estado"]

    saveDB(db)
    return jsonify({"resultado": "OK", "cps_afectados": cps_afectados})

# ==============================
# CPs
# ==============================

@app.route("/cps", methods=["GET"])
def obtener_cps():
    return jsonify(CPS)

# ==============================
# ESTADO GENERAL
# ==============================

@app.route("/estado", methods=["GET"])
def estado_general():
    return jsonify({
        "central": "ACTIVA",
        "num_cps": len(CPS),
        "alertas_clima": CLIMATE_ALERTS
    })

# ==============================
# AUDITORÍA
# ==============================

@app.route("/auditoria", methods=["GET"])
def obtener_auditoria():
    pagina = int(request.args.get("pagina", 1))
    limite = int(request.args.get("limite", 10))
    inicio = (pagina - 1) * limite
    fin = inicio + limite
    total = len(AUDIT_LOG)
    total_paginas = (total + limite - 1) // limite
    return jsonify({
        "eventos": AUDIT_LOG[::-1][inicio:fin],
        "pagina_actual": pagina,
        "total_paginas": total_paginas
    })

# ==============================
# REVOCACIÓN DE CLAVES
# ==============================

@app.route("/revocar/<id_cp>", methods=["POST"])
def revocar_clave(id_cp):
    if id_cp not in CP_KEYS:
        audit("REVOCACION_FALLIDA", f"CP {id_cp} no tiene clave activa", request.remote_addr)
        return jsonify({"error": f"CP {id_cp} no tiene clave activa"}), 404

    del CP_KEYS[id_cp]
    updateStatusCP(id_cp, "OFFLINE")

    conn = CP_SOCKETS.get(id_cp)
    if conn:
        try:
            conn.send("central|REVOCADO".encode(FORMAT))
        except Exception:
            pass

    audit("REVOCACION_CLAVE", f"Clave AES de CP {id_cp} revocada. CP fuera de servicio.", request.remote_addr)
    return jsonify({"resultado": "OK", "mensaje": f"Clave de CP {id_cp} revocada. El CP debe re-autenticarse."})

def run_api():
    app.run(host="0.0.0.0", port=8000, debug=False, use_reloader=False)

# =========================================================
# BBDD JSON
# =========================================================

def loadDB():
    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"clientes": [], "cps": [], "climas": []}

def saveDB(db):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=4)

def cargarCPs():
    db = loadDB()
    cps = db.get("cps", [])
    ids = [cp["id"] for cp in cps]
    return cps, ids

def insertToCPsBD(id_cp, loc_cp, estado):
    db = loadDB()
    for cp in db["cps"]:
        if cp["id"] == id_cp:
            return False
    db["cps"].append({
        "id": id_cp,
        "location": loc_cp,
        "registrado": True,
        "estado": estado,
        "token": "",
        "aes_key": ""
    })
    saveDB(db)
    return True

def updateStatusCP(id_cp, estado):
    db = loadDB()
    for cp in db["cps"]:
        if cp["id"] == id_cp:
            cp["estado"] = estado
            saveDB(db)
            for cp_mem in CPS:
                if cp_mem["id"] == id_cp:
                    cp_mem["estado"] = estado
            return True
    return False

# =========================================================
# AUTENTICACIÓN CONTRA EV_REGISTRY
# =========================================================

def validar_jwt(token: str, id_cp: str) -> bool:
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=["HS256"])
        return payload.get("id") == id_cp
    except jwt.ExpiredSignatureError:
        audit("JWT_EXPIRADO", f"Token expirado para CP {id_cp}")
        return False
    except jwt.InvalidTokenError as e:
        audit("JWT_INVALIDO", f"Token inválido para CP {id_cp}: {e}")
        return False

def obtener_clave_registry(id_cp: str):
    try:
        url = f"http://{REGISTRY_HOST}:{REGISTRY_PORT}/cp/{id_cp}"
        resp = requests.get(url, timeout=5)
        if resp.status_code == 200:
            return resp.json().get("aes_key")
        else:
            audit("REGISTRY_ERROR", f"Registry devolvió {resp.status_code} para CP {id_cp}")
            return None
    except Exception as e:
        audit("REGISTRY_ERROR", f"No se pudo contactar con Registry para CP {id_cp}: {e}")
        return None

# =========================================================
# SOCKET CP ↔ CENTRAL
# Protocolo de dos pasos:
#   Paso 1 (en claro): monitor|PETICION|<id>|IDLE
#            Central responde: "1" y luego "REGISTRADO" o "DESCONOCIDO"
#   Paso 2 (cifrado):  monitor|AUTENTIFICACION|<id>|<loc>|<token>
#            Central responde: status con la clave AES si todo OK
# =========================================================

def handle_client(conn, addr):
    global CONEX_ACTIVAS
    CONEX_ACTIVAS += 1
    ip_origen = addr[0]
    id_cp_sesion = None
    fernet_cp = None

    try:
        # -------------------------------------------------------
        # PASO 1: Primer mensaje en claro — PETICION
        # -------------------------------------------------------
        msg_length = conn.recv(HEADER).decode(FORMAT).strip()
        if not msg_length:
            conn.close()
            CONEX_ACTIVAS -= 1
            return

        raw = conn.recv(int(msg_length)).decode(FORMAT)
        parts = raw.split("|")

        # Esperamos: monitor|PETICION|<id>|IDLE
        if parts[0] != "monitor" or len(parts) < 3:
            conn.send("0".encode(FORMAT))
            conn.close()
            CONEX_ACTIVAS -= 1
            return

        id_cp_sesion = parts[2]

        # Verificar si hay demasiadas conexiones
        if CONEX_ACTIVAS > 10:
            conn.send("DEMASIADAS CONEXIONES".encode(FORMAT))
            conn.close()
            CONEX_ACTIVAS -= 1
            return

        # Aceptar conexión
        conn.send("1".encode(FORMAT))

        # Indicar si el CP ya está registrado en nuestra BD
        if id_cp_sesion in CPS_IDX:
            conn.send("REGISTRADO".encode(FORMAT))
            # CP ya conocido: obtener su clave del Registry para descifrar mensajes siguientes
            clave_aes = obtener_clave_registry(id_cp_sesion)
            if clave_aes:
                CP_KEYS[id_cp_sesion] = clave_aes
                CP_SOCKETS[id_cp_sesion] = conn
                fernet_cp = Fernet(clave_aes.encode() if isinstance(clave_aes, str) else clave_aes)
            audit("RECONEXION", f"CP {id_cp_sesion} reconectado", ip_origen)
        else:
            conn.send("DESCONOCIDO".encode(FORMAT))
            # Obtener clave del Registry ya ahora para poder descifrar el siguiente mensaje
            clave_aes = obtener_clave_registry(id_cp_sesion)
            if clave_aes:
                fernet_cp = Fernet(clave_aes.encode() if isinstance(clave_aes, str) else clave_aes)

        # -------------------------------------------------------
        # PASO 2 en adelante: mensajes cifrados
        # -------------------------------------------------------
        while True:
            msg_length = conn.recv(HEADER).decode(FORMAT).strip()
            if not msg_length:
                continue

            raw_bytes = conn.recv(int(msg_length))

            # Descifrar si tenemos Fernet, si no intentar en claro
            if fernet_cp:
                try:
                    msg = fernet_cp.decrypt(raw_bytes).decode(FORMAT)
                except Exception:
                    audit("ERROR_DESCIFRADO", f"No se pudo descifrar mensaje de CP {id_cp_sesion}", ip_origen)
                    break
            else:
                msg = raw_bytes.decode(FORMAT)

            if msg == FIN:
                break

            parts = msg.split("|")
            if parts[0] != "monitor" or len(parts) < 3:
                continue

            peticion = parts[1]
            id_cp    = parts[2]

            # AUTENTIFICACION: monitor|AUTENTIFICACION|<id>|<loc>|<token>
            if peticion in ("AUTENTIFICACION", "AUTENTICACION"):
                loc   = parts[3] if len(parts) > 3 else "UNKNOWN"
                token = parts[4] if len(parts) > 4 else None

                if not token or not validar_jwt(token, id_cp):
                    audit("AUTENTICACION_FALLIDA", f"CP {id_cp} JWT inválido o ausente", ip_origen)
                    conn.send("CP no autorizado".encode(FORMAT))
                    continue

                clave_aes = obtener_clave_registry(id_cp)
                if not clave_aes:
                    audit("AUTENTICACION_FALLIDA", f"CP {id_cp} no encontrado en Registry", ip_origen)
                    conn.send("CP no registrado en Registry".encode(FORMAT))
                    continue

                CP_KEYS[id_cp] = clave_aes
                CP_SOCKETS[id_cp] = conn
                fernet_cp = Fernet(clave_aes.encode() if isinstance(clave_aes, str) else clave_aes)

                insertToCPsBD(id_cp, loc, "AVAILABLE")
                if id_cp not in CPS_IDX:
                    CPS_IDX.append(id_cp)
                    CPS.append({"id": id_cp, "location": loc, "estado": "AVAILABLE"})

                audit("AUTENTICACION_OK", f"CP {id_cp} autenticado. Clave AES entregada.", ip_origen)
                conn.send(f"central|OK|{clave_aes}".encode(FORMAT))

            # ESTADO: monitor|ESTADO|<id>|<estado>
            elif peticion == "ESTADO":
                estado = parts[3] if len(parts) > 3 else "UNKNOWN"
                if id_cp not in CPS_IDX:
                    audit("ESTADO_CP_FALLIDO", f"CP {id_cp} no autenticado", ip_origen)
                    conn.send("central|ERROR|NO_AUTENTICADO".encode(FORMAT))
                    continue
                updateStatusCP(id_cp, estado)
                audit("ESTADO_CP", f"CP {id_cp} -> {estado}", ip_origen)
                conn.send("central|OK".encode(FORMAT))

            # TOKEN: monitor|TOKEN|<id>|<nuevo_token>
            elif peticion == "TOKEN":
                nuevo_token = parts[3] if len(parts) > 3 else None
                if not nuevo_token or not validar_jwt(nuevo_token, id_cp):
                    audit("TOKEN_RENOVACION_FALLIDA", f"CP {id_cp} token inválido", ip_origen)
                    conn.send("central|ERROR|JWT_INVALIDO".encode(FORMAT))
                    continue
                audit("TOKEN_RENOVADO", f"Token de CP {id_cp} renovado", ip_origen)
                conn.send("central|OK".encode(FORMAT))

            else:
                audit("PETICION_DESCONOCIDA", f"CP {id_cp}: {peticion}", ip_origen)
                conn.send("central|ERROR|DESCONOCIDA".encode(FORMAT))

    except Exception as e:
        audit("ERROR_SOCKET", f"Error con cliente {addr}: {e}")

    finally:
        if id_cp_sesion and CP_SOCKETS.get(id_cp_sesion) is conn:
            del CP_SOCKETS[id_cp_sesion]
        conn.close()
        CONEX_ACTIVAS -= 1

# =========================================================
# KAFKA
# =========================================================

def create_consumer(bootstrap):
    return KafkaConsumer(
        TOPIC_DTC, TOPIC_ETC,
        bootstrap_servers=[bootstrap],
        value_deserializer=lambda v: v,
        auto_offset_reset="earliest",
        group_id="central-group"
    )

def receive_messages(consumer):
    for msg in consumer:
        try:
            raw = msg.value.decode(FORMAT)
            audit("MENSAJE_KAFKA", raw)
        except Exception as e:
            audit("ERROR_KAFKA", f"Mensaje ilegible: {e}")

def run_kafka_loop(bootstrap):
    try:
        consumer = create_consumer(bootstrap)
        receive_messages(consumer)
    except Exception as e:
        audit("ERROR_KAFKA", f"No se pudo conectar al broker: {e}")

# =========================================================
# SOCKET SERVER GENERAL
# =========================================================

def start(server):
    server.listen()
    print(f"[CENTRAL] Escuchando en {ADDR}")
    while True:
        conn, addr = server.accept()
        threading.Thread(
            target=handle_client,
            args=(conn, addr),
            daemon=True
        ).start()

# =========================================================
# MAIN
# =========================================================

def main():
    global SERVER, ADDR, CPS, CPS_IDX, REGISTRY_HOST

    SERVER = input("Introduce la IP del servidor central: ").strip()
    ADDR = (SERVER, PORT)

    registry_ip = input(f"Introduce la IP del EV_Registry [{REGISTRY_HOST}]: ").strip()
    if registry_ip:
        REGISTRY_HOST = registry_ip

    CPS, CPS_IDX = cargarCPs()
    print(f"[CENTRAL] {len(CPS)} CPs cargados desde BD")

    threading.Thread(target=run_kafka_loop, args=(SERVER,), daemon=True).start()
    threading.Thread(target=run_api, daemon=True).start()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", PORT))
    start(server)

if __name__ == "__main__":
    main()