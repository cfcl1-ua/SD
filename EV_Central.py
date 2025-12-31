import socket
import threading
import time
import json
from kafka import KafkaProducer, KafkaConsumer
from flask import Flask, jsonify, request
from flask_cors import CORS
from cryptography.fernet import Fernet

# =========================================================
# CONFIGURACIÓN GENERAL
# =========================================================

HEADER = 64
PORT = 5050
FORMAT = "utf-8"
FIN = "FIN"
MAX_CONEXIONES = 5
PRICE_PER_KWH = 0.28

TOPIC_DTC = "driver-to-central"
TOPIC_ETC = "engine-to-central"

DB_FILE = "db.json"

# =========================================================
# ESTADO GLOBAL
# =========================================================

SERVER = "localhost"
ADDR = (SERVER, PORT)

CPS = []               # CPs registrados
CPS_IDX = []           # IDs CPs
CP_KEYS = {}           # id_cp -> clave simétrica

CLIMATE_ALERTS = {}    # localizacion -> OK | KO
AUDIT_LOG = []         # auditoría en memoria

CONEX_ACTIVAS = 0

# =========================================================
# FLASK API (inspirada en API_Central_ANTONIO)
# =========================================================

app = Flask(__name__)
CORS(app)

def audit(action, description, origin=None):
    evento = {
        "fecha_hora": time.strftime("%Y-%m-%d %H:%M:%S"),
        "origen": origin or SERVER,
        "accion": action,
        "descripcion": description
    }
    AUDIT_LOG.append(evento)
    print("[AUDITORIA]", evento)

@app.route("/clima", methods=["POST"])
def recibir_clima():
    data = request.json
    loc = data.get("localizacion")
    estado = data.get("estado")

    if not loc or not estado:
        return jsonify({"error": "datos incompletos"}), 400

    CLIMATE_ALERTS[loc] = estado
    audit("ALERTA_CLIMATOLOGICA", f"{loc} -> {estado}", request.remote_addr)

    return jsonify({"resultado": "OK"})

@app.route("/cps")
def obtener_cps():
    return jsonify(CPS)

@app.route("/estado")
def estado_general():
    return jsonify({
        "central": "ACTIVA",
        "num_cps": len(CPS),
        "alertas_clima": CLIMATE_ALERTS
    })

@app.route("/auditoria")
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

def run_api():
    app.run(host="0.0.0.0", port=8000, debug=False)

# =========================================================
# CIFRADO SIMÉTRICO CP ↔ CENTRAL
# =========================================================

def encrypt_msg(id_cp, msg):
    f = Fernet(CP_KEYS[id_cp].encode())
    return f.encrypt(msg.encode())

def decrypt_msg(id_cp, msg):
    f = Fernet(CP_KEYS[id_cp].encode())
    return f.decrypt(msg).decode()

def revoke_cp(id_cp):
    if id_cp in CP_KEYS:
        del CP_KEYS[id_cp]
        audit("REVOCACION", f"Clave revocada CP {id_cp}")

# =========================================================
# BBDD JSON (MISMA QUE TU PRÁCTICA 1)
# =========================================================

def loadDB():
    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"clientes": [], "cps": []}

def saveDB(db):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=4)

def cargarCPs():
    db = loadDB()
    cps = db["cps"]
    ids = [cp["id"] for cp in cps]
    return cps, ids

def insertToCPsBD(id_cp, loc_cp, estado):
    db = loadDB()
    for cp in db["cps"]:
        if cp["id"] == id_cp:
            return False
    db["cps"].append({
        "id": id_cp,
        "loc": loc_cp,
        "estado": estado
    })
    saveDB(db)
    return True

def updateStatusCP(id_cp, estado):
    db = loadDB()
    for cp in db["cps"]:
        if cp["id"] == id_cp:
            cp["estado"] = estado
            saveDB(db)
            return True
    return False

# =========================================================
# SOCKET CP ↔ CENTRAL (AUTENTICACIÓN)
# =========================================================

def attendToMonitor(peticion, id_cp, var):
    if peticion == "AUTENTICACION":
        if id_cp in CP_KEYS:
            audit("AUTENTICACION_FALLIDA", f"CP {id_cp} ya autenticado")
            return "central|ERROR"

        clave = Fernet.generate_key().decode()
        CP_KEYS[id_cp] = clave

        insertToCPsBD(id_cp, var, "IDLE")
        CPS_IDX.append(id_cp)
        CPS.append({"id": id_cp, "loc": var, "estado": "IDLE"})

        audit("AUTENTICACION", f"CP {id_cp} autenticado correctamente")
        return f"central|OK|{clave}"

    elif peticion == "ESTADO":
        if id_cp not in CPS_IDX:
            return "central|ERROR"

        updateStatusCP(id_cp, var)
        audit("ESTADO_CP", f"{id_cp} -> {var}")
        return "central|OK"

    return "central|ERROR"

# =========================================================
# KAFKA
# =========================================================

def create_producer(bootstrap):
    return KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda v: v if isinstance(v, bytes) else v.encode(FORMAT)
    )

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
        except:
            audit("ERROR_KAFKA", "Mensaje ilegible")

def run_kafka_loop(bootstrap):
    consumer = create_consumer(bootstrap)
    receive_messages(consumer)

# =========================================================
# SOCKET SERVER GENERAL
# =========================================================

def start(server):
    global CONEX_ACTIVAS
    server.listen()
    while True:
        conn, addr = server.accept()
        CONEX_ACTIVAS += 1
        threading.Thread(target=handle_client, args=(conn, addr)).start()

def handle_client(conn, addr):
    global CONEX_ACTIVAS
    while True:
        try:
            msg_length = conn.recv(HEADER).decode(FORMAT)
            if not msg_length:
                continue

            msg = conn.recv(int(msg_length)).decode(FORMAT)
            if msg == FIN:
                break

            parts = msg.split("|")
            if parts[0] == "monitor":
                respuesta = attendToMonitor(parts[1], parts[2], parts[3])
                conn.send(respuesta.encode(FORMAT))
        except:
            break

    conn.close()
    CONEX_ACTIVAS -= 1

# =========================================================
# MAIN
# =========================================================

def main():
    global SERVER, ADDR, CPS, CPS_IDX

    SERVER = input("Introduce la IP del servidor: ").strip()
    ADDR = (SERVER, PORT)

    CPS, CPS_IDX = cargarCPs()

    threading.Thread(target=run_kafka_loop, args=(SERVER,), daemon=True).start()
    threading.Thread(target=run_api, daemon=True).start()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(ADDR)
    start(server)

if __name__ == "__main__":
    main()