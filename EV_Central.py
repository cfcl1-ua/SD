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
PRICE_PER_KWH = 0.20 # Precio por kWh ficticio

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

EVW_URL = ""  # URL de EV_W, se rellena en main() con --evw

# =========================================================
# NOTIFICACIÓN A EV_W
# =========================================================

def notificar_evw_ciudad(ciudad):
    """Informa a EV_W de una nueva ciudad para que empiece a monitorizarla."""
    if not EVW_URL:
        return
    try:
        r = requests.post(f"{EVW_URL.rstrip('/')}/ciudad", json={"ciudad": ciudad}, timeout=3)
        if r.status_code == 200:
            print(f"[CENTRAL] EV_W notificado: nueva ciudad '{ciudad}'")
        else:
            print(f"[CENTRAL][WARN] EV_W respondió {r.status_code} para ciudad '{ciudad}'")
    except Exception as e:
        print(f"[CENTRAL][WARN] No se pudo notificar a EV_W: {e}")

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

@app.route("/climas", methods=["GET"])
def obtener_climas():
    db = loadDB()
    return jsonify(db.get("climas", []))

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

# =========================================================
# CLIENTES
# =========================================================
@app.route("/api/clientes", methods=["GET"])
def obtener_clientes():
    db = loadDB()
    return jsonify(db.get("clientes", []))

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
        print(f"[DEBUG] JWT payload: {payload}")
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
    print(f"[DEBUG] Nueva conexion desde {addr}")

    try:
        # PASO 1: mensaje en claro
        msg_length = conn.recv(HEADER).decode(FORMAT).strip()
        print(f"[DEBUG] Header recibido: '{msg_length}'")
        if not msg_length:
            conn.close()
            CONEX_ACTIVAS -= 1
            return

        raw = conn.recv(int(msg_length)).decode(FORMAT)
        print(f"[DEBUG] Mensaje paso1: '{raw}'")
        parts = raw.split("|")

        if parts[0] != "monitor" or len(parts) < 3:
            print(f"[DEBUG] Mensaje invalido: {parts}")
            conn.send("0".encode(FORMAT))
            conn.close()
            CONEX_ACTIVAS -= 1
            return

        id_cp_sesion = parts[2]
        print(f"[DEBUG] CP identificado: {id_cp_sesion}")

        if CONEX_ACTIVAS > 10:
            conn.send("DEMASIADAS CONEXIONES".encode(FORMAT))
            conn.close()
            CONEX_ACTIVAS -= 1
            return

        if id_cp_sesion in CPS_IDX:
            print(f"[DEBUG] CP conocido -> enviando REGISTRADO")
            conn.send("REGISTRADO".encode(FORMAT).ljust(HEADER))
            clave_aes = obtener_clave_registry(id_cp_sesion)
            print(f"[DEBUG] Clave del Registry: {clave_aes is not None}")
            if clave_aes:
                CP_KEYS[id_cp_sesion] = clave_aes
                CP_SOCKETS[id_cp_sesion] = conn
                fernet_cp = Fernet(clave_aes.encode() if isinstance(clave_aes, str) else clave_aes)
            audit("RECONEXION", f"CP {id_cp_sesion} reconectado", ip_origen)
        else:
            print(f"[DEBUG] CP desconocido -> enviando DESCONOCIDO")
            conn.send("DESCONOCIDO".encode(FORMAT).ljust(HEADER))
            clave_aes = obtener_clave_registry(id_cp_sesion)
            print(f"[DEBUG] Clave del Registry: {clave_aes is not None}")
            if clave_aes:
                fernet_cp = Fernet(clave_aes.encode() if isinstance(clave_aes, str) else clave_aes)
            else:
                print(f"[DEBUG] AVISO: sin clave AES, no se podra descifrar el siguiente mensaje")

        # PASO 2: mensajes cifrados
        while True:
            msg_length = conn.recv(HEADER).decode(FORMAT).strip()
            print(f"[DEBUG] Header paso2: '{msg_length}'")
            if not msg_length:
                continue

            raw_bytes = conn.recv(int(msg_length))
            print(f"[DEBUG] Bytes recibidos: {len(raw_bytes)}, fernet: {fernet_cp is not None}")

            if fernet_cp:
                try:
                    msg = fernet_cp.decrypt(raw_bytes).decode(FORMAT)
                    print(f"[DEBUG] Descifrado OK: '{msg[:80]}'")
                except Exception as e:
                    print(f"[DEBUG] ERROR descifrado: {e}")
                    audit("ERROR_DESCIFRADO", f"No se pudo descifrar mensaje de CP {id_cp_sesion}", ip_origen)
                    break
            else:
                msg = raw_bytes.decode(FORMAT)
                print(f"[DEBUG] Mensaje en claro: '{msg[:80]}'")

            if msg == FIN:
                break

            parts = msg.split("|")
            if parts[0] != "monitor" or len(parts) < 3:
                continue

            peticion = parts[1]
            id_cp    = parts[2]

            if peticion in ("AUTENTIFICACION", "AUTENTICACION"):
                loc   = parts[3] if len(parts) > 3 else "UNKNOWN"
                token = parts[4] if len(parts) > 4 else None
                print(f"[DEBUG] Autenticacion de CP {id_cp}, loc={loc}, token={'si' if token else 'no'}")

                if not token or not validar_jwt(token, id_cp):
                    audit("AUTENTICACION_FALLIDA", f"CP {id_cp} JWT invalido o ausente", ip_origen)
                    print(f"[DEBUG] JWT invalido para CP {id_cp}")
                    conn.send("CP no autorizado".encode(FORMAT))
                    continue

                clave_aes = obtener_clave_registry(id_cp)
                if not clave_aes:
                    audit("AUTENTICACION_FALLIDA", f"CP {id_cp} no encontrado en Registry", ip_origen)
                    print(f"[DEBUG] CP {id_cp} no encontrado en Registry")
                    conn.send("CP no registrado en Registry".encode(FORMAT))
                    continue

                CP_KEYS[id_cp] = clave_aes
                CP_SOCKETS[id_cp] = conn
                fernet_cp = Fernet(clave_aes.encode() if isinstance(clave_aes, str) else clave_aes)

                es_nuevo = insertToCPsBD(id_cp, loc, "AVAILABLE")
                if id_cp not in CPS_IDX:
                    CPS_IDX.append(id_cp)
                    CPS.append({"id": id_cp, "location": loc, "estado": "AVAILABLE"})

                if es_nuevo:
                    threading.Thread(target=notificar_evw_ciudad, args=(loc,), daemon=True).start()

                audit("AUTENTICACION_OK", f"CP {id_cp} autenticado. Clave AES entregada.", ip_origen)
                print(f"[DEBUG] Autenticacion OK para CP {id_cp}")
                conn.send(f"central|OK|{clave_aes}".encode(FORMAT))

            elif peticion == "ESTADO":
                estado = parts[3] if len(parts) > 3 else "UNKNOWN"
                if id_cp not in CPS_IDX:
                    audit("ESTADO_CP_FALLIDO", f"CP {id_cp} no autenticado", ip_origen)
                    conn.send("central|ERROR|NO_AUTENTICADO".encode(FORMAT))
                    continue
                updateStatusCP(id_cp, estado)
                audit("ESTADO_CP", f"CP {id_cp} -> {estado}", ip_origen)
                conn.send("central|OK".encode(FORMAT))

            elif peticion == "TOKEN":
                nuevo_token = parts[3] if len(parts) > 3 else None
                if not nuevo_token or not validar_jwt(nuevo_token, id_cp):
                    audit("TOKEN_RENOVACION_FALLIDA", f"CP {id_cp} token invalido", ip_origen)
                    conn.send("central|ERROR|JWT_INVALIDO".encode(FORMAT))
                    continue
                audit("TOKEN_RENOVADO", f"Token de CP {id_cp} renovado", ip_origen)
                conn.send("central|OK".encode(FORMAT))

            else:
                audit("PETICION_DESCONOCIDA", f"CP {id_cp}: {peticion}", ip_origen)
                conn.send("central|ERROR|DESCONOCIDA".encode(FORMAT))

    except Exception as e:
        print(f"[DEBUG] Excepcion en handle_client: {e}")
        audit("ERROR_SOCKET", f"Error con cliente {addr}: {e}")

    finally:
        if id_cp_sesion and CP_SOCKETS.get(id_cp_sesion) is conn:
            del CP_SOCKETS[id_cp_sesion]
        conn.close()
        CONEX_ACTIVAS -= 1
        
def addCustomer(driver_id):
    """Añade el ID del conductor a la lista de clientes si no existe."""
    db = loadDB()
    if "clientes" not in db:
        db["clientes"] = []
    
    id_str = str(driver_id)
    if id_str not in db["clientes"]:
        db["clientes"].append(id_str)
        saveDB(db)
        print(f"[CENTRAL] Conductor {id_str} guardado exitosamente en db.json")
        return True
    return False

# =========================================================
# LÓGICA DE BROKERS Y KAFKA
# =========================================================

def topics_id(id):
    return f"central-to-consumer-{id}"

def replyToDriver(producer, respuesta, cp_id, driver_id):
    if cp_id is None:
        cp_id = ""

    payload = f"central|{respuesta}|{cp_id}|{driver_id}"
    topic_resp = topics_id(driver_id)

    try:
        producer.send(topic_resp, payload.encode(FORMAT))
        producer.flush(1)
        print(f"[CENTRAL → DRIVER {driver_id}] {payload}")
    except Exception as e:
        print(f"[ERROR] Fallo enviando respuesta al driver {driver_id}: {e}")
        
def replyToEngine(producer, peticion, cp_id, driver_id):
    topic_resp = f"central-to-engine-{cp_id}"
    payload = f"central|{peticion}|{cp_id}|{driver_id}"
    
    # Ciframos el mensaje hacia el Engine usando su clave AES
    clave = CP_KEYS.get(cp_id)
    if clave:
        try:
            f = Fernet(clave.encode() if isinstance(clave, str) else clave)
            payload_cifrado = f.encrypt(payload.encode(FORMAT))
            producer.send(topic_resp, payload_cifrado)
            producer.flush(1)
            print(f"[CENTRAL → ENGINE {cp_id}] Petición cifrada enviada.")
        except Exception as e:
            print(f"[ERROR] No se pudo cifrar hacia ENGINE {cp_id}: {e}")
    else:
        print(f"[AVISO] El CP {cp_id} no tiene clave registrada. No se puede enviar.")
    
def attendToDriver(peticion, cp_id, driver_id, producer=None):
    if peticion == "AUTENTIFICACION":
        if addCustomer(driver_id):
            replyToDriver(producer, "OK", "", driver_id)
            return "central|OK"
        else:
            replyToDriver(producer, "ERROR:YA_REGISTRADO", "", driver_id)
            return "central|ERROR"

    if peticion in ("AUTORIZACION", "ESTADO", "FIN"):
        if cp_id not in CPS_IDX:
            replyToDriver(producer, f"{peticion}:ERROR_CP_DESCONOCIDO", cp_id, driver_id)
            return "central|ERROR"

        if peticion in ("AUTORIZACION", "ESTADO"):
            replyToDriver(producer, f"{peticion}:ENVIADO_AL_ENGINE", cp_id, driver_id)

        if producer is not None:
            replyToEngine(producer, peticion, cp_id, driver_id)

        return "central|OK"

    replyToDriver(producer, "ERROR:PETICION_DESCONOCIDA", cp_id, driver_id)
    return "central|ERROR"

def attendToEngine(producer, respuesta, cp_id, driver_id):
    if respuesta.isdigit():
        segundos = int(respuesta)
        horas = segundos / 3600
        precio = horas * PRICE_PER_KWH
        mensaje = f"FIN|TIEMPO:{segundos}|PRECIO:{precio:.2f}"
        replyToDriver(producer, mensaje, cp_id, driver_id)
        
        # Al terminar, liberamos el cargador actualizándolo en la BBDD
        updateStatusCP(cp_id, "AVAILABLE")
        print(f"[CENTRAL] Enviado FIN con tiempo y precio a Driver {driver_id}")
    else:
        mensaje = f"ESTADO|{respuesta}"
        replyToDriver(producer, mensaje, cp_id, driver_id)
        
        # Guardamos en la base de datos el nuevo estado (ej. CHARGING, ERROR...)
        updateStatusCP(cp_id, respuesta)
        print(f"[CENTRAL] Estado {respuesta} reenviado al Driver {driver_id} y guardado en DB")
        
        
def create_producer(bootstrap):
    try:
        # Nota: quitamos el value_serializer global para poder mandar bytes (cifrados)
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap],
            linger_ms=10,
        )
        print(f"[CENTRAL] Productor conectado a {bootstrap}")
        return producer
    except Exception as e:
        print(f"[CENTRAL] No puedo crear el productor en '{bootstrap}': {e}")
        raise


def create_consumer(bootstrap):
    return KafkaConsumer(
            TOPIC_DTC,
            TOPIC_ETC,
            bootstrap_servers=[bootstrap],
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="central-mixed-group"
        )


def receive_messages(consumer, producer):
    """
    Bucle unificado que separa el tráfico:
    - TOPIC_DTC (Driver): Texto plano.
    - TOPIC_ETC (Engine): Cifrado AES (Fernet).
    """
    for msg in consumer:
        raw_bytes = msg.value
        if not raw_bytes:
            continue

        origen_topic = msg.topic

        # ===============================================
        # CANAL DRIVER (TEXTO PLANO)
        # ===============================================
        if origen_topic == TOPIC_DTC:
            try:
                text = raw_bytes.decode(FORMAT)
                parts = text.split("|")
                if len(parts) >= 4 and parts[0].lower() == "driver":
                    peticion  = parts[1]
                    cp_id     = parts[2]
                    driver_id = parts[3]
                    print(f"\n[CENTRAL-KAFKA] Recibido DRIVER: {text}")
                    attendToDriver(peticion, cp_id, driver_id, producer)
            except Exception as e:
                print(f"[ERROR] Fallo leyendo mensaje de driver: {e}")

        # ===============================================
        # CANAL ENGINE (CIFRADO FERNET)
        # ===============================================
        elif origen_topic == TOPIC_ETC:
            descifrado_exitoso = False
            text = ""
            
            # Como el Engine cifra todo el payload, intentamos descifrar 
            # probando las claves activas hasta que una funcione
            for cp_key_id, aes_key in CP_KEYS.items():
                try:
                    f = Fernet(aes_key.encode() if isinstance(aes_key, str) else aes_key)
                    text = f.decrypt(raw_bytes).decode(FORMAT)
                    descifrado_exitoso = True
                    break
                except Exception:
                    continue
            
            if descifrado_exitoso:
                parts = text.split("|")
                if len(parts) >= 4 and parts[0].lower() == "engine":
                    peticion  = parts[1]
                    cp_id     = parts[2]
                    driver_id = parts[3]
                    
                    print(f"\n[CENTRAL-KAFKA] Recibido ENGINE (Descifrado): {text}")
                    attendToEngine(producer, peticion, cp_id, driver_id)
            else:
                print("\n[CENTRAL-KAFKA] AVISO: Recibido paquete ENGINE cifrado que no pudo ser desencriptado.")

def run_kafka_loop(bootstrap):
    """Crea consumer/producer y entra al bucle Kafka (hilo dedicado)."""
    consumer = None
    producer = None
    try:
        consumer = create_consumer(bootstrap)
        producer = create_producer(bootstrap)
        receive_messages(consumer, producer)   # bucle bloqueante
    except Exception as e:
        print(f"[CENTRAL] Hilo Kafka terminado con error: {e}")
    finally:
        try:
            if consumer is not None:
                consumer.close()
        except Exception:
            pass
        try:
            if producer is not None:
                producer.flush()
                producer.close()
        except Exception:
            pass    
    

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
    global SERVER, ADDR, CPS, CPS_IDX, REGISTRY_HOST, EVW_URL

    SERVER = input("Introduce la IP del servidor central: ").strip()
    ADDR = (SERVER, PORT)

    registry_ip = input(f"Introduce la IP del EV_Registry [{REGISTRY_HOST}]: ").strip()
    if registry_ip:
        REGISTRY_HOST = registry_ip

    evw_url = input("Introduce la URL de EV_W (ej. http://172.21.42.5:6060) [dejar vacío si no hay]: ").strip()
    if evw_url:
        EVW_URL = evw_url

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