import socket
from kafka import KafkaProducer, KafkaConsumer
import threading
import sys
from ChargingPoint import ChargingPoint
import time
import json

HEADER = 64
PORT = 5050
SERVER = "localhost"
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 5
PRICE_PER_KWH = 0.28

DB_FILE = "db.json"
CPS = []
CPS_IDX = []
CUSTOMER_IDX = []
CONEX_ACTIVAS = 0

TOPIC_DTC = "driver-to-central"
TOPIC_ETC = "engine-to-central"

# ---------------------------------------------------------
#   JSON DATABASE
# ---------------------------------------------------------
def loadDB():
    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        return {"clientes": [], "cps": []}

def saveDB(db):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=4)

def searchCustomer(id_cliente):
    db = loadDB()
    return id_cliente in db["clientes"]

def addCustomer(id_cliente):
    db = loadDB()
    if id_cliente in db["clientes"]:
        return False
    db["clientes"].append(id_cliente)
    saveDB(db)
    return True

def cargarClientes():
    db = loadDB()
    return db["clientes"]

def cargarCPs():
    db = loadDB()
    cps = db["cps"]
    ids = [cp["id"] for cp in cps]
    return cps, ids

def insertToCPsBD(id_cp, loc_cp, estado_cp):
    if not id_cp or not loc_cp or not estado_cp:
        return False
    db = loadDB()
    for cp in db["cps"]:
        if cp["id"] == id_cp:
            return False
    db["cps"].append({
        "id": id_cp,
        "loc": loc_cp,
        "estado": estado_cp.upper()
    })
    saveDB(db)
    return True

def updateStatusCP(id_cp, estado):
    db = loadDB()
    for cp in db["cps"]:
        if cp["id"] == id_cp:
            cp["estado"] = estado.upper()
            saveDB(db)
            return True
    return False

def attendToMonitor(peticion, id_cp, var):
    """
    peticion: 'AUTENTIFICACION' | 'ESTADO'
    """
    if peticion == "AUTENTIFICACION":
        # ¿ya está en memoria?
        if id_cp in CPS_IDX:
            return "central|ERROR"

        if not insertToCPsBD(id_cp, var, "IDLE"):
            return "central|ERROR"

        CPS_IDX.append(id_cp)
        return "central|OK"

    elif peticion == "ESTADO":
        if id_cp not in CPS_IDX:
            return "central|ERROR"

        if not updateStatusCP(id_cp, var):
            return "central|ERROR"

        return "central|OK"

    else:
        if id_cp in CPS_IDX:
            return "REGISTRADO"
        return "DESCONOCIDO"

# ---------------------------------------------------------
#   DRIVER COMMUNICATION
# ---------------------------------------------------------
def topics_id(id):
    return f"central-to-consumer-{id}"

def replyToDriver(producer, respuesta, cp_id, driver_id):
    if cp_id is None:
        cp_id = ""
    payload = f"central|{respuesta}|{cp_id}|{driver_id}"
    topic_resp = topics_id(driver_id)
    try:
        producer.send(topic_resp, payload)
        producer.flush(1)
    except:
        pass

def replyToEngine(producer, peticion, cp_id, driver_id):
    topic_resp = topics_id(cp_id)
    payload = f"central|{peticion}|{cp_id}|{driver_id}"
    producer.send(topic_resp, payload)
    producer.flush(1)

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

# ---------------------------------------------------------
#   ENGINE RESPONSES
# ---------------------------------------------------------
def attendToEngine(producer, respuesta, cp_id, driver_id):
    if respuesta.isdigit():
        segundos = int(respuesta)
        horas = segundos / 3600
        precio = horas * PRICE_PER_KWH
        mensaje = f"FIN|TIEMPO:{segundos}|PRECIO:{precio:.2f}"
        replyToDriver(producer, mensaje, cp_id, driver_id)
        return
    else:
        mensaje = f"ESTADO|{respuesta}"
        replyToDriver(producer, mensaje, cp_id, driver_id)
        return

# ---------------------------------------------------------
#   KAFKA LOOP
# ---------------------------------------------------------
def create_producer(bootstrap):
    return KafkaProducer(
        bootstrap_servers=[bootstrap],
        value_serializer=lambda s: s.encode(FORMAT),
        linger_ms=10,
    )

def create_consumer(bootstrap):
    return KafkaConsumer(
        TOPIC_DTC, TOPIC_ETC,
        bootstrap_servers=[bootstrap],
        value_deserializer=lambda m: m.decode(FORMAT),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="central-group",
        client_id="central-1",
    )

def receive_messages(consumer, producer):
    for msg in consumer:
        text = msg.value or ""
        parts = text.split("|")
        remitente = parts[0].lower()
        peticion = parts[1]
        cp_id = parts[2]
        driver_id = parts[3]

        if remitente == "driver":
            attendToDriver(peticion, cp_id, driver_id, producer)
        elif remitente == "engine":
            attendToEngine(producer, peticion, cp_id, driver_id)


def run_kafka_loop(bootstrap):
    consumer = create_consumer(bootstrap)
    producer = create_producer(bootstrap)
    receive_messages(consumer, producer)

# ---------------------------------------------------------
#   SOCKET SERVER
# ---------------------------------------------------------
def start(server):
    global CONEX_ACTIVAS
    server.listen()
    while True:
        conn, addr = server.accept()

        CONEX_ACTIVAS += 1

        if CONEX_ACTIVAS <= MAX_CONEXIONES:
            conn.send("1".encode(FORMAT))
            print("NUEVA CONEXION DESDE:", addr)
            print("CONEXIONES ACTIVAS:", CONEX_ACTIVAS)
            threading.Thread(target=handle_client, args=(conn, addr)).start()
        else:
            conn.send("OOppsss... DEMASIADAS CONEXIONES".encode(FORMAT))
            print("CONEXION RECHAZADA (LIMITE ALCANZADO):", addr)
            conn.close()


def handle_client(conn, addr):
    global CONEX_ACTIVAS
    print(f"[NUEVA CONEXION] {addr}")

    connected = True
    while connected:
        try:
            # --- Recibir header ---
            msg_length = conn.recv(HEADER).decode(FORMAT)
            if not msg_length:
                continue

            # --- Recibir mensaje ---
            msg = conn.recv(int(msg_length)).decode(FORMAT)

            # --- Fin de conexión ---
            if msg == FIN:
                connected = False
                break

            # --- Procesar mensaje ---
            parts = msg.split("|")

            if parts[0] == "monitor":

                # parts = ["monitor", peticion, id_cp, var]
                peticion = parts[1]
                id_cp    = parts[2]
                var      = parts[3]

                # Delegar TODA la lógica al método original
                respuesta = attendToMonitor(peticion, id_cp, var)

                # Enviar respuesta al monitor
                conn.send(respuesta.encode(FORMAT))

            else:
                conn.send("central|ERROR_ORIGEN".encode(FORMAT))

        except Exception as e:
            print(f"[ERROR handle_client]: {e}")
            break

    conn.close()
    CONEX_ACTIVAS -= 1
    print(f"CONEXION CERRADA: {addr} | ACTIVAS: {CONEX_ACTIVAS}")

# ---------------------------------------------------------
#   MAIN
# ---------------------------------------------------------
def main():
    global SERVER, ADDR, CPS, CPS_IDX

    SERVER = input("Introduce la IP del servidor: ").strip()
    ADDR = (SERVER, PORT)

    CPS, CPS_IDX = cargarCPs()

    bootstrap = SERVER
    threading.Thread(target=run_kafka_loop, args=(bootstrap,), daemon=True).start()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(ADDR)
    start(server)

if __name__ == "__main__":
    main()
