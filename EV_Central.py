import socket
from kafka import KafkaProducer, KafkaConsumer
import threading
import sys
from ChargingPoint import ChargingPoint
#from Interfaz import interfaz
import time

HEADER = 64
PORT = 5050
SERVER = "localhost"
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 64
PRICE_PER_KWH = 0.28  # €/kWh
CLIENTES_FILE = "Clientes.txt"
CPS_FILE = "Cps.txt"
CPS = []
CPS_IDX = []
CUSTOMER_IDX = []
CONEX_ACTIVAS = 0

# ================== TOPICS (alineados con EV_Driver) ==================
TOPIC_DTC = "driver-to-central"              
TOPIC_ETC = "engine-to-central"
   
########################## MONITOR ######################

##########################################################
# ==== DASHBOARD WEB PARA MONITORIZAR LOS PUNTOS DE RECARGA ====
##########################################################


            
def isEmpty(x):
    return x==None or x==""


def updateStatusCP(id_cp, estado):
    """
    Actualiza el estado en Cps.txt.
    Devuelve False si falta dato o si no se encontró el CP; True si actualiza.
    """
    if isEmpty(id_cp) or isEmpty(estado):
        return False

    # Leer todas las líneas
    try:
        with open(CPS_FILE, "r", encoding=FORMAT) as f:
            lines = f.readlines()
    except FileNotFoundError:
        return False

    for i, line in enumerate(lines):
        parts = [p.strip() for p in line.split(",")]
        if len(parts) < 3:
            continue
        if parts[0] == id_cp:
            parts[2] = estado.strip()
            lines[i] = f"{parts[0]}, {parts[1]}, {parts[2]}\n"
            break

    with open(CPS_FILE, "w", encoding=FORMAT) as f:
        f.writelines(lines)
    return True


def insertToCPsBD(id_cp, loc_cp, estado_cp):
    """
    Inserta: id_cp, loc_cp, estado_cp
    Devuelve False si falta dato; True si escribe.
    """
    if isEmpty(id_cp) or isEmpty(loc_cp) or isEmpty(estado_cp):
        return False

    with open(CPS_FILE, "a", encoding=FORMAT) as f:
        f.write(f"{id_cp.strip()}, {loc_cp.strip()}, {estado_cp.strip()}\n")
    return True


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
         
            
########################## DRIVER #######################
# Tarea 2b
def searchCP(cp_id):
    """
    Busca un Charging Point por su ID.
    Devuelve:
        (True, cp)  -> si el CP existe
        (False, None) -> si no se encuentra
    """
    for cp in CPs:
        if cp.getId() == cp_id:
            return True, cp
    return False, None

def replyToEngine(producer, peticion, cp_id, driver_id):
    # Mensaje: central|PETICION|CP|DRIVER
    topic_resp = topics_id(cp_id)
    payload = f"central|{peticion}|{cp_id}|{driver_id}"
    producer.send(topic_resp, payload)
    producer.flush(1)

def replyToDriver(producer, respuesta, cp_id, driver_id):
    """
    Envía SIEMPRE un mensaje al driver con formato:
        central|RESPUESTA|CP_ID|DRIVER_ID
    """
    if cp_id is None:
        cp_id = ""

    payload = f"central|{respuesta}|{cp_id}|{driver_id}"
    topic_resp = topics_id(driver_id)

    try:
        producer.send(topic_resp, payload)
        producer.flush(1)
        print(f"[CENTRAL → DRIVER {driver_id}] {payload}")
    except Exception as e:
        print(f"[ERROR] Fallo enviando respuesta al driver {driver_id}: {e}")


def searchCustomer(id_cliente):
    return id_cliente in CUSTOMER_IDX


def addCustomer(id_cliente):
    if not searchCustomer(id_cliente):  
        CUSTOMER_IDX.append(id_cliente)
        with open(CLIENTES_FILE, "a", encoding="utf-8") as f:
            f.write(id_cliente + "\n")
        return True
    return False


def getCPStatus(cp_id):
    for cp in CPs:
        if cp.getId() == cp_id:
            return cp.estado
    return None

def attendToDriver(peticion, cp_id, driver_id, producer=None):
    """
    Gestiona todas las peticiones del Driver.
    SIEMPRE ENVÍA RESPUESTA AL DRIVER, incluso en caso de error.
    """

    # ===================== AUTENTIFICACIÓN =====================
    if peticion == "AUTENTIFICACION":
        if addCustomer(driver_id):
            print("AUTENTIFICACION OK")
            replyToDriver(producer, "OK", "", driver_id)
            return "central|OK"
        else:
            print("AUTENTIFICACION ERROR (cliente ya existe)")
            replyToDriver(producer, "ERROR:YA_REGISTRADO", "", driver_id)
            return "central|ERROR"


    # ============= AUTORIZACIÓN / ESTADO / FIN ================
    if peticion in ("AUTORIZACION", "ESTADO", "FIN"):

        # --- ERROR: CP NO REGISTRADO EN CENTRAL ---
        if cp_id not in CPS_IDX:
            msg = f"{peticion}:ERROR_CP_DESCONOCIDO"
            print(msg)
            replyToDriver(producer, msg, cp_id, driver_id)
            return "central|ERROR"

        # --- OK: REENVIAR AL ENGINE ---
        print(f"{peticion}: reenviado a Engine (cp={cp_id}, driver={driver_id})")
        if producer is not None:
            replyToEngine(producer, peticion, cp_id, driver_id)

        replyToDriver(producer, f"{peticion}:ENVIADO_AL_ENGINE", cp_id, driver_id)
        return "central|OK"


    # ===================== PETICIÓN NO SOPORTADA =====================
    msg = f"ERROR:PETICION_NO_SOPORTADA={peticion}"
    print(msg)
    replyToDriver(producer, msg, cp_id, driver_id)
    return "central|ERROR"

def cargarClientes(fich):
    """
    Lee un fichero de clientes con formato:
        ID_CLIENTE

    Devuelve: lista[str] con los campos en orden plano:
        [id1, id2, ...]
    """
    clientes = []
    with open(fich, "r", encoding=FORMAT) as f:
        for line in f:
            if line.strip():  # Ignora líneas vacías
                cli_id = line.strip()
                clientes.append(cli_id)
    return clientes

# Tarea 1
def cargarCPs(fich):
    """
    Lee un fichero de puntos de recarga con formato:
        CP01, Aparcamiento Norte, IDLE
        CP02, Parking Sur, OUT OF ORDER

    Devuelve:
        cps -> lista de diccionarios con id, loc, estado
        idx -> lista de identificadores (para CPs_IDX)
    """
    cps = []
    idx = []

    try:
        with open(fich, "r", encoding="utf-8") as f:
            for line in f:
                if line.strip():
                    parts = [p.strip() for p in line.split(",")]
                    cp_id, loc, estado = parts[0], parts[1], parts[2]
                    cps.append({
                        "id": cp_id,
                        "loc": loc,
                        "estado": estado.upper()
                    })
                    idx.append(cp_id)
    except FileNotFoundError:
        print(f"[AVISO] No se encontró el fichero {fich}.")
    except Exception as e:
        print(f"[ERROR] al cargar {fich}: {e}")

    return cps, idx

######################### ENGINE #########################

def attendToEngine(producer,respuesta, cp_id, driver_id):
    """
    Procesa una respuesta que viene del Engine (por Kafka) y la reenvía al driver.
    Formatos esperados:
        - "driver|120|DRIVER_ID"     -> 120 segundos
        - "driver|IDLE|DRIVER_ID"    -> estado del CP

    Siempre reenvía con:
        "central|...|DRIVER_ID"
    usando replyToDriver(producer, driver_id, texto)
    """
    
    # 1) Caso TIEMPO --> si la respuesta es un número entero
    if respuesta.isdigit():
        segundos = int(respuesta)
        horas = segundos / 3600
        precio = horas * PRICE_PER_KWH
        resp = f"precio:.2f"
        replyToDriver(producer, resp, cp_id, driver_id)
        return

    # 2) Caso ESTADO → si no es número, lo tratamos como texto de estado
    replyToDriver(producer, respuesta, cp_id, driver_id)
    return

######################### KAFKA ##########################

def topics_id(id):
    return f"central-to-consumer-{id}"

def create_producer(bootstrap):
    """
    Crea un productor Kafka con la configuración básica.
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap],
            value_serializer=lambda s: s.encode(FORMAT),
            linger_ms=10,
        )
        print(f"[CENTRAL] Productor conectado a {bootstrap}")
        return producer
    except Exception as e:
        print(f"[CENTRAL] No puedo crear el productor en '{bootstrap}': {e}")
        print("--> Verifica que Kafka está arrancado, el puerto es 9092 y advertised.listeners es accesible.")
        raise


def create_consumer(bootstrap):
    try:
        consumer = KafkaConsumer(
            TOPIC_DTC, TOPIC_ETC,
            bootstrap_servers=[bootstrap],
            value_deserializer=lambda m: m.decode(FORMAT),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="central-group",
            client_id="central-1",
        )
        print(f"[CENTRAL] Conectado a {bootstrap}, escuchando '{TOPIC_DTC}'…")
        return consumer
    except Exception as e:
        print(f"[CENTRAL] No puedo conectar con el broker '{bootstrap}': {e}")
        print("--> Verifica que Kafka está arrancado, el puerto es 9092 y advertised.listeners es accesible.")
        raise

def receive_messages(consumer, producer):
    for msg in consumer:
        text = msg.value or ""
        parts = text.split("|")
    
        # driver|PETICION|CP_ID|DRIVER_ID   o   engine|RESPUESTA|CP_ID|DRIVER_ID
        remitente = parts[0].lower()
        peticion  = parts[1]
        cp_id     = parts[2]
        driver_id = parts[3]

        print(f"[CENTRAL] Recibido: {text}")
        print(f"  --> remitente: {remitente}, Driver: {driver_id}, CP: {cp_id}")

        if remitente == "driver":
            # tu attendToDriver ahora recibe también el producer
            attendToDriver(peticion, cp_id, driver_id, producer)

        elif remitente == "engine":
            # el engine habla como "engine|...", pero tu attendToEngine
            # quiere "driver|..." -> lo adaptamos aquí sin cambiar tu lógica
            if peticion == FIN:
                print(f"[CENTRAL] Driver {driver_id} cerró sesión.")
            attendToEngine(producer, peticion, cp_id, driver_id)


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
        
        
######################### SOCKET ##########################
        
def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")

    connected = True
    msg_aux = ""  # Lo usamos para mostrar el mensaje del cliente una vez
    while connected:
        try:
            msg_length = conn.recv(HEADER).decode(FORMAT)
            if msg_length:
                msg_length = int(msg_length)
                msg = conn.recv(msg_length).decode(FORMAT)
                if msg == FIN:
                    connected = False
                else:
                    parts = msg.split("|")  # split quita los separadores y convierte el mensaje en una lista 
                    if parts[0] == "monitor":       
                        resp = attendToMonitor(parts[1], parts[2], parts[3])  # LLAMAR A LA FUNCION attendToMonitor
                        conn.send(resp.encode(FORMAT))
                    else:
                        resp = "ERROR: peticion de origen desconocido"
                if msg != msg_aux:
                    print(f" He recibido del cliente [{addr}] el mensaje: {msg}")
                msg_aux = msg

        except ConnectionResetError:
            print(f"[{addr}] Conexión interrumpida por el cliente (WinError 10054).")
            break  # salimos del bucle si el cliente se desconecta abruptamente

    print("ADIOS. TE ESPERO EN OTRA OCASION")
    conn.close()    

def start(server):
    global CONEX_ACTIVAS
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {SERVER}")
    print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
    while True:
        conn, addr = server.accept()
        CONEX_ACTIVAS += 1
        if (CONEX_ACTIVAS <= MAX_CONEXIONES):
            msg = "1"
            conn.send(msg.encode(FORMAT)) # Enviamos un 1 para informar que nos conectamos correctamente
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()
            print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
            print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES-CONEX_ACTIVAS)
        else:
            print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
            conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
            conn.close()
            #CONEX_ACTUALES = threading.active_count()-1

######################### MAIN ##########################

def main():
    
    print("****** EV_Central ******")
    
    # Pedir la IP al usuario
    SERVER = input("Introduce la IP del servidor: ").strip()
    print(f"[INFO] Servidor configurado en {SERVER}:{PORT}\n")
    ADDR = (SERVER, PORT)
    
    clientes = cargarClientes(CLIENTES_FILE)
    i = 0
    for c in clientes:
        if i%2 == 0:
            CUSTOMER_IDX.append(c)
        i+=1
    
    bootstrap = SERVER
    t_kafka = threading.Thread(target=run_kafka_loop, args=(bootstrap,), daemon=True)
    t_kafka.start()
    
    CPS, CPS_IDX = cargarCPs(CPS_FILE)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # opcional pero útil
    server.bind(ADDR)

    print("[STARTING] Servidor inicializándose...")
    start(server)

if __name__=="__main__": main()
