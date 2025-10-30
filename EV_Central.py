import socket
from kafka import KafkaProducer, KafkaConsumer
import threading
import sys
from ChargingPoint import ChargingPoint

HEADER = 64
PORT = 5050
SERVER = "localhost"
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 2
PRICE_PER_KWH = 0.35  # €/kWh
CLIENTES_FILE = "Clientes.txt"
CPS_FILE = "Cps.txt"
CPs_IDX = []
CUSTOMER_IDX = []

# ================== TOPICS (alineados con EV_Driver) ==================
TOPIC_DTC = "driver-to-central"       # Drivers -> Central
TOPIC_CTD = "central-to-driver"       # Central -> Drivers
TOPIC_ENGINE = "central-to-engine"    # Central -> Engine (si existe un Engine)
TOPIC_REQUESTS = TOPIC_DTC            # Central escucha peticiones del driver
   
########################## MONITOR ######################
            
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
        if id_cp in CPs_IDX:
            return "central|ERROR"

        if not insertToCPsBD(id_cp, var, "IDLE"):
            return "central|ERROR"

        CPs_IDX.append(id_cp)
        return "central|OK"

    elif peticion == "ESTADO":
        if id_cp not in CPs_IDX:
            return "central|ERROR"

        if not updateStatusCP(id_cp, var):
            return "central|ERROR"

        return "central|OK"

    else:
        if id_cp in CPs_IDX:
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
    payload = f"central|{peticion}|{cp_id}|{driver_id}"
    producer.send(TOPIC_ENGINE, payload)
    producer.flush(1)

def replyToDriver(producer, texto):
    # Mensaje: central|RESPUESTA
    payload = f"central|{texto}"
    producer.send(TOPIC_DRIVER, payload)
    producer.flush(1)

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
    - AUTENTIFICACION: lo gestiona Central (Clientes.txt) -> "central|OK"/"central|ERROR".
    - AUTORIZACION y ESTADO: si el CP existe en CPs_IDX, reenvía al Engine; si no, "central|ERROR".
    Devuelve string breve para logs.
    """
    if peticion == "AUTENTIFICACION":
        if addCustomer(driver_id):
            print("AUTENTIFICACION OK")
            return "central|OK"
        else:
            print("AUTENTIFICACION ERROR")
            return "central|ERROR"

    if peticion in ("AUTORIZACION", "ESTADO"):
        if cp_id not in CPs_IDX:
            print(f"{peticion}: ERROR (CP '{cp_id}' no encontrado en Central)")
            return "central|ERROR"
        else:
            print(f"{peticion}: reenviado a Engine (cp={cp_id}, driver={driver_id})")
            if producer is not None:
                replyToEngine(producer, peticion, cp_id, driver_id)
            return "central|OK"

    print(f"Petición no soportada en Central: {peticion}")
    return "central|ERROR"

def cargarClientes(fich):
    """
    Lee un fichero de clientes con formato:
        ID_CLIENTE, otro_dato

    Devuelve: lista[str] con los campos en orden plano:
        [id1, total1, id2, total2, ...]
    """
    clientes = []
    with open(fich, "r", encoding=FORMAT) as f:
        for line in f:
            if line.strip():  # Ignora líneas vacías
                cli_id, total_gastado = line.strip().split(",", 1)
                clientes.append(cli_id.strip())
                clientes.append(total_gastado.strip())
    return clientes

# Tarea 1
def cargarCPs(fich):
    """
    Lee un fichero de puntos de recarga con formato fijo:
        CP01, Aparcamiento Norte
        CP02, Parking Sur

    Devuelve: lista[str] con los identificadores de los CPs.
    """
    cps_idx = []
    with open(fich, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():  # Ignora líneas vacías
                cp_id, _ = line.strip().split(", ", 1)
                cps_idx.append(cp_id.strip())
    return cps_idx

######################### ENGINE #########################

def attendToEngine(producer, msg_txt: str):
    """
    Procesa una respuesta que viene del Engine (por Kafka) y la reenvía al driver.
    Formatos esperados:
        - "driver|120|DRIVER_ID"     -> 120 segundos
        - "driver|IDLE|DRIVER_ID"    -> estado del CP

    Siempre reenvía con:
        "central|...|DRIVER_ID"
    usando replyToDriver(producer, texto)
    """

    parts = msg_txt.split("|")

    # Mensaje mínimo: driver|respuesta|DRIVER_ID
    if len(parts) != 3:
        replyToDriver(producer, "central|ERROR")
        return

    origen = parts[0]
    respuesta = parts[1]
    driver_id = parts[2]

    # 1) Caso TIEMPO --> si la respuesta es un número entero
    if respuesta.isdigit():
        segundos = int(respuesta)
        horas = segundos / 3600
        precio = horas * PRICE_PER_KWH
        resp = f"central|TIEMPO={segundos}s;PRECIO={precio:.2f}€|{driver_id}"
        replyToDriver(producer, resp)
        return

    # 2) Caso ESTADO → si no es número, lo tratamos como texto de estado
    resp = f"central|ESTADO={respuesta}|{driver_id}"
    replyToDriver(producer, resp)
    return

######################### KAFKA ##########################

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
            TOPIC_REQUESTS,
            bootstrap_servers=[bootstrap],
            value_deserializer=lambda m: m.decode(FORMAT),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="central-group",
            client_id="central-1",
        )
        print(f"[CENTRAL] Conectado a {bootstrap}, escuchando '{TOPIC_REQUESTS}'…")
        return consumer
    except Exception as e:
        print(f"[CENTRAL] No puedo conectar con el broker '{bootstrap}': {e}")
        print("--> Verifica que Kafka está arrancado, el puerto es 9092 y advertised.listeners es accesible.")
        raise

def receive_messages(consumer, producer):
    for msg in consumer:
        text = msg.value or ""
        parts = text.split("|")

        if len(parts) == 3:
            # driver|PETICION|DRIVER_ID
            remitente = parts[0]
            peticion  = parts[1]
            driver_id = parts[2]
            cp_id     = ""   # para que exista la variable al imprimir
        elif len(parts) == 4:
            # driver|PETICION|CP_ID|DRIVER_ID   o   engine|RESPUESTA|CP_ID|DRIVER_ID
            remitente = parts[0]
            peticion  = parts[1]
            cp_id     = parts[2]
            driver_id = parts[3]
        else:
            print(f"[CENTRAL] Formato inválido: {text}")
            continue

        if peticion == FIN:
            print(f"[CENTRAL] Driver {driver_id} cerró sesión.")
            continue

        print(f"[CENTRAL] Recibido: {text}")
        print(f"  --> remitente: {remitente}, Driver: {driver_id}, CP: {cp_id}")

        if remitente == "driver":
            # tu attendToDriver ahora recibe también el producer
            attendToDriver(peticion, cp_id, driver_id, producer)

        elif remitente == "engine":
            # el engine habla como "engine|...", pero tu attendToEngine
            # quiere "driver|..." -> lo adaptamos aquí sin cambiar tu lógica
            msg_txt = f"driver|{peticion}|{driver_id}"
            attendToEngine(producer, msg_txt)


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
    server.listen()
    print(f"[LISTENING] Servidor a la escucha en {SERVER}")
    CONEX_ACTIVAS = threading.active_count()-1
    print(CONEX_ACTIVAS)
    while True:
        conn, addr = server.accept()
        CONEX_ACTIVAS = threading.active_count()
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
            CONEX_ACTUALES = threading.active_count()-1

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
    
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else "172.20.243.108:9092"
    t_kafka = threading.Thread(target=run_kafka_loop, args=(bootstrap,), daemon=True)
    t_kafka.start()
    
    CPs_IDX = cargarCPs(CPS_FILE) 

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # opcional pero útil
    server.bind(ADDR)

    print("[STARTING] Servidor inicializándose...")
    start(server)

if __name__=="__main__": main()
