import socket
from kafka import KafkaProducer, KafkaConsumer
import threading
import sys
from ChargingPoint import ChargingPoint

HEADER = 64
PORT = 5050
SERVER = "172.21.42.16" # Poner la ip del pc 
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 2
CLIENTES_FILE = "Clientes.txt"
CPS_FILE = "Cps.txt"
CPs = []
CPs_AUX = []
CPs_IDX = []
CUSTOMER_IDX = []
TOPIC_REQUESTS = "driver-to-central"

def handle_client(conn, addr):
    print(f"[NUEVA CONEXION] {addr} connected.")

    connected = True
    while connected:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == FIN:
                connected = False
            else:
                parts = msg.split("|") # split quita los separadores y convierte el mensaje en una lista 
                if parts[0] == "driver":
                    # El mensaje viene de un driver
                    resp = attendToDriver(parts[1], parts[2]) # peticion, cliente
                elif parts[0] == "monitor":
                    conectado = "1"
                    conn.send(conectado.encode(FORMAT))
                    resp = "OK: atendiendo peticiones del monitor" # LLAMAR A LA FUNCION attendToMonitor
                else:
                    resp = "ERROR: peticion de origen desconocido"
            print(f" He recibido del cliente [{addr}] el mensaje: {msg}")
            #conn.send(f"HOLA CLIENTE: He recibido tu mensaje: {msg} ".encode(FORMAT))
            conn.send(resp.encode(FORMAT))
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
            thread = threading.Thread(target=handle_client, args=(conn, addr))
            thread.start()
            print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
            print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES-CONEX_ACTIVAS)
        else:
            print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
            conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
            conn.close()
            CONEX_ACTUALES = threading.active_count()-1
        
#########################################################

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


def attendToDriver(peticion, id_cliente, id_cp=None):
    p = peticion.strip()

    if p == "REGISTRO":
        if addCustomer(id_cliente):
            return "Usuario registrado"
        else:
            return "Error: Usuario ya registrado"

    elif p == "AUTORIZACION":
        cp_id = id_cp.strip()
        found, cp = searchCP(cp_id)
        if not found:
            return "Charging Point no encontrado"
        else:
            if cp.getStatus() == "IDLE":
                cp.start_supply()
                return "Peticion aceptada"
            else:
                return "Peticion denegada"

    else:  # ESTADO
        cp_id = (id_cp or "").strip()
        found, cp = searchCP(cp_id)

        if not found:
            return "Charging Point no encontrado"
        else:
            return cp.getStatus() 

# Tarea 2a
def registrarCP(fich: str, nuevo_cp: ChargingPoint):
    """
    Registra (da de alta) un nuevo punto de recarga en el fichero.
    - Comprueba si ya existe un CP con el mismo ID.
    - Si no existe, lo añade al final del fichero.
    - Devuelve True si se ha registrado correctamente, False si ya existía.
    """
    cps = cargarCPs(fich)

    # comprobar si ya existe un CP con la misma ID
    for cp in cps:
        if cp.id == nuevo_cp.id:   # adapta el nombre del atributo si es distinto
            print(f"[WARN] Ya existe un CP con ID '{nuevo_cp.id}'. No se registra.")
            return False

    # si no existe, lo añadimos al fichero
    with open(fich, "a", encoding="utf-8") as f:
        f.write(f"{nuevo_cp.id}, {nuevo_cp.location}\n")

    print(f"[INFO] CP '{nuevo_cp.id}' registrado correctamente en {fich}.")
    return True

# Tarea 1
def cargarCPs(fich: str):
    """
    Lee un fichero de puntos de recarga con formato fijo:
        CP01, Aparcamiento Norte
        CP02, Parking Sur

    Devuelve: lista[ChargingPoint]
    """
    cps = []
    with open(fich, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():  # Ignora líneas vacías
                cp_id, location = line.strip().split(", ", 1)
                cps.append(ChargingPoint(cp_id, location))
    return cps

######################### KAFKA ##########################

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
        print("→ Verifica que Kafka está arrancado, el puerto es 9092 y advertised.listeners es accesible.")
        raise

def receive_messages(consumer):
    for msg in consumer:
        text = msg.value or ""
        parts = text.split("|")
        tipo = parts[0].upper() if len(parts) > 0 else ""
        driver_id = parts[1] if len(parts) > 1 else ""
        cp_id = parts[2] if len(parts) > 2 else None

        if tipo == FIN:
            print(f"[CENTRAL] Driver {driver_id} cerró sesión.")
            continue

        print(f"[CENTRAL] Recibido: {text}")
        print(f"  → Tipo: {tipo}, Driver: {driver_id}, CP: {cp_id}")
        # handle_request(tipo, driver_id, cp_id)

######################### MAIN ##########################

def main():
    
    '''
    print("****** EV_Central ******")
    bootstrap = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
    consumer = create_consumer(bootstrap)
    receive_messages(consumer)
    '''
    fich = sys.argv[1] if len(sys.argv) > 1 else CPS_FILE
    CPs_AUX = cargarCPs(fich)
    
    for p in CPs_AUX:
        CPs.append(p)
        CPs_IDX.append(p.getId())
        print(p.get_info()) 

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # opcional pero útil
    server.bind(ADDR)

    print("[STARTING] Servidor inicializándose...")
    start(server)

if __name__=="__main__": main()
