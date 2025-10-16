import socket 
import threading
import sys
from ChargingPoint import ChargingPoint

HEADER = 64
PORT = 5050
SERVER = "172.21.42.4" # Poner la ip del pc 
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 2

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
            print(f" He recibido del cliente [{addr}] el mensaje: {msg}")
            conn.send(f"HOLA CLIENTE: He recibido tu mensaje: {msg} ".encode(FORMAT))
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


######################### MAIN ##########################
    

def main():
    fich = sys.argv[1] if len(sys.argv) > 1 else "Cps.txt"
    CPs = cargarCPs(fich)

    for p in CPs:
        p.deactivate(self)
        print(p.get_info()) 

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)  # opcional pero útil
    server.bind(ADDR)

    print("[STARTING] Servidor inicializándose...")
    start(server)

if __name__=="__main__": main()
