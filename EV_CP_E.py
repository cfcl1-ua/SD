import socket 
import threading
from confluent_kafka import Consumer, KafkaException, KafkaError
from pymongo import MongoClient
from json import loads


HEADER = 64
PORT = 8080
SERVER = socket.gethostbyname(socket.gethostname())
ADDR = (SERVER, PORT)
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 3
'''
consumer_config = {
    'bootstrap.servers': 'localhost:9092',   # Kafka broker
    'group.id': 'my-group',                   # Consumer group id
    'auto.offset.reset': 'earliest',          # Start reading at the earliest offset
    'enable.auto.commit': True                # Enable auto-commit of offsets
}
'''
def handle_client(conn, addr, connected, status):
    print(f"[NUEVA CONEXION] {addr} connected.")

    while True:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == "ok":
                if connected.is_set():
                    conn.send("ENGINE|{status}".encode(FORMAT))
                else:
                    status="ERROR"
                    conn.send("ENGINE|{status}".encode(FORMAT))
                    break
    print("Se rompio la conexion")
    conn.close()
    
class Engine:
    def __init__(self, SERVER, PORT_SERVER):
        self.SERVER = SERVER
        self.PORT_SERVER = PORT_SERVER
        self.HOST = 'localhost'
        self.PORT = PORT
        self.ADDR = (self.HOST, self.PORT)
        self.ADDR_SERVER = (SERVER, PORT_SERVER)
        self.connected = False
        self.status = "IDLE"    # OFFLINE, IDLE, CHARGING, ERROR

        #consumer = Consumer(consumer_config)
        # Subscribe to the 'numtest' topic
        #consumer.subscribe(['numtest'])
        
    def estado(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(self.ADDR)
        server.listen()
        print(f"[LISTENING] Servidor a la escucha en {SERVER}")
        CONEX_ACTIVAS = threading.active_count()-1
        print(CONEX_ACTIVAS)
        self.connected=threading.Event() #lo pongo como evento para que todos los hilos puedan ver el cambio
        self.connected.set()
        server.settimeout(10.0)
        while True:
            try:
                conn, addr = server.accept()
            except TimeoutError:
                continue
            except Exception as e:
                print(f"[ERROR] {e}")
                continue
            
            CONEX_ACTIVAS = threading.active_count()
            if (CONEX_ACTIVAS <= MAX_CONEXIONES): 
                thread = threading.Thread(target=handle_client, args=(conn, addr, self.connected, self.status))
                thread.start()
                print("Se ha conectado con el monitor")
            else:
                print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
                conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
                conn.close()
                CONEX_ACTUALES = threading.active_count()-1
    
    def boton_ko(self):
        self.status="ERROR"
        self.connected.clear()
        
    def menu_manual(self):
        #preguntar comohacerlo manualmente
        print("<<MENU MANUAL>>")
        print("ID del driver:")
        input()
    
    def enchufar(self):
        self.status="CHARGING"
    def opciones(self, opc):
        match int(opc):
            case 1: self.enchufar()
            case 3: self.boton_ko()
            
    def menu(self):
        print("<<MENU CHARGING POINT>>")
        print("Elige una de las opciones:")
        #Se introduce la id del driver para realizar las funciones
        print("1. Usar Manualmente")
        #Aqui espera las peticiones del driver mediante broker
        print("2. Usar la aplicacion")
        #boton para parar el engine
        print("3. Boton KO")
        opc=input()
        self.opciones(opc)
    
            
                
        
        