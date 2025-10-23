import socket 
import threading
from confluent_kafka import Consumer, KafkaException, KafkaError
from pymongo import MongoClient
from json import loads


HEADER = 64
PORT = 5050
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
def handle_client(conn, addr, connected):
    print(f"[NUEVA CONEXION] {addr} connected.")

    while True:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            if msg == "ok":
                if connected == True:
                    print("h")
                    conn.send("1".encode(FORMAT))
                else:
                    conn.send("0".encode(FORMAT))
                    break
    print("Se rompio la conexion")
    conn.close()
    
class Engine:
    def __init__(self, SERVER, PORT_SERVER):
        self.SERVER = SERVER
        self.PORT_SERVER = PORT_SERVER
        self.HOST = 'localhost'
        self.PORT = 5050
        self.ADDR = (self.HOST, self.PORT)
        self.ADDR_SERVER = (SERVER, PORT_SERVER)
        self.connected = True
        
       # consumer = Consumer(consumer_config)
        # Subscribe to the 'numtest' topic
        #consumer.subscribe(['numtest'])
        
    def estado(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(self.ADDR)
        server.listen()
        print(f"[LISTENING] Servidor a la escucha en {SERVER}")
        CONEX_ACTIVAS = threading.active_count()-1
        print(CONEX_ACTIVAS)
        while self.connected:
            server.settimeout(10.0)
            conn, addr = server.accept()
            CONEX_ACTIVAS = threading.active_count()
            if (CONEX_ACTIVAS <= MAX_CONEXIONES): 
                thread = threading.Thread(target=handle_client, args=(conn, addr, self.connected))
                thread.start()
                print(f"[CONEXIONES ACTIVAS] {CONEX_ACTIVAS}")
                print("CONEXIONES RESTANTES PARA CERRAR EL SERVICIO", MAX_CONEXIONES-CONEX_ACTIVAS)
            else:
                print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
                conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
                conn.close()
                CONEX_ACTUALES = threading.active_count()-1
    
    def boton_ko(self):
        self.connected = False
        
    
        
        