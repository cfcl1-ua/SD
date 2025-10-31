import socket 
import threading
import time
from kafka import KafkaProducer, KafkaConsumer
from json import loads
import json

HEADER = 64
PORT = 8080
SERVER = socket.gethostbyname(socket.gethostname())
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 3
TOPIC_ENGINE = "central-to-engine"
TOPIC_CENTRAL = "engine-to-central"


#Al conectarse correctamente empieza a enviar periodicamente el estado al monitor
def handle_client(conn, addr, engine):
    print(f"MONITOR connected.")

    while True:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            #Recibe un primer mensaje de confirmacion
            if msg == "ok":
                #Se avisa al monitor del estado y si es un error el engine lo reporta y se desconecta del monitor
                if engine.status!="ERROR":
                    conn.send(f"ENGINE|{engine.status}".encode(FORMAT))
                else:
                    engine.status="ERROR"
                    conn.send(f"ENGINE|{engine.status}".encode(FORMAT))
                    break
    print("Se rompio la conexion")
    conn.shutdown(socket.SHUT_RDWR)
    conn.close()
    engine.conexiones_activas -= 1
    
class Engine:
    def __init__(self, SERVER, PORT_SERVER):
        self.SERVER = SERVER
        self.PORT_SERVER = PORT_SERVER
        self.HOST = 'localhost'
        self.PORT = PORT
        self.ADDR = (self.HOST, self.PORT)
        self.ADDR_SERVER = f"{self.SERVER}:{self.PORT_SERVER}"
        self.status = "IDLE"    # OFFLINE, IDLE, CHARGING, ERROR
        self.tiempo=None
        self.conexiones=0
        
        self.consumer = KafkaConsumer(
        TOPIC_ENGINE,
        bootstrap_servers=[self.ADDR_SERVER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='grupo-consumidor',
        value_deserializer=lambda v: v.decode(FORMAT)           # Enable auto-commit of offsets
        )
        
        self.producer = KafkaProducer(
        bootstrap_servers=[self.ADDR_SERVER],
        value_serializer=lambda v: v.encode(FORMAT)
        )
        
    def estado(self):
        #Engine empieza a escuchar para que el monitor pueda conectarse a el
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(self.ADDR)
        server.listen()
        print(f"[LISTENING] Servidor a la escucha en {SERVER}")
        server.settimeout(10.0)
        while True:
            try:
                conn, addr = server.accept()
            except TimeoutError:
                continue
            except Exception as e:
                print(f"[ERROR] {e}")
                continue
            
            if (self.conexiones <= MAX_CONEXIONES):
                self.conexiones += 1
                thread = threading.Thread(target=handle_client, args=(conn, addr, self))
                thread.start()
                print("Se ha conectado con el monitor")
            else:
                print("OOppsss... DEMASIADAS CONEXIONES. ESPERANDO A QUE ALGUIEN SE VAYA")
                conn.send("OOppsss... DEMASIADAS CONEXIONES. Tendrás que esperar a que alguien se vaya".encode(FORMAT))
                conn.close()
                CONEX_ACTUALES = threading.active_count()-1
    
    def boton_ko(self):
        print("Boton KO presionado")
        if self.status == "CHARGING":
            time_end=time.time() - self.start_time
            print(f"Engine: Carga detenida. Tiempo: {time_end:.2f} seg")
            start_time=None
            self.producer.send(TOPIC_CENTRAL, value=time_end)
            time.sleep(2)
            self.consumer.close()
            self.producer.close()
        self.status="ERROR"
    #Funcion que actuara como un enchufado para el driver
    def enchufar(self):
        if self.status == "IDLE":
            self.status="CHARGING"
            self.start_time=time.time()
            print("Engine: Iniciando carga del coche...")
        elif self.status == "ERROR" or self.status == "OFFLINE":
            print("El punto de carga esta fuera de servicio")
        else:
            print("El punto de carga ya esta en uso.")
    
    #Funcion que actuara como desenchufado del driver
    def desenchufar(self, driver_id):
        if self.status == "CHARGING":
            self.status="IDLE"
            time_end=time.time() - self.start_time
            print(f"Engine: Carga detenida. Tiempo: {time_end:.2f} seg")
            #engine|TIMPO|ID_DRIVER
            mensaje=("engine|{time_end:.2f}|{driver_id}")
            self.producer.send(TOPIC_CENTRAL, value=mensaje)
            print("Respesta enviada")
            time.sleep(1)
    
    def menu_driver(self):
        print("ID del driver:\n")
        driver=input()
        while True:
            print("<<MENU CHARGING POINT>>")
            print("Elige una de las opciones:")
            print("1. Enchufar coche")
            #Boton ko
            print("2. Desenchufar coche")
            print("3. Estado del engine")
            print("4. Volver al menu principal")
            opc=input()
            
            match int(opc):
                case 1: self.enchufar()
                case 2: self.desenchufar(driver)
                case 3: self.estado_driver(driver)
                case 4: break 
        
        
    def opciones(self, opc):
        match int(opc):
            case 1: self.menu_driver()
            case 2: self.boton_ko()
            case _: print("Mensaje no  valido")
            
    def menu(self):
        while True:
            print("<<MENU CHARGING POINT>>")
            print("Elige una de las opciones:")
            #Se introduce la id del driver para realizar las funciones
            print("1. Peticion manualmente")
            #Boton ko
            print("2. Boton KO")
            #Sale del CP
            print("3. Salir")
            opc=input()
            if int(opc) == 3:
                print("Saliendo del programa")
                break
            else:
                self.opciones(opc)


    #Envia el estado a central
    def estado_driver(self, driver_id):
        #engine|ESTADO|DRIVER_ID
        mensaje=f"engine|{self.status}|{driver_id}"
        self.producer.send(TOPIC_CENTRAL, value=mensaje)
        

    # Funcion que sera la encargada de satisfacer los servicios enviados por la central desde engine
    def servicios(self):
        for message in self.consumer:
            text = message.value or ""
            parts = text.split("|")
            peticion = parts[1]
            cp_id     = parts[2]
            driver_id = parts[3]
            print("[Mensaje recibido de central]")
            print(f"[Mensaje del driver: {driver_id}]")
            #Me dice de enchufar central
            if peticion == "AUTORIZACION":
                self.enchufar()
            #Me dice que lo desenchufe del driver 
            elif peticion == "FIN":
                self.desenchufar(driver_id)
            '''
            elif peticion == "ESTADO":
                self.estado_driver(driver_id)
            else:
            '''    
    
                
                
                
                
    

            
        
        