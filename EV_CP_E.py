import socket 
import threading
import time
from kafka import KafkaProducer, KafkaConsumer
import json

HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"
MAX_CONEXIONES = 3
TOPIC_CENTRAL = "engine-to-central"      

#Al conectarse correctamente empieza a enviar periodicamente el estado al monitor
def handle_client(conn, addr, engine):
    print("MONITOR connected.")

    while True:
        msg_length = conn.recv(HEADER).decode(FORMAT)
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            parts=msg.split("|")
            engine.id=parts[1]
            engine.consumidor()
            stat=parts[2]
            
            #Recibe un primer mensaje de confirmacion
            if stat == "ok":
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
    engine.conexiones -= 1
    
class Engine:
    def __init__(self, server_broker, port_server, puerto):
        self.SERVER = server_broker
        self.PORT_SERVER = port_server
        self.HOST = 'localhost'
        self.PORT = puerto
        self.ADDR = (self.HOST, self.PORT)
        self.ADDR_SERVER = f"{self.SERVER}:{self.PORT_SERVER}"
        self.status = "IDLE"    # OFFLINE, IDLE, CHARGING, ERROR
        self.tiempo=None
        self.conexiones=0
        self.id=None
        self.driver=None
        self.consumer=None
        self.producer = KafkaProducer(
        bootstrap_servers=[self.ADDR_SERVER],
        value_serializer=lambda v: v.encode(FORMAT)
        )
        
        
    def consumidor(self):
        topic=f"central-to-consumer-{self.id}"
        self.consumer = KafkaConsumer(
        topic,
        bootstrap_servers=[self.ADDR_SERVER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='cliente_A',
        value_deserializer=lambda v: v.decode(FORMAT)   
        )
        
    def estado(self):
        #Engine empieza a escuchar para que el monitor pueda conectarse a el
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind(self.ADDR)
        server.listen()
        print(f"[LISTENING] Servidor a la escucha en {self.HOST}")
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
    
    def boton_ko(self):
        print("Boton KO presionado")
        if self.status == "CHARGING":
            time_end=time.time() - self.start_time
            print(f"Engine: Carga detenida. Tiempo: {time_end:.2f} seg")
            start_time=None
            mensaje=(f"engine|{time_end:.2f}|{self.driver}")
            self.producer.send(TOPIC_CENTRAL, value=mensaje)
            self.producer.flush(1)
            time.sleep(2)
            self.consumer.close()
            self.producer.close()
        self.status="ERROR"
        

    #Funcion que actuara como un enchufado para el driver
    def enchufar(self, cp_id, driver_id):
        if cp_id == self.id and self.driver==driver_id and self.status == "IDLE":
            self.driver=driver_id
            self.status="CHARGING"
            self.start_time=time.time()
            print("Engine: Iniciando carga del coche...")
        elif self.status == "ERROR" or self.status == "OFFLINE":
            print("El punto de carga esta fuera de servicio")
        else:
            print("El punto de carga ya esta en uso.")
    
    #Funcion que actuara como desenchufado del driver
    def desenchufar(self, driver_id, cp_id):
        if cp_id==self.id:
            if self.status == "CHARGING" and self.driver == driver_id:
                self.status="IDLE"
                time_end=time.time() - self.start_time
                print(f"Engine: Carga detenida. Tiempo: {time_end:.2f} seg")
                #engine|TIMPO|ID_DRIVER
                mensaje=(f"engine|{time_end:.2f}|{driver_id}")
                self.producer.send(TOPIC_CENTRAL, value=mensaje)
                self.producer.flush(1)
                print("Respesta enviada")
                time.sleep(1)
            elif self.status=="IDLE":
                print("El punto de carga no esta conectado a ningun dispositivo")
            else:
                print("No esta conectado")
                
        
    def menu_driver(self):
        print("ID del driver:\n")
        driver=input()
        self.driver=driver
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
                case 1: self.enchufar(driver, self.id)
                case 2: self.desenchufar(driver, self.id)
                case 3: self.estado_driver(driver, self.id)
                case 4: break 
        
        
    def opciones(self, opc):
        match int(opc):
            case 1: self.menu_driver()
            case 2: self.boton_ko()
            case 3: self.desenchufar(self.driver, self.id)
            case _: print("Mensaje no  valido")
            
    def menu(self):
        while True:
            self.driver=None
            print("<<MENU CHARGING POINT>>")
            print("Elige una de las opciones:")
            #Se introduce la id del driver para realizar las funciones
            print("1. Peticion manualmente")
            #Boton ko
            print("2. Boton KO")
            #Cortar el grifo del driver
            print("3. Terminar suministro")
            #Sale del CP
            print("4. Salir")
            opc=input("Elija una opcion: ")
            try:
                if int(opc) == 4:
                    print("Saliendo del programa")
                    break
                else:
                    self.opciones(opc)
            except ValueError:
                print("Entrada inválida. Por favor ingresa un número.")


    #Envia el estado a central
    def estado_driver(self, driver_id, cp_id):
        #engine|ESTADO|DRIVER_ID
        if cp_id==self.id:
            mensaje=f"engine|{self.status}|{driver_id}|{cp_id}"
            self.producer.send(TOPIC_CENTRAL, value=mensaje)
            self.producer.flush(1)
            

    # Funcion que sera la encargada de satisfacer los servicios enviados por la central desde engine
    def servicios(self):
        try:
            while self.consumer is None:
                time.sleep(0.1)
            for message in self.consumer:
                text = message.value or ""
                parts = text.split("|")
                peticion = parts[1]
                cp_id     = parts[2]
                driver_id = parts[3]
                self.driver=parts[3]
                print("[Mensaje recibido de central]")
                print(f"[Mensaje del driver: {text}]")
                #Me dice de enchufar central
                if peticion == "AUTORIZACION":
                    self.enchufar(cp_id, driver_id)
                #Me dice que lo desenchufe del driver 
                elif peticion == "FIN":
                    self.desenchufar(driver_id, cp_id)
            
                elif peticion == "ESTADO":
                    self.estado_driver(driver_id, cp_id)
                elif peticion == "APAGAR":
                    self.boton_ko
                else:
                    print("[Peticion no identificada]")
        except AssertionError:
            print("El engine se desactivo")
    
                
                
                
                
    

            
        
        