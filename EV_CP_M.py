import socket
import sys
import time

HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"

#Mensaje que mandara al servidor 
def send(msg, client_socket):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    try:    
        client_socket.send(send_length)
        client_socket.send(message)
    except (ConnectionResetError, BrokenPipeError):
        print("[ERROR] Conexión con engine cerrada.")

#clase monitor
class Monitor:
    #variables de entrada
    def __init__(self, SERVER, PORT, ENGINE, ENGINE_PORT, ID, LOC):
        self.SERVER = SERVER
        self.PORT = PORT
        self.ENGINE = ENGINE
        self.ENGINE_PORT = ENGINE_PORT
        self.ID = ID
        self.LOC=LOC
        self.addr=(SERVER, PORT)
        self.addr_engine=(ENGINE, ENGINE_PORT)
        self.sock=None
        
    #conecta con central
    def conectar_central(self):
        print("[DEBUG] Creando socket del Monitor...")
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect(self.addr)
        
        msg=f"monitor|PETICION|{self.ID}|IDLE"
        send(msg, self.sock)
        
        response=self.sock.recv(2048).decode(FORMAT)
        #mensaje de confirmacion de central
        print(f"[DEBUG] Respuesta recibida: '{response}'")
        
        if "DEMASIADAS CONEXIONES" in response.upper():
            print("⚠️ El servidor central está lleno. Espera un momento e inténtalo de nuevo.")
            self.sock.close()
            return False
        if int(response) == 1:
            print("conectado con central")
            
            #respuesta 
            msg_length=self.sock.recv(HEADER).decode(FORMAT)
            #El CP se registra a la base de datos o ya esta registrado
            if msg_length == "DESCONOCIDO":
                msg=f"monitor|AUTENTIFICACION|{self.ID}|{self.LOC}"
                send(msg, self.sock)
                status=self.sock.recv(2048).decode(FORMAT)
                print(status)
                return True
            elif msg_length=="REGISTRADO":
                return True
            else:
                return False
            
        else:
            print("Error al conectar con central")
            self.sock.close()
            return False
        
    #verifica el estado de engine
    def estado(self):  #cliente
        client_engine= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_engine.settimeout(8)
        try:
            client_engine.connect(self.addr_engine)
            print ("Conexion establecida con engine")
            while True:
                
                msg_stat="ok"
                send(msg_stat, client_engine)
                status=client_engine.recv(2048).decode(FORMAT)
                time.sleep(1)
                  
                #el engine activa el boton KO o no funciona
                if(status == "ENGINE|ERROR"):
                      
                    msg_stat=f"monitor|ESTADO|{self.ID}|ERROR"
                    send(msg_stat, self.sock)
                      
                    print("Averia reportada")
                    break
                    #El engine esta en uso
                elif (status=="ENGINE|CHARGING"):
                      
                    msg_stat=f"monitor|ESTADO|{self.ID}|CHARGING"
                    send(msg_stat, self.sock)
                    #El engine no envia mas mensajes por lo tanto esta cerrado
                elif not status:
                    print("Se cerro la conexion")
                    msg_stat=f"monitor|ESTADO|{self.ID}|ERROR"
                    send(msg_stat, self.sock)
                      #El estado funciona perfectamente y no esta en uso
                else:
                    msg_stat=f"monitor|ESTADO|{self.ID}|IDLE"
                    send(msg_stat, self.sock)
        #Si el monitor no logra conectarse al engine se interpretara que el engine esta desconectado
        except socket.timeout:
            print("No se pudo conectar al Engine.")
            msg_stat="monitor|ESTADO|{self.ID}|OFFLINE"
            send(msg_stat, self.sock)
             
        except ConnectionRefusedError:
            print("El servidor no está disponible.")
        
        finally:
            print("[DEBUG] Conexión con engine cerrada, pero la conexión con central sigue activa.")
            client_engine.close()
    