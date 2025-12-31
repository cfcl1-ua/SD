import socket
import sys
import time

HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"


SECRET_TOKEN = "1"


token_actual = None

def cargar_clave_aes(id_cp):
    ruta = f"claves/{id_cp}.key"
    try:
        with open(ruta, 'rb') as f:
            clave = f.read()
            return Fernet(clave)
    except Exception as e:
        print(f"[ERROR] No se pudo leer la clave AES para CP {id_cp}: {e}")
        return None

def monitor_token_renovable(id_cp, ip_registry="localhost", puerto_registry=9100):
    global token_actual, socket_activo
    while True:
        token = obtener_token(id_cp, ip_registry, puerto_registry)
            if token:
                token_actual = token
                print("[TOKEN] Token renovado correctamente.")
                try:
                    if socket_activo:
                        socket_activo.sendall(f"TOKEN#{token_actual}".encode())
                        print("[TOKEN] Nuevo token notificado a central.")
                except Exception as e:
                    print(f"[ERROR] No se pudo notificar nuevo token a central: {e}")
        else:
            print("[TOKEN] Fallo al renovar token.")
        time.sleep(60)




def crear_y_guardar_clave_aes(id_cp):
    ruta = f"claves/{id_cp}.key"
    os.makedirs("claves", exist_ok=True)
    if not os.path.exists(ruta):
        clave = Fernet.generate_key()
        with open(ruta, 'wb') as f:
            f.write(clave)
    with open(ruta, 'rb') as f:
        return f.read()


def generar_token(id_cp):
    tiempo_actual = int(time.time())
    payload = {
        "id": id_cp,
        "exp": tiempo_actual + 60
    }
    return jwt.encode(payload, SECRET_TOKEN, algorithm="HS256")




def obtener_token(id_cp, ip_registry="localhost", puerto_registry=9100):
    url = f"https://{ip_registry}:{puerto_registry}/token"
    try:
        response = requests.post(url, json={"id": id_cp}, verify=False, timeout=5)
        if response.status_code == 200:
            return response.json()["token"]
        else:
            print(f"[TOKEN] Error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"[ERROR] No se pudo obtener token: {e}")
    return None

#Mensaje que mandara al servidor 
def send(msg, client_socket):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length)) 
    client_socket.send(send_length)
    client_socket.send(message)

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
        self.token = None
        self.fernet = None
        
        
    
        
    def autenticar_registry(self):
                
        print("[REGISTRY] Solicitando token de autenticación...")
        self.token = obtener_token(self.ID)
        if not self.token:
            print("[REGISTRY] No se pudo obtener token")
            return False


        print("[REGISTRY] Token recibido")
        self.fernet = cargar_clave_aes(self.ID)
        if not self.fernet:
            clave = crear_y_guardar_clave_aes(self.ID)
            self.fernet = Fernet(clave)


        hilo_token = threading.Thread(
        target=monitor_token_renovable,
        args=(self.ID,),
        daemon=True
        )
        hilo_token.start()
        if not self.fernet:
            print("[REGISTRY] No se encontró clave AES, generando nueva...")
            clave = Fernet.generate_key()
            self.fernet = Fernet(clave)
            try:
                import os
                os.makedirs("claves", exist_ok=True)
                with open(f"claves/{self.ID}.key", "wb") as f:
                    f.write(clave)
            except Exception as e:
                print(f"[ERROR] No se pudo guardar la clave AES: {e}")
                return False
        return True
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
            print("El servidor central está lleno. Espera un momento e inténtalo de nuevo.")
            self.sock.close()
            return False
        if int(response) == 1:
            print("conectado con central")
            
            #respuesta 
            msg_length=self.sock.recv(HEADER).decode(FORMAT)
            #El CP se registra a la base de datos o ya esta registrado
            if msg_length == "DESCONOCIDO":
                msg=f"monitor|AUTENTIFICACION|{self.ID}|{self.LOC}|{self.token}|{self.fernet}"
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
                
                msg_stat=f"ENGINE|{self.ID}|ok"
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
            msg_stat=f"monitor|ESTADO|{self.ID}|OFFLINE"
            send(msg_stat, self.sock)
             
        except ConnectionRefusedError:
            print("El servidor no está disponible.")
            
        except (ConnectionResetError, BrokenPipeError):
            print("[ERROR] Conexión con engine cerrada.")
        
        finally:
            print("[DEBUG] Conexión con central cerrada")
            client_engine.close()
    