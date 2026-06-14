import socket
import sys
import time
import requests
import jwt
from cryptography.fernet import Fernet
import os
import threading

HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"


SECRET_TOKEN = "evregistry2425"

socket_activo = None
token_actual = None

def cargar_clave_aes(id_cp):
    ruta = f"claves/{id_cp}.key"
    try:
        with open(ruta, 'rb') as f:
            return f.read()
    except Exception as e:
        print(f"[ERROR] No se pudo leer la clave AES para CP {id_cp}: {e}")
        return None

def monitor_token_renovable(id_cp, ip_registry="localhost", puerto_registry=9100, localizacion="desconocida"):
    global token_actual, socket_activo
    while True:
        token = obtener_token(id_cp, ip_registry, puerto_registry, localizacion)
        if token:
            token_actual = token
            #print("[TOKEN] Token renovado correctamente.")
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
        clave=f.read()
    return clave


def generar_token(id_cp):
    tiempo_actual = int(time.time())
    payload = {
        "id": id_cp,
        "exp": tiempo_actual + 300
    }
    return jwt.encode(payload, SECRET_TOKEN, algorithm="HS256")




def obtener_token(id_cp, ip_registry="localhost", puerto_registry=9100 ,localizacion="desconocida"):
    url = f"http://{ip_registry}:{puerto_registry}/token"
    try:
        response = requests.post(url, json={"id": id_cp}, timeout=5)
        if response.status_code == 200:
            return response.json()["token"]
        elif response.status_code == 403:
            # CP no registrado → registrar automáticamente
            print("[REGISTRY] CP no registrado, intentando registro automático...")
            reg = requests.put(
                f"http://{ip_registry}:{puerto_registry}/register",
                json={"id": id_cp, "location": localizacion},
                timeout=5
            )
            if reg.status_code == 201:
                data = reg.json()
                print("[REGISTRY] CP registrado correctamente.")
                # Guardar la clave AES que devuelve el registry
                os.makedirs("claves", exist_ok=True)
                with open(f"claves/{id_cp}.key", "wb") as f:
                    f.write(data["aes_key"].encode())
                return data["token"]  
            else:
                print(f"[REGISTRY] Error al registrar: {reg.text}")
        else:
            print(f"[TOKEN] Error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"[ERROR] No se pudo obtener token: {e}")
    return None

#Mensaje que mandara al servidor 
def send(msg, client_socket, fernet):
    if fernet is not None:
        message = fernet.encrypt(msg.encode(FORMAT))
    # Si nos pasan None, lo enviamos en texto claro normal
    else:
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
        self.clave_aes = None

        
    def autenticar_registry(self):
                
        print("[REGISTRY] Solicitando token de autenticación...")
        self.token = obtener_token(self.ID, localizacion=self.LOC)
        if not self.token:
            print("[REGISTRY] No se pudo obtener token")
            return False


        print("[REGISTRY] Token recibido")
        clave = cargar_clave_aes(self.ID)
        if not clave:
            print("[REGISTRY] No se encontró clave AES, generando nueva...")
            clave = crear_y_guardar_clave_aes(self.ID)
        self.fernet = Fernet(clave)


        hilo_token = threading.Thread(
        target=monitor_token_renovable,
        args=(self.ID,"localhost", 9100, self.LOC),
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
        try:
            self.sock.connect(self.addr)
            print(f"[DEBUG] Token a enviar: {self.token}")

            msg=f"monitor|PETICION|{self.ID}|IDLE"
            msg_length = str(len(msg.encode(FORMAT)))
            self.sock.send(msg_length.encode(FORMAT).ljust(HEADER))
            self.sock.send(msg.encode(FORMAT))
            
            response=self.sock.recv(2048).decode(FORMAT).strip()
            #mensaje de confirmacion de central
            print(f"[DEBUG] Respuesta recibida: '{response}'")
            
            if response == "REGISTRADO":
                print("CP ya registrado en Central.")
            elif response == "DESCONOCIDO":
                print("CP nuevo para la Central.")
            else:
                print(f"Respuesta inesperada: {response}")
                return

            print(f"[DEBUG] Token a enviar: {self.token}")
            # 2. Enviar autenticación en claro
            msg_auth = f"monitor|AUTENTIFICACION|{self.ID}|{self.LOC}|{self.token}"
            send(msg_auth, self.sock, self.fernet)
                
            respuesta = self.sock.recv(1024).decode(FORMAT)
            parts = respuesta.split("|")
                
            if parts[0] == "central" and parts[1] == "OK":
                if len(parts) > 2:
                    clave_str = parts[2]
                    self.clave_aes = clave_str # ¡AÑADIDO! Nos guardamos la clave
                        
                    print(f"[MONITOR] Clave AES recibida: {clave_str}")
                    self.fernet = Fernet(clave_str.encode() if isinstance(clave_str, str) else clave_str)
                    print("[MONITOR] Cifrado con Fernet configurado exitosamente.")
                    return True
            else: 
                print(f"[MONITOR] Autenticación fallida: {respuesta}")
                    
        except Exception as e:
            print(f"[MONITOR] Error de conexión: {e}")
        
    #verifica el estado de engine
    def estado(self):  #cliente
        print("[MONITOR] Hilo de supervisión iniciado.")
        client_engine= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_engine.settimeout(8)
        conectado = False
        intentos = 0
        
        # Bucle de reconexión: intentará conectar al Engine hasta 10 veces
        while not conectado and intentos < 10:
            try:
                client_engine.connect(self.addr_engine)
                conectado = True
                print("[MONITOR] Conectado al Engine local correctamente.")
            except (ConnectionRefusedError, socket.timeout):
                print(f"[MONITOR] Esperando a que el Engine arranque... (Intento {intentos+1}/10)")
                time.sleep(2)
                intentos += 1
        if not conectado:
            print("[MONITOR] No se pudo conectar al Engine tras varios intentos. Abortando.")
            return
        
        try:
            while True:
                    
                if self.clave_aes:
                    msg_stat = f"monitor|{self.ID}|ok|{self.clave_aes}"
                else:
                    msg_stat = f"monitor|{self.ID}|ok"
                    
                msg_bytes = msg_stat.encode(FORMAT)
                msg_length = str(len(msg_bytes)).encode(FORMAT)
                msg_length += b' ' * (HEADER - len(msg_length))
                client_engine.send(msg_length)
                client_engine.send(msg_bytes)
                status=client_engine.recv(2048).decode(FORMAT)
                time.sleep(1)
                      
                #el engine activa el boton KO o no funciona
                if(status == "ENGINE|ERROR"):
                          
                    msg_stat=f"monitor|ESTADO|{self.ID}|ERROR"
                    send(msg_stat, self.sock, self.fernet)
                          
                    print("Averia reportada")
                    break
                        #El engine esta en uso
                elif (status=="ENGINE|CHARGING"):
                          
                    msg_stat=f"monitor|ESTADO|{self.ID}|CHARGING"
                    send(msg_stat, self.sock, self.fernet)
                        #El engine no envia mas mensajes por lo tanto esta cerrado
                elif not status:
                    print("Se cerro la conexion")
                    msg_stat=f"monitor|ESTADO|{self.ID}|ERROR"
                    send(msg_stat, self.sock, self.fernet)
                          #El estado funciona perfectamente y no esta en uso
                else:
                    msg_stat=f"monitor|ESTADO|{self.ID}|IDLE"
                    send(msg_stat, self.sock, self.fernet)
                        
        #Si el monitor no logra conectarse al engine se interpretara que el engine esta desconectado
        except socket.timeout:
            print("No se pudo conectar al Engine.")
            msg_stat=f"monitor|ESTADO|{self.ID}|OFFLINE"
            send(msg_stat, self.sock, self.fernet)
                 
        except ConnectionRefusedError:
            print("El servidor no está disponible.")
                
        except (ConnectionResetError, BrokenPipeError):
            print("[ERROR] Conexión con engine cerrada.")
            
        finally:
            print("[DEBUG] Conexión con central cerrada")
            client_engine.close()
    
