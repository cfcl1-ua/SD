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
    #cambiar esto ya que es solo para central y no para engine
    client_socket.send(send_length)
    client_socket.send(message)

#clase monitor
class Monitor:
    #variables de entrada
    def __init__(self, SERVER, PORT, ENGINE, ENGINE_PORT, ID. LOC):
        self.SERVER = SERVER
        self.PORT = PORT
        self.ENGINE = ENGINE
        self.ENGINE_PORT = ENGINE_PORT
        self.ID = ID
        self.LOC=LOC
        self.addr=(SERVER, PORT)
        self.addr_engine=(ENGINE, ENGINE_PORT)
        
        
    #conecta con central
    def conectar_central(self): 
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(self.addr)
        
        msg=f"monitor|IDLE|{self.ID}"
        send(msg, client)
        
        response=client.recv(2048).decode(FORMAT)
        #mensaje de confirmacion de central
        print(response)
        if int(response) == 1:
            print(f"Autentificacion correcta")
            response=client.recv(HEADER).decode(FORMAT)
            msg_length = int(msg_length)
            msg_received = conn.recv(msg_length).decode(FORMAT)
            if msg_received == "Patata":
                msg=f"monitor|AUTENTIFICACION|{self.ID}|{self.LOC}"
                send(msg, client)
                return True
            elif msg_received=="naranja":
                return True
            else:
                return False
        else:
            print(f"Error al conectar con central")
            client.close()
            return False
    #verifica el estado de engine
    def estado(self):  #cliente
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(self.addr)
        client_engine= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_engine.connect(self.addr_engine)
        print (f"Conexion establecida con engine")
        while True:
          try:
              msg_stat="ok"
              send(msg_stat, client_engine)
              status=client_engine.recv(2048).decode(FORMAT)
              time.sleep(1)
                #si el server te dice que ko
              if(status == "0"):
                   # send(msg_stat, client)
                  msg_stat=f"monitor|ERROR|{self.ID}"
                  send(msg_stat, client)
                  print("Averia reportada")
                  time.sleep(2)
                  client.close()
                  break
              else:
                  msg_stat=f"monitor|IDLE|{self.ID}"
                  send(msg_stat, client)
                    
          except BrokenPipeError:
              print(f"se perdio la conexion")
              msg_stat=f"monitor|ERROR|{self.ID}"
              send(msg_stat, client)
              print("Averia reportada a central")
              break
          except BrokenPipeError:
              print(f"se perdio la conexion")
              msg_stat=f"monitor|ERROR|{self.ID}"
              send(msg_stat, client)
              print("Averia reportada a central")
              break
            
    