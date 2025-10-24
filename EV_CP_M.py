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
    def __init__(self, SERVER, PORT, ENGINE, ENGINE_PORT, ID):
        self.SERVER = SERVER
        self.PORT = PORT
        self.ENGINE = ENGINE
        self.ENGINE_PORT = ENGINE_PORT
        self.ID = ID
        self.addr=(SERVER, PORT)
        self.addr_engine=(ENGINE, ENGINE_PORT)
        
    #conecta con central
    def conectar_central(self): 
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.connect(self.addr)
        
        msg=f"monitor|ok|{self.ID}"
        send(msg, client)
        
        response=client.recv(2048).decode(FORMAT)
        #mensaje de confirmacion de central
        print(response)
        if int(response) == 1:
            print(f"Autentificacion correcta")
            
        else:
            print(f"Error al conectar con central")
            client.close()
            return None
        return client
    #verifica el estado de engine
    def estado(self):  #cliente
        client_engine= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_engine.connect(self.addr_engine)
        print (f"Conexion establecida con engine")
        while True:
          try:
              msg_stat="ok"
              send(msg_stat, client_engine)
              status=client_engine.recv(2048).decode(FORMAT)
              print("Recibo del Servidor: ", status)
              time.sleep(1)
                #si el server te dice que ko
              if(status == "0"):
                  msg_stat="ko"
                   # send(msg_stat, client)
                  print("Averia reportada")
                  break
                    
          except BrokenPipeError:
              print(f"se perdio la conexion")
              msg_stat="ko"
              send(msg_stat, client)
              print("Averia reportada a central")
              break
          except BrokenPipeError:
              print(f"se perdio la conexion")
              msg_stat="ko"
              send(msg_stat, client)
              print("Averia reportada a central")
              break
                