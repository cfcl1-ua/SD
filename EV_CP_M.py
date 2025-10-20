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
    
#funcion que conecta con central
def conectar_central(addr, ID):
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(addr)
    
    msg=f"monitor|ok|{ID}"
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
########## MAIN ##########

def main():
    print("****** Arrancar monitor de CP ****")
#lista de argumentos
    if (len(sys.argv) == 4):
        SERVER = sys.argv[1]
        PORT = int(sys.argv[2])
        IP_CP=sys.argv[3]
        ADDR = (SERVER, PORT)
        client=conectar_central(ADDR, IP_CP)
        client.close()
        '''
        Engine=sys.argv[3]
        Eng_Port=sys.argv[4]
        Eng_addr=(Engine, Eng_Port)
        IP_CP=sys.argv[5]
      '''  
    '''
        client_engine= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client_engine.connect(ADDR)
        print (f"Conexion establecida con engine")
    '''
        
        
        #mensaje repetitivo de monitor a engine
'''
      while True:
          try:
              
              send(msg_stat, client_engine)
              print("Recibo del Servidor: ", client_engine.recv(2048).decode(FORMAT))
              status=client_engine.recv(2048).decode(FORMAT)
              time.sleep(1)
                #si el server te dice que ko
                if(status=="ko"):
                    msg_stat="ko"
                    send(msg_stat, client_engine)
                    printf("Averia reportada")
                
            except (BrokenPipeError, ConnectionResetError):
                    print(f"se perdio la conexion")
                    msg_stat="ko"
                    send(msg_stat, client)
                    printf("Averia reportada a central")
                    break

        print ("SE ACABO LO QUE SE DABA")
        client.close()
        client_engine.close()
     '''
    #else:
     #   print ("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <Server_engine> <Puerto_engine> <ID>")

if __name__=="__main__": main()