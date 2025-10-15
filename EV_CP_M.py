import socket
import sys
import time

HEADER = 64
FORMAT = 'utf-8'
FIN = "FIN"

def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)
    
########## MAIN ##########


print("****** WELCOME TO OUR BRILLIANT SD UA CURSO 2020/2021 SOCKET CLIENT ****")

if  (len(sys.argv) == 6):
    SERVER = sys.argv[1]
    PORT = sys-argv[2]
    ADDR = (SERVER, PORT)
    Engine=sys.argv[3]
    Eng_Port=sys.argv[4]
    Eng_addr=(Engine, Eng_Port)
    IP_CP=sys.argv[5]
    
    
    client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client.connect(ADDR)
    print (f"Establecida conexión con central")
    
    client_engine= socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_engine.connect(ADDR)
    print (f"Conexion establecida con engine")

    #mensaje de confirmacion de central
    msg_conf=IP_CP
    msg_stat="Ok"
    print("Enviar estado de CP: ", msg_conf)
    client.sendall(msg_conf)
    #mensaje repetitivo de monitor a engine
    while True:    
        try:
            client_engine.sendall(msg_stat)
            print("Recibo del Servidor: ", client_engine.recv(2048).decode(FORMAT))
            msg=input()
            status=client_engine.recv(2048).decode(FORMAT)
            time.sleep(1)
            #si el server te dice que ko
            if(status=="ko"):
                msg_stat="ko"
                client.sendall(msg_stat)
            
        except (BrokenPipeError, ConnectionResetError):
                print(f"se perdio la conexion")
                msg_stat="ko"
                client.sendall(msg_stat)
                break

    print ("SE ACABO LO QUE SE DABA")
    client.close()
    client_engine.close()
else:
    print ("Oops!. Parece que algo falló. Necesito estos argumentos: <ServerIP> <Puerto> <Server_engine> <Puerto_engine> <ID>")
