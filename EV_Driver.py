import socket
import sys

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"

def send(msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    client.send(send_length)
    client.send(message)

DELAYS = 4  # segundos entre solicitudes

def SelectServices(sock, cliente):
    """
    Muestra una vez los servicios disponibles y permite al usuario,
    en bucle, solicitar servicios cada 4 segundos. Imprime la respuesta
    de la Central para cada solicitud. Escribe 0 o FIN para salir.
    """
    print("\n=== Servicios disponibles ===")
    print("1 - Registro de usuario")
    print("2 - Solicitar autorización de suministro")
    print("3 - Consultar estado de un CP")
    print("Cualquier otra tecla salir")
    print("=============================\n")

    while True:
        op = input("Selecciona servicio (1, 2 o 3): ").strip()

        # Validar opción -> crear Servicio
        try:
            n = int(op)
            servicio = Servicio(n)
            
            # Preparar y enviar mensaje según servicio
            if servicio.get_tipo() == Servicio.Tipo.REGISTRO:
                msg = f"REGISTRO|{cliente.getID()}"
                print("[DRV]: ", msg)
                send(sock, msg)
                resp = recv(sock)
                print("[CENTRAL]: ", resp)

            elif servicio.get_tipo() == Servicio.Tipo.AUTORIZACION:
                cp_id = input("Introduce el ID del CP: ").strip()
                if not cp_id:
                    print("CP_ID vacío, cancelo solicitud.")
                    continue
                msg = f"AUTORIZACION|{cliente.getID()}|{cp_id}"
                print("[DRV] ", msg)
                send(sock, msg)
                resp = recv(sock)
                print("[CENTRAL] ", resp)

            else:
                cp_id = input("Introduce el ID del CP: ").strip()
                if not cp_id:
                    print("CP_ID vacío, cancelo consulta.")
                    continue
                msg = f"STATUS|{cp_id}"
                print("[DRV] ", msg)
                send(sock, msg)
                resp = recv(sock)
                print("[CENTRAL] ", resp)
            
        except ValueError:
            print("Opción inválida. Usa 1, 2 o 3.")
            continue

        # Espera obligatoria tras cada respuesta (éxito o error)
        time.sleep(DELAYS)

########## MAIN ##########


def main():
    print("****** EV_Driver ******")
    if len(sys.argv) < 3:
        print("Uso: python EV_Driver.py <ServerIP> <Puerto> <DriverID>")
        return

    server_ip = sys.argv[1]
    port = int(sys.argv[2])
    driver = Cliente(sys.argv[3]) # creamos el cliente con la id que recibimos por parametro

    addr = (server_ip, port)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(addr)
    print(f"[EV_Driver] Conectado a CENTRAL en {addr}")
    
    SelectServices(sock, cliente)

    print("Cerrando sesión…")
    send(sock, FIN)
    sock.close()

if __name__ == "__main__":
    main()