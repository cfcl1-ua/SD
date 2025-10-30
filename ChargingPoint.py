from EV_CP_M import Monitor
from EV_CP_E import Engine
import threading
import time
import argparse

class ChargingPoint:
    """
    Representa un punto de recarga (CP) con los atributos y estados básicos.
    Será instanciado por la Central a partir de la información de un archivo .txt.
    """

    def __init__(self, cp_id, location, IP, PORT, IP_ENGINE, PORT_ENGINE, IP_BROKER, PORT_BROKER):
        self.id = cp_id
        self.location = location
        self.active = False        # Si el punto está encendido o no
        self.supplying = False     # Si está suministrando energía
        self.status = "OFFLINE"    # OFFLINE, IDLE, CHARGING, ERROR
        self.Monitor=Monitor(IP, PORT, IP_ENGINE, PORT_ENGINE, cp_id, location)
        self.Engine=Engine(IP_BROKER, PORT_BROKER)

    # --- Estados básicos ---
    def activate(self):
        """Activa el punto de recarga."""
        self.active = True
        self.status = "IDLE"

    def deactivate(self):
        """Desactiva el punto de recarga."""
        self.active = False
        self.supplying = False
        self.status = "OFFLINE"

    # --- Simulación de suministro ---
    def start_supply(self):
        """Inicia el suministro de energía si está activo."""
        if not self.active:
            raise RuntimeError(f"El CP {self.id} no está activo.")
        self.supplying = True
        self.status = "CHARGING"

    def stop_supply(self):
        """Detiene el suministro de energía."""
        self.supplying = False
        self.status = "IDLE" if self.active else "OFFLINE"

    # --- Información del punto ---
    def get_info(self) -> dict:
        """Devuelve un resumen del estado actual."""
        return {
            "id": self.id,
            "location": self.location,
            "active": self.active,
            "supplying": self.supplying,
            "status": self.status
        }
    
    def getId(self):
        return self.id
    
    def getStatus(self):
        return self.status
    

    def __repr__(self):
        return f"<ChargingPoint id={self.id} status={self.status} active={self.active}>"
    
if __name__ == "__main__":
    
    parser = argparse.ArgumentParser(description="Argumentos para el CP")
    parser.add_argument("--central", type=str, required=True,
                    help="IP y puerto en formato IP:PUERTO, ejemplo 127.0.0.1:5000")
    parser.add_argument("--engine", type=str, required=True,
                    help="IP y puerto en formato IP:PUERTO, ejemplo 127.0.0.1:5000")
    parser.add_argument("--id", type=str, required=True,
                    help="ID del CP")
    parser.add_argument("--localizacion", type=str, required=True,
                    help="lugar del CP")
    parser.add_argument("--broker", type=str, required=True,
                    help="IP y puerto en formato IP:PUERTO, ejemplo 127.0.0.1:5000")
    args = parser.parse_args()
    
    ip, puerto = args.central.split(":")
    ip_engine, port_engine= args.engine.split(":")
    ip_broker, port_broker = args.broker.split(":")
    
    Punto=ChargingPoint(ip, int(puerto), ip_engine, int(port_engine), args.id, args.localizacion, ip_broker, int(port_broker))
        
    #hilo donde monitor checara el estado del punto de arga
    hilo_trabajo = threading.Thread(target=Punto.Engine.estado)
    hilo_supervision = threading.Thread(target=Punto.Monitor.estado)
    #hilo donde el engine realizara los servicios enviados por central
    hilo_peticiones = threading.Thread(target=Punto.Engine.servicios)
    
    #Lo conectamos con central
    activo=Punto.Monitor.conectar_central()
    
    if activo:

        hilo_trabajo.start()
        time.sleep(3)
        hilo_supervision.start()
        hilo_peticiones.start()
        
        #al presionar enter se activa el boton de ko de engine
        Punto.Engine.menu()
                    
        hilo_supervision.join(timeout=5)
        hilo_trabajo.join(timeout=5)
            
            
        print("finish")
        
    else:
        print("error")
