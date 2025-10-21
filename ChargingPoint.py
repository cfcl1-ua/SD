from EV_CP_M import Monitor
from EV_CP_E import Engine
import threading
import time
from confluent_kafka import Consumer, KafkaException, KafkaError

IP='localhost'
PORT=6006
IP_ENGINE='localhost'
PORT_ENGINE=5050
IP_BROKER='localhost'
PORT_BROKER=5643

class ChargingPoint:
    """
    Representa un punto de recarga (CP) con los atributos y estados básicos.
    Será instanciado por la Central a partir de la información de un archivo .txt.
    """

    def __init__(self, cp_id, location):
        self.id = cp_id
        self.location = location
        self.active = False        # Si el punto está encendido o no
        self.supplying = False     # Si está suministrando energía
        self.status = "OFFLINE"    # OFFLINE, IDLE, CHARGING, ERROR
        self.Monitor=Monitor(IP, PORT, IP_ENGINE, PORT_ENGINE, cp_id)
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
    Punto = ChargingPoint(2, "san vicente")
    hilo_trabajo = threading.Thread(target=Punto.Engine.estado)
    hilo_supervision = threading.Thread(target=Punto.Monitor.estado)
    
    hilo_trabajo.start()
    time.sleep(2)
    hilo_supervision.start()
    
    time.sleep(10)
    print("d")
    Punto.Engine.boton_ko()
    
    hilo_trabajo.join()
    hilo_supervision.join()
    
    print("finish")
