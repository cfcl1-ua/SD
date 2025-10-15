class ChargingPoint:
    """
    Representa un punto de recarga (CP) con los atributos y estados básicos.
    Será instanciado por la Central a partir de la información de un archivo .txt.
    """

    def __init__(self, cp_id: str, location: str):
        self.id = cp_id
        self.location = location
        self.active = False        # Si el punto está encendido o no
        self.supplying = False     # Si está suministrando energía
        self.status = "OFFLINE"    # OFFLINE, IDLE, CHARGING, ERROR

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

    def __repr__(self):
        return f"<ChargingPoint id={self.id} status={self.status} active={self.active}>"
