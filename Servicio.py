from enum import Enum

class Servicio:
    class Tipo(Enum):
        REGISTRO = 1         # Alta de usuario/cliente en la Central
        AUTORIZACION = 2     # Solicitud de autorización de suministro
        ESTADO = 3           # Consulta del estado de un punto de recarga

    def __init__(self, tipo: int):
        """
        Crea un objeto Servicio a partir de un número entero.
        - 1 -> REGISTRO
        - 2 -> AUTORIZACION
        - 3 -> ESTADO
        """
        try:
            self.tipo = Servicio.Tipo(tipo)
        except ValueError:
            raise ValueError(f"Número de servicio inválido: {tipo}. Usa 1, 2 o 3.")

    def get_tipo(self):
        """Devuelve el tipo de servicio (enumerado)."""
        return self.tipo

    def get_nombre(self):
        """Devuelve el nombre del servicio como cadena."""
        return self.tipo.name

    def __str__(self):
        return f"Servicio[{self.tipo.name}]"
