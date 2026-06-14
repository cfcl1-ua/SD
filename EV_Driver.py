from kafka import KafkaProducer, KafkaConsumer
import sys
import time
import threading
import selectors  # Importante para el parche

FORMAT = 'utf-8'
DELAYS = 4  # segundos entre solicitudes

# TOPICS globales (iguales para todos)
TOPIC_DTC = "driver-to-central"       # Drivers -> Central

for selector_name in ['SelectSelector', 'PollSelector', 'EpollSelector', 'KqueueSelector']:
    if hasattr(selectors, selector_name):
        cls = getattr(selectors, selector_name)
        orig_unregister = cls.unregister
        def _crear_safe(func_original):
            def safe_unregister(self, fileobj):
                try:
                    return func_original(self, fileobj)
                except (KeyError, ValueError):
                    return None
            return safe_unregister
        cls.unregister = _crear_safe(orig_unregister)
# =========================================================================

def menu():
    opciones = {"1": "AUTENTIFICACION", "2": "AUTORIZACION", "3": "ESTADO", "4": "FIN"}
    while True:
        print("\n===== MENÚ =====")
        print("1. AUTENTIFICACION")
        print("2. AUTORIZACION")
        print("3. ESTADO")
        print("4. FIN")
        eleccion = input("Selecciona (1-4): ").strip()
        if eleccion in opciones:
            return opciones[eleccion]
        print("ERROR: Opción no válida. Elige un número del 1 al 4.")

def sendRequests(bootstrap_server: str, driver_id: str):
    """
    Formato (4 campos): REM|PETICION|CP|DRIVER
      AUTENTIFICACION -> DRIVER|AUTENTIFICACION|NA|<DRIVER_ID>
      AUTORIZACION    -> DRIVER|AUTORIZACION|<CP_ID>|<DRIVER_ID>
      ESTADO          -> DRIVER|ESTADO|<CP_ID>|<DRIVER_ID>
      FIN             -> DRIVER|FIN|NA|<DRIVER_ID>
    """
    try:
        producer = KafkaProducer(
            bootstrap_servers=[bootstrap_server],
            acks='all',
            retries=3
        )
    except Exception as e:
        print(f"[DRIVER][CRÍTICO] Error al conectar con el Broker de Kafka: {e}")
        return
        
    print(f"[DRIVER] Conectado a Kafka en {bootstrap_server}")
    cp_id = "None"
    while True:
        tipo = menu()

        # CP según el tipo
        if tipo in ("AUTORIZACION", "ESTADO"):
            cp_id = input("Introduce el ID del CP: ").strip()
            if not cp_id:
                print("ERROR: CP vacío. Cancelo envío.")
                continue

        msg = f"DRIVER|{tipo}|{cp_id}|{driver_id}"
        try:
            print(f"[DRIVER] Enviando mensaje: '{msg}'...")
            # Enviamos como bytes puros UTF-8 para sincronizar con el receptor limpio de Central
            futuro = producer.send(TOPIC_DTC, value=msg.encode(FORMAT))
            
            # Bloqueamos un instante para comprobar si el broker aceptó el mensaje de verdad
            resultado_metadata = futuro.get(timeout=5)
            print(f"[DRIVER][ÉXITO] Mensaje recibido por Kafka en el topic: {resultado_metadata.topic}")
            
            if tipo == "FIN":
                print("[DRIVER] Petición de finalización enviada. Saliendo del bucle de peticiones...")
                break
                
        except Exception as e:
            print(f"[DRIVER][ERROR] No se pudo depositar el mensaje en Kafka: {e}")

        time.sleep(DELAYS)

def topics_id(id):
    return f"central-to-consumer-{id}"


def topic_respuestas_driver(driver_id):
    return f"central-to-consumer-{driver_id}"


def receiveAnswers(bootstrap_server: str, driver_id: str):
    """
    Escucha respuestas de la Central en topic_respuestas_driver(driver_id).
    Formato esperado: RESPUESTA|<DRIVER_ID>|<MENSAJE>
    """
    topic_resp = topic_respuestas_driver(driver_id)
    
    consumer = KafkaConsumer(
        topic_resp,
        bootstrap_servers=[bootstrap_server],
        value_deserializer=lambda m: m.decode(FORMAT),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=f"driver-{driver_id}",  # grupo común de drivers
    )
    print(f"[DRIVER] Escuchando respuestas en '{topic_resp}'…")
    for rec in consumer:
        text = rec.value or ""
        print(text)
        parts = text.split("|", 2)
        if len(parts) >= 3 and parts[1] == driver_id:
            mensaje = parts[2] 
            print(f"[CENTRAL --> {driver_id}]: {mensaje}")

def main():
    print("****** EV_Driver ******")
    if len(sys.argv) < 3:
        print("Uso: python EV_Driver.py <BrokerIP:Puerto> <DriverID>")
        return

    bootstrap_server = sys.argv[1]
    driver_id = sys.argv[2]

    t = threading.Thread(target=receiveAnswers, args=(bootstrap_server, driver_id), daemon=True)
    t.start()

    sendRequests(bootstrap_server, driver_id)
    print("Cerrando Driver…")
    time.sleep(1)
    
    

if __name__ == "__main__":
    main()
