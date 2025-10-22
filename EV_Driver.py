from kafka import KafkaProducer, KafkaConsumer
import sys
import time
import threading

FORMAT = 'utf-8'
DELAYS = 4  # segundos entre solicitudes

# TOPICS
TOPIC_DTC = "driver-to-central"   # REGISTRO y FIN
TOPIC_DTE  = "driver-to-engine"    # ESTADO y AUTORIZACION 
TOPIC_ANSWERS    = "messenger-to-driver"    # Respuestas 

def menu():
    opciones = {"1": "REGISTRO", "2": "AUTORIZACION", "3": "ESTADO", "4": "FIN"}
    while True:
        print("\n===== MENÚ =====")
        print("1. REGISTRO")
        print("2. AUTORIZACION")
        print("3. ESTADO")
        print("4. FIN")
        eleccion = input("Selecciona (1-4): ").strip()
        if eleccion in opciones:
            return opciones[eleccion]
        print("ERROR: Opción no válida. Elige un número del 1 al 4.")

def sendRequests(bootstrap_server: str, driver_id: str):
    """
    Formatos enviados:
      REGISTRO -> 'REGISTRO|<DRIVER_ID>'        (a CENTRAL)
      AUTORIZACION -> 'AUTORIZACION|<DRIVER_ID>'(a ENGINE)
      ESTADO -> 'ESTADO|<DRIVER_ID>'            (a ENGINE)
      FIN -> 'FIN|<DRIVER_ID>'                  (a CENTRAL)
    """
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_server],
        value_serializer=lambda v: v.encode(FORMAT)  # string -> bytes
    )
    print(f"[DRIVER] Conectado a Kafka en {bootstrap_server}")

    while True:
        tipo = menu()

        if tipo == "FIN":
            msg = f"DRIVER|FIN|{driver_id}"
            producer.send(TOPIC_DTC, msg)
            producer.flush()
            print(f"[DRIVER] Enviado: {msg}")
            print("[DRIVER] Fin de sesión.")
            break

        if tipo == "REGISTRO":
            topic = TOPIC_DTC
            msg = f"DRIVER|REGISTRO|{driver_id}"
        elif tipo in ("AUTORIZACION", "ESTADO"):
            topic = TOPIC_DTE
            msg = f"DRIVER|{tipo}|{driver_id}"
            
        producer.send(topic, msg)
        producer.flush()
        print(f"[DRIVER] Enviado ({topic}): {msg}")
        time.sleep(DELAYS)

    producer.close()

def receiveAnswers(bootstrap_server: str, driver_id: str):
    """
    Espera respuestas en TOPIC_ANSWERS con formato:
      'RESPUESTA|<DRIVER_ID>|<MENSAJE>'
    """
    consumer = KafkaConsumer(
        TOPIC_ANSWERS,
        bootstrap_servers=[bootstrap_server],
        value_deserializer=lambda m: m.decode(FORMAT),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=f"driver-{driver_id}",
    )
    print(f"[DRIVER] Escuchando respuestas en '{TOPIC_ANSWERS}'…")
    for rec in consumer:
        text = rec.value or ""
        parts = text.split("|", 2)
        if len(parts) >= 2 and parts[1] == driver_id:
            mensaje = parts[2] if len(parts) >= 3 else ""
            print(f"[RESPUESTA → {driver_id}]: {mensaje}")

def main():
    print("****** EV_Driver ******")
    if len(sys.argv) < 3:
        print("Uso: python EV_Driver.py <BrokerIP:Puerto> <DriverID>")
        return

    bootstrap_server = sys.argv[1]
    driver_id = sys.argv[2]

    # Hilo para escuchar respuestas mientras enviamos peticiones
    t = threading.Thread(target=receiveAnswers, args=(bootstrap_server, driver_id), daemon=True)
    t.start()

    sendRequests(bootstrap_server, driver_id)
    print("Cerrando Driver…")
    time.sleep(1)

if __name__ == "__main__":
    main()
    