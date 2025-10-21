from kafka import KafkaProducer, KafkaConsumer
import sys
import time
import threading
from Cliente import Cliente

HEADER = 64
PORT = 5050
FORMAT = 'utf-8'
FIN = "FIN"
DELAYS = 4  # segundos entre solicitudes

# Tópicos
TOPIC_REQUESTS = "driver-to-central"     # Driver -> Central
TOPIC_ANSWERS  = "central-to-driver"     # Central -> Driver

def sendRequests(bootstrap_server, driver):
    """
    Envía mensajes en texto plano, separados por '|', al topic 'driver-to-central'.
    Ejemplo de formato: 'REGISTRO|DRV001|CP01'
    """
    producer = KafkaProducer(
        bootstrap_servers=[bootstrap_server],
        value_serializer=lambda v: v.encode(FORMAT)  # string -> bytes
    )

    print(f"[DRIVER] Conectado a Kafka en {bootstrap_server}")
    print("Introduce los mensajes en formato: TIPO|DRIVER_ID|CP_ID  (usa FIN para salir)")

    while True:
        msg = input("> ").strip()
        if not msg:
            continue

        if msg.upper().startswith(FIN):
            # Enviamos FIN incluyendo el driver_id para que la central pueda filtrar si lo necesita
            fin_msg = f"{FIN}|{driver.getId()}"
            producer.send(TOPIC_REQUESTS, fin_msg)
            producer.flush()
            print("[DRIVER] Fin de sesión.")
            break

        producer.send(TOPIC_REQUESTS, msg)
        producer.flush()
        print(f"[DRIVER] Enviado: {msg}")

        # Pausa exigida por la práctica
        time.sleep(DELAYS)

    producer.close()


def receiveAnswers(bootstrap_server, driver):
    """
    --- Kafka Consumer: para recibir respuestas de la central ---
    Se asume que la central publica strings con formato: 'RESPUESTA|<DRIVER_ID>|<MENSAJE>'
    Este consumidor muestra SOLO las respuestas destinadas a este driver.
    """
    consumer = KafkaConsumer(
        TOPIC_ANSWERS,
        bootstrap_servers=[bootstrap_server],
        value_deserializer=lambda m: m.decode(FORMAT),  # bytes -> string
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=f"driver-{driver.getId()}",
    )

    print(f"[DRIVER] Escuchando respuestas en '{TOPIC_ANSWERS}'…")
    for msg in consumer:
        text = msg.value or ""
        parts = text.split("|", 2)  # ['RESPUESTA', '<DRIVER_ID>', '<MENSAJE>']
        if len(parts) < 2:
            # Formato inesperado; lo mostramos tal cual por depuración
            print(f"[CENTRAL] (formato desconocido) {text}")
            continue

        # Filtramos por driver_id si viene en la posición 1
        resp_driver_id = parts[1] if len(parts) >= 2 else ""
        if resp_driver_id == driver.getId():
            mensaje = parts[2] if len(parts) >= 3 else ""
            print(f"[CENTRAL --> {driver.getId()}]: {mensaje}")


def main():
    print("****** EV_Driver ******")
    # Uso: python EV_Driver.py <BrokerIP:Puerto> <DriverID>
    if len(sys.argv) < 3:
        print("Uso: python EV_Driver.py <BrokerIP:Puerto> <DriverID>")
        return

    bootstrap_server = sys.argv[1]   # ej: localhost:9092
    driver_id = sys.argv[2]
    driver = Cliente(driver_id)      # tu clase existente

    # Hilo para respuestas, si la central las publica
    t = threading.Thread(target=receiveAnswers, args=(bootstrap_server, driver), daemon=True)
    t.start()

    # Envío de peticiones
    sendRequests(bootstrap_server, driver)

    print("Cerrando Driver…")
    time.sleep(1)


if __name__ == "__main__":
    main()
    