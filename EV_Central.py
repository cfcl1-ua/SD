# EV_Central.py
# Coordinador central de la práctica EVCharging (sin BD, logs en txt)

import json, time, signal, os
from dataclasses import dataclass, field
from typing import Dict
from confluent_kafka import Consumer, Producer, KafkaException

BOOTSTRAP_SERVERS = "localhost:9092"  # ⚠️ cambia por IP del broker
GROUP_ID = "ev-central"

TOPIC_CP_REGISTER   = "cp.register"
TOPIC_CP_HEALTH     = "cp.health"
TOPIC_SUPPLY_REQ    = "supply.requests"
TOPIC_TELEMETRY     = "telemetry.stream"
TOPIC_CMDS          = "central.commands"
TOPIC_SUPPLY_STATUS = "supply.status"
TOPIC_TICKETS       = "tickets"

def logline(fname, obj):
    os.makedirs("logs", exist_ok=True)
    with open(os.path.join("logs", fname), "a", encoding="utf-8") as f:
        f.write(json.dumps(obj, ensure_ascii=False) + "\n")

@dataclass
class ChargePoint:
    cpId: str
    location: str|None = None
    price: float|None = None
    status: str = "UNKNOWN"
    last_heartbeat: float = field(default_factory=time.time)
    current_driver: str|None = None

class EVCentral:
    def __init__(self):
        self.producer = Producer({"bootstrap.servers": BOOTSTRAP_SERVERS})
        self.consumer = Consumer({
            "bootstrap.servers": BOOTSTRAP_SERVERS,
            "group.id": GROUP_ID,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        })
        self.cps: Dict[str, ChargePoint] = {}
        self.running = True

    def _send(self, topic, value):
        self.producer.produce(topic, json.dumps(value).encode("utf-8"))
        self.producer.poll(0)
        logline("central.txt", {"out": topic, **value})

    def _log(self, *args): print("[CENTRAL]", *args)

    def handle_cp_register(self, msg):
        cp_id = msg.get("cpId")
        if not cp_id: return
        cp = self.cps.get(cp_id) or ChargePoint(cpId=cp_id)
        cp.location = msg.get("location")
        cp.price = msg.get("price")
        cp.status = "ACTIVO"
        cp.last_heartbeat = time.time()
        self.cps[cp_id] = cp
        self._log("Registrado", cp_id)
        logline("central.txt", {"event":"register", **msg})

    def handle_cp_health(self, msg):
        cp_id = msg.get("cpId")
        if not cp_id: return
        cp = self.cps.get(cp_id) or ChargePoint(cpId=cp_id)
        cp.status = msg.get("status","ACTIVO")
        cp.last_heartbeat = time.time()
        self.cps[cp_id] = cp
        self._log("Heartbeat", cp_id, cp.status)
        logline("central.txt", {"event":"heartbeat", **msg})

    def handle_supply_request(self, msg):
        driver = msg.get("driverId"); cp_id = msg.get("cpId")
        if not driver or not cp_id: return
        cp = self.cps.get(cp_id)
        if not cp or cp.status!="ACTIVO" or cp.current_driver:
            self._send(TOPIC_SUPPLY_STATUS, {"driverId":driver,"cpId":cp_id,"status":"REJECTED"})
            self._log("Solicitud rechazada", driver, cp_id)
        else:
            cp.current_driver = driver; cp.status="OCUPADO"
            self._send(TOPIC_SUPPLY_STATUS, {"driverId":driver,"cpId":cp_id,"status":"ACCEPTED"})
            self._send(TOPIC_CMDS, {"type":"start","cpId":cp_id,"driverId":driver})
            self._log("Solicitud aceptada", driver, cp_id)
        logline("central.txt", {"event":"request", **msg})

    def handle_telemetry(self, msg):
        cp_id = msg.get("cpId"); driver = msg.get("driverId")
        self._log("Telemetría", cp_id, msg)
        logline("central.txt", {"event":"telemetry", **msg})
        if msg.get("type")=="finished":
            cp = self.cps.get(cp_id)
            if cp: cp.status="ACTIVO"; cp.current_driver=None
            ticket = {"driverId":driver,"cpId":cp_id,
                      "totalKWh":msg.get("totalKWh"),"totalEuros":msg.get("totalEuros")}
            self._send(TOPIC_TICKETS, ticket)
            self._log("Ticket emitido", ticket)

    def run(self):
        self.consumer.subscribe([TOPIC_CP_REGISTER,TOPIC_CP_HEALTH,TOPIC_SUPPLY_REQ,TOPIC_TELEMETRY])
        self._log("Escuchando topics…")
        while self.running:
            msg = self.consumer.poll(1.0)
            if not msg: continue
            if msg.error(): raise KafkaException(msg.error())
            try: payload = json.loads(msg.value().decode())
            except: continue
            t = msg.topic()
            if t==TOPIC_CP_REGISTER: self.handle_cp_register(payload)
            elif t==TOPIC_CP_HEALTH: self.handle_cp_health(payload)
            elif t==TOPIC_SUPPLY_REQ: self.handle_supply_request(payload)
            elif t==TOPIC_TELEMETRY: self.handle_telemetry(payload)
        self.consumer.close()

def main():
    ev = EVCentral()
    signal.signal(signal.SIGINT, lambda s,f: setattr(ev, "running", False))
    ev.run()

if __name__=="__main__": main()
