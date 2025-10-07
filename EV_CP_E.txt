import json, time, os, argparse, random
from confluent_kafka import Consumer, Producer, KafkaException
BOOTSTRAP_SERVERS="localhost:9092"
TOPIC_CMDS="central.commands"
TOPIC_TELEMETRY="telemetry.stream"

def logline(fname,obj):
    os.makedirs("logs",exist_ok=True)
    with open(os.path.join("logs",fname),"a",encoding="utf-8") as f:
        f.write(json.dumps(obj)+"\n")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cpId",required=True)
    ap.add_argument("--price",type=float,default=0.25)
    ap.add_argument("--duration",type=int,default=10)
    args=ap.parse_args()

    c=Consumer({
        "bootstrap.servers":BOOTSTRAP_SERVERS,
        "group.id":f"cp-e-{args.cpId}",
        "auto.offset.reset":"latest"
    })
    p=Producer({"bootstrap.servers":BOOTSTRAP_SERVERS})
    c.subscribe([TOPIC_CMDS])
    print(f"[CP_E {args.cpId}] Esperando órdenes…")

    while True:
        msg=c.poll(1)
        if not msg: continue
        if msg.error(): raise KafkaException(msg.error())
        try: cmd=json.loads(msg.value().decode())
        except: continue
        if cmd.get("cpId")!=args.cpId: continue
        if cmd.get("type")=="start":
            driver=cmd.get("driverId")
            print(f"[CP_E {args.cpId}] START {driver}")
            total=0.0
            for _ in range(args.duration):
                total+=random.uniform(0.3,0.6)
                euros=round(total*args.price,2)
                tel={"cpId":args.cpId,"driverId":driver,"kWh":round(total,2),"euros":euros}
                p.produce(TOPIC_TELEMETRY,json.dumps(tel).encode());p.poll(0)
                logline("cp_executor.txt",{"event":"telemetry",**tel})
                time.sleep(1)
            fin={"type":"finished","cpId":args.cpId,"driverId":driver,"totalKWh":round(total,2),"totalEuros":round(total*args.price,2)}
            p.produce(TOPIC_TELEMETRY,json.dumps(fin).encode());p.flush()
            logline("cp_executor.txt",{"event":"finished",**fin})
            print("[CP_E] FIN",fin)

if __name__=="__main__":main()
