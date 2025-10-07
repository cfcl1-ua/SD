import json, os, time, argparse
from confluent_kafka import Producer, Consumer, KafkaException
BOOTSTRAP_SERVERS="localhost:9092"
TOPIC_SUPPLY_REQ="supply.requests"
TOPIC_SUPPLY_STATUS="supply.status"
TOPIC_TICKETS="tickets"

def logline(fname,obj):
    os.makedirs("logs",exist_ok=True)
    with open(os.path.join("logs",fname),"a",encoding="utf-8") as f:
        f.write(json.dumps(obj)+"\n")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--driverId",required=True)
    ap.add_argument("--cpId",required=True)
    args=ap.parse_args()

    p=Producer({"bootstrap.servers":BOOTSTRAP_SERVERS})
    c=Consumer({
        "bootstrap.servers":BOOTSTRAP_SERVERS,
        "group.id":f"driver-{args.driverId}",
        "auto.offset.reset":"earliest"
    })
    c.subscribe([TOPIC_SUPPLY_STATUS,TOPIC_TICKETS])

    req={"driverId":args.driverId,"cpId":args.cpId}
    p.produce(TOPIC_SUPPLY_REQ,json.dumps(req).encode());p.flush()
    logline("driver.txt",{"event":"request",**req})
    print("[DRV]",args.driverId,"pide",args.cpId)

    while True:
        msg=c.poll(1)
        if not msg: continue
        if msg.error(): raise KafkaException(msg.error())
        val=json.loads(msg.value().decode())
        if msg.topic()==TOPIC_SUPPLY_STATUS and val.get("driverId")==args.driverId:
            print("[DRV] STATUS",val);logline("driver.txt",{"event":"status",**val})
        if msg.topic()==TOPIC_TICKETS and val.get("driverId")==args.driverId:
            print("[DRV] TICKET",val);logline("driver.txt",{"event":"ticket",**val});break
    c.close()

if __name__=="__main__":main()
