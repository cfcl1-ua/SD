import json, time, os, argparse
from confluent_kafka import Producer
BOOTSTRAP_SERVERS="localhost:9092"
TOPIC_CP_REGISTER="cp.register"
TOPIC_CP_HEALTH="cp.health"

def logline(fname, obj):
    os.makedirs("logs", exist_ok=True)
    with open(os.path.join("logs", fname),"a",encoding="utf-8") as f:
        f.write(json.dumps(obj)+"\n")

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--cpId",required=True)
    ap.add_argument("--location",default="LAB")
    ap.add_argument("--price",type=float,default=0.25)
    ap.add_argument("--period",type=int,default=5)
    args=ap.parse_args()

    p=Producer({"bootstrap.servers":BOOTSTRAP_SERVERS})
    reg={"cpId":args.cpId,"location":args.location,"price":args.price}
    p.produce(TOPIC_CP_REGISTER,json.dumps(reg).encode());p.flush()
    logline("cp_manager.txt",{"event":"register",**reg})
    print("[CP_M]",args.cpId,"registrado")

    while True:
        hb={"cpId":args.cpId,"status":"ACTIVO","ts":int(time.time())}
        p.produce(TOPIC_CP_HEALTH,json.dumps(hb).encode());p.poll(0)
        logline("cp_manager.txt",{"event":"heartbeat",**hb})
        print("[CP_M] HB",args.cpId)
        time.sleep(args.period)

if __name__=="__main__":main()
