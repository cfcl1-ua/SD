from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import json
import threading
import os

app = Flask(__name__)

@app.route("/ping")
def ping():
    print(">>> PING RECIBIDO")
    return "pong"

CORS(app)

DB_FILE = "db.json"
lock = threading.Lock()

# ==============================
# UTILIDADES
# ==============================

def cargar_db():
    if not os.path.exists(DB_FILE):
        return {
            "clientes": [],
            "cps": []
        }

    with open(DB_FILE, "r", encoding="utf-8") as f:
        return json.load(f)


def guardar_db(db):
    with open(DB_FILE, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=4)


def buscar_cp(cps, id_cp):
    return next((cp for cp in cps if cp["id"] == id_cp), None)


# ==============================
# CPs
# ==============================


@app.route("/estado", methods=["GET"])
def Estado():
    db = cargar_db()
    cps = db.get("cps", [])
    temperaturas = db.get("climas", [])
    
    for cp in cps:
        estado=cp["estado"]
        temp = next((t["temperatura"] for t in temperaturas if t["ciudad"].lower() == cp["location"].lower()), None)
        cp["temperatura"] = temp if temp is not None else "-"
        cp["estado"] = estado if temp > 0 else "ERROR"
    
    return  {"cps": cps}


@app.route("/api/cps/<id_cp>", methods=["GET"])
def obtener_cp(id_cp):
    db = cargar_db()
    cp = buscar_cp(db["cps"], id_cp)

    if not cp:
        return jsonify({"error": "CP no encontrado"}), 404

    return jsonify(cp)


@app.route("/api/cps/<id_cp>/estado", methods=["PUT"])
def actualizar_estado_cp(id_cp):
   
    data = request.json
    if not data or "estado" not in data:
        return jsonify({"error": "Campo 'estado' requerido"}), 400

    with lock:
        db = cargar_db()
        cp = buscar_cp(db["cps"], id_cp)

        if not cp:
            return jsonify({"error": "CP no encontrado"}), 404

        cp["estado"] = data["estado"]
        guardar_db(db)

    return jsonify({"ok": "Estado actualizado"})

@app.post("/token")
def emitir_token(cp: CPId):
    db = cargar_db()
    if cp.id not in db:
        raise HTTPException(status_code=403, detail="CP no registrado")

    token = generar_token(cp.id)
    db[cp.id]["token"] = token
    guardar_db(db)

    return {"token": token}

@app.route("/")
def index():
    return send_from_directory("static", "index.html")

# ==============================
# CLIENTES
# ==============================

@app.route("/api/clientes", methods=["GET"])
def obtener_clientes():
    db = cargar_db()
    return jsonify(db["clientes"])


@app.route("/api/clientes/<id_cliente>", methods=["GET"])
def obtener_cliente(id_cliente):
    db = cargar_db()

    if id_cliente not in db["clientes"]:
        return jsonify({"error": "Cliente no encontrado"}), 404

    return jsonify({"id": id_cliente})

# ==============================
# CLIMA
# ==============================
@app.route("/clima", methods=["PUT"])
def recibir_clima():
    data = request.json
    print("Notificación recibida:", data)
    return jsonify({"status": "ok"})

# ==============================
# ARRANQUE
# ==============================

def start_api(host="0.0.0.0", port=8000):
    app.run(
        host="0.0.0.0",
        port=8000,
        debug=True,
        use_reloader=False
    )


if __name__ == "__main__":
    start_api()

