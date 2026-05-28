from flask import Flask, jsonify, request, send_from_directory
from flask_cors import CORS
import json
import threading
import os
import requests
 
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
 
app = Flask(__name__, static_folder=os.path.join(BASE_DIR, "static"))
CORS(app)
 
DB_FILE = os.path.join(BASE_DIR, "db.json")
lock = threading.Lock()
 
# IP y puerto de EV_Central (su Flask interno, puerto 8000)
# API_Central corre en el 8080
CENTRAL_URL = "http://127.0.0.1:8000"
 
# ==============================
# UTILIDADES (solo lectura)
# ==============================
 
def cargar_db():
    if not os.path.exists(DB_FILE):
        return {"clientes": [], "cps": [], "climas": []}
    with open(DB_FILE, "r", encoding="utf-8") as f:
        return json.load(f)
 
def buscar_cp(cps, id_cp):
    return next((cp for cp in cps if cp["id"] == id_cp), None)
 
# ==============================
# FRONTEND
# ==============================
 
@app.route("/")
def index():
    return send_from_directory(os.path.join(BASE_DIR, "static"), "index.html")
 
@app.route("/ping")
def ping():
    return "pong"
 
# ==============================
# CPs — solo lectura
# ==============================
 
@app.route("/estado", methods=["GET"])
def estado():
    db = cargar_db()
    cps = db.get("cps", [])
    temperaturas = db.get("climas", [])
 
    resultado = []
    for cp in cps:
        cp_out = dict(cp)
        temp = next(
            (t["temperatura"] for t in temperaturas
             if t["ciudad"].lower() == cp.get("location", "").lower()),
            None
        )
        cp_out["temperatura"] = temp if temp is not None else "-"
        resultado.append(cp_out)
 
    return jsonify({"cps": resultado})
 
 
@app.route("/api/cps/<id_cp>", methods=["GET"])
def obtener_cp(id_cp):
    db = cargar_db()
    cp = buscar_cp(db["cps"], id_cp)
    if not cp:
        return jsonify({"error": "CP no encontrado"}), 404
    return jsonify(cp)
 
# ==============================
# CLIENTES — solo lectura
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
# CLIMAS — solo lectura
# ==============================
 
@app.route("/climas", methods=["GET"])
def obtener_climas():
    db = cargar_db()
    return jsonify({"climas": db.get("climas", [])})
 
# ==============================
# CLIMA — escritura delegada a EV_Central
# ==============================
 
@app.route("/clima", methods=["PUT"])
def recibir_clima():
    """
    Recibe notificación de EV_W y la delega a EV_Central para que
    sea ella quien actualice la BD y gestione el estado de los CPs.
    Espera JSON: { "ciudad": "Barrow", "temperatura": -6.99 }
    """
    data = request.json
    if not data or "ciudad" not in data or "temperatura" not in data:
        return jsonify({"error": "Faltan campos: ciudad, temperatura"}), 400
 
    print(f"[API_Central] Notificación de clima recibida: {data} — delegando a EV_Central")
 
    try:
        # Adaptamos el formato al que espera EV_Central: { "localizacion": ..., "estado": ... }
        ciudad = data["ciudad"]
        temperatura = data["temperatura"]
        estado_clima = "KO" if temperatura <= 0 else "OK"
 
        resp = requests.post(
            f"{CENTRAL_URL}/clima",
            json={
                "localizacion": ciudad,
                "estado": estado_clima,
                "temperatura": temperatura
            },
            timeout=5
        )
        return jsonify({
            "status": "delegado",
            "central_status": resp.status_code,
            "central_response": resp.json()
        }), resp.status_code
 
    except requests.exceptions.ConnectionError:
        return jsonify({"error": "No se puede conectar con EV_Central"}), 502
    except requests.exceptions.Timeout:
        return jsonify({"error": "EV_Central no respondió a tiempo"}), 504
 
# ==============================
# ARRANQUE
# ==============================
 
def start_api(host="0.0.0.0", port=8080):
    app.run(host=host, port=port, debug=True, use_reloader=False)
 
if __name__ == "__main__":
    start_api()
 