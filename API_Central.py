from flask import Flask, jsonify, request
from flask_cors import CORS
import time

# =========================================================
# CONFIGURACIÓN
# =========================================================

app = Flask(__name__)
CORS(app)

# =========================================================
# ESTADO COMPARTIDO (lo rellena EV_Central)
# =========================================================

CPS = []                    # [{id, localizacion, estado}]
DRIVERS = []                # [{id, estado, cp_asignado}]
TRANSACTIONS = []           # [{driver, cp, inicio, estado}]
CLIMATE = {}                # localizacion -> {estado, temperatura}
ERRORS = []                 # mensajes de error
AUDIT = []                  # auditoría

# =========================================================
# UTILIDADES
# =========================================================

def audit(action, description, origin="api"):
    evento = {
        "fecha_hora": time.strftime("%Y-%m-%d %H:%M:%S"),
        "origen": origin,
        "accion": action,
        "descripcion": description
    }
    AUDIT.append(evento)

# =========================================================
# ENDPOINTS DE CONSULTA (FRONT / COMPONENTES)
# =========================================================

@app.route("/estado", methods=["GET"])
def estado_general():
    """
    Estado global del sistema
    """
    return jsonify({
        "cps": CPS,
        "drivers": DRIVERS,
        "transacciones": TRANSACTIONS,
        "clima": CLIMATE,
        "errores": ERRORS
    })


@app.route("/cps", methods=["GET"])
def estado_cps():
    return jsonify(CPS)


@app.route("/drivers", methods=["GET"])
def estado_drivers():
    return jsonify(DRIVERS)


@app.route("/transacciones", methods=["GET"])
def estado_transacciones():
    return jsonify(TRANSACTIONS)


@app.route("/auditoria", methods=["GET"])
def obtener_auditoria():
    return jsonify(AUDIT[::-1])

# =========================================================
# ENDPOINTS CLIMA (EV_W → CENTRAL)
# =========================================================

@app.route("/clima", methods=["PUT"])
def alerta_climatica():
    """
    EV_W notifica alerta o estado normal
    """
    data = request.json
    localizacion = data.get("localizacion")
    estado = data.get("estado")            # OK | KO
    temperatura = data.get("temperatura")

    if not localizacion or estado not in ("OK", "KO"):
        ERRORS.append("Petición clima inválida")
        return jsonify({"error": "datos inválidos"}), 400

    CLIMATE[localizacion] = {
        "estado": estado,
        "temperatura": temperatura
    }

    audit(
        "CLIMA_ALERTA" if estado == "KO" else "CLIMA_OK",
        f"{localizacion} -> {estado} ({temperatura}°C)",
        origin=request.remote_addr
    )

    return jsonify({"resultado": "OK"})


@app.route("/clima/<localizacion>", methods=["DELETE"])
def cancelar_alerta(localizacion):
    """
    Cancelar alerta climatológica
    """
    if localizacion in CLIMATE:
        del CLIMATE[localizacion]
        audit(
            "CLIMA_CANCELADA",
            f"Alerta cancelada en {localizacion}",
            origin=request.remote_addr
        )
        return jsonify({"resultado": "OK"})

    return jsonify({"error": "localizacion no encontrada"}), 404

# =========================================================
# ENDPOINTS DE ERRORES
# =========================================================

@app.route("/errores", methods=["GET"])
def obtener_errores():
    return jsonify(ERRORS)

# =========================================================
# MAIN
# =========================================================

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, debug=True)
