from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import uvicorn
import time
import jwt
from cryptography.fernet import Fernet
import os
import json

# =========================
# CONFIGURACIÓN
# =========================

SECRET_KEY = "evregistry2425"   # Clave para firmar JWT
CLAVES_DIR = "claves"            # Directorio claves AES
DB_FILE = "db.json"       # Archivo JSON como base de datos

# =========================
# APP FASTAPI
# =========================

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"]
)

# =========================
# MODELOS
# =========================

class CPRegistro(BaseModel):
    id: str
    location: str

class CPId(BaseModel):
    id: str

# =========================
# UTILIDADES JSON
# =========================

def cargar_db():
    if not os.path.exists(DB_FILE):
        return {"cps": [], "clientes": [], "climas": []}
    with open(DB_FILE, "r") as f:
        data = json.load(f)
    if "cps" not in data or not isinstance(data["cps"], list):
        data["cps"] = []
    return data


def guardar_db(db):
    with open(DB_FILE, "w") as f:
        json.dump(db, f, indent=4)

def buscar_cp(db, id_cp: str):
    """Devuelve el dict del CP con ese id, o None si no existe."""
    return next((cp for cp in db["cps"] if cp["id"] == id_cp), None)

# =========================
# UTILIDADES SEGURIDAD
# =========================

def crear_y_guardar_clave_aes(id_cp: str) -> str:
    os.makedirs(CLAVES_DIR, exist_ok=True)
    ruta = f"{CLAVES_DIR}/{id_cp}.key"

    if not os.path.exists(ruta):
        clave = Fernet.generate_key()
        with open(ruta, 'wb') as f:
            f.write(clave)
    else:
        with open(ruta, 'rb') as f:
            clave = f.read()

    return clave.decode()


def generar_token(id_cp: str) -> str:
    payload = {
        "id": id_cp,
        "exp": time.time() + 300
    }
    token = jwt.encode(payload, SECRET_KEY, algorithm="HS256")
    if isinstance(token, bytes):
        token = token.decode()
    return token

# =========================
# API REST - REGISTRY
# =========================

@app.put("/register", status_code=201)
def registrar_cp(cp: CPRegistro):
    db = cargar_db()
    if buscar_cp(db, cp.id):
        raise HTTPException(status_code=409, detail="CP ya registrado")
    
    clave_aes = crear_y_guardar_clave_aes(cp.id)
    token = generar_token(cp.id)

    db["cps"].append({
        "id": cp.id,
        "location": cp.location,
        "registrado": True,
        "estado":"OFFLINE",
        "token": token,
        "aes_key": clave_aes
    })
    guardar_db(db)


    return {
        "status": "registrado",
        "id": cp.id,
        "token": token,
        "aes_key": clave_aes
    }


@app.delete("/unregister")
def eliminar_cp(cp: CPId):
    db = cargar_db()
    entrada = buscar_cp(db, cp.id)
    if not entrada:
        raise HTTPException(status_code=404, detail="CP no registrado")

    db["cps"].remove(entrada)
    guardar_db(db)

    return {"message": f"CP {cp.id} eliminado correctamente"}


@app.post("/token")
def emitir_token(cp: CPId):
    db = cargar_db()
    entrada = buscar_cp(db, cp.id)
    print(entrada)
    if not entrada:
        raise HTTPException(status_code=403, detail="CP no registrado")

    token = generar_token(cp.id)
    entrada["token"] = token
    guardar_db(db)
    
    return {"token": token, "aes_key": entrada.get("aes_key", "")}


@app.get("/cp/{id}")
def consultar_cp(id: str):
    db = cargar_db()
    entrada = buscar_cp(db, id)
    if not entrada:
        raise HTTPException(status_code=404, detail="CP no registrado")
    return entrada

# =========================
# MAIN
# =========================

if __name__ == "__main__":
    os.makedirs(CLAVES_DIR, exist_ok=True)
    if not os.path.exists(DB_FILE):
        guardar_db({"cps": []})

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=9100,
    )
