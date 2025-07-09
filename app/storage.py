# storage.py

import json
import requests
from pymongo import MongoClient
from app.utils import has_internet

# URL para token y backend
KEYCLOAK_URL = "https://auth.llantatech.org.pe/realms/rutakids_llanta/protocol/openid-connect/token"
CLIENT_ID = "ruta-kids-back-service"
CLIENT_SECRET = "7iMqszFKAQve4X2HnRQ4Cbdb13RlcRjB"
BACKEND_URL = "https://api.rutakids.llantatech.org.pe/api/v1/iot"

_mongo_client = None


def get_mongo_connection():
    global _mongo_client
    if _mongo_client is None:
        _mongo_client = MongoClient("mongodb://mongo-egde:27017/")
    return _mongo_client


mongo_client = get_mongo_connection()
mongo_db = mongo_client["edge_db"]
mongo_col = mongo_db["eventos_pendientes"]


def save_to_mongo(data):
    try:
        mongo_col.insert_one(data)
        print("[MONGO] Guardado localmente:", data)
    except Exception as e:
        print("[MONGO] Error al guardar:", e)


def resend_from_mongo(token):
    if not has_internet():
        print("[REINTENTO] No hay conexión a internet")
        return

    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    docs = list(mongo_col.find())

    for doc in docs:
        doc_id = doc["_id"]
        try:
            payload = {k: v for k, v in doc.items() if k != "_id"}
            r = requests.post(BACKEND_URL, json=payload, headers=headers)
            if r.status_code == 200:
                mongo_col.delete_one({"_id": doc_id})
                print("[REINTENTO] Enviado y eliminado de Mongo:", payload)
            else:
                print("[REINTENTO] Error del backend:", r.status_code, r.text)
        except Exception as e:
            print("[REINTENTO] Error al reenviar:", e)


def get_token():
    try:
        response = requests.post(KEYCLOAK_URL, data={
            'grant_type': 'client_credentials',
            'client_id': CLIENT_ID,
            'client_secret': CLIENT_SECRET
        })

        if response.ok:
            token = response.json()['access_token']
            print("[TOKEN] Token:", token)
            return token
        else:
            print("[TOKEN] Error al obtener token:", response.text)
            return None
    except Exception as e:
        print("[TOKEN] Error en conexión:", e)
        return None


def send_to_backend(data, token):
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        r = requests.post(BACKEND_URL, json=data, headers=headers)
        if r.status_code == 200:
            print("[BACKEND] Enviado correctamente")
            return True
        else:
            print("[BACKEND] Error:", r.status_code, r.text)
            return False
    except Exception as e:
        print("[BACKEND] Fallo al conectar:", e)
        return False
