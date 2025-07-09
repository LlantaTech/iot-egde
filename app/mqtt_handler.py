# mqtt_handler.py

import json

import paho.mqtt.client as mqtt

from app.utils import has_internet
from app.storage import get_mongo_connection, get_token, send_to_backend, save_to_mongo

BROKER = "mosquitto.llantatech.org.pe"
PORT = 1883
TOPICS = [
    ("sensors/temperatura/#", 0),
    ("passengers/request/#", 0),
    ("transport/events/#", 0),
    ("tracking/gps/#", 0)
]

client = mqtt.Client()

BACKEND_URL = "https://api.rutakids.llantatech.org.pe/api/v1"

# Obtener conexión MongoDB centralizada
mongo_client = get_mongo_connection()
mongo_db = mongo_client["edge_db"]
mongo_col_kids = mongo_db["ultima_lista_pasajeros"]

def get_passengers_from_backup():
    try:
        doc = mongo_col_kids.find_one()
        if doc and "children" in doc:
            print("[MONGO] Usando lista de respaldo local.")
            return doc["children"]
        else:
            print("[MONGO] No hay lista local de respaldo disponible.")
            return []
    except Exception as e:
        print("[MONGO] Error accediendo a respaldo:", e)
        return []

def handle_passenger_request(device_id):
    pasajeros = get_passengers_from_backup()
    topic = f"passengers/list/{device_id}"
    payload = json.dumps(pasajeros)
    client.publish(topic, payload, qos=1, retain=True)
    print(f"[REPLY] Lista de respaldo enviada a {device_id}")

def handle_generic_data(topic, payload):
    data = {
        "topic": topic,
        "payload": payload
    }
    print(data)
    if has_internet():
        token = get_token()
        success = send_to_backend(data, token)
        if not success:
            save_to_mongo(data)
    else:
        save_to_mongo(data)

def on_connect(client, userdata, flags, rc):
    print("[MQTT] Conectado con código:", rc)
    for topic in TOPICS:
        client.subscribe(topic)
        print(f"[MQTT] Suscrito a {topic[0]}")

def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = msg.payload.decode()
        # print(f"[MQTT] Mensaje en {topic}: {payload}")

        if topic.startswith("passengers/request/"):
            device_id = topic.split("/")[-1]
            if payload == "GET":
                handle_passenger_request(device_id)
        else:
            try:
                parsed_payload = json.loads(payload)
            except:
                parsed_payload = {"raw": payload}

            handle_generic_data(topic, parsed_payload)

    except Exception as e:
        print("[MQTT] Error procesando mensaje:", e)

def start_mqtt_client():
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(BROKER, PORT, 60)
    client.loop_forever()