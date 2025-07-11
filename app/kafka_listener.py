# kafka_listener.py

import json
import threading

from kafka import KafkaConsumer

from app.storage import get_mongo_connection
from app.mqtt_handler import client as mqtt_client

KAFKA_BROKER = "kafka.llantatech.org.pe:9093"
KAFKA_TOPIC = "esp32-topic"
KAFKA_GROUP_ID = "rutakids-group"

# Lista de dispositivos conocidos
KNOWN_DEVICES = ["esp32-01", "esp32-02", "esp32-03"]

# Configuración MongoDB
mongo_client = get_mongo_connection()
mongo_db = mongo_client["edge_db"]
mongo_col_kids = mongo_db["ultima_lista_pasajeros"]


def guardar_lista_en_mongo(payload):
    mongo_col_kids.delete_many({})
    mongo_col_kids.insert_one({"children": payload})
    print("[MONGO] Última lista de niños guardada.")


def obtener_ultima_lista():
    doc = mongo_col_kids.find_one()
    return doc.get("children", []) if doc else []


def lista_ha_cambiado(nueva_lista):
    anterior = obtener_ultima_lista()
    return json.dumps(anterior, sort_keys=True) != json.dumps(nueva_lista, sort_keys=True)


def start_kafka_listener():
    def run():
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC,
                bootstrap_servers=[KAFKA_BROKER],
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset='latest',
                group_id=KAFKA_GROUP_ID
            )

            print("[KAFKA] Escuchando comandos desde Kafka...")
            for message in consumer:
                try:
                    data = message.value
                    jsonData = json.loads(data)
                    children = jsonData.get("children", [])

                    if not children:
                        print("[KAFKA] Mensaje sin 'children'. Ignorando.")
                        continue

                    payload = [
                        {"dni": c["dni"], "name": c["fullName"]}
                        for c in children
                    ]

                    if not lista_ha_cambiado(payload):
                        print("[KAFKA] Lista sin cambios. No se envía a MQTT.")
                        continue

                    guardar_lista_en_mongo(payload)

                    for device_id in KNOWN_DEVICES:
                        topic = f"passengers/list/{device_id}"
                        mqtt_client.publish(topic, json.dumps(payload), qos=1, retain=True)
                        print(f"[KAFKA → MQTT] Publicado en {topic}")

                except Exception as e:
                    print("[KAFKA] Error al procesar mensaje:", e)
        except Exception as e:
            print("[KAFKA] Error al conectar con Kafka:", e)

    threading.Thread(target=run, daemon=True).start()
