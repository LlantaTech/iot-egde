# edge.py

import threading
import time

import schedule

from app.kafka_listener import start_kafka_listener
from app.mqtt_handler import start_mqtt_client
from app.storage import resend_from_mongo, get_token


def run_scheduler():
    schedule.every(30).seconds.do(lambda: resend_from_mongo(get_token()))
    while True:
        schedule.run_pending()
        time.sleep(1)


def main():
    threading.Thread(target=start_kafka_listener, daemon=True).start()
    threading.Thread(target=run_scheduler, daemon=True).start()
    start_mqtt_client()


if __name__ == '__main__':
    main()
