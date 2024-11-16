import os
import json
import threading

from shapely.geometry import Point, Polygon
from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver

GEOFENCE_POLYGON = Polygon([
    (37.6173, 55.7558),  # Москва
    (37.6200, 55.7500),
    (37.6300, 55.7600),
    (37.6173, 55.7558)
])

MODULE_NAME: str = os.getenv("MODULE_NAME")

def check_speed_and_coor(speed, coordinates):
    if speed > 200:
        return "stop"
    
def is_within_geofence(latitude, longitude):
    point = Point(latitude, longitude)
    return GEOFENCE_POLYGON.contains(point)

def send_to_manage_drive(event_details):
    event_details["deliver_to"] = "manage-drive"
    proceed_to_deliver(event_details)

def send_to_sender_car(event_details):
    event_details["deliver_to"] = "sender-car"
    proceed_to_deliver(event_details)

def handle_event(event_id, event_details_json):
    """ Обработчик входящих в модуль задач. """
    event_details = json.loads(event_details_json)

    source: str = event_details.get("source")
    deliver_to: str = event_details.get("deliver_to")
    data: str = event_details.get("data")
    operation: str = event_details.get("operation")

    print(f"[info] handling event {event_id}, "
          f"{source}->{deliver_to}: {operation}")

    if operation != "telemetry":
        return send_to_manage_drive(event_details)
    else:
        speed = data.get('speed')
        coordinates = data.get('coordinates')
        command = check_speed_and_coor(speed, coordinates)
        if command == "stop":
            event_details["operation"] = "stop"
            return send_to_sender_car(event_details)
        else:
            return send_to_manage_drive(event_details)

def consumer_job(args, config):
    consumer = Consumer(config)

    def reset_offset(verifier_consumer, partitions):
        if not args.reset:
            return

        for p in partitions:
            p.offset = OFFSET_BEGINNING
        verifier_consumer.assign(partitions)

    topic = MODULE_NAME
    consumer.subscribe([topic], on_assign=reset_offset)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                pass
            elif msg.error():
                print(f"[error] {msg.error()}")
            else:
                try:
                    event_id = msg.key().decode('utf-8')
                    event_details_json = msg.value().decode('utf-8')
                    handle_event(event_id, event_details_json)
                except Exception as e:
                    print(f"[error] Malformed event received from " \
                          f"topic {topic}: {msg.value()}. {e}")
    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()

def start_consumer(args, config):
    print(f'{MODULE_NAME}_consumer started')
    threading.Thread(target=lambda: consumer_job(args, config)).start()
