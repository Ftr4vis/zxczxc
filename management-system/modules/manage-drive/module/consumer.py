import os
import json
import threading

from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver


MODULE_NAME: str = os.getenv("MODULE_NAME")


def send_to_verify(event_details):
    event_details["deliver_to"] = "verify"
    proceed_to_deliver(event_details)


def send_to_profile_client(event_details):
    event_details["deliver_to"] = "profile-client"
    proceed_to_deliver(event_details)


def handle_event(event_id, event_details_json):
    """ Обработчик входящих в модуль задач. """
    event_details = json.loads(event_details_json)

    source: str = event_details.get("source")
    deliver_to: str = event_details.get("deliver_to")
    data: str = event_details.get("data")
    operation: str = event_details.get("operation")

    print(f"[info] handling event {event_id}, "
          f"{source}->{deliver_to}: {operation},"
          f"data: {data}")

    if operation == "get_cars":
        return send_to_verify(event_details)
    elif operation == "answer_cars":
        event_details["data"] = [car['brand'] for car in data if car['occupied_by'] is None]
        return send_to_profile_client(event_details)
    elif operation == "get_status":
        return send_to_verify(event_details)
    elif operation == "answer_status":
        return send_to_profile_client(event_details)
    elif operation == "access":
        return send_to_profile_client(event_details)
    elif operation == "confirm_access":
        return send_to_verify(event_details)
    elif operation == "return":
        return send_to_profile_client(event_details)
    elif operation == "telemetry":
        speed = data.get('speed')
        coordinates = data.get('coordinates')
        print(f"{event_details["car"]} Скорость: {speed:.2f} км/ч, Координаты: {coordinates}")


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
