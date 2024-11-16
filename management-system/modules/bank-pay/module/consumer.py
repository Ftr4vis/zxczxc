import os
import json
import threading
import requests
import multiprocessing
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver

PAYMENT_URL = 'http://payment_system:8000'
_response_queue: multiprocessing.Queue = None
MODULE_NAME = os.getenv("MODULE_NAME")


def send_to_profile_client(event_id, details):
    details["deliver_to"] = "profile-client"
    details["event_id"] = event_id
    proceed_to_deliver(event_id, details)


def create_payment(data):
    name = data[0]
    amount = data[1]
    response = requests.post(f'{PAYMENT_URL}/clients', json={'name': name})
    if response.status_code in (200, 201):
        response = requests.post(f'{PAYMENT_URL}/invoices', json={'client_id': response.json()[0]['id'], 'amount': amount})
        return response.json()


def create_prepayment(data):
    name = data[0]
    amount = data[1]
    response = requests.post(f'{PAYMENT_URL}/clients', json={'name': name})
    if response.status_code in (200, 201):
        response = requests.post(f'{PAYMENT_URL}/clients/{response.json()[0]['id']}/prepayment', json={'amount': amount})
        return response.json()


def handle_event(event_id, details_str):

    """ Обработчик входящих в модуль задач. """
    details = json.loads(details_str)

    source: str = details.get("source")
    deliver_to: str = details.get("deliver_to")
    data: str = details.get("data")
    operation: str = details.get("operation")

    print(f"[info] handling event {event_id}, "
          f"{source}->{deliver_to}: {operation}")

    if operation in ("get_prepayment_id", "get_payment_id"):
        details["data"] = create_prepayment(data)
        return send_to_profile_client(event_id, details)

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
                    details_str = msg.value().decode('utf-8')
                    handle_event(event_id, details_str)
                except Exception as e:
                    print(f"[error] Malformed event received from " \
                          f"topic {topic}: {msg.value()}. {e}")

    except KeyboardInterrupt:
        pass

    finally:
        consumer.close()


def start_consumer(args, config, response_queue):
    global _response_queue
    _response_queue = response_queue
    print(f'{MODULE_NAME}_consumer started')

    threading.Thread(target=lambda: consumer_job(args, config)).start()
