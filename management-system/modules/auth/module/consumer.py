import os
import json
import threading
import jwt
import datetime
from jwt.exceptions import ExpiredSignatureError, InvalidTokenError

from uuid import uuid4
from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver

SECRET_KEY = "supersecretkey"  # Используйте из config.ini
ALGORITHM = "HS256"

MODULE_NAME: str = os.getenv("MODULE_NAME")

#def auth(data):

def send_to_sender_car(event_details):
    event_details["deliver_to"] = "sender-car"
    proceed_to_deliver(event_details)

def create_token(user_id, role, expiration_minutes=30):
    payload = {
        "user_id": user_id,
        "role": role,
        "exp": datetime.datetime.utcnow() + datetime.timedelta(minutes=expiration_minutes)
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def verify_token(token):
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except ExpiredSignatureError:
        raise ValueError("Token has expired")
    except InvalidTokenError:
        raise ValueError("Invalid token")

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

    return send_to_sender_car(event_details)


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
