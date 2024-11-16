import os
import json
import threading
import multiprocessing

from confluent_kafka import Consumer, OFFSET_BEGINNING

from .producer import proceed_to_deliver


_response_queue: multiprocessing.Queue = None
MODULE_NAME = os.getenv("MODULE_NAME")


def handle_event(event_id, event_details_json):
    global _response_queue

    """ Обработчик входящих в модуль задач. """
    event_details = json.loads(event_details_json)

    source: str = event_details.get("source")
    deliver_to: str = event_details.get("deliver_to")
    data: str = event_details.get("data")
    operation: str = event_details.get("operation")
    
    print(f"[info] handling event {event_id}, "
          f"{source}->{deliver_to}: {operation},"
          f"data: {data}")


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


def start_consumer(args, config, response_queue):
    global _response_queue
    _response_queue = response_queue

    print(f'{MODULE_NAME}_consumer started')

    threading.Thread(target=lambda: consumer_job(args, config)).start()
