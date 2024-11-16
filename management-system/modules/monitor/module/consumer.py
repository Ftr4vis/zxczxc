import os
import json
import threading

from confluent_kafka import Consumer, OFFSET_BEGINNING

from .policies import check_operation
from .producer import proceed_to_deliver


MODULE_NAME = os.getenv('MODULE_NAME')


def handle_event(event_id, event_details_json):
    event_details = json.loads(event_details_json)

    print(f"[info] handling event {event_id}, " \
          f"{event_details['source']}->{event_details['deliver_to']}: " \
          f"{event_details['operation']}")

    if check_operation(event_id, event_details):
        return proceed_to_deliver(event_details)

    print(f"[error] !!!! policies check failed, delivery unauthorized !!! " \
          f"event_id: {event_id}, {event_details['source']}->{event_details['deliver_to']}: " \
          f"{event_details['operation']}")

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
                event_id = msg.key().decode('utf-8')
                event_details_json = msg.value().decode('utf-8')
                handle_event(event_id, event_details_json)

                # except Exception as e:
                #     print(f"[error] Malformed event received from " \
                #           f"topic {topic}: {msg.value()}, {e}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()


def start_consumer(args, config):
    print(f'{MODULE_NAME}_consumer started')
    threading.Thread(target=lambda: consumer_job(args, config)).start()
