import pika
import os
import json

amqp_url = os.environ.get(
    'RABBITMQ_URL',
    'amqp://guest:guest@rabbit_mq?connection_attempts=10&retry_delay=10'
)
url_params = pika.URLParameters(amqp_url)
connection = pika.BlockingConnection(url_params)
channel = connection.channel()

exchange_name = 'image_topic'
channel.exchange_declare(exchange=exchange_name, exchange_type='topic')

queue_name = 'status_queue'
channel.queue_declare(queue=queue_name)
channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key='image.status')


def callback(ch, method, properties, body):
    try:
        data = json.loads(body)
        status = data.get("status")
        filename = data.get("filename")
        message = data.get("message", "")

        if status == "SUCCESS":
            print(f"[OK] Image {filename} successfully processed and saved to HTML.", flush=True)
        elif status == "ERROR":
            print(f"[FAIL] Error processing {filename}. Reason: {message}", flush=True)
        else:
            print(f"[UNKNOWN] {data}", flush=True)
    except Exception as e:
        print(f"Error parsing status message: {e}", flush=True)

    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_consume(queue=queue_name, on_message_callback=callback)

print('Consumer2 (Monitor) started. Waiting for statuses...', flush=True)
channel.start_consuming()
