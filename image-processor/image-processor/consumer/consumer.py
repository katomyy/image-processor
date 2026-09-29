import pika
import os
import base64
import io
from PIL import Image
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

queue_name = 'image_queue'
channel.queue_declare(queue=queue_name)
channel.queue_bind(exchange=exchange_name, queue=queue_name, routing_key='image.process')

OUTPUT_DIR = '/usr/src/app/consumer/output'
os.makedirs(OUTPUT_DIR, exist_ok=True)


def send_status(status, filename, message=""):
    status_msg = json.dumps({
        "status": status,
        "filename": filename,
        "message": message
    })
    channel.basic_publish(
        exchange=exchange_name,
        routing_key='image.status',
        body=status_msg
    )


def callback(ch, method, properties, body):
    filename = properties.headers.get('filename', 'unknown.jpg') if properties.headers else 'unknown.jpg'
    print(f"Received: {filename}", flush=True)

    ext = os.path.splitext(filename)[1].lower()
    if ext == '.png':
        mime_type = 'image/png'
    elif ext in ['.jpg', '.jpeg']:
        mime_type = 'image/jpeg'
    else:
        mime_type = 'image/jpeg'

    try:
        original_b64 = f"data:{mime_type};base64,{base64.b64encode(body).decode('utf-8')}"
        image = Image.open(io.BytesIO(body))
        image.thumbnail((200, 200))
        buffered = io.BytesIO()
        image.convert("RGB").save(buffered, format="JPEG", quality=20)
        compressed_b64 = f"data:image/jpeg;base64,{base64.b64encode(buffered.getvalue()).decode('utf-8')}"

        html_content = f"""
        <html>
        <head>
            <title>Сравнение: Оригинал и Сжатие</title>
            <style>
                body {{ font-family: Arial, sans-serif; text-align: center; background-color: #f0f0f0; }}
                h1 {{ color: #333; }}
                .container {{ display: flex; justify-content: center; gap: 30px; margin-top: 20px; flex-wrap: wrap; }}
                .box {{ border: 1px solid #ccc; padding: 15px; border-radius: 8px; background: white; box-shadow: 0 4px 8px rgba(0,0,0,0.1); width: 450px; }}
                img {{ max-width: 100%; height: auto; border-radius: 4px; }}
                .label {{ font-weight: bold; margin-bottom: 10px; color: #555; font-size: 1.1em; }}
            </style>
        </head>
        <body>
            <h1>Файл: {filename}</h1>
            <div class="container">
                <div class="box">
                    <div class="label">Оригинал</div>
                    <img src="{original_b64}" alt="Original">
                </div>
                <div class="box">
                    <div class="label">Сжатая</div>
                    <img src="{compressed_b64}" alt="Compressed">
                </div>
            </div>
        </body>
        </html>
        """
        output_filename = os.path.splitext(filename)[0] + ".html"
        output_path = os.path.join(OUTPUT_DIR, output_filename)
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write(html_content)

        print(f"Processed and saved: {output_filename}", flush=True)
        send_status("SUCCESS", filename)

    except Exception as e:
        print(f"Error processing {filename}: {e}", flush=True)
        send_status("ERROR", filename, str(e))

    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(queue=queue_name, on_message_callback=callback)

print('Consumer waiting for images. To exit press CTRL+C', flush=True)
channel.start_consuming()
