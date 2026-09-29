import pika
import os
import time

amqp_url = os.environ.get(
    'RABBITMQ_URL',
    'amqp://guest:guest@rabbit_mq?connection_attempts=10&retry_delay=10'
)

IMAGE_DIR = '/usr/src/app/producer/images'
SENT_LOG = '/usr/src/app/producer/sent_files.txt'
EXCHANGE_NAME = 'image_topic'

connection = None
channel = None


def get_sent_files():
    if os.path.exists(SENT_LOG):
        with open(SENT_LOG, 'r') as f:
            return set(line.strip() for line in f if line.strip())
    return set()


def mark_as_sent(filename, sent_set):
    sent_set.add(filename)
    with open(SENT_LOG, 'a') as f:
        f.write(filename + '\n')
        f.flush()
        os.fsync(f.fileno())


def connect():
    """Устанавливает соединение с RabbitMQ. Возвращает True, если удалось."""
    global connection, channel
    try:
        if connection and connection.is_open:
            return True
        url_params = pika.URLParameters(amqp_url)
        connection = pika.BlockingConnection(url_params)
        channel = connection.channel()
        channel.exchange_declare(exchange=EXCHANGE_NAME, exchange_type='topic')
        print("[Producer] Connected to RabbitMQ", flush=True)
        return True
    except Exception as e:
        print(f"[Producer] Connection failed: {e}", flush=True)
        connection = None
        channel = None
        return False


print("Waiting 20 seconds for consumers to be ready...", flush=True)
time.sleep(20)

sent_files = get_sent_files()
print("Producer started. Watching folder:", IMAGE_DIR, flush=True)
print("Already sent:", sent_files, flush=True)

# Ждём, пока установится соединение
while not connect():
    print("Waiting for RabbitMQ...", flush=True)
    time.sleep(5)

try:
    while True:
        # Проверяем соединение перед каждой итерацией
        if not connect():
            time.sleep(5)
            continue

        if os.path.exists(IMAGE_DIR):
            files = [
                f for f in os.listdir(IMAGE_DIR)
                if f.lower().endswith(('.png', '.jpg', '.jpeg'))
            ]
            for filename in files:
                if filename not in sent_files:
                    filepath = os.path.join(IMAGE_DIR, filename)
                    try:
                        with open(filepath, 'rb') as f:
                            image_bytes = f.read()
                        channel.basic_publish(
                            exchange=EXCHANGE_NAME,
                            routing_key='image.process',
                            body=image_bytes,
                            properties=pika.BasicProperties(
                                headers={'filename': filename}
                            )
                        )
                        print(f"Sent: {filename}", flush=True)
                        mark_as_sent(filename, sent_files)
                    except Exception as e:
                        print(f"Error sending {filename}: {e}", flush=True)
                        # Соединение, скорее всего, разорвалось — сбросим
                        connection = None
                        channel = None
                        break

        time.sleep(5)

except KeyboardInterrupt:
    print("\nStopped by user.")
finally:
    if connection and connection.is_open:
        connection.close()