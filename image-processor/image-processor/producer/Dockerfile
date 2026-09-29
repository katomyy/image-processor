FROM python:3.11-slim-bookworm
WORKDIR /usr/src/app/producer
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY . .
CMD ["python", "producer.py"]
