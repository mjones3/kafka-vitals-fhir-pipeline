FROM python:3.11-slim

WORKDIR /app

# Install confluent-kafka runtime deps
RUN apt-get update && \
    apt-get install -y --no-install-recommends librdkafka-dev gcc && \
    rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY random_vitals_producer.py .
RUN chmod +x random_vitals_producer.py

CMD ["./random_vitals_producer.py"]
