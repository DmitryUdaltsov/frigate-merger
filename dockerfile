FROM python:3.11-slim

RUN apt-get update \
 && apt-get install -y --no-install-recommends ffmpeg \
 && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir paho-mqtt requests

WORKDIR /app
COPY event-merger.py .

CMD ["python", "-u", "event-merger.py"]
