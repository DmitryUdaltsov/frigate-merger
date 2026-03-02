FROM nvidia/cuda:12.3.2-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        python3 \
        python3-pip \
        ffmpeg \
    && \
    rm -rf /var/lib/apt/lists/*

RUN pip3 install --no-cache-dir 'requests[socks]' paho-mqtt

WORKDIR /app
COPY event-merger.py config.py ./

CMD ["python3", "-u", "event-merger.py"]