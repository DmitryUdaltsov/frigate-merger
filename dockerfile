FROM nvidia/cuda:12.3.2-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y software-properties-common && \
    add-apt-repository ppa:jonathonf/ffmpeg-6 -y && \
    apt-get update && \
    apt-get install -y \
        python3 \
        python3-pip \
        ffmpeg \
    && \
    rm -rf /var/lib/apt/lists/*

RUN ffmpeg -version

RUN pip3 install --no-cache-dir paho-mqtt requests

WORKDIR /app
COPY event-merger.py .

CMD ["python3", "-u", "event-merger.py"]