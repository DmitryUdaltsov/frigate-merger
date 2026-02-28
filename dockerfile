FROM nvidia/cuda:12.3.2-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive

RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \
        ca-certificates \
        python3 \
        python3-pip \
        xz-utils \
    && \
    rm -rf /var/lib/apt/lists/*

# Скачивание и установка статического FFmpeg с поддержкой NVENC
RUN wget https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz && \
    mkdir -p /tmp/ffmpeg && \
    tar -xf ffmpeg-release-amd64-static.tar.xz -C /tmp/ffmpeg && \
    chmod +x /tmp/ffmpeg/*/ffmpeg /tmp/ffmpeg/*/ffprobe && \
    cp /tmp/ffmpeg/*/ffmpeg /tmp/ffmpeg/*/ffprobe /usr/local/bin/ && \
    rm -rf /tmp/ffmpeg ffmpeg-release-amd64-static.tar.xz

# Проверка версии FFmpeg
RUN ffmpeg -version

# Установка Python-зависимостей
RUN pip3 install --no-cache-dir paho-mqtt requests

# Рабочая директория и копирование скрипта
WORKDIR /app
COPY event-merger.py .

# Запуск скрипта
CMD ["python3", "-u", "event-merger.py"]