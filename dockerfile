FROM nvidia/cuda:12.3.2-runtime-ubuntu22.04

ENV DEBIAN_FRONTEND=noninteractive

# Установка зависимостей
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        wget \
        ca-certificates \
        python3 \
        python3-pip \
    && \
    rm -rf /var/lib/apt/lists/*

# Скачивание и установка статического FFmpeg (с NVENC поддержкой)
# Используем последнюю стабильную версию с https://johnvansickle.com/ffmpeg/
RUN wget https://johnvansickle.com/ffmpeg/releases/ffmpeg-git-amd64-static.tar.xz && \
    tar xJf ffmpeg-git-amd64-static.tar.xz --strip-components=1 -C /usr/local/bin/ \
        ffmpeg-git-*/ffmpeg \
        ffmpeg-git-*/ffprobe && \
    rm ffmpeg-git-amd64-static.tar.xz

# Проверка версии FFmpeg
RUN ffmpeg -version

# Установка Python-зависимостей
RUN pip3 install --no-cache-dir paho-mqtt requests

# Рабочая директория и копирование скрипта
WORKDIR /app
COPY event-merger.py .

# Запуск скрипта
CMD ["python3", "-u", "event-merger.py"]