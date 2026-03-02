#!/usr/bin/env python3
"""
event-merger.py
Скрипт для группировки событий Frigate, скачивания видео и объединения в общие клипы.
Использует аппаратное кодирование h264_nvenc (доступно в контейнере).
"""

import os
import sys
import json
import time
import logging
import subprocess
import threading
import queue
import glob
import shutil
from datetime import datetime

import requests
import paho.mqtt.client as mqtt

# ========== КОНСТАНТЫ ==========
# Используем IP сервера для доступа из контейнера
FRIGATE_API_URL = "http://192.168.0.226:5000"
MQTT_BROKER = "192.168.0.226"
MQTT_TOPIC = "frigate/events"
MQTT_PORT = 1883
MQTT_USER = "frigate"
MQTT_PASS = "frigate"

# Директории
BASE_DIR = "/app"  # рабочая директория в контейнере
NEW_EVENT_DIR = os.path.join(BASE_DIR, "new_event")
SEND_DIR = os.path.join(BASE_DIR, "send")
TEMP_DIR = os.path.join(BASE_DIR, "temp")

# Параметры обработки видео
GROUP_TIMEOUT = 60          # секунд ожидания новых событий
TARGET_WIDTH = 1280
TARGET_HEIGHT = 720
TARGET_FPS = 20
MAX_SEGMENT_SIZE_MB = 40
MAX_SEGMENT_SIZE_BYTES = MAX_SEGMENT_SIZE_MB * 1024 * 1024

# Параметры кодирования для nvenc
VIDEO_BITRATE = "5M"
NVENC_PRESET = "p4"

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("event-merger")

# ========== ИНИЦИАЛИЗАЦИЯ ДИРЕКТОРИЙ ==========
os.makedirs(NEW_EVENT_DIR, exist_ok=True)
os.makedirs(SEND_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

# Очередь событий от MQTT
event_queue = queue.Queue()

# ========== ФУНКЦИИ РАБОТЫ С FRIGATE ==========
def download_event_video(event_id: str, camera: str) -> str | None:
    """Скачивает видео события из Frigate. Возвращает путь к сохранённому файлу или None при ошибке."""
    url = f"{FRIGATE_API_URL}/api/events/{event_id}/clip.mp4"
    try:
        resp = requests.get(url, stream=True, timeout=30)
        resp.raise_for_status()
    except Exception as e:
        logger.error(f"Ошибка запроса видео для события {event_id}: {e}")
        return None

    # Генерируем уникальное имя файла
    timestamp = int(time.time())
    filename = f"{event_id}_{camera}_{timestamp}.mp4"
    filepath = os.path.join(NEW_EVENT_DIR, filename)

    try:
        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)
        logger.info(f"Скачано видео события {event_id} -> {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"Ошибка сохранения видео {event_id}: {e}")
        return None


# ========== ФУНКЦИИ ОБРАБОТКИ ВИДЕО ==========
def create_merged_video(file_list, output_path):
    """
    Объединяет несколько видеофайлов в один с перекодированием в 720p/20fps.
    Использует аппаратное кодирование h264_nvenc (доступно в контейнере).
    Возвращает True при успехе.
    """
    # Создаём файл списка для ffmpeg concat
    list_file = os.path.join(TEMP_DIR, f"filelist_{int(time.time())}.txt")
    try:
        with open(list_file, "w") as f:
            for path in file_list:
                f.write(f"file '{path}'\n")
    except Exception as e:
        logger.error(f"Не удалось создать список файлов: {e}")
        return False

    # Формируем команду ffmpeg с NVENC
    cmd = [
        "ffmpeg",
        "-f", "concat",
        "-safe", "0",
        "-i", list_file,
        "-vf", f"scale={TARGET_WIDTH}:{TARGET_HEIGHT},fps={TARGET_FPS}",
        "-c:v", "h264_nvenc",
        "-preset", NVENC_PRESET,
        "-b:v", VIDEO_BITRATE,
        "-maxrate", VIDEO_BITRATE,
        "-bufsize", "10M",
        "-rc", "vbr",
        "-cq", "23",
        "-c:a", "aac",
        "-b:a", "128k",
        "-movflags", "+faststart",
        "-y",
        output_path
    ]

    logger.info(f"Запуск объединения видео: {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            logger.error(f"ffmpeg ошибка: {result.stderr}")
            return False
        logger.info(f"Объединённое видео создано: {output_path}")
        return True
    except subprocess.TimeoutExpired:
        logger.error("Превышено время ожидания ffmpeg")
        return False
    except Exception as e:
        logger.error(f"Ошибка запуска ffmpeg: {e}")
        return False
    finally:
        # Удаляем временный список
        try:
            os.unlink(list_file)
        except:
            pass


def split_video_by_size(input_path, output_dir, base_name, max_size_bytes):
    """
    Разбивает видео на части размером не более max_size_bytes.
    Использует ffmpeg с segment и перекодированием (не copy, чтобы избежать проблем с ключевыми кадрами).
    Возвращает список созданных файлов.
    """
    pattern = os.path.join(output_dir, f"{base_name}_part_%03d.mp4")
    cmd = [
        "ffmpeg",
        "-i", input_path,
        "-map", "0",
        "-c:v", "h264_nvenc",           # перекодируем видео
        "-preset", NVENC_PRESET,
        "-b:v", VIDEO_BITRATE,
        "-maxrate", VIDEO_BITRATE,
        "-bufsize", "10M",
        "-rc", "vbr",
        "-cq", "23",
        "-c:a", "aac",                   # перекодируем аудио
        "-b:a", "128k",
        "-f", "segment",
        "-segment_time", "60",           # базовое время, реально ограничим размером
        "-fs", str(max_size_bytes),      # ограничение размера на сегмент
        "-reset_timestamps", "1",
        "-segment_format", "mp4",
        "-y",
        pattern
    ]
    logger.info(f"Разбиение видео на части (с перекодированием): {' '.join(cmd)}")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=600)
        if result.returncode != 0:
            logger.error(f"Ошибка разбиения: {result.stderr}")
            return []
        part_files = sorted(glob.glob(os.path.join(output_dir, f"{base_name}_part_*.mp4")))
        logger.info(f"Создано {len(part_files)} частей")
        return part_files
    except Exception as e:
        logger.error(f"Исключение при разбиении: {e}")
        return []


def process_batch(file_paths):
    """
    Обрабатывает группу видеофайлов:
      - объединяет их в одно видео (720p, 20fps)
      - если размер >40 МБ, разбивает на части (с перекодированием)
      - перемещает готовые файлы в SEND_DIR
      - удаляет исходные файлы
    """
    if not file_paths:
        logger.warning("process_batch вызван с пустым списком")
        return

    logger.info(f"Обработка пачки из {len(file_paths)} файлов")

    # Создаём уникальное имя для объединённого видео
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    merged_name = f"merged_{timestamp}.mp4"
    merged_path = os.path.join(TEMP_DIR, merged_name)

    # 1. Объединение
    if not create_merged_video(file_paths, merged_path):
        logger.error("Не удалось создать объединённое видео. Пачка пропущена.")
        return

    # 2. Проверка размера
    file_size = os.path.getsize(merged_path)
    logger.info(f"Размер объединённого видео: {file_size / (1024*1024):.2f} МБ")

    if file_size <= MAX_SEGMENT_SIZE_BYTES:
        # Просто перемещаем в SEND
        dest_path = os.path.join(SEND_DIR, merged_name)
        shutil.move(merged_path, dest_path)
        logger.info(f"Видео перемещено в {dest_path}")
        result_files = [dest_path]
    else:
        # Разбиваем на части
        base_name = f"merged_{timestamp}"
        part_files = split_video_by_size(merged_path, SEND_DIR, base_name, MAX_SEGMENT_SIZE_BYTES)
        if not part_files:
            logger.error("Не удалось разбить видео, файл остаётся во временной папке")
            # Перемещаем как есть (может не отправиться из-за размера)
            shutil.move(merged_path, os.path.join(SEND_DIR, merged_name))
            logger.warning(f"Видео превышает {MAX_SEGMENT_SIZE_MB} МБ, но разбить не удалось. Отправлено как есть.")
        else:
            # Удаляем объединённый файл, оставляем части
            os.unlink(merged_path)
            result_files = part_files

    # 3. Удаляем исходные файлы из new_event
    for fp in file_paths:
        try:
            os.unlink(fp)
            logger.debug(f"Удалён исходный файл {fp}")
        except Exception as e:
            logger.warning(f"Не удалось удалить {fp}: {e}")

    logger.info(f"Обработка пачки завершена, готово файлов: {len(result_files) if 'result_files' in locals() else 1}")


# ========== ПОТОК-ОБРАБОТЧИК ==========
def worker_loop():
    """Основной цикл обработки событий из очереди."""
    logger.info("Worker thread started")
    while True:
        # Ждём первое событие (блокирующее получение)
        try:
            event_data = event_queue.get(block=True)
        except Exception as e:
            logger.error(f"Ошибка получения из очереди: {e}")
            continue

        # Начинаем новую сессию
        session_files = []
        logger.info("Начало новой сессии группировки")

        # Обрабатываем первое событие
        try:
            event_id = event_data["after"]["id"]
            camera = event_data["after"]["camera"]
        except (KeyError, TypeError) as e:
            logger.error(f"Не удалось извлечь id/camera из события: {e}, событие пропущено")
            continue

        file_path = download_event_video(event_id, camera)
        if file_path:
            session_files.append(file_path)

        # Цикл ожидания дополнительных событий с таймаутом
        while True:
            try:
                # Ждём следующее событие не более GROUP_TIMEOUT секунд
                next_event = event_queue.get(block=True, timeout=GROUP_TIMEOUT)
            except queue.Empty:
                # Таймаут – пачка готова
                logger.info("Таймаут ожидания новых событий, обрабатываем пачку")
                break
            except Exception as e:
                logger.error(f"Ошибка при получении из очереди: {e}")
                break

            # Получили новое событие – скачиваем и добавляем в сессию
            try:
                eid = next_event["after"]["id"]
                cam = next_event["after"]["camera"]
            except (KeyError, TypeError) as e:
                logger.error(f"Некорректное событие пропущено: {e}")
                continue

            fpath = download_event_video(eid, cam)
            if fpath:
                session_files.append(fpath)
            # Таймер сброшен, продолжаем ожидание

        # Здесь вышли по таймауту, обрабатываем собранные файлы
        if session_files:
            process_batch(session_files)
        else:
            logger.warning("Сессия не содержала ни одного файла")


# ========== MQTT CALLBACKS ==========
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info("Подключено к MQTT брокеру")
        client.subscribe(MQTT_TOPIC)
    else:
        logger.error(f"Ошибка подключения MQTT, код {rc}")


def on_message(client, userdata, msg):
    try:
        payload = msg.payload.decode("utf-8")
        data = json.loads(payload)
        logger.debug(f"Получено MQTT сообщение: {data.get('type')} для {data.get('after',{}).get('camera')}")
        event_queue.put(data)
    except json.JSONDecodeError:
        logger.error("Не удалось распарсить JSON сообщение")
    except Exception as e:
        logger.error(f"Ошибка обработки MQTT сообщения: {e}")


# ========== ЗАПУСК ==========
def main():
    # Запускаем поток-обработчик
    worker_thread = threading.Thread(target=worker_loop, daemon=True)
    worker_thread.start()

    # Настройка MQTT клиента
    client = mqtt.Client()
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
    except Exception as e:
        logger.error(f"Не удалось подключиться к MQTT брокеру: {e}")
        sys.exit(1)

    logger.info("Запуск MQTT loop")
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        logger.info("Получен сигнал завершения")
        client.disconnect()
    finally:
        logger.info("Выход")


if __name__ == "__main__":
    main()