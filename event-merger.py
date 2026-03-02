#!/usr/bin/env python3
"""
event-merger.py — объединяет клипы Frigate.
Использует архитектуру очереди + умную нормализацию.
"""

import os
import sys
import json
import time
import logging
import subprocess
import threading
import queue
import shutil
from pathlib import Path

import requests
import paho.mqtt.client as mqtt

# ========== КОНФИГ ==========
FRIGATE_API_URL = "http://192.168.0.226:5000"
MQTT_BROKER = "192.168.0.226"
MQTT_TOPIC = "frigate/events"
MQTT_PORT = 1883
MQTT_USER = "frigate"
MQTT_PASS = "frigate"

BASE_DIR = Path("/config")
NEW_DIR = BASE_DIR / "new_event"
SEND_DIR = BASE_DIR / "send"
TEMP_DIR = BASE_DIR / "temp_merge"

GROUP_TIMEOUT = 60          # ожидание новых событий (сек)
MAX_FILES = 10              # принудительное слияние при достижении
MAX_DOWNLOAD_ATTEMPTS = 4   # количество попыток скачивания
DOWNLOAD_RETRY_DELAY = 15   # пауза между попытками (сек)
MAX_SAFE_SIZE_MB = 45       # максимальный размер неразбитого видео
TARGET_SEGMENT_MB = 32      # целевой размер сегмента при разбиении
MAX_SEGMENT_BYTES = TARGET_SEGMENT_MB * 1024 * 1024

# Настройка логов
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("event-merger")

# ========== ИНИЦИАЛИЗАЦИЯ ==========
NEW_DIR.mkdir(parents=True, exist_ok=True)
SEND_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)

event_queue = queue.Queue()
merge_lock = threading.Lock()

# ========== УТИЛИТЫ ==========
def has_audio_stream(path):
    try:
        result = subprocess.run([
            "ffprobe", "-v", "error", "-select_streams", "a",
            "-show_entries", "stream=codec_type", "-of", "csv=p=0",
            str(path)
        ], capture_output=True, text=True, timeout=10, check=True)
        return len(result.stdout.strip()) > 0
    except Exception:
        return False

def get_duration(path):
    try:
        result = subprocess.run([
            "ffprobe", "-v", "error", "-show_entries", "format=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", str(path)
        ], capture_output=True, text=True, timeout=10, check=True)
        return float(result.stdout.strip())
    except Exception:
        return 0.0

def run_ffmpeg(cmd, timeout=300):
    try:
        result = subprocess.run(cmd, check=True, capture_output=True, text=True, timeout=timeout)
        return result
    except subprocess.CalledProcessError as e:
        logger.error(f"FFmpeg error:\n{' '.join(cmd)}\n{e.stderr}")
        raise
    except Exception as e:
        logger.error(f"FFmpeg failed: {e}")
        raise

# ========== НОРМАЛИЗАЦИЯ ==========
def normalize_video(input_path, output_path):
    audio_args = [] if has_audio_stream(input_path) else ["-an"]
    cmd = [
        "ffmpeg", "-hwaccel", "cuda", "-fflags", "+genpts",
        "-i", str(input_path),
        "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,"
               "pad=1280:720:(ow-iw)/2:(oh-ih)/2:black,fps=15",
        "-c:v", "h264_nvenc", "-preset", "p5", "-tune", "hq",
        "-profile:v", "high", "-level", "4.1",
        "-force_key_frames", "expr:gte(t,n_forced*2)",
        "-b:v", "2M", "-maxrate", "2.5M", "-bufsize", "5M",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "96k", "-movflags", "+faststart",
        "-y", str(output_path)
    ] + audio_args
    run_ffmpeg(cmd)
    os.chmod(output_path, 0o664)

# ========== СКАЧИВАНИЕ ==========
def download_clip(event_id, camera, start_time):
    url = f"{FRIGATE_API_URL}/api/events/{event_id}/clip.mp4"
    filename = f"{int(start_time)}_{camera}_{event_id}.mp4"
    filepath = NEW_DIR / filename

    for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
        try:
            with requests.get(url, stream=True, timeout=(10, 120)) as r:
                r.raise_for_status()
                with open(filepath, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
            if get_duration(filepath) < 1:
                filepath.unlink(missing_ok=True)
                logger.warning(f"Downloaded file {filename} has zero duration, retrying...")
                continue
            os.chmod(filepath, 0o664)
            logger.info(f"Downloaded: {filename}")
            return filepath
        except Exception as e:
            logger.warning(f"Download attempt {attempt} failed for {event_id}: {e}")
            if attempt < MAX_DOWNLOAD_ATTEMPTS:
                time.sleep(DOWNLOAD_RETRY_DELAY)
    logger.error(f"Failed to download {event_id} after {MAX_DOWNLOAD_ATTEMPTS} attempts")
    return None

# ========== РАЗБИЕНИЕ ==========
def split_video(input_path, prefix):
    size_mb = os.path.getsize(input_path) / (1024 * 1024)
    if size_mb <= MAX_SAFE_SIZE_MB:
        return [input_path]

    duration = get_duration(input_path)
    bitrate = 4_000_000
    segment_duration = max(5, int((MAX_SEGMENT_BYTES * 8) / bitrate))

    parts = []
    current = 0
    index = 1
    has_audio = has_audio_stream(input_path)

    while current < duration:
        out = SEND_DIR / f"{prefix}_p{index:03d}.mp4"
        cmd = [
            "ffmpeg", "-hwaccel", "cuda", "-fflags", "+genpts",
            "-i", str(input_path), "-ss", str(current), "-t", str(segment_duration),
            "-vf", "fps=15", "-c:v", "h264_nvenc", "-preset", "p5",
            "-force_key_frames", "expr:gte(t,n_forced*2)",
            "-b:v", "2M", "-maxrate", "2.5M", "-bufsize", "5M",
            "-pix_fmt", "yuv420p", "-movflags", "+faststart",
            "-y", str(out)
        ]
        if has_audio:
            cmd.extend(["-c:a", "aac", "-b:a", "96k"])
        else:
            cmd.append("-an")
        run_ffmpeg(cmd)
        part_size = os.path.getsize(out) / (1024 * 1024)
        logger.info(f"Segment created: {out.name}, size: {part_size:.2f}MB")
        parts.append(out)
        current += get_duration(out)
        index += 1

    return parts

# ========== ОБРАБОТКА ПАЧКИ ==========
def process_batch(file_paths):
    if not file_paths:
        return

    logger.info(f"Processing batch of {len(file_paths)} files")
    temp_dir = TEMP_DIR / f"batch_{int(time.time())}"
    temp_dir.mkdir(exist_ok=True)

    try:
        normalized = []
        for i, f in enumerate(file_paths):
            norm = temp_dir / f"norm_{i:03d}.mp4"
            try:
                normalize_video(f, norm)
                normalized.append(norm)
            except Exception as e:
                logger.error(f"Normalization failed {f}: {e}")

        if not normalized:
            return

        list_file = temp_dir / "list.txt"
        with open(list_file, "w") as lf:
            for nf in normalized:
                lf.write(f"file '{nf}'\n")

        merged = NEW_DIR / f"merged_{int(time.time())}.mp4"

        # Формируем команду concat с учётом аудио
        concat_cmd = [
            "ffmpeg", "-f", "concat", "-safe", "0", "-i", str(list_file),
            "-c:v", "h264_nvenc", "-preset", "p5", "-b:v", "4M",
            "-pix_fmt", "yuv420p", "-movflags", "+faststart",
        ]
        if has_audio_stream(normalized[0]):
            concat_cmd.extend(["-c:a", "aac", "-b:a", "96k"])
        else:
            concat_cmd.append("-an")
        concat_cmd.extend(["-y", str(merged)])

        run_ffmpeg(concat_cmd)

        parts = split_video(merged, merged.stem)
        merged.unlink(missing_ok=True)

        for f in file_paths:
            f.unlink(missing_ok=True)

        logger.info(f"Batch processed, {len(parts)} parts sent")

    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)

# ========== WORKER ==========
def worker_loop():
    logger.info("Worker started")
    while True:
        session_files = []

        # Первое событие
        data = event_queue.get()
        if not data.get("after", {}).get("id"):
            continue
        eid, cam, ts = data["after"]["id"], data["after"]["camera"], data["after"]["start_time"]
        f = download_clip(eid, cam, ts)
        if f:
            session_files.append(f)

        # Ожидание новых событий
        while True:
            # Проверка лимита файлов (принудительное слияние)
            if len(session_files) >= MAX_FILES:
                logger.info(f"File count limit reached ({MAX_FILES}), forcing merge")
                break

            try:
                next_data = event_queue.get(timeout=GROUP_TIMEOUT)
                neid, ncam, nts = next_data["after"]["id"], next_data["after"]["camera"], next_data["after"]["start_time"]
                nf = download_clip(neid, ncam, nts)
                if nf:
                    session_files.append(nf)
            except queue.Empty:
                # Таймаут истёк — новых событий нет
                break

        if session_files:
            process_batch(session_files)

# ========== MQTT ==========
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info("Connected to MQTT")
        client.subscribe(MQTT_TOPIC)
    else:
        logger.error(f"MQTT connect failed: {rc}")

def on_message(client, userdata, msg):
    try:
        data = json.loads(msg.payload.decode())
        logger.debug(f"MQTT: {data.get('type')} {data.get('after',{}).get('camera')}")
        if data.get("type") == "end":
            event_queue.put(data)
    except Exception as e:
        logger.error(f"MQTT error: {e}")

# ========== MAIN ==========
def main():
    # Проверка файлов при старте
    initial = list(NEW_DIR.glob("*.mp4"))
    if initial:
        logger.info(f"Startup: {len(initial)} files found → force merge")
        threading.Thread(target=lambda: process_batch(initial), daemon=True).start()

    threading.Thread(target=worker_loop, daemon=True).start()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()

if __name__ == "__main__":
    main()