#!/usr/bin/env python3
"""
event-merger.py — объединяет клипы Frigate, отправляет в Telegram через прокси,
переводит описания с английского на русский (опционально).
Конфигурация вынесена в отдельный файл config.py
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

# ========== ИМПОРТ КОНФИГУРАЦИИ ==========
from config import *

# ========== ПУТИ ==========
base_path = Path(BASE_DIR)
NEW_DIR = base_path / "new_event"
SEND_DIR = base_path / "send"
TEMP_DIR = base_path / "temp_merge"

# ========== ИНИЦИАЛИЗАЦИЯ ==========
NEW_DIR.mkdir(parents=True, exist_ok=True)
SEND_DIR.mkdir(parents=True, exist_ok=True)
TEMP_DIR.mkdir(parents=True, exist_ok=True)

event_queue = queue.Queue()
merge_lock = threading.Lock()
event_descriptions = {}

# ========== ЛОГГЕР ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("event-merger")

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

def translate_to_russian(text):
    """Переводит английский текст на русский через Ollama (если включено)."""
    if not TRANSLATE_TO_RUSSIAN or not text:
        return text

    # Если текст уже содержит кириллицу, не переводим
    if any('\u0400' <= c <= '\u04FF' for c in text):
        logger.debug("Text already contains Cyrillic, skipping translation")
        return text

    if len(text) < 3:
        return text

    # Несколько вариантов промпта для повышения шанса успеха
    prompts = [
        f"""Translate the following text from English to Russian. Provide only the translation, no additional text.

Text: {text}

Russian translation:""",
        f"""Переведи следующий текст с английского на русский. Только перевод, без пояснений.

{text}"""
    ]

    for prompt in prompts:
        try:
            response = requests.post(
                f"{OLLAMA_API_URL}/api/generate",
                json={
                    "model": TRANSLATION_MODEL,
                    "prompt": prompt,
                    "stream": False,
                    "options": {
                        "temperature": 0.1,
                        "num_predict": 512
                    }
                },
                timeout=TRANSLATION_TIMEOUT
            )
            response.raise_for_status()
            result = response.json().get("response", "").strip()
            result = result.replace('"', '').strip()

            if result:
                logger.info(f"Translated: '{text[:30]}...' -> '{result[:30]}...'")
                return result
        except Exception as e:
            logger.warning(f"Translation attempt with prompt #{prompts.index(prompt)+1} failed: {e}")
            continue

    logger.error(f"All translation attempts failed for: {text[:50]}...")
    return text  # возвращаем оригинал

def send_telegram_video(file_path, caption):
    """Отправляет видео в Telegram через прокси (если настроено)."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendVideo"

    proxies = None
    if TELEGRAM_PROXY_HOST and TELEGRAM_PROXY_PORT:
        proxy_auth = ""
        if TELEGRAM_PROXY_USER and TELEGRAM_PROXY_PASS:
            proxy_auth = f"{TELEGRAM_PROXY_USER}:{TELEGRAM_PROXY_PASS}@"
        proxy_url = f"{TELEGRAM_PROXY_TYPE}://{proxy_auth}{TELEGRAM_PROXY_HOST}:{TELEGRAM_PROXY_PORT}"
        proxies = {"http": proxy_url, "https": proxy_url}
        logger.info(f"Using proxy: {TELEGRAM_PROXY_TYPE}://{TELEGRAM_PROXY_HOST}:{TELEGRAM_PROXY_PORT}")

    for attempt in range(1, TELEGRAM_RETRY_ATTEMPTS + 1):
        try:
            with open(file_path, 'rb') as video_file:
                files = {'video': video_file}
                data = {'chat_id': TELEGRAM_CHAT_ID, 'caption': caption}
                response = requests.post(url, files=files, data=data, timeout=60, proxies=proxies)
                response.raise_for_status()
                logger.info(f"Telegram send success: {file_path.name}")
                return True
        except Exception as e:
            logger.warning(f"Telegram send attempt {attempt} failed: {e}")
            if attempt < TELEGRAM_RETRY_ATTEMPTS:
                time.sleep(TELEGRAM_RETRY_DELAY)
    logger.error(f"Failed to send {file_path.name} after {TELEGRAM_RETRY_ATTEMPTS} attempts")
    return False

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
def process_batch(file_paths):  # file_paths: list of tuples (path, event_id, description)
    if not file_paths:
        return

    logger.info(f"Processing batch of {len(file_paths)} files")

    descriptions = [desc for _, _, desc in file_paths if desc]
    if descriptions:
        final_description = descriptions[0]
    else:
        final_description = "Обнаружено движение"

    temp_dir = TEMP_DIR / f"batch_{int(time.time())}"
    temp_dir.mkdir(exist_ok=True)

    try:
        normalized = []
        for i, (f_path, eid, desc) in enumerate(file_paths):
            norm = temp_dir / f"norm_{i:03d}.mp4"
            try:
                normalize_video(f_path, norm)
                normalized.append(norm)
            except Exception as e:
                logger.error(f"Normalization failed {f_path}: {e}")

        if not normalized:
            return

        list_file = temp_dir / "list.txt"
        with open(list_file, "w") as lf:
            for nf in normalized:
                lf.write(f"file '{nf}'\n")

        merged = NEW_DIR / f"merged_{int(time.time())}.mp4"

        concat_cmd = [
            "ffmpeg", "-f", "concat", "-safe", "0", "-i", str(list_file),
            "-c:v", "h264_nvenc", "-preset", "p5",
            "-b:v", "2M", "-maxrate", "2.5M", "-bufsize", "5M",
            "-pix_fmt", "yuv420p", "-movflags", "+faststart",
        ]
        if has_audio_stream(normalized[0]):
            concat_cmd.extend(["-c:a", "aac", "-b:a", "96k"])
        else:
            concat_cmd.append("-an")
        concat_cmd.extend(["-y", str(merged)])

        run_ffmpeg(concat_cmd)

        merged_size_mb = os.path.getsize(merged) / (1024 * 1024)
        if merged_size_mb <= MAX_SAFE_SIZE_MB:
            final = SEND_DIR / f"{merged.stem}.mp4"
            shutil.move(str(merged), str(final))
            os.chmod(final, 0o664)

            if send_telegram_video(final, final_description):
                final.unlink(missing_ok=True)
                logger.info(f"Sent and deleted: {final.name}")
            else:
                logger.error(f"Failed to send {final.name}, keeping file")
        else:
            parts = split_video(merged, merged.stem)
            merged.unlink(missing_ok=True)

            all_sent = True
            for part in parts:
                if send_telegram_video(part, final_description):
                    part.unlink(missing_ok=True)
                else:
                    all_sent = False
                    logger.error(f"Failed to send {part.name}, keeping file")

            if all_sent:
                logger.info(f"All {len(parts)} parts sent and deleted")
            else:
                logger.warning(f"Some parts failed to send, kept in {SEND_DIR}")

        for f_path, eid, _ in file_paths:
            f_path.unlink(missing_ok=True)
            event_descriptions.pop(eid, None)

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
            # Получаем описание из словаря (если уже есть)
            raw_desc = event_descriptions.get(eid, "")
            if raw_desc:
                raw_desc = translate_to_russian(raw_desc)
            session_files.append((f, eid, raw_desc))

        # Ожидание новых событий
        while True:
            if len(session_files) >= MAX_FILES:
                logger.info(f"File count limit reached ({MAX_FILES}), forcing merge")
                break

            try:
                next_data = event_queue.get(timeout=GROUP_TIMEOUT)
                neid, ncam, nts = next_data["after"]["id"], next_data["after"]["camera"], next_data["after"]["start_time"]
                nf = download_clip(neid, ncam, nts)
                if nf:
                    raw_ndesc = event_descriptions.get(neid, "")
                    if raw_ndesc:
                        raw_ndesc = translate_to_russian(raw_ndesc)
                    session_files.append((nf, neid, raw_ndesc))
            except queue.Empty:
                # Таймаут истёк — новых событий нет
                break

        # Обновляем описания из словаря на случай, если пришли после таймаута, но до обработки
        for i, (fpath, eid, desc) in enumerate(session_files):
            if not desc and eid in event_descriptions:
                translated = translate_to_russian(event_descriptions[eid])
                session_files[i] = (fpath, eid, translated)
                logger.info(f"Updated and translated description for event {eid}")

        if session_files:
            process_batch(session_files)

# ========== MQTT ==========
def on_connect(client, userdata, flags, rc, properties=None):
    if rc == 0:
        logger.info("Connected to MQTT")
        client.subscribe(MQTT_TOPIC)
        client.subscribe(MQTT_TOPIC_DESCR)
    else:
        logger.error(f"MQTT connect failed: {rc}")

def on_message(client, userdata, msg):
    try:
        if msg.topic == MQTT_TOPIC:
            data = json.loads(msg.payload.decode())
            logger.debug(f"MQTT event: {data.get('type')} {data.get('after',{}).get('camera')}")
            if data.get("type") == "end":
                event_queue.put(data)

        elif msg.topic == MQTT_TOPIC_DESCR:
            data = json.loads(msg.payload.decode())
            if data.get("type") == "description":
                event_id = data.get("id")
                description = data.get("description")
                if event_id and description:
                    event_descriptions[event_id] = description
                    logger.info(f"Stored description for event {event_id}: {description}")
    except Exception as e:
        logger.error(f"MQTT error: {e}")

# ========== MAIN ==========
def main():
    # Проверка файлов при старте
    initial = list(NEW_DIR.glob("*.mp4"))
    if initial:
        logger.info(f"Startup: {len(initial)} files found → force merge")
        # Для старых файлов описаний нет, используем пустые
        fake_list = [(p, "", "") for p in initial]
        threading.Thread(target=lambda: process_batch(fake_list), daemon=True).start()

    threading.Thread(target=worker_loop, daemon=True).start()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()

if __name__ == "__main__":
    main()