#!/usr/bin/env python3
"""
event-merger.py — объединяет клипы Frigate, отправляет в Telegram (основной и дополнительный чаты),
переводит описания на русский, добавляет имена распознанных лиц (несколько через запятую).
Видео с камеры "balcony" обрабатываются отдельно и отправляются только в первый чат.
Конфигурация в config.py
"""

import os
import sys
import json
import re
import time
import logging
import subprocess
import threading
import queue
import shutil
from contextlib import ExitStack
from pathlib import Path
from urllib.parse import quote

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
balcony_event_queue = queue.Queue()
merge_lock = threading.Lock()
event_descriptions = {}
event_faces = {}  # {event_id: [{"name": str, "score": float}, ...]}
seen_event_ids = {}

# Семафор для ограничения параллельных NVENC сессий (1 одновременно — безопасно для GTX 1650)
nvenc_semaphore = threading.Semaphore(1)
delivery_lock = threading.Lock()
seen_events_lock = threading.Lock()

# ========== ЛОГГЕР ==========
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    stream=sys.stdout
)
logger = logging.getLogger("event-merger")

# Дополнительные настройки имеют безопасные значения по умолчанию, поэтому
# старый config.py продолжит работать без изменений.
FRIGATE_CLIP_PRE_CAPTURE = float(globals().get("FRIGATE_CLIP_PRE_CAPTURE", 1))
FRIGATE_CLIP_POST_CAPTURE = float(globals().get("FRIGATE_CLIP_POST_CAPTURE", 1))
FRIGATE_CLIP_FINALIZE_DELAY = float(globals().get("FRIGATE_CLIP_FINALIZE_DELAY", 10))
FRIGATE_CLIP_DURATION_TOLERANCE = float(globals().get("FRIGATE_CLIP_DURATION_TOLERANCE", 3))
TELEGRAM_PENDING_RETRY_INTERVAL = int(globals().get("TELEGRAM_PENDING_RETRY_INTERVAL", 300))
EVENT_DEDUP_TTL = int(globals().get("EVENT_DEDUP_TTL", 86400))
BALCONY_GROUP_TIMEOUT = float(globals().get("BALCONY_GROUP_TIMEOUT", 10))
BALCONY_MIN_EVENT_DURATION = float(globals().get("BALCONY_MIN_EVENT_DURATION", 3))
BALCONY_MAX_GROUP_EVENTS = int(globals().get("BALCONY_MAX_GROUP_EVENTS", 10))
BALCONY_EVENT_MERGE_GAP = float(globals().get("BALCONY_EVENT_MERGE_GAP", 5))
BALCONY_MAX_CLIP_DURATION = float(globals().get("BALCONY_MAX_CLIP_DURATION", 60))
BALCONY_LONG_CLIP_MIN_DURATION = float(
    globals().get("BALCONY_LONG_CLIP_MIN_DURATION", 10)
)

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

def get_video_duration(path):
    """Возвращает длительность видеодорожки, не подменяя её более длинным аудио."""
    try:
        result = subprocess.run([
            "ffprobe", "-v", "error", "-select_streams", "v:0",
            "-show_entries", "stream=duration",
            "-of", "default=noprint_wrappers=1:nokey=1", str(path)
        ], capture_output=True, text=True, timeout=10, check=True)
        value = result.stdout.strip()
        if value and value != "N/A":
            return float(value)
    except Exception:
        pass
    return get_duration(path)

def run_ffmpeg(cmd, timeout=300):
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            encoding='utf-8',
            errors='ignore',
            timeout=timeout
        )
        return result
    except subprocess.CalledProcessError as e:
        stderr = e.stderr if e.stderr else ''
        logger.error(f"FFmpeg error:\n{' '.join(cmd)}\n{stderr}")
        raise
    except subprocess.TimeoutExpired:
        logger.error(f"FFmpeg timeout:\n{' '.join(cmd)}")
        raise
    except Exception as e:
        logger.error(f"FFmpeg failed: {e}")
        raise

def translate_to_russian(text):
    """Переводит английский текст на русский через Ollama (если включено)."""
    if not TRANSLATE_TO_RUSSIAN or not text:
        return text

    if any('\u0400' <= c <= '\u04FF' for c in text):
        return text

    if len(text) < 3:
        return text

    prompts = [
        f"""Translate the following text from English to Russian. Provide only the translation, no additional text.

Text: {text}

Russian translation:""",
        f"""Переведи следующий текст с английского на русский. Только перевод, без пояснений.

{text}"""
    ]

    for i, prompt in enumerate(prompts, 1):
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
            logger.warning(f"Translation attempt {i} failed: {e}")
            continue

    logger.error(f"All translation attempts failed for: {text[:50]}...")
    return text

def get_proxies():
    """Возвращает словарь прокси для requests, если настроено."""
    if TELEGRAM_PROXY_HOST and TELEGRAM_PROXY_PORT:
        proxy_auth = ""
        if TELEGRAM_PROXY_USER and TELEGRAM_PROXY_PASS:
            proxy_auth = f"{TELEGRAM_PROXY_USER}:{TELEGRAM_PROXY_PASS}@"
        proxy_url = f"{TELEGRAM_PROXY_TYPE}://{proxy_auth}{TELEGRAM_PROXY_HOST}:{TELEGRAM_PROXY_PORT}"
        return {"http": proxy_url, "https": proxy_url}
    return None

def redact_telegram_secrets(value, bot_token=None):
    """Удаляет токены ботов из исключений requests и URL перед логированием."""
    message = str(value or "")
    tokens = [
        bot_token,
        globals().get("TELEGRAM_BOT_TOKEN"),
        globals().get("SECOND_TELEGRAM_BOT_TOKEN"),
    ]
    for token in tokens:
        if token:
            message = message.replace(str(token), "<redacted>")
    return re.sub(
        r"(https?://api\.telegram\.org/bot)[^/\s]+",
        r"\1<redacted>",
        message,
        flags=re.IGNORECASE,
    )

def telegram_error_message(error, response, bot_token):
    details = getattr(response, "text", "") if response is not None else ""
    combined = f"{error}; {details[:500]}" if details else str(error)
    return redact_telegram_secrets(combined, bot_token)

def send_telegram_media_group(video_path, photo_path, caption, chat_id, bot_token):
    """Отправляет фото и видео как группу медиа в указанный чат."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMediaGroup"
    proxies = get_proxies()

    media = []
    if photo_path and Path(photo_path).exists():
        media.append({
            'type': 'photo',
            'media': 'attach://photo',
            'caption': caption,
        })
    media.append({
        'type': 'video',
        'media': 'attach://video',
    })
    payload = {'chat_id': chat_id, 'media': json.dumps(media)}

    for attempt in range(1, TELEGRAM_RETRY_ATTEMPTS + 1):
        response = None
        try:
            with ExitStack() as stack:
                files = {}
                if photo_path and Path(photo_path).exists():
                    files['photo'] = stack.enter_context(open(photo_path, 'rb'))
                files['video'] = stack.enter_context(open(video_path, 'rb'))
                response = requests.post(url, data=payload, files=files, timeout=60, proxies=proxies)
                response.raise_for_status()
                logger.info(f"Media group sent to {chat_id}: {video_path.name}")
                return True
        except Exception as e:
            error_message = telegram_error_message(e, response, bot_token)
            logger.warning(f"Media group attempt {attempt} to {chat_id} failed: {error_message}")
        if attempt < TELEGRAM_RETRY_ATTEMPTS:
            time.sleep(TELEGRAM_RETRY_DELAY)
    return False

def send_telegram_video(video_path, caption, chat_id, bot_token):
    """Отправляет только видео в указанный чат."""
    url = f"https://api.telegram.org/bot{bot_token}/sendVideo"
    proxies = get_proxies()

    for attempt in range(1, TELEGRAM_RETRY_ATTEMPTS + 1):
        response = None
        try:
            with open(video_path, 'rb') as video_file:
                files = {'video': video_file}
                data = {'chat_id': chat_id, 'caption': caption}
                response = requests.post(url, files=files, data=data, timeout=60, proxies=proxies)
                response.raise_for_status()
                logger.info(f"Video sent to {chat_id}: {video_path.name}")
                return True
        except Exception as e:
            error_message = telegram_error_message(e, response, bot_token)
            logger.warning(f"Video send attempt {attempt} to {chat_id} failed: {error_message}")
            if attempt < TELEGRAM_RETRY_ATTEMPTS:
                time.sleep(TELEGRAM_RETRY_DELAY)
    logger.error(f"Failed to send {video_path.name} to {chat_id} after {TELEGRAM_RETRY_ATTEMPTS} attempts")
    return False

def delivery_state_path(video_path):
    return SEND_DIR / f"{Path(video_path).name}.delivery.json"

def write_delivery_state(state_path, state):
    """Атомарно сохраняет состояние доставки без токенов Telegram."""
    temp_path = state_path.with_suffix(state_path.suffix + ".tmp")
    with open(temp_path, "w", encoding="utf-8") as state_file:
        json.dump(state, state_file, ensure_ascii=False, indent=2)
    temp_path.replace(state_path)

def configured_delivery_targets(caption, include_second_chat):
    targets = [{
        "name": "primary",
        "chat_id": str(TELEGRAM_CHAT_ID),
        "caption": caption,
        "sent": False,
    }]
    if include_second_chat and SECOND_TELEGRAM_CHAT_ID:
        targets.append({
            "name": "secondary",
            "chat_id": str(SECOND_TELEGRAM_CHAT_ID),
            "caption": "",
            "sent": False,
        })
    return targets

def attempt_pending_delivery(state_path):
    """Отправляет ещё не доставленные адресатам вложения и удаляет только после полного успеха."""
    state_path = Path(state_path)
    try:
        with open(state_path, "r", encoding="utf-8") as state_file:
            state = json.load(state_file)
    except Exception as e:
        logger.error(f"Cannot read delivery state {state_path.name}: {e}")
        return False

    video_path = SEND_DIR / state["video"]
    snapshot_name = state.get("snapshot")
    snapshot_path = SEND_DIR / snapshot_name if snapshot_name else None
    if not video_path.exists():
        logger.error(f"Pending video is missing: {video_path}")
        return False

    for target in state["targets"]:
        if target.get("sent"):
            continue
        bot_token = (
            SECOND_TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN
            if target["name"] == "secondary"
            else TELEGRAM_BOT_TOKEN
        )
        if snapshot_path and snapshot_path.exists():
            sent = send_telegram_media_group(
                video_path, snapshot_path, target.get("caption", ""),
                target["chat_id"], bot_token
            )
        else:
            sent = send_telegram_video(
                video_path, target.get("caption", ""),
                target["chat_id"], bot_token
            )
        if sent:
            target["sent"] = True
            write_delivery_state(state_path, state)

    if all(target.get("sent") for target in state["targets"]):
        video_path.unlink(missing_ok=True)
        if snapshot_path:
            snapshot_path.unlink(missing_ok=True)
        state_path.unlink(missing_ok=True)
        logger.info(f"Delivery completed and files deleted: {video_path.name}")
        return True

    logger.warning(f"Delivery remains pending: {video_path.name}")
    return False

def queue_delivery(video_path, snapshot_path, caption, include_second_chat=True):
    """Ставит готовый файл в сохраняемую очередь и сразу делает первую попытку."""
    video_path = Path(video_path)
    snapshot_path = Path(snapshot_path) if snapshot_path else None
    state_path = delivery_state_path(video_path)
    state = {
        "video": video_path.name,
        "snapshot": snapshot_path.name if snapshot_path else None,
        "targets": configured_delivery_targets(caption, include_second_chat),
        "created_at": time.time(),
    }
    with delivery_lock:
        write_delivery_state(state_path, state)
        return attempt_pending_delivery(state_path)

def retry_pending_deliveries():
    with delivery_lock:
        states = sorted(SEND_DIR.glob("*.delivery.json"))
        if states:
            logger.info(f"Retrying {len(states)} pending Telegram deliveries")
        for state_path in states:
            attempt_pending_delivery(state_path)

def recover_untracked_send_files():
    """Подхватывает файлы, оставленные старой версией после неудачной отправки."""
    for video_path in sorted(SEND_DIR.glob("*.mp4")):
        if delivery_state_path(video_path).exists():
            continue
        size_mb = video_path.stat().st_size / (1024 * 1024)
        if size_mb > MAX_SAFE_SIZE_MB:
            logger.warning(f"Legacy pending file is too large for Telegram: {video_path.name}")
            continue
        snapshot_path = video_path.with_suffix(".jpg")
        queue_delivery(
            video_path,
            snapshot_path if snapshot_path.exists() else None,
            "Повторная отправка сохранённого события",
            include_second_chat=True,
        )

def delivery_retry_loop():
    while True:
        try:
            retry_pending_deliveries()
        except Exception as e:
            logger.error(f"Pending delivery retry failed: {e}")
        time.sleep(max(30, TELEGRAM_PENDING_RETRY_INTERVAL))

# ========== НОРМАЛИЗАЦИЯ С FALLBACK И СТАБИЛЬНОЙ СИНХРОНИЗАЦИЕЙ ==========
def normalize_video(input_path, output_path):
    input_duration = get_video_duration(input_path)
    input_container_duration = get_duration(input_path)
    has_audio = has_audio_stream(input_path)
    video_filter = (
        "scale=1280:720:force_original_aspect_ratio=decrease,"
        "pad=1280:720:(ow-iw)/2:(oh-ih)/2:black,"
        "setpts=PTS-STARTPTS,fps=20"
    )
    audio_args = (
        ["-c:a", "aac", "-b:a", "96k", "-ar", "48000", "-ac", "1",
         "-af", "asetpts=PTS-STARTPTS,aresample=async=1:first_pts=0"]
        if has_audio else ["-an"]
    )

    # NVENC (без hwaccel в decode — стабильнее на вашей сборке)
    cmd_nvenc = [
        "ffmpeg", "-i", str(input_path),
        "-vf", video_filter,
        "-c:v", "h264_nvenc", "-preset", "p4", "-tune", "hq",
        "-profile:v", "high", "-level", "4.1",
        "-b:v", "1800k", "-maxrate", "2200k", "-bufsize", "4400k",
        "-pix_fmt", "yuv420p",
        "-force_key_frames", "expr:gte(t,n_forced*2)",
    ] + audio_args + [
        "-movflags", "+faststart",
        "-y", str(output_path)
    ]

    # CPU fallback
    cmd_sw = [
        "ffmpeg", "-i", str(input_path),
        "-vf", video_filter,
        "-c:v", "libx264", "-preset", "fast", "-crf", "24",
        "-pix_fmt", "yuv420p",
    ] + audio_args + [
        "-movflags", "+faststart",
        "-y", str(output_path)
    ]

    try:
        with nvenc_semaphore:
            run_ffmpeg(cmd_nvenc, timeout=360)
        logger.info(f"Normalized with NVENC: {input_path.name}")
    except Exception as e:
        logger.warning(f"NVENC failed → CPU fallback: {e}")
        run_ffmpeg(cmd_sw, timeout=600)
        logger.info(f"Normalized with CPU: {input_path.name}")

    output_duration = get_video_duration(output_path)
    if input_duration > 0 and output_duration < input_duration - 1:
        output_path.unlink(missing_ok=True)
        raise RuntimeError(
            f"Normalization shortened video from {input_duration:.2f}s to {output_duration:.2f}s"
        )
    os.chmod(output_path, 0o664)
    logger.info(
        f"Normalized: {output_path.name}, video duration {input_duration:.2f}s -> "
        f"{output_duration:.2f}s, input container {input_container_duration:.2f}s "
        f"({os.path.getsize(output_path)/1024/1024:.2f} MB)"
    )

# ========== СКАЧИВАНИЕ ==========
def download_clip(
    event_id, camera, start_time, end_time=None, minimum_duration_override=None
):
    """Скачивает полный диапазон события с pre/post capture и snapshot."""
    expected_duration = None
    if end_time:
        clip_start = float(start_time) - FRIGATE_CLIP_PRE_CAPTURE
        clip_end = float(end_time) + FRIGATE_CLIP_POST_CAPTURE
        expected_duration = clip_end - clip_start
        camera_path = quote(str(camera), safe="")
        video_url = (
            f"{FRIGATE_API_URL}/api/{camera_path}/start/{clip_start:.3f}"
            f"/end/{clip_end:.3f}/clip.mp4"
        )

        # MQTT end описывает конец детекции, но post-capture и последний 10-секундный
        # сегмент записи ещё могут находиться в кеше Frigate.
        ready_at = clip_end + FRIGATE_CLIP_FINALIZE_DELAY
        wait_seconds = max(0, ready_at - time.time())
        if wait_seconds:
            logger.info(
                f"Waiting {wait_seconds:.1f}s for Frigate to finalize clip {event_id} "
                f"({expected_duration:.1f}s expected)"
            )
            time.sleep(wait_seconds)
    else:
        logger.warning(f"Event {event_id} has no end_time; using object clip endpoint")
        video_url = f"{FRIGATE_API_URL}/api/events/{event_id}/clip.mp4"

    snapshot_url = f"{FRIGATE_API_URL}/api/events/{event_id}/snapshot.jpg"
    filename_base = f"{int(start_time)}_{camera}_{event_id}"
    video_path = NEW_DIR / f"{filename_base}.mp4"
    snapshot_path = NEW_DIR / f"{filename_base}.jpg"

    video_ok = False
    snapshot_ok = False

    for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
        try:
            with requests.get(video_url, stream=True, timeout=(10, 120)) as r:
                r.raise_for_status()
                with open(video_path, "wb") as f:
                    for chunk in r.iter_content(8192):
                        f.write(chunk)
            downloaded_duration = get_video_duration(video_path)
            container_duration = get_duration(video_path)
            minimum_duration = (
                max(1, float(minimum_duration_override))
                if minimum_duration_override is not None
                else max(1, expected_duration - FRIGATE_CLIP_DURATION_TOLERANCE)
                if expected_duration else 1
            )
            if downloaded_duration < minimum_duration:
                video_path.unlink(missing_ok=True)
                logger.warning(
                    f"Video {video_path.name} is incomplete: {downloaded_duration:.2f}s, "
                    f"expected at least {minimum_duration:.2f}s; retrying"
                )
                if attempt < MAX_DOWNLOAD_ATTEMPTS:
                    time.sleep(DOWNLOAD_RETRY_DELAY)
                continue
            os.chmod(video_path, 0o664)
            video_ok = True
            logger.info(
                f"Downloaded clip video duration: {downloaded_duration:.2f}s, "
                f"container duration: {container_duration:.2f}s"
                + (f" (requested {expected_duration:.2f}s)" if expected_duration else "")
            )
            break
        except Exception as e:
            video_path.unlink(missing_ok=True)
            logger.warning(f"Video download attempt {attempt} failed for {event_id}: {e}")
            if attempt < MAX_DOWNLOAD_ATTEMPTS:
                time.sleep(DOWNLOAD_RETRY_DELAY)

    if not video_ok:
        logger.error(f"Failed to download video for {event_id}")
        return (None, None)

    for attempt in range(1, 3):
        try:
            r = requests.get(snapshot_url, timeout=(10, 30))
            if r.status_code == 200:
                with open(snapshot_path, "wb") as f:
                    f.write(r.content)
                if os.path.getsize(snapshot_path) > 1000:
                    os.chmod(snapshot_path, 0o664)
                    snapshot_ok = True
                    break
                else:
                    snapshot_path.unlink(missing_ok=True)
            else:
                logger.warning(f"Snapshot attempt {attempt} returned {r.status_code}")
        except Exception as e:
            logger.warning(f"Snapshot attempt {attempt} failed: {e}")
        if attempt < 2:
            time.sleep(2)

    if not snapshot_ok:
        logger.info(f"No snapshot for event {event_id}")

    logger.info(f"Downloaded: {video_path.name}" + (f" + snapshot" if snapshot_ok else ""))
    return (video_path, snapshot_path if snapshot_ok else None)

# ========== РАЗБИЕНИЕ С FALLBACK (единые параметры) ==========
def split_video(input_path, prefix):
    size_mb = os.path.getsize(input_path) / (1024 * 1024)
    if size_mb <= MAX_SAFE_SIZE_MB:
        return [input_path]

    duration = get_video_duration(input_path)
    if duration <= 0:
        logger.error(f"split_video: cannot get duration of {input_path}")
        return [input_path]

    bitrate = 4_000_000
    segment_duration = max(5, int((MAX_SEGMENT_BYTES * 8) / bitrate))
    logger.info(f"Splitting {input_path.name}, duration={duration:.2f}s, segment_duration={segment_duration}s")

    parts = []
    current = 0
    index = 1
    has_audio = has_audio_stream(input_path)

    # Единый видеофильтр для всех сегментов (сохраняет пропорции и добавляет паддинг)
    vf_scale_pad = (
        "scale=1280:720:force_original_aspect_ratio=decrease,"
        "pad=1280:720:(ow-iw)/2:(oh-ih)/2:black,"
        "setpts=PTS-STARTPTS,fps=20"
    )
    audio_args = (
        ["-c:a", "aac", "-b:a", "96k", "-ar", "48000", "-ac", "1",
         "-af", "asetpts=PTS-STARTPTS,aresample=async=1:first_pts=0"]
        if has_audio else ["-an"]
    )

    while current < duration - 0.1:
        out = SEND_DIR / f"{prefix}_p{index:03d}.mp4"
        logger.info(f"Creating segment {out.name} from {current:.2f}s to {current+segment_duration:.2f}s")

        # NVENC команда для сегмента
        cmd_nvenc = [
            "ffmpeg", "-i", str(input_path), "-ss", str(current), "-t", str(segment_duration),
            "-vf", vf_scale_pad,
            "-c:v", "h264_nvenc", "-preset", "p4",
            "-b:v", "1800k", "-maxrate", "2200k", "-bufsize", "4400k",
            "-pix_fmt", "yuv420p",
        ] + audio_args + [
            "-movflags", "+faststart",
            "-y", str(out)
        ]
        # Программная команда для сегмента (теперь с теми же параметрами аудио и синхронизации)
        cmd_sw = [
            "ffmpeg", "-i", str(input_path), "-ss", str(current), "-t", str(segment_duration),
            "-vf", vf_scale_pad,
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-pix_fmt", "yuv420p",
        ] + audio_args + [
            "-movflags", "+faststart",
            "-y", str(out)
        ]

        try:
            with nvenc_semaphore:
                run_ffmpeg(cmd_nvenc)
        except Exception as e:
            logger.warning(f"NVENC failed for segment {out.name}, falling back to software. Error: {e}")
            try:
                run_ffmpeg(cmd_sw)
            except Exception as e2:
                logger.error(f"Software encoding also failed for {out.name}: {e2}")
                out.unlink(missing_ok=True)
                break

        if not out.exists():
            logger.error(f"Segment {out.name} was not created")
            break

        part_dur = get_video_duration(out)
        part_size = os.path.getsize(out)
        if part_dur <= 0 or part_size < 1024:
            logger.error(f"Segment {out.name} has zero duration or too small ({part_size} bytes), aborting split")
            out.unlink(missing_ok=True)
            break

        logger.info(f"Segment created: {out.name}, duration: {part_dur:.2f}s, size: {part_size/1024/1024:.2f}MB")
        parts.append(out)
        current += part_dur
        index += 1

    return parts

# ========== ОБРАБОТКА ОДИНОЧНОГО ВИДЕО (например, с балкона) ==========
def process_single_video(video_path, snapshot_path, event_id, description, faces_list, camera):
    logger.info(f"Processing single video from {camera}: {video_path.name}")

    temp_dir = TEMP_DIR / f"single_{time.time_ns()}_{camera}"
    temp_dir.mkdir(exist_ok=True)
    source_is_safe = False

    try:
        norm_path = temp_dir / f"norm_{video_path.stem}.mp4"
        try:
            normalize_video(video_path, norm_path)
        except Exception as e:
            logger.error(f"Normalization failed for {video_path.name}: {e}")
            return

        size_mb = os.path.getsize(norm_path) / (1024 * 1024)

        if faces_list:
            sorted_faces = sorted(faces_list, key=lambda f: f["score"], reverse=True)
            names_str = ", ".join([f["name"] for f in sorted_faces])
        else:
            names_str = None

        if INCLUDE_FACE_NAME and names_str:
            if INCLUDE_DESCRIPTION_IN_MAIN and description:
                caption = f"{names_str}: {description}"
            else:
                caption = f"Обнаружены: {names_str}"
        else:
            if INCLUDE_DESCRIPTION_IN_MAIN and description:
                caption = description
            else:
                caption = "Обнаружено движение"

        if size_mb <= MAX_SAFE_SIZE_MB:
            final_video = SEND_DIR / f"single_{video_path.stem}.mp4"
            shutil.move(str(norm_path), str(final_video))
            final_snapshot = None
            if snapshot_path and Path(snapshot_path).exists():
                final_snapshot = SEND_DIR / f"single_{video_path.stem}.jpg"
                shutil.copy2(str(snapshot_path), str(final_snapshot))
            queue_delivery(final_video, final_snapshot, caption, include_second_chat=False)
            source_is_safe = True
        else:
            parts = split_video(norm_path, f"{norm_path.stem}_part")
            split_duration = sum(get_video_duration(part) for part in parts)
            original_duration = get_video_duration(norm_path)
            if parts and split_duration >= original_duration - 0.5:
                for part in parts:
                    queue_delivery(part, None, caption, include_second_chat=False)
                norm_path.unlink(missing_ok=True)
                source_is_safe = True
            else:
                preserved_video = SEND_DIR / f"oversized_{video_path.stem}.mp4"
                shutil.move(str(norm_path), str(preserved_video))
                source_is_safe = True
                logger.error(
                    f"Split incomplete; full video preserved without deletion: {preserved_video.name}"
                )
    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
        if source_is_safe:
            video_path.unlink(missing_ok=True)
            if snapshot_path:
                snapshot_path.unlink(missing_ok=True)
            event_descriptions.pop(event_id, None)
            event_faces.pop(event_id, None)
        else:
            logger.warning(f"Source retained for recovery: {video_path}")

# ========== ОБРАБОТКА ПАЧКИ ОБЫЧНЫХ ВИДЕО ==========
def process_batch(file_paths):  # список кортежей (video_path, snapshot_path, event_id, description, faces_list)
    if not file_paths:
        return

    logger.info(f"Processing batch of {len(file_paths)} files")

    descriptions = [desc for _, _, _, desc, _ in file_paths if desc]

    all_names = set()
    for _, _, _, _, faces in file_paths:
        if faces:
            for face in faces:
                all_names.add(face["name"])
    names_str = ", ".join(sorted(all_names)) if all_names else None

    if INCLUDE_FACE_NAME and names_str:
        if INCLUDE_DESCRIPTION_IN_MAIN and descriptions:
            final_description = f"{names_str}: {descriptions[0]}"
        else:
            final_description = f"Обнаружены: {names_str}"
    else:
        if INCLUDE_DESCRIPTION_IN_MAIN and descriptions:
            final_description = descriptions[0]
        else:
            final_description = "Обнаружено движение"

    first_snapshot = file_paths[0][1] if file_paths[0][1] and Path(file_paths[0][1]).exists() else None

    temp_dir = TEMP_DIR / f"batch_{time.time_ns()}"
    temp_dir.mkdir(exist_ok=True)
    normalized_sources = []
    output_is_safe = False

    try:
        normalized = []
        for i, (video_path, snap_path, eid, desc, faces) in enumerate(file_paths):
            norm = temp_dir / f"norm_{i:03d}.mp4"
            try:
                normalize_video(video_path, norm)
                normalized.append(norm)
                normalized_sources.append((video_path, snap_path, eid))
            except Exception as e:
                logger.error(f"Normalization failed {video_path}: {e}")

        if not normalized:
            return

        list_file = temp_dir / "list.txt"
        with open(list_file, "w") as lf:
            for nf in normalized:
                lf.write(f"file '{nf}'\n")

        merged = NEW_DIR / f"merged_{time.time_ns()}.mp4"

        # Нормализованные клипы начинаются с нулевых PTS; повторно сбрасываем
        # временную шкалу после concat, чтобы Telegram не видел пустое начало.
        concat_has_audio = has_audio_stream(normalized[0])
        concat_audio_args = (
            ["-c:a", "aac", "-b:a", "96k", "-ar", "48000", "-ac", "1",
             "-af", "asetpts=PTS-STARTPTS,aresample=async=1:first_pts=0"]
            if concat_has_audio else ["-an"]
        )
        concat_video_filter = "setpts=PTS-STARTPTS,fps=20"
        concat_cmd_nvenc = [
            "ffmpeg", "-f", "concat", "-safe", "0", "-i", str(list_file),
            "-vf", concat_video_filter,
            "-c:v", "h264_nvenc", "-preset", "p4",
            "-b:v", "1800k", "-maxrate", "2200k", "-bufsize", "4400k",
            "-pix_fmt", "yuv420p",
        ] + concat_audio_args + [
            "-movflags", "+faststart",
            "-y", str(merged)
        ]
        concat_cmd_sw = [
            "ffmpeg", "-f", "concat", "-safe", "0", "-i", str(list_file),
            "-vf", concat_video_filter,
            "-c:v", "libx264", "-preset", "medium", "-crf", "23",
            "-pix_fmt", "yuv420p",
        ] + concat_audio_args + [
            "-movflags", "+faststart",
            "-y", str(merged)
        ]

        try:
            with nvenc_semaphore:
                run_ffmpeg(concat_cmd_nvenc)
        except Exception as e:
            logger.warning(f"NVENC concat failed, falling back to software encoding. Error: {e}")
            run_ffmpeg(concat_cmd_sw)

        expected_merged_duration = sum(get_video_duration(path) for path in normalized)
        actual_merged_duration = get_video_duration(merged)
        if actual_merged_duration < expected_merged_duration - 1:
            merged.unlink(missing_ok=True)
            raise RuntimeError(
                f"Concat shortened video from {expected_merged_duration:.2f}s "
                f"to {actual_merged_duration:.2f}s"
            )

        merged_size_mb = os.path.getsize(merged) / (1024 * 1024)

        if merged_size_mb <= MAX_SAFE_SIZE_MB:
            final_video = SEND_DIR / f"{merged.stem}.mp4"
            shutil.move(str(merged), str(final_video))
            os.chmod(final_video, 0o664)

            final_snapshot = None
            if first_snapshot:
                final_snapshot = SEND_DIR / f"{merged.stem}.jpg"
                shutil.copy2(str(first_snapshot), str(final_snapshot))
                os.chmod(final_snapshot, 0o664)

            queue_delivery(final_video, final_snapshot, final_description, include_second_chat=True)
            output_is_safe = True
        else:
            parts = split_video(merged, merged.stem)
            split_duration = sum(get_video_duration(part) for part in parts)
            merged_duration = get_video_duration(merged)
            if parts and split_duration >= merged_duration - 0.5:
                for part in parts:
                    queue_delivery(part, None, final_description, include_second_chat=True)
                merged.unlink(missing_ok=True)
                output_is_safe = True
            else:
                preserved_video = SEND_DIR / merged.name
                shutil.move(str(merged), str(preserved_video))
                output_is_safe = True
                logger.error(
                    f"Split incomplete; full merged video preserved: {preserved_video.name}"
                )

        if output_is_safe:
            for video_path, snap_path, eid in normalized_sources:
                video_path.unlink(missing_ok=True)
                if snap_path:
                    snap_path.unlink(missing_ok=True)
                event_descriptions.pop(eid, None)
                event_faces.pop(eid, None)

    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)

# ========== WORKER ==========
def register_event_once(event_id):
    """Возвращает False для повторного MQTT end с тем же event_id."""
    now = time.time()
    cutoff = now - max(60, EVENT_DEDUP_TTL)
    with seen_events_lock:
        expired = [eid for eid, seen_at in seen_event_ids.items() if seen_at < cutoff]
        for eid in expired:
            seen_event_ids.pop(eid, None)
        if event_id in seen_event_ids:
            return False
        seen_event_ids[event_id] = now
        return True

def clear_event_metadata(events):
    for event in events:
        event_id = event.get("id")
        if event_id:
            event_descriptions.pop(event_id, None)
            event_faces.pop(event_id, None)

def split_balcony_event_groups(events):
    """Группирует события по времени и не даёт одному объекту растянуть клип."""
    max_duration = max(BALCONY_MIN_EVENT_DURATION, BALCONY_MAX_CLIP_DURATION)
    merge_gap = max(0, BALCONY_EVENT_MERGE_GAP)
    groups = []

    for event in sorted(events, key=lambda item: float(item["start_time"])):
        start_time = float(event["start_time"])
        raw_end_time = max(start_time, float(event["end_time"]))
        effective_end_time = min(raw_end_time, start_time + max_duration)

        if not groups:
            groups.append({
                "events": [event],
                "start_time": start_time,
                "end_time": effective_end_time,
                "raw_end_time": raw_end_time,
            })
            continue

        group = groups[-1]
        group_limit = group["start_time"] + max_duration
        if (
            event.get("camera") == group["events"][0].get("camera")
            and start_time <= group["end_time"] + merge_gap
            and start_time <= group_limit
        ):
            group["events"].append(event)
            group["end_time"] = min(
                group_limit, max(group["end_time"], effective_end_time)
            )
            group["raw_end_time"] = max(group["raw_end_time"], raw_end_time)
        else:
            groups.append({
                "events": [event],
                "start_time": start_time,
                "end_time": effective_end_time,
                "raw_end_time": raw_end_time,
            })

    return groups

def process_balcony_event_group(group):
    valid_events = group["events"]
    start_time = group["start_time"]
    end_time = group["end_time"]
    event_duration = max(0, end_time - start_time)
    raw_duration = max(0, group["raw_end_time"] - start_time)
    event_ids = [event["id"] for event in valid_events]

    if event_duration < BALCONY_MIN_EVENT_DURATION:
        logger.info(
            f"Ignoring short balcony event group: {event_duration:.2f}s, "
            f"events={event_ids}"
        )
        return

    was_capped = raw_duration > event_duration + 0.01
    if was_capped:
        logger.warning(
            f"Capping balcony group from {raw_duration:.2f}s to "
            f"{event_duration:.2f}s, events={event_ids}"
        )

    first_event = valid_events[0]
    primary_event_id = first_event["id"]
    camera = first_event["camera"]
    logger.info(
        f"Processing balcony group: {len(valid_events)} event(s), "
        f"duration={event_duration:.2f}s, events={event_ids}"
    )
    minimum_duration = None
    if was_capped:
        minimum_duration = min(event_duration, BALCONY_LONG_CLIP_MIN_DURATION)
    video_path, snapshot_path = download_clip(
        primary_event_id, camera, start_time, end_time,
        minimum_duration_override=minimum_duration
    )
    if not video_path:
        return

    description = next(
        (event_descriptions.get(event["id"], "") for event in valid_events
         if event_descriptions.get(event["id"])),
        ""
    )
    if description:
        description = translate_to_russian(description)

    faces = []
    seen_faces = set()
    for event in valid_events:
        for face in event_faces.get(event["id"], []):
            face_key = (face.get("name"), face.get("score"))
            if face_key not in seen_faces:
                seen_faces.add(face_key)
                faces.append(face)

    process_single_video(
        video_path, snapshot_path, primary_event_id,
        description, faces, camera
    )

def balcony_worker_loop():
    """Объединяет близкие события балкона и отправляет один ролик вместо серии дублей."""
    logger.info("Balcony worker started")
    while True:
        first_data = balcony_event_queue.get()
        events = [first_data["after"]]

        while len(events) < max(1, BALCONY_MAX_GROUP_EVENTS):
            try:
                next_data = balcony_event_queue.get(timeout=max(0.1, BALCONY_GROUP_TIMEOUT))
                events.append(next_data["after"])
            except queue.Empty:
                break

        valid_events = [
            event for event in events
            if event.get("id") and event.get("start_time") is not None
            and event.get("end_time") is not None
        ]
        if not valid_events:
            clear_event_metadata(events)
            continue

        try:
            for group in split_balcony_event_groups(valid_events):
                process_balcony_event_group(group)
        finally:
            clear_event_metadata(events)

def resilient_balcony_worker_loop():
    """Перезапускает обработчик балкона после неожиданной ошибки одного события."""
    while True:
        try:
            balcony_worker_loop()
        except Exception:
            logger.exception("Balcony worker failed; restarting")
            time.sleep(1)

def worker_loop():
    logger.info("Worker started")
    while True:
        session_files = []

        data = event_queue.get()
        if not data.get("after", {}).get("id"):
            continue
        event = data["after"]
        eid, cam, ts = event["id"], event["camera"], event["start_time"]
        video_path, snap_path = download_clip(eid, cam, ts, event.get("end_time"))
        if video_path:
            raw_desc = event_descriptions.get(eid, "")
            if raw_desc:
                raw_desc = translate_to_russian(raw_desc)

            faces_list = event_faces.get(eid, [])

            if cam == "balcony":
                process_single_video(video_path, snap_path, eid, raw_desc, faces_list, cam)
                continue

            session_files.append((video_path, snap_path, eid, raw_desc, faces_list))

        while True:
            if len(session_files) >= MAX_FILES:
                logger.info(f"File count limit reached ({MAX_FILES}), forcing merge")
                break

            try:
                next_data = event_queue.get(timeout=GROUP_TIMEOUT)
                next_event = next_data["after"]
                neid, ncam, nts = next_event["id"], next_event["camera"], next_event["start_time"]
                nvideo, nsnap = download_clip(neid, ncam, nts, next_event.get("end_time"))
                if nvideo:
                    raw_ndesc = event_descriptions.get(neid, "")
                    if raw_ndesc:
                        raw_ndesc = translate_to_russian(raw_ndesc)

                    nfaces = event_faces.get(neid, [])

                    if ncam == "balcony":
                        process_single_video(nvideo, nsnap, neid, raw_ndesc, nfaces, ncam)
                        continue

                    session_files.append((nvideo, nsnap, neid, raw_ndesc, nfaces))
            except queue.Empty:
                break

        for i, (vpath, spath, eid, desc, faces) in enumerate(session_files):
            updated = False
            if not desc and eid in event_descriptions:
                desc = translate_to_russian(event_descriptions[eid])
                updated = True
            if not faces and eid in event_faces:
                faces = event_faces[eid]
                updated = True
            if updated:
                session_files[i] = (vpath, spath, eid, desc, faces)
                logger.info(f"Updated data for event {eid}: desc='{desc[:30]}', faces={len(faces)}")

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
                event = data.get("after", {})
                event_id = event.get("id")
                if not event_id:
                    return
                if not register_event_once(event_id):
                    logger.info(f"Ignoring duplicate MQTT end event: {event_id}")
                    return
                if event.get("false_positive"):
                    logger.info(f"Ignoring false-positive event: {event_id}")
                    clear_event_metadata([event])
                    return
                if event.get("camera") == "balcony":
                    balcony_event_queue.put(data)
                else:
                    event_queue.put(data)

        elif msg.topic == MQTT_TOPIC_DESCR:
            data = json.loads(msg.payload.decode())

            if data.get("type") == "description":
                event_id = data.get("id")
                description = data.get("description")
                if event_id and description:
                    event_descriptions[event_id] = description
                    logger.info(f"Stored description for event {event_id}: {description}")

            elif data.get("type") == "face":
                event_id = data.get("id")
                name = data.get("name")
                score = data.get("score", 0)
                if event_id and name and score >= FACE_CONFIDENCE_THRESHOLD:
                    if event_id not in event_faces:
                        event_faces[event_id] = []
                    event_faces[event_id].append({"name": name, "score": score})
                    logger.info(f"Face recognized for event {event_id}: {name} (confidence: {score:.2f})")

    except Exception as e:
        logger.error(f"MQTT error: {e}")

# ========== MAIN ==========
def cleanup_orphan_new_snapshots():
    """Удаляет snapshots, для которых в new_event уже нет исходного видео."""
    removed = 0
    for snapshot_path in NEW_DIR.glob("*.jpg"):
        if not snapshot_path.with_suffix(".mp4").exists():
            snapshot_path.unlink(missing_ok=True)
            removed += 1
    if removed:
        logger.info(f"Startup: removed {removed} orphan snapshots from {NEW_DIR}")

def main():
    recover_untracked_send_files()
    threading.Thread(target=delivery_retry_loop, daemon=True).start()

    cleanup_orphan_new_snapshots()
    initial = sorted(NEW_DIR.glob("*.mp4"))
    if initial:
        logger.info(f"Startup: {len(initial)} files found → force merge")
        fake_list = []
        for video_path in initial:
            snapshot_path = video_path.with_suffix(".jpg")
            fake_list.append((
                video_path,
                snapshot_path if snapshot_path.exists() else None,
                "", "", []
            ))
        threading.Thread(target=lambda: process_batch(fake_list), daemon=True).start()

    threading.Thread(target=worker_loop, daemon=True).start()
    threading.Thread(target=resilient_balcony_worker_loop, daemon=True).start()

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_forever()

if __name__ == "__main__":
    main()
