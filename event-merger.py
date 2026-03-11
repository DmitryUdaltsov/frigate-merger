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
event_faces = {}  # {event_id: [{"name": str, "score": float}, ...]}

# Семафор для ограничения параллельных NVENC сессий (2 одновременно)
nvenc_semaphore = threading.Semaphore(2)

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

def send_telegram_media_group(video_path, photo_path, caption, chat_id, bot_token):
    """Отправляет фото и видео как группу медиа в указанный чат."""
    url = f"https://api.telegram.org/bot{bot_token}/sendMediaGroup"
    proxies = get_proxies()

    media = []
    files = {}
    try:
        if photo_path and Path(photo_path).exists():
            media.append({
                'type': 'photo',
                'media': 'attach://photo',
                'caption': caption,
            })
            files['photo'] = open(photo_path, 'rb')
        media.append({
            'type': 'video',
            'media': 'attach://video',
        })
        files['video'] = open(video_path, 'rb')

        payload = {
            'chat_id': chat_id,
            'media': json.dumps(media)
        }

        for attempt in range(1, TELEGRAM_RETRY_ATTEMPTS + 1):
            try:
                response = requests.post(url, data=payload, files=files, timeout=60, proxies=proxies)
                response.raise_for_status()
                logger.info(f"Media group sent to {chat_id}: {video_path.name}")
                return True
            except Exception as e:
                logger.warning(f"Media group attempt {attempt} to {chat_id} failed: {e}")
                if attempt < TELEGRAM_RETRY_ATTEMPTS:
                    time.sleep(TELEGRAM_RETRY_DELAY)
        return False
    finally:
        for f in files.values():
            f.close()

def send_telegram_video(video_path, caption, chat_id, bot_token):
    """Отправляет только видео в указанный чат."""
    url = f"https://api.telegram.org/bot{bot_token}/sendVideo"
    proxies = get_proxies()

    for attempt in range(1, TELEGRAM_RETRY_ATTEMPTS + 1):
        try:
            with open(video_path, 'rb') as video_file:
                files = {'video': video_file}
                data = {'chat_id': chat_id, 'caption': caption}
                response = requests.post(url, files=files, data=data, timeout=60, proxies=proxies)
                response.raise_for_status()
                logger.info(f"Video sent to {chat_id}: {video_path.name}")
                return True
        except Exception as e:
            logger.warning(f"Video send attempt {attempt} to {chat_id} failed: {e}")
            if attempt < TELEGRAM_RETRY_ATTEMPTS:
                time.sleep(TELEGRAM_RETRY_DELAY)
    logger.error(f"Failed to send {video_path.name} to {chat_id} after {TELEGRAM_RETRY_ATTEMPTS} attempts")
    return False

# ========== НОРМАЛИЗАЦИЯ С FALLBACK И РАЗНЫМ FPS ДЛЯ КАМЕР ==========
def normalize_video(input_path, output_path):
    audio_args = [] if has_audio_stream(input_path) else ["-an"]

    # Единый FPS для всех камер (компромисс между 20 и 25)
    target_fps = 20

    # Базовая команда для NVENC
    cmd_nvenc = [
        "ffmpeg",
        "-i", str(input_path),
        "-vf", f"scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2:black,fps={target_fps}",
        "-c:v", "h264_nvenc", "-preset", "p5",
        "-b:v", "2M", "-maxrate", "2.5M", "-bufsize", "5M",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "96k",
        "-movflags", "+faststart",
        "-y", str(output_path)
    ] + audio_args

    # Запасная программная команда (libx264)
    cmd_sw = [
        "ffmpeg",
        "-i", str(input_path),
        "-vf", f"scale=1280:720:force_original_aspect_ratio=decrease,pad=1280:720:(ow-iw)/2:(oh-ih)/2:black,fps={target_fps}",
        "-c:v", "libx264", "-preset", "medium", "-crf", "23",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac", "-b:a", "96k",
        "-movflags", "+faststart",
        "-y", str(output_path)
    ] + audio_args

    try:
        with nvenc_semaphore:
            run_ffmpeg(cmd_nvenc)
    except Exception as e:
        logger.warning(f"NVENC failed for {input_path.name}, falling back to software encoding. Error: {e}")
        run_ffmpeg(cmd_sw)
    os.chmod(output_path, 0o664)
    logger.info(f"Normalized: {output_path.name} ({os.path.getsize(output_path)/1024/1024:.2f} MB)")

# ========== СКАЧИВАНИЕ ==========
def download_clip(event_id, camera, start_time):
    """Скачивает видео и snapshot. Возвращает (video_path, snapshot_path)."""
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
            if get_duration(video_path) < 1:
                video_path.unlink(missing_ok=True)
                logger.warning(f"Video {video_path.name} zero duration, retrying...")
                continue
            os.chmod(video_path, 0o664)
            video_ok = True
            break
        except Exception as e:
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

# ========== РАЗБИЕНИЕ С FALLBACK ==========
def split_video(input_path, prefix):
    size_mb = os.path.getsize(input_path) / (1024 * 1024)
    if size_mb <= MAX_SAFE_SIZE_MB:
        return [input_path]

    duration = get_duration(input_path)
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

    while current < duration - 0.1:
        out = SEND_DIR / f"{prefix}_p{index:03d}.mp4"
        logger.info(f"Creating segment {out.name} from {current:.2f}s to {current+segment_duration:.2f}s")

        # NVENC команда для сегмента
        cmd_nvenc = [
            "ffmpeg",
            "-i", str(input_path), "-ss", str(current), "-t", str(segment_duration),
            "-c:v", "h264_nvenc", "-preset", "p5",
            "-b:v", "2M", "-maxrate", "2.5M", "-bufsize", "5M",
            "-pix_fmt", "yuv420p", "-movflags", "+faststart",
        ]
        # Программная команда для сегмента
        cmd_sw = [
            "ffmpeg",
            "-i", str(input_path), "-ss", str(current), "-t", str(segment_duration),
            "-c:v", "libx264", "-preset", "fast", "-crf", "23",
            "-pix_fmt", "yuv420p", "-movflags", "+faststart",
        ]
        if has_audio:
            cmd_nvenc.extend(["-c:a", "aac", "-b:a", "64k"])
            cmd_sw.extend(["-c:a", "aac", "-b:a", "64k"])
        else:
            cmd_nvenc.append("-an")
            cmd_sw.append("-an")
        cmd_nvenc.extend(["-y", str(out)])
        cmd_sw.extend(["-y", str(out)])

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

        part_dur = get_duration(out)
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

    temp_dir = TEMP_DIR / f"single_{int(time.time())}_{camera}"
    temp_dir.mkdir(exist_ok=True)

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
            if snapshot_path and Path(snapshot_path).exists():
                ok = send_telegram_media_group(
                    norm_path, snapshot_path, caption,
                    TELEGRAM_CHAT_ID, TELEGRAM_BOT_TOKEN
                )
            else:
                ok = send_telegram_video(
                    norm_path, caption,
                    TELEGRAM_CHAT_ID, TELEGRAM_BOT_TOKEN
                )
            if ok:
                logger.info(f"Single video sent: {norm_path.name}")
                norm_path.unlink(missing_ok=True)
            else:
                logger.error(f"Failed to send single video {norm_path.name}, keeping in temp")
        else:
            parts = split_video(norm_path, f"{norm_path.stem}_part")
            all_sent = True
            for part in parts:
                if send_telegram_video(part, caption, TELEGRAM_CHAT_ID, TELEGRAM_BOT_TOKEN):
                    part.unlink(missing_ok=True)
                else:
                    all_sent = False
                    logger.error(f"Failed to send part {part.name}, keeping")
            if all_sent:
                logger.info("All parts of single video sent")
                norm_path.unlink(missing_ok=True)
            else:
                logger.warning("Some parts of single video failed to send")
    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
        video_path.unlink(missing_ok=True)
        if snapshot_path:
            snapshot_path.unlink(missing_ok=True)
        event_descriptions.pop(event_id, None)
        event_faces.pop(event_id, None)

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

    temp_dir = TEMP_DIR / f"batch_{int(time.time())}"
    temp_dir.mkdir(exist_ok=True)

    try:
        normalized = []
        for i, (video_path, snap_path, eid, desc, faces) in enumerate(file_paths):
            norm = temp_dir / f"norm_{i:03d}.mp4"
            try:
                normalize_video(video_path, norm)
                normalized.append(norm)
            except Exception as e:
                logger.error(f"Normalization failed {video_path}: {e}")

        if not normalized:
            return

        list_file = temp_dir / "list.txt"
        with open(list_file, "w") as lf:
            for nf in normalized:
                lf.write(f"file '{nf}'\n")

        merged = NEW_DIR / f"merged_{int(time.time())}.mp4"

        # Конкатенация с fallback
        concat_cmd_nvenc = [
            "ffmpeg", "-f", "concat", "-safe", "0", "-i", str(list_file),
            "-c:v", "h264_nvenc", "-preset", "p5",
            "-b:v", "2M", "-maxrate", "2.5M", "-bufsize", "5M",
            "-pix_fmt", "yuv420p", "-movflags", "+faststart",
        ]
        concat_cmd_sw = [
            "ffmpeg", "-f", "concat", "-safe", "0", "-i", str(list_file),
            "-c:v", "libx264", "-preset", "medium", "-crf", "23",
            "-pix_fmt", "yuv420p", "-movflags", "+faststart",
        ]
        if has_audio_stream(normalized[0]):
            concat_cmd_nvenc.extend(["-c:a", "aac", "-b:a", "64k"])
            concat_cmd_sw.extend(["-c:a", "aac", "-b:a", "64k"])
        else:
            concat_cmd_nvenc.append("-an")
            concat_cmd_sw.append("-an")
        concat_cmd_nvenc.extend(["-y", str(merged)])
        concat_cmd_sw.extend(["-y", str(merged)])

        try:
            with nvenc_semaphore:
                run_ffmpeg(concat_cmd_nvenc)
        except Exception as e:
            logger.warning(f"NVENC concat failed, falling back to software encoding. Error: {e}")
            run_ffmpeg(concat_cmd_sw)

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

            if final_snapshot:
                ok = send_telegram_media_group(
                    final_video, final_snapshot, final_description,
                    TELEGRAM_CHAT_ID, TELEGRAM_BOT_TOKEN
                )
            else:
                ok = send_telegram_video(
                    final_video, final_description,
                    TELEGRAM_CHAT_ID, TELEGRAM_BOT_TOKEN
                )

            if ok:
                if SECOND_TELEGRAM_CHAT_ID:
                    if final_snapshot:
                        send_telegram_media_group(
                            final_video, final_snapshot, "",
                            SECOND_TELEGRAM_CHAT_ID,
                            SECOND_TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN
                        )
                    else:
                        send_telegram_video(
                            final_video, "",
                            SECOND_TELEGRAM_CHAT_ID,
                            SECOND_TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN
                        )
                final_video.unlink(missing_ok=True)
                if final_snapshot:
                    final_snapshot.unlink(missing_ok=True)
                logger.info(f"Sent and deleted: {final_video.name}")
            else:
                logger.error(f"Failed to send {final_video.name}, keeping files")
        else:
            parts = split_video(merged, merged.stem)
            merged.unlink(missing_ok=True)

            all_sent = True
            for part in parts:
                if send_telegram_video(part, final_description, TELEGRAM_CHAT_ID, TELEGRAM_BOT_TOKEN):
                    if SECOND_TELEGRAM_CHAT_ID:
                        send_telegram_video(
                            part, "",
                            SECOND_TELEGRAM_CHAT_ID,
                            SECOND_TELEGRAM_BOT_TOKEN or TELEGRAM_BOT_TOKEN
                        )
                    part.unlink(missing_ok=True)
                else:
                    all_sent = False
                    logger.error(f"Failed to send {part.name}, keeping file")

            if all_sent:
                logger.info(f"All {len(parts)} parts sent and deleted")
            else:
                logger.warning(f"Some parts failed to send, kept in {SEND_DIR}")

        for video_path, snap_path, eid, _, _ in file_paths:
            video_path.unlink(missing_ok=True)
            if snap_path:
                snap_path.unlink(missing_ok=True)
            event_descriptions.pop(eid, None)
            event_faces.pop(eid, None)

    finally:
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)

# ========== WORKER ==========
def worker_loop():
    logger.info("Worker started")
    while True:
        session_files = []

        data = event_queue.get()
        if not data.get("after", {}).get("id"):
            continue
        eid, cam, ts = data["after"]["id"], data["after"]["camera"], data["after"]["start_time"]
        video_path, snap_path = download_clip(eid, cam, ts)
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
                neid, ncam, nts = next_data["after"]["id"], next_data["after"]["camera"], next_data["after"]["start_time"]
                nvideo, nsnap = download_clip(neid, ncam, nts)
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
def main():
    initial = list(NEW_DIR.glob("*.mp4"))
    if initial:
        logger.info(f"Startup: {len(initial)} files found → force merge")
        fake_list = [(p, None, "", "", []) for p in initial]
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