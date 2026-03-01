#!/usr/bin/python3
import os
import time
import json
import threading
import subprocess
import shutil
import requests
import logging
from pathlib import Path

# ==================== CONFIG ====================
FRIGATE_API_URL = "http://localhost:5000"
MQTT_BROKER = "localhost"
MQTT_TOPIC = "frigate/events"

NEW_DIR = Path("/config/new_event")
SEND_DIR = Path("/config/send")

MAX_WAIT = 120          # секунд после первого события
MAX_FILES = 10          # макс. файлов до принудительного слияния

DOWNLOAD_DELAY_SEC = 25
MAX_DOWNLOAD_ATTEMPTS = 4
MAX_SAFE_SIZE_MB = 45
TARGET_SEGMENT_MB = 32
MAX_SEGMENT_BYTES = TARGET_SEGMENT_MB * 1024 * 1024

# ==================== GLOBALS ====================
merge_lock = threading.Lock()
first_event_time = None
merge_scheduled = False
timer_lock = threading.Lock()
timer = None

# ==================== UTILS ====================
def run_ffmpeg(cmd, timeout=300):
    try:
        result = subprocess.run(
            cmd,
            check=True,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        return result
    except subprocess.CalledProcessError as e:
        logging.error(f"FFmpeg error:\n{' '.join(cmd)}\n{e.stderr}")
        raise
    except subprocess.TimeoutExpired:
        logging.error(f"FFmpeg timeout:\n{' '.join(cmd)}")
        raise

def get_duration(path):
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(path)
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=True
        )
        return float(result.stdout.strip())
    except (subprocess.SubprocessError, ValueError, OSError) as e:
        logging.warning(f"Failed to get duration for {path}: {e}")
        return 0.0

def has_audio_stream(path):
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-select_streams", "a",
                "-show_entries", "stream=codec_type",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(path)
            ],
            capture_output=True,
            text=True,
            timeout=10,
            check=True
        )
        return bool(result.stdout.strip())
    except Exception:
        return False

# ==================== NORMALIZE ====================
def normalize_video(input_path, output_path):
    audio_option = [] if has_audio_stream(input_path) else ["-an"]

    cmd = [
        "ffmpeg",
        "-hwaccel", "cuda",
        "-fflags", "+genpts",
        "-i", str(input_path),
        "-vf", "scale=1280:720:force_original_aspect_ratio=decrease,"
               "pad=1280:720:(ow-iw)/2:(oh-ih)/2:black,fps=20",  # FPS в фильтре
        "-c:v", "h264_nvenc",
        "-preset", "p5",
        "-tune", "hq",
        "-profile:v", "high",
        "-level", "4.1",
        "-force_key_frames", "expr:gte(t,n_forced*2)",
        "-b:v", "4M",
        "-maxrate", "5M",
        "-bufsize", "8M",
        "-pix_fmt", "yuv420p",
        "-c:a", "aac",
        "-b:a", "96k",
        "-movflags", "+faststart",
        "-y",
        str(output_path)
    ] + audio_option

    run_ffmpeg(cmd)
    os.chmod(output_path, 0o664)

# ==================== DOWNLOAD ====================
def download_clip(event_id, camera, start_time):
    url = f"{FRIGATE_API_URL}/api/events/{event_id}/clip.mp4"
    filename = f"{int(start_time)}_{camera}_{event_id}.mp4"
    filepath = NEW_DIR / filename

    for attempt in range(1, MAX_DOWNLOAD_ATTEMPTS + 1):
        if attempt > 1:
            time.sleep(15)

        try:
            with requests.get(url, stream=True, timeout=(10, 120)) as resp:
                try:
                    resp.raise_for_status()
                except requests.RequestException as e:
                    logging.warning(f"HTTP error {resp.status_code} for {event_id}: {e}")
                    continue

                with open(filepath, "wb") as f:
                    for chunk in resp.iter_content(8192):
                        f.write(chunk)

            if get_duration(filepath) < 1:
                filepath.unlink(missing_ok=True)
                logging.warning(f"Downloaded file {filename} has zero duration, deleted.")
                continue

            os.chmod(filepath, 0o664)
            logging.info(f"Downloaded: {filepath.name}")
            return filepath

        except Exception as e:
            logging.warning(f"Download error {event_id}: {e}")
            time.sleep(5)

    logging.error(f"Failed to download clip: {event_id}")
    return None

# ==================== SPLIT ====================
def split_video(input_path, prefix):
    """Разрезать видео на части с перекодированием (если слишком большое)."""
    duration = get_duration(input_path)
    if duration <= 0:
        return [input_path]

    size_mb = os.path.getsize(input_path) / (1024 * 1024)
    logging.info(f"Splitting {input_path.name}, duration: {duration:.2f}s, size: {size_mb:.2f}MB")
    if size_mb <= MAX_SAFE_SIZE_MB:
        return [input_path]

    bitrate = 4_000_000
    segment_duration = max(5, int((MAX_SEGMENT_BYTES * 8) / bitrate))

    parts = []
    current = 0
    index = 1
    has_audio = has_audio_stream(input_path)

    while current < duration:
        out = SEND_DIR / f"{prefix}_p{index:03d}.mp4"

        cmd = [
            "ffmpeg",
            "-hwaccel", "cuda",
            "-fflags", "+genpts",
            "-i", str(input_path),
            "-ss", str(current),
            "-t", str(segment_duration),
            "-vf", "fps=20",  # FPS здесь тоже через фильтр
            "-c:v", "h264_nvenc",
            "-preset", "p5",
            "-tune", "hq",
            "-profile:v", "high",
            "-level", "4.1",
            "-force_key_frames", "expr:gte(t,n_forced*2)",
            "-b:v", "4M",
            "-maxrate", "5M",
            "-bufsize", "8M",
            "-pix_fmt", "yuv420p",
            "-movflags", "+faststart",
            "-avoid_negative_ts", "make_zero",
            "-y",
            str(out)
        ]

        if has_audio:
            cmd.extend(["-c:a", "aac", "-b:a", "96k"])
        else:
            cmd.append("-an")

        run_ffmpeg(cmd, timeout=180)

        part_dur = get_duration(out)
        part_size = os.path.getsize(out) / (1024 * 1024)
        if part_dur <= 0:
            logging.error(f"Segment {out.name} has zero duration, deleting")
            out.unlink(missing_ok=True)
            break

        logging.info(f"Created segment {out.name}, duration: {part_dur:.2f}s, size: {part_size:.2f}MB")
        os.chmod(out, 0o664)
        parts.append(out)

        current += part_dur
        index += 1

    return parts

# ==================== MERGE ====================
def merge_videos():
    global first_event_time, merge_scheduled, timer
    logging.info("merge_videos called")

    if not merge_lock.acquire(blocking=False):
        return

    temp_dir = None

    try:
        files = sorted(list(NEW_DIR.glob("*.mp4")))
        if not files:
            return

        logging.info(f"Starting merge of {len(files)} files")

        timestamp = int(time.time())
        temp_dir = NEW_DIR / f"temp_{timestamp}"
        temp_dir.mkdir(exist_ok=True)

        normalized_files = []

        for idx, file in enumerate(files):
            target = temp_dir / f"norm_{idx:03d}.mp4"
            try:
                normalize_video(file, target)
                normalized_files.append(target)
            except Exception as e:
                logging.error(f"Failed to normalize {file.name}: {e}")
                file.unlink(missing_ok=True)

        if not normalized_files:
            logging.warning("No files successfully normalized, aborting merge")
            return

        list_file = temp_dir / "list.txt"
        with open(list_file, "w") as f:
            for nf in normalized_files:
                f.write(f"file '{nf.absolute()}'\n")

        merged_path = NEW_DIR / f"merged_{timestamp}.mp4"

        has_audio = has_audio_stream(normalized_files[0])

        concat_cmd = [
            "ffmpeg",
            "-f", "concat",
            "-safe", "0",
            "-i", str(list_file),
            "-c:v", "h264_nvenc",
            "-preset", "p5",
            "-tune", "hq",
            "-profile:v", "high",
            "-level", "4.1",
            "-force_key_frames", "expr:gte(t,n_forced*2)",
            "-b:v", "4M",
            "-maxrate", "5M",
            "-bufsize", "8M",
            "-pix_fmt", "yuv420p",
            "-c:a", "aac",
            "-b:a", "96k",
            "-movflags", "+faststart",
            "-y",
            str(merged_path)
        ]

        if not has_audio:
            concat_cmd.insert(-2, "-an")  # Вставляем -an перед выходным путём

        run_ffmpeg(concat_cmd)

        size_mb = os.path.getsize(merged_path) / (1024 * 1024)

        if size_mb <= MAX_SAFE_SIZE_MB:
            final = SEND_DIR / f"merged_{timestamp}.mp4"
            shutil.move(str(merged_path), str(final))
            os.chmod(final, 0o664)
            logging.info(f"Merged video moved to send: {final.name}")
        else:
            parts = split_video(merged_path, f"merged_{timestamp}")
            merged_path.unlink(missing_ok=True)
            logging.info(f"Split into {len(parts)} parts")

        for f in files:
            f.unlink(missing_ok=True)

    except Exception as e:
        logging.error(f"MERGE ERROR: {e}")

    finally:
        merge_lock.release()
        if temp_dir and temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)
        first_event_time = None
        merge_scheduled = False

# ==================== TIMER ====================
def schedule_merge():
    global merge_scheduled, timer
    with timer_lock:
        if merge_scheduled:
            return
        merge_scheduled = True
        timer = threading.Timer(MAX_WAIT, merge_videos)
        timer.daemon = True
        timer.start()
        logging.debug(f"Merge scheduled in {MAX_WAIT}s")

# ==================== EVENT HANDLER ====================
def handle_event_download(event_id, camera, start_time):
    global first_event_time, merge_scheduled, timer

    path = download_clip(event_id, camera, start_time)
    if not path:
        return

    with timer_lock:
        if first_event_time is None:
            first_event_time = time.time()
            schedule_merge()
            logging.info("First event in batch, timer started")  # было debug

    file_count = len(list(NEW_DIR.glob("*.mp4")))
    if file_count >= MAX_FILES:
        logging.info(f"File count reached {file_count}, forcing merge")
        # Отменяем таймер, если он ещё не сработал
        with timer_lock:
            if timer:
                timer.cancel()
                timer = None
            merge_scheduled = False  # разрешим новое планирование после merge
        merge_videos()

def on_message(client, userdata, msg):
    try:
        payload = json.loads(msg.payload.decode())
        if payload.get("type") != "end":
            return

        after = payload.get("after", {})
        event_id = after.get("id")
        camera = after.get("camera")
        start_time = after.get("start_time")

        if not all([event_id, camera, start_time]):
            return

        threading.Thread(
            target=handle_event_download,
            args=(event_id, camera, start_time),
            daemon=True
        ).start()

    except Exception as e:
        logging.error(f"MQTT message error: {e}")

# ==================== MAIN ====================
def main():
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        stream=sys.stdout
    )

    NEW_DIR.mkdir(parents=True, exist_ok=True)
    SEND_DIR.mkdir(parents=True, exist_ok=True)

    logging.info("=" * 50)
    logging.info("Скрипт запущен")
    logging.info(f"MAX_WAIT = {MAX_WAIT}s, MAX_FILES = {MAX_FILES}")
    logging.info("=" * 50)

    import paho.mqtt.client as mqtt
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    client.connect(MQTT_BROKER, 1883, 60)
    client.subscribe(MQTT_TOPIC)
    client.loop_forever()

if __name__ == "__main__":
    main()