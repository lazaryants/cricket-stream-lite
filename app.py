#!/usr/bin/env python3
"""
Cricket Stream Bridge - Flask Application
Управление рестримом с YouTube/Twitch/Kick на RTMP сервер.
"""
from flask import Flask, jsonify, request, render_template, send_file
import json
import ipaddress
import os
import re
import shlex
import signal
import socket
import subprocess
import tempfile
import uuid
import unicodedata
import threading
import time
from datetime import datetime
from urllib.parse import urlsplit
from pathlib import Path

app = Flask(__name__)

CONFIG_PATH = '/opt/stream-bridge/config/config.json'
LOG_DIR = '/opt/stream-bridge/logs'
DOWNLOAD_DIR = '/opt/stream-bridge/downloads'
BIN_DIR = '/opt/stream-bridge-env/bin'

active_streams = {}
recovering_streams = set()
starting_streams = set()
active_streams_lock = threading.RLock()
config_lock = threading.RLock()
download_jobs_lock = threading.RLock()
download_jobs = {}

RESTART_DELAYS = [5, 10, 20, 40, 60]
MAX_RESTART_ATTEMPTS = len(RESTART_DELAYS)
AUTOSTART_DELAY = 3
STABLE_RESET_SECONDS = 60
LOG_TAIL_BYTES = 256 * 1024
LOG_HEAD_BYTES = 64 * 1024
monitoring_active = True
DOWNLOAD_MAX_AGE_SECONDS = 3600
DOWNLOAD_MAX_CONCURRENT = 1
COMPONENT_STATUS_PATH = '/opt/stream-bridge/config/component-status.json'
COMPONENT_CHECK_SERVICE = 'cricket-stream-components-check.service'
COMPONENT_UPDATE_SERVICES = {
    'streamlink': 'cricket-stream-update-streamlink.service',
    'yt-dlp': 'cricket-stream-update-ytdlp.service',
}


def load_config():
    with config_lock:
        try:
            with open(CONFIG_PATH, 'r', encoding='utf-8') as file:
                config = json.load(file)
            if not isinstance(config, dict):
                raise ValueError('Config root must be an object')
            if not isinstance(config.get('streams', []), list):
                raise ValueError('"streams" must be a list')
            config.setdefault('streams', [])
            config.setdefault('settings', {})
            return config
        except Exception as exc:
            app.logger.error(f"Error loading config: {exc}")
            return {"streams": [], "settings": {}}


def save_config(config):
    """Atomically write config so an interrupted write cannot truncate it."""
    config_dir = os.path.dirname(CONFIG_PATH)
    os.makedirs(config_dir, exist_ok=True)

    with config_lock:
        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                mode='w',
                encoding='utf-8',
                dir=config_dir,
                prefix='.config.',
                suffix='.tmp',
                delete=False,
            ) as file:
                temp_path = file.name
                json.dump(config, file, ensure_ascii=False, indent=2)
                file.write('\n')
                file.flush()
                os.fsync(file.fileno())

            os.chmod(temp_path, 0o664)
            os.replace(temp_path, CONFIG_PATH)

            try:
                directory_fd = os.open(config_dir, os.O_DIRECTORY)
                try:
                    os.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            except OSError:
                pass
            return True
        except Exception as exc:
            app.logger.error(f"Error saving config: {exc}")
            if temp_path:
                try:
                    os.unlink(temp_path)
                except OSError:
                    pass
            return False


def clean_text(value, field_name, max_length):
    if not isinstance(value, str):
        raise ValueError(f'{field_name} must be a string')

    value = value.strip()
    if not value:
        raise ValueError(f'{field_name} cannot be empty')
    if len(value) > max_length:
        raise ValueError(f'{field_name} is too long')
    if any(ord(char) < 32 or ord(char) == 127 for char in value):
        raise ValueError(f'{field_name} contains control characters')
    return value


def normalize_url(value, field_name, allowed_schemes):
    value = clean_text(value, field_name, 4096)
    parsed = urlsplit(value)

    if parsed.scheme.lower() not in allowed_schemes:
        allowed = ', '.join(f'{scheme}://' for scheme in sorted(allowed_schemes))
        raise ValueError(f'{field_name} must start with {allowed}')
    if not parsed.netloc:
        raise ValueError(f'{field_name} has no host')
    if parsed.username or parsed.password:
        raise ValueError(f'{field_name} must not contain URL credentials')
    if any(char.isspace() for char in value):
        raise ValueError(f'{field_name} must not contain spaces or line breaks')

    return value


def reject_private_download_target(url):
    parsed = urlsplit(url)
    hostname = parsed.hostname
    if not hostname:
        raise ValueError('URL has no host')
    if hostname.lower() in {'localhost', 'localhost.localdomain'}:
        raise ValueError('Local addresses are not allowed')
    try:
        addresses = {item[4][0] for item in socket.getaddrinfo(hostname, parsed.port or 443, type=socket.SOCK_STREAM)}
    except socket.gaierror as exc:
        raise ValueError(f'Host cannot be resolved: {exc}') from exc
    for address in addresses:
        ip = ipaddress.ip_address(address.split('%', 1)[0])
        if not ip.is_global:
            raise ValueError('Private or local network addresses are not allowed')


def normalize_stream_payload(data, existing=None):
    if not isinstance(data, dict):
        raise ValueError('JSON object expected')

    existing = existing or {}
    name = clean_text(data.get('name', existing.get('name', 'Stream')), 'name', 200)
    source_url = normalize_url(
        data.get('source_url', existing.get('source_url')),
        'source_url',
        {'http', 'https'},
    )
    target_rtmp = normalize_url(
        data.get('target_rtmp', existing.get('target_rtmp')),
        'target_rtmp',
        {'rtmp', 'rtmps'},
    )

    source_type = clean_text(
        data.get('source_type', existing.get('source_type', 'youtube')),
        'source_type',
        40,
    ).lower()
    if not re.fullmatch(r'[a-z0-9_-]+', source_type):
        raise ValueError('source_type contains invalid characters')

    engine = clean_text(
        data.get('engine', existing.get('engine', 'auto')),
        'engine',
        20,
    ).lower()
    if engine not in {'auto', 'streamlink', 'yt-dlp'}:
        raise ValueError('engine must be auto, streamlink, or yt-dlp')

    enabled = data.get('enabled', existing.get('enabled', True))
    if not isinstance(enabled, bool):
        raise ValueError('enabled must be true or false')

    show_on_monitoring = data.get(
        'show_on_monitoring',
        existing.get('show_on_monitoring', True),
    )
    if not isinstance(show_on_monitoring, bool):
        raise ValueError('show_on_monitoring must be true or false')

    return {
        'name': name,
        'source_url': source_url,
        'source_type': source_type,
        'engine': engine,
        'target_rtmp': target_rtmp,
        'enabled': enabled,
        'show_on_monitoring': show_on_monitoring,
        'desired_active': existing.get('desired_active', False),
    }


def get_stream_config(stream_id):
    config = load_config()
    stream = next(
        (item for item in config.get('streams', []) if item.get('id') == stream_id),
        None,
    )
    return config, stream


def update_stream_state(stream_id, **changes):
    config, stream = get_stream_config(stream_id)
    if not stream:
        return False
    stream.update(changes)
    stream['updated_at'] = datetime.now().isoformat()
    return save_config(config)


def extract_last_error(log_file, fallback=None):
    if not log_file or not os.path.exists(log_file):
        return fallback
    try:
        with open(log_file, 'rb') as file:
            file.seek(0, os.SEEK_END)
            size = file.tell()
            file.seek(max(0, size - LOG_TAIL_BYTES))
            content = file.read(LOG_TAIL_BYTES).decode('utf-8', errors='replace')
        patterns = (
            'already publishing', 'error', 'failed', 'failure', 'offline', 'no playable streams',
            'connection refused', 'broken pipe', 'timed out', 'timeout',
            'server returned', 'unable to open', 'could not open',
        )
        candidates = []
        for raw_line in content.splitlines():
            line = raw_line.strip()
            lowered = line.lower()
            if line and any(pattern in lowered for pattern in patterns):
                candidates.append(line)
        
        if candidates:
            selected = candidates[-1]
            for candidate in reversed(candidates):
                if 'already publishing' in candidate.lower():
                    return 'RTMP destination is already publishing'
            return selected[:500]
        return fallback
    except Exception as exc:
        app.logger.warning(f'Could not extract stream error: {exc}')
        return fallback


def cleanup_zombies():
    try:
        while True:
            pid, _status = os.waitpid(-1, os.WNOHANG)
            if pid == 0:
                break
    except ChildProcessError:
        pass
    except OSError:
        pass


def close_log_fd(stream_info):
    log_fd = stream_info.get('log_fd') if stream_info else None
    if log_fd and not log_fd.closed:
        try:
            log_fd.flush()
        except Exception:
            pass
        try:
            log_fd.close()
        except Exception:
            pass


def process_group_exists(pgid):
    """Return True while at least one process still belongs to the group."""
    if not pgid:
        return False
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError:
        return False


def wait_for_process_group_exit(pgid, timeout):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if not process_group_exists(pgid):
            return True
        cleanup_zombies()
        time.sleep(0.1)
    return not process_group_exists(pgid)


def terminate_stream_group(stream_info, stream_id, graceful_timeout=10):
    """Stop the entire Streamlink/FFmpeg process group reliably."""
    if not stream_info:
        return True

    proc = stream_info.get('proc')
    pgid = stream_info.get('pgid') or stream_info.get('pid')
    if not pgid and proc is not None:
        pgid = proc.pid

    try:
        if pgid and process_group_exists(pgid):
            app.logger.info(f'Stream {stream_id}: sending SIGINT to process group {pgid}')
            try:
                os.killpg(pgid, signal.SIGINT)
            except ProcessLookupError:
                pass

            if not wait_for_process_group_exit(pgid, graceful_timeout):
                app.logger.warning(
                    f'Stream {stream_id}: process group {pgid} did not stop after SIGINT; sending SIGTERM'
                )
                try:
                    os.killpg(pgid, signal.SIGTERM)
                except ProcessLookupError:
                    pass

                if not wait_for_process_group_exit(pgid, 3):
                    app.logger.warning(
                        f'Stream {stream_id}: process group {pgid} did not stop after SIGTERM; sending SIGKILL'
                    )
                    try:
                        os.killpg(pgid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    if not wait_for_process_group_exit(pgid, 3):
                        raise RuntimeError(
                            f'process group {pgid} is still alive after SIGKILL'
                        )

        if proc is not None:
            try:
                proc.wait(timeout=0.5)
            except (subprocess.TimeoutExpired, ChildProcessError):
                pass

        cleanup_zombies()
        return True
    finally:
        close_log_fd(stream_info)


def get_hls_url_from_rtmp(rtmp_url):
    if rtmp_url.startswith('rtmp://'):
        parts = rtmp_url.replace('rtmp://', '', 1).split('/')
        if len(parts) >= 3:
            host = parts[0]
            path = '/'.join(parts[1:])
            return f"https://{host}/hls/{path}.m3u8"
    return None


def read_log_head_tail(path):
    """Read only enough log data to find startup metadata and current metrics."""
    with open(path, 'rb') as file:
        head = file.read(LOG_HEAD_BYTES)
        file.seek(0, os.SEEK_END)
        size = file.tell()
        tail_start = max(0, size - LOG_TAIL_BYTES)
        file.seek(tail_start)
        tail = file.read(LOG_TAIL_BYTES)

    if tail_start <= len(head):
        data = head + tail[max(0, len(head) - tail_start):]
    else:
        data = head + b'\n...\n' + tail
    return data.decode('utf-8', errors='replace')


def get_stream_metrics(stream_id):
    with active_streams_lock:
        stream_info = active_streams.get(stream_id)
        if not stream_info:
            return None

        metrics = {
            'status': 'unknown',
            'fps': None,
            'bitrate': None,
            'bitrate_numeric': 0,
            'resolution': stream_info.get('resolution'),
            'dropped_frames': 0,
            'uptime': 0,
            'restart_count': stream_info.get('restart_count', 0),
            'last_restart': stream_info.get('last_restart'),
            'last_error': stream_info.get('last_error'),
        }

        proc = stream_info.get('proc')
        if not proc or proc.poll() is not None:
            metrics['status'] = 'stopped'
            return metrics

        metrics['status'] = 'running'
        started_at = stream_info.get('started_at')
        log_file = stream_info.get('log_file')

    if started_at:
        try:
            start_time = datetime.fromisoformat(started_at)
            metrics['uptime'] = int((datetime.now() - start_time).total_seconds())
        except (TypeError, ValueError):
            pass

    if log_file and os.path.exists(log_file):
        try:
            content = read_log_head_tail(log_file)
            tail_lines = content.splitlines()[-200:]

            fps_matches = re.findall(r'(\d+(?:\.\d+)?)\s+fps', content)
            if fps_matches:
                metrics['fps'] = float(fps_matches[-1])

            bitrate_matches = re.findall(
                r'bitrate[=:\s]+(\d+(?:\.\d+)?)\s*(kbits/s|kb/s|mbps|mbit/s)',
                content,
                re.IGNORECASE,
            )
            if bitrate_matches:
                bitrate_value, unit = bitrate_matches[-1]
                bitrate = float(bitrate_value)
                unit = unit.lower()
                is_mbps = 'mbps' in unit or 'mbit' in unit
                metrics['bitrate_numeric'] = bitrate * 1000 if is_mbps else bitrate
                metrics['bitrate'] = (
                    f"{bitrate:.2f} Mbps" if is_mbps else f"{bitrate:.0f} kbps"
                )

            if not metrics['resolution']:
                resolution_matches = re.findall(r'(\d{3,4})x(\d{3,4})', content)
                if resolution_matches:
                    metrics['resolution'] = (
                        f"{resolution_matches[-1][0]}x{resolution_matches[-1][1]}"
                    )
                    with active_streams_lock:
                        current = active_streams.get(stream_id)
                        if current:
                            current['resolution'] = metrics['resolution']

            drop_matches = re.findall(r'(\d+)\s+dropped', content, re.IGNORECASE)
            if drop_matches:
                metrics['dropped_frames'] = int(drop_matches[-1])

            error_lines = [
                line for line in tail_lines
                if 'error' in line.lower() or 'failed' in line.lower()
            ]
            if error_lines:
                metrics['last_error'] = error_lines[-1].strip()[:200]
                with active_streams_lock:
                    current = active_streams.get(stream_id)
                    if current:
                        current['last_error'] = metrics['last_error']
        except Exception as exc:
            app.logger.error(f"Error reading metrics for stream {stream_id}: {exc}")

    return metrics


def resolve_ytdlp_url(source_url, log_file):
    ytdlp_bin = os.path.join(BIN_DIR, 'yt-dlp')
    if not os.path.isfile(ytdlp_bin) or not os.access(ytdlp_bin, os.X_OK):
        return None, f'yt-dlp is not executable: {ytdlp_bin}'

    args = [
        ytdlp_bin,
        '--no-playlist',
        '--no-warnings',
        '--get-url',
        '-f', 'best',
        source_url,
    ]
    try:
        result = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=35,
            check=False,
        )
    except subprocess.TimeoutExpired:
        return None, 'yt-dlp URL extraction timed out'
    except Exception as exc:
        return None, f'yt-dlp extraction error: {exc}'

    if result.returncode != 0:
        error = (result.stderr or result.stdout or 'yt-dlp could not extract media URL').strip()
        with open(log_file, 'a', encoding='utf-8') as file:
            file.write('\n=== yt-dlp extraction ===\n')
            file.write(error + '\n')
        return None, error[-500:]

    urls = [line.strip() for line in result.stdout.splitlines() if line.strip().startswith(('http://', 'https://'))]
    if not urls:
        return None, 'yt-dlp did not return a direct media URL'
    if len(urls) > 1:
        return None, 'yt-dlp returned separate audio/video URLs; format best must be combined'
    return urls[0], None


def build_stream_command(stream, engine, source_url, target_rtmp, log_file):
    ffmpeg_bin = os.path.join(BIN_DIR, 'ffmpeg')
    if not os.path.isfile(ffmpeg_bin) or not os.access(ffmpeg_bin, os.X_OK):
        return None, f'FFmpeg is not executable: {ffmpeg_bin}'

    ffmpeg_tail = [
        '-c:v', 'copy',
        '-c:a', 'copy',
        '-flvflags', 'no_duration_filesize',
        '-f', 'flv',
        target_rtmp,
    ]

    if engine == 'streamlink':
        streamlink_bin = os.path.join(BIN_DIR, 'streamlink')
        if not os.path.isfile(streamlink_bin) or not os.access(streamlink_bin, os.X_OK):
            return None, f'Streamlink is not executable: {streamlink_bin}'
        streamlink_args = [
            streamlink_bin,
            '--hls-live-restart',
            '--stream-segment-attempts', '10',
            '--stream-timeout', '10',
            '--stdout',
            source_url,
            'best',
        ]
        ffmpeg_args = [ffmpeg_bin, '-fflags', '+genpts', '-i', 'pipe:0', *ffmpeg_tail]
        return 'set -o pipefail; ' + shlex.join(streamlink_args) + ' | ' + shlex.join(ffmpeg_args), None

    direct_url, error = resolve_ytdlp_url(source_url, log_file)
    if error:
        return None, error
    ffmpeg_args = [ffmpeg_bin, '-fflags', '+genpts', '-i', direct_url, *ffmpeg_tail]
    return 'exec ' + shlex.join(ffmpeg_args), None


def start_stream_process(stream, is_restart=False):
    stream_id = stream['id']
    with active_streams_lock:
        starting_streams.add(stream_id)

    try:
        source_url = normalize_url(stream.get('source_url'), 'source_url', {'http', 'https'})
        target_rtmp = normalize_url(stream.get('target_rtmp'), 'target_rtmp', {'rtmp', 'rtmps'})
    except ValueError as exc:
        error = f'Invalid stream URL: {exc}'
        update_stream_state(stream_id, last_error=error)
        with active_streams_lock:
            starting_streams.discard(stream_id)
        return False

    log_file = os.path.join(LOG_DIR, f'stream_{stream_id}.log')
    os.makedirs(LOG_DIR, exist_ok=True)
    with active_streams_lock:
        previous_info = active_streams.get(stream_id)
        previous_resolution = previous_info.get('resolution') if previous_info else None
        close_log_fd(previous_info)

    if is_restart and os.path.exists(log_file):
        with open(log_file, 'a', encoding='utf-8') as file:
            file.write(f"\n\n=== RESTART at {datetime.now().isoformat()} ===\n\n")
    elif os.path.exists(log_file):
        os.remove(log_file)

    requested_engine = stream.get('engine', 'auto')
    engines = ['streamlink', 'yt-dlp'] if requested_engine == 'auto' else [requested_engine]
    last_error = None

    for engine in engines:
        cmd, error = build_stream_command(stream, engine, source_url, target_rtmp, log_file)
        if error:
            last_error = f'{engine}: {error}'
            app.logger.warning(f'Stream {stream_id}: {last_error}')
            continue

        log_fd = None
        proc = None
        try:
            with open(log_file, 'a', encoding='utf-8') as marker:
                marker.write(f"\n=== ENGINE {engine} at {datetime.now().isoformat()} ===\n")
            log_fd = open(log_file, 'a', encoding='utf-8', buffering=1)
            proc = subprocess.Popen(
                ['/bin/bash', '-c', cmd],
                stdout=log_fd,
                stderr=log_fd,
                start_new_session=True,
                close_fds=True,
            )
            deadline = time.monotonic() + 5
            while time.monotonic() < deadline:
                return_code = proc.poll()
                if return_code is not None:
                    last_error = f'{engine}: ' + extract_last_error(
                        log_file,
                        f'Process exited during initialization with code {return_code}',
                    )
                    close_log_fd({'log_fd': log_fd})
                    break
                time.sleep(0.2)
            else:
                now = datetime.now().isoformat()
                _config, stored_stream = get_stream_config(stream_id)
                restart_count = int((stored_stream or {}).get('restart_count', 0))
                with active_streams_lock:
                    active_streams[stream_id] = {
                        'proc': proc,
                        'pid': proc.pid,
                        'pgid': proc.pid,
                        'log_fd': log_fd,
                        'started_at': now,
                        'stream': (stored_stream or stream).copy(),
                        'log_file': log_file,
                        'restart_count': restart_count,
                        'last_restart': now if is_restart else (stored_stream or {}).get('last_restart'),
                        'last_error': None,
                        'resolution': previous_resolution,
                        'recovering': False,
                        'engine_used': engine,
                    }
                changes = {'last_error': None, 'engine_used': engine}
                if is_restart:
                    changes['last_restart'] = now
                update_stream_state(stream_id, **changes)
                app.logger.info(f"{'Restarted' if is_restart else 'Started'} stream {stream_id} with {engine}: PID {proc.pid}")
                with active_streams_lock:
                    starting_streams.discard(stream_id)
                return True
        except Exception as exc:
            last_error = f'{engine}: Error starting stream: {exc}'
            if proc and proc.poll() is None:
                try:
                    os.killpg(proc.pid, signal.SIGKILL)
                except OSError:
                    pass
            close_log_fd({'log_fd': log_fd})

    last_error = last_error or 'No engine could start the stream'
    update_stream_state(stream_id, last_error=last_error, engine_used=None)
    with active_streams_lock:
        starting_streams.discard(stream_id)
    return False

def stop_stream_process(stream_id):
    with active_streams_lock:
        stream_info = active_streams.get(stream_id)
    if not stream_info:
        return True

    try:
        terminate_stream_group(stream_info, stream_id, graceful_timeout=10)
        with active_streams_lock:
            current = active_streams.get(stream_id)
            if current is stream_info:
                active_streams.pop(stream_id, None)
        time.sleep(2)  # Let the destination release the old RTMP session.
        app.logger.info(f"Stopped stream {stream_id}")
        return True
    except Exception as exc:
        app.logger.error(f"Error stopping stream {stream_id}: {exc}")
        with active_streams_lock:
            current = active_streams.get(stream_id)
            if current is stream_info and not check_stream_health(stream_id):
                active_streams.pop(stream_id, None)
        return False


def check_stream_health(stream_id):
    with active_streams_lock:
        stream_info = active_streams.get(stream_id)
        proc = stream_info.get('proc') if stream_info else None
    return bool(proc and proc.poll() is None)


def recovery_worker(stream_id, reason=None):
    is_autostart = reason == '__autostart__'
    with active_streams_lock:
        if stream_id in recovering_streams:
            return
        recovering_streams.add(stream_id)

    try:
        while monitoring_active:
            _config, stream = get_stream_config(stream_id)
            if not stream or not stream.get('desired_active', False):
                return

            restart_count = int(stream.get('restart_count', 0))
            if restart_count >= MAX_RESTART_ATTEMPTS:
                error = stream.get('last_error') or reason or 'Maximum automatic restart attempts exceeded'
                update_stream_state(
                    stream_id,
                    desired_active=False,
                    restart_count=MAX_RESTART_ATTEMPTS,
                    last_error=error,
                )
                app.logger.warning(f'Stream {stream_id} exceeded max restart attempts')
                return

            delay = RESTART_DELAYS[restart_count]
            attempt = restart_count + 1
            error = None if is_autostart else (reason or stream.get('last_error') or 'Stream process stopped unexpectedly')
            changes = {
                'restart_count': attempt,
                'last_restart': datetime.now().isoformat(),
            }
            if is_autostart:
                changes['last_error'] = None
                app.logger.info(
                    f'Stream {stream_id}: autostart attempt {attempt}/{MAX_RESTART_ATTEMPTS} in {delay}s'
                )
            else:
                changes['last_error'] = error
                app.logger.warning(
                    f'Stream {stream_id}: automatic restart {attempt}/{MAX_RESTART_ATTEMPTS} in {delay}s'
                )
            update_stream_state(stream_id, **changes)

            deadline = time.monotonic() + delay
            while time.monotonic() < deadline:
                time.sleep(min(1, max(0, deadline - time.monotonic())))
                _config, current = get_stream_config(stream_id)
                if not current or not current.get('desired_active', False):
                    return

            _config, current = get_stream_config(stream_id)
            if current and start_stream_process(current, is_restart=True):
                app.logger.info(f'Stream {stream_id} recovered on attempt {attempt}')
                return

            _config, current = get_stream_config(stream_id)
            reason = (current or {}).get('last_error') or 'Automatic restart failed'
            is_autostart = False
    finally:
        with active_streams_lock:
            recovering_streams.discard(stream_id)


def schedule_recovery(stream_id, reason=None):
    with active_streams_lock:
        if stream_id in recovering_streams:
            return
    threading.Thread(
        target=recovery_worker,
        args=(stream_id, reason),
        daemon=True,
    ).start()


def monitoring_loop():
    app.logger.info('Monitoring loop started')
    while monitoring_active:
        try:
            with active_streams_lock:
                stream_ids = list(active_streams.keys())

            for stream_id in stream_ids:
                if check_stream_health(stream_id):
                    with active_streams_lock:
                        healthy_info = active_streams.get(stream_id)
                        started_at = (healthy_info or {}).get('started_at')
                    _config, healthy_stream = get_stream_config(stream_id)
                    restart_count = int((healthy_stream or {}).get('restart_count', 0))
                    if started_at and restart_count > 0:
                        try:
                            stable_seconds = (datetime.now() - datetime.fromisoformat(started_at)).total_seconds()
                        except (TypeError, ValueError):
                            stable_seconds = 0
                        if stable_seconds >= STABLE_RESET_SECONDS:
                            if update_stream_state(
                                stream_id,
                                restart_count=0,
                                last_error=None,
                            ):
                                with active_streams_lock:
                                    current_info = active_streams.get(stream_id)
                                    if current_info:
                                        current_info['restart_count'] = 0
                                        current_info['last_error'] = None
                                app.logger.info(
                                    f'Stream {stream_id}: restart counter reset after '
                                    f'{STABLE_RESET_SECONDS}s of stable operation'
                                )
                    continue

                with active_streams_lock:
                    dead_info = active_streams.pop(stream_id, None)
                if not dead_info:
                    continue

                reason = extract_last_error(
                    dead_info.get('log_file'),
                    'Stream process stopped unexpectedly',
                )
                terminate_stream_group(dead_info, stream_id, graceful_timeout=2)
                _config, stream = get_stream_config(stream_id)
                if stream and stream.get('desired_active', False):
                    schedule_recovery(stream_id, reason)

            cleanup_zombies()
        except Exception as exc:
            app.logger.error(f'Error in monitoring loop: {exc}')
        time.sleep(2)


def autostart_configured_streams():
    time.sleep(AUTOSTART_DELAY)
    config = load_config()
    for stream in config.get('streams', []):
        if stream.get('enabled', True) and stream.get('desired_active', False):
            app.logger.info(f'Scheduling autostart for stream {stream.get("id")}')
            schedule_recovery(stream.get('id'), '__autostart__')


threading.Thread(target=monitoring_loop, daemon=True).start()
threading.Thread(target=autostart_configured_streams, daemon=True).start()

@app.route('/')
def index():
    return render_template('monitoring.html')


@app.route('/admin/')
def admin():
    return render_template('admin.html')


@app.route('/api/status')
def api_status():
    config = load_config()
    with active_streams_lock:
        active_count = sum(
            1 for info in active_streams.values()
            if info.get('proc') and info['proc'].poll() is None
        )
    return jsonify({
        'status': 'ok',
        'active_streams': active_count,
        'total_streams': len(config.get('streams', [])),
        'timestamp': datetime.now().isoformat(),
    })


@app.route('/api/streams')
def api_streams():
    config = load_config()
    result = []

    for stream in config.get('streams', []):
        stream_id = stream.get('id')
        stream_copy = stream.copy()

        with active_streams_lock:
            stream_info = active_streams.get(stream_id)
            proc = stream_info.get('proc') if stream_info else None
            is_running = bool(proc and proc.poll() is None)
            is_recovering = stream_id in recovering_streams
            is_starting = stream_id in starting_streams
            if stream_info:
                stream_copy['started_at'] = stream_info.get('started_at')
                stream_copy['engine_used'] = stream_info.get('engine_used')

        stream_copy['active'] = is_running
        stream_copy['recovering'] = is_recovering
        stream_copy['starting'] = is_starting
        stream_copy['healthy'] = is_running
        if is_running:
            stream_copy['state'] = 'running'
        elif is_starting:
            stream_copy['state'] = 'starting'
        elif is_recovering:
            stream_copy['state'] = 'recovering'
        elif stream.get('last_error'):
            stream_copy['state'] = 'error'
        else:
            stream_copy['state'] = 'stopped'
        stream_copy['hls_url'] = get_hls_url_from_rtmp(stream.get('target_rtmp', ''))
        if stream_info:
            metrics = get_stream_metrics(stream_id)
            if metrics:
                stream_copy['metrics'] = metrics
        else:
            stream_copy['metrics'] = {
                'status': 'error' if stream.get('last_error') else 'stopped',
                'fps': None,
                'bitrate': None,
                'bitrate_numeric': 0,
                'resolution': None,
                'dropped_frames': 0,
                'uptime': 0,
                'restart_count': int(stream.get('restart_count', 0)),
                'last_restart': stream.get('last_restart'),
                'last_error': stream.get('last_error'),
            }

        result.append(stream_copy)

    return jsonify(result)

@app.route('/api/streams/<int:stream_id>/monitoring-visibility', methods=['POST'])
def api_set_monitoring_visibility(stream_id):
    data = request.get_json(silent=True) or {}
    show_on_monitoring = data.get('show_on_monitoring')
    if not isinstance(show_on_monitoring, bool):
        return jsonify({'error': 'show_on_monitoring must be true or false'}), 400

    config = load_config()
    stream = next(
        (item for item in config.get('streams', []) if item.get('id') == stream_id),
        None,
    )
    if not stream:
        return jsonify({'error': 'Stream not found'}), 404

    stream['show_on_monitoring'] = show_on_monitoring
    stream['updated_at'] = datetime.now().isoformat()
    if not save_config(config):
        return jsonify({'error': 'Failed to save config'}), 500

    return jsonify({
        'status': 'updated',
        'stream_id': stream_id,
        'show_on_monitoring': show_on_monitoring,
    })


@app.route('/api/streams/<int:stream_id>/start', methods=['POST'])
def api_start_stream(stream_id):
    config, stream = get_stream_config(stream_id)
    if not stream:
        return jsonify({'error': 'Stream not found'}), 404

    with active_streams_lock:
        if stream_id in recovering_streams:
            return jsonify({'error': 'Automatic recovery is already in progress'}), 409
        if check_stream_health(stream_id):
            return jsonify({'error': 'Stream already running'}), 400
        stale = active_streams.pop(stream_id, None)
    if stale:
        try:
            terminate_stream_group(stale, stream_id, graceful_timeout=5)
        except Exception as exc:
            return jsonify({'error': f'Could not stop stale stream process: {exc}'}), 500

    stream['desired_active'] = True
    stream['restart_count'] = 0
    stream['last_error'] = None
    stream['last_restart'] = None
    stream['updated_at'] = datetime.now().isoformat()
    if not save_config(config):
        return jsonify({'error': 'Failed to save desired state'}), 500

    if start_stream_process(stream):
        return jsonify({'status': 'started', 'stream_id': stream_id})
    return jsonify({'error': stream.get('last_error') or 'Failed to start stream'}), 500

@app.route('/api/streams/<int:stream_id>/stop', methods=['POST'])
def api_stop_stream(stream_id):
    config, stream = get_stream_config(stream_id)
    if not stream:
        return jsonify({'error': 'Stream not found'}), 404

    stream['desired_active'] = False
    stream['restart_count'] = 0
    stream['last_error'] = None
    stream['updated_at'] = datetime.now().isoformat()
    if not save_config(config):
        return jsonify({'error': 'Failed to save desired state'}), 500

    with active_streams_lock:
        is_known = stream_id in active_streams
    if is_known and not stop_stream_process(stream_id):
        stream['last_error'] = 'Failed to stop the existing stream process group'
        stream['updated_at'] = datetime.now().isoformat()
        save_config(config)
        return jsonify({'error': stream['last_error']}), 500
    return jsonify({'status': 'stopped', 'stream_id': stream_id})

@app.route('/api/streams/<int:stream_id>/restart', methods=['POST'])
def api_restart_stream(stream_id):
    config, stream = get_stream_config(stream_id)
    if not stream:
        return jsonify({'error': 'Stream not found'}), 404

    stream['desired_active'] = True
    stream['restart_count'] = 0
    stream['last_error'] = None
    stream['last_restart'] = None
    stream['updated_at'] = datetime.now().isoformat()
    if not save_config(config):
        return jsonify({'error': 'Failed to save desired state'}), 500

    with active_streams_lock:
        is_known = stream_id in active_streams
    if is_known:
        if not stop_stream_process(stream_id):
            return jsonify({'error': 'Failed to stop the current stream before restart'}), 500

    if start_stream_process(stream):
        return jsonify({'status': 'restarted', 'stream_id': stream_id})
    return jsonify({'error': 'Failed to restart stream'}), 500

@app.route('/api/streams', methods=['POST'])
def api_add_stream():
    try:
        data = request.get_json(silent=False)
        normalized = normalize_stream_payload(data)
    except (ValueError, TypeError) as exc:
        return jsonify({'error': str(exc)}), 400
    except Exception:
        return jsonify({'error': 'Invalid JSON body'}), 400

    config = load_config()
    streams = config.get('streams', [])
    new_id = max((item.get('id', 0) for item in streams), default=0) + 1

    new_stream = {
        'id': new_id,
        **normalized,
        'created_at': datetime.now().isoformat(),
        'updated_at': datetime.now().isoformat(),
        'desired_active': False,
        'restart_count': 0,
        'last_restart': None,
        'last_error': None,
    }
    streams.append(new_stream)
    config['streams'] = streams

    if not save_config(config):
        return jsonify({'error': 'Failed to save config'}), 500
    return jsonify(new_stream), 201


@app.route('/api/streams/<int:stream_id>', methods=['PUT'])
def api_update_stream(stream_id):
    config = load_config()
    stream = next(
        (item for item in config.get('streams', []) if item.get('id') == stream_id),
        None,
    )
    if not stream:
        return jsonify({'error': 'Stream not found'}), 404

    with active_streams_lock:
        if stream_id in active_streams:
            return jsonify({
                'error': 'Stop the stream before editing it'
            }), 409

    try:
        data = request.get_json(silent=False)
        normalized = normalize_stream_payload(data, existing=stream)
    except (ValueError, TypeError) as exc:
        return jsonify({'error': str(exc)}), 400
    except Exception:
        return jsonify({'error': 'Invalid JSON body'}), 400

    stream.update(normalized)
    stream['updated_at'] = datetime.now().isoformat()

    if not save_config(config):
        return jsonify({'error': 'Failed to save config'}), 500
    return jsonify(stream)


@app.route('/api/streams/<int:stream_id>', methods=['DELETE'])
def api_delete_stream(stream_id):
    config = load_config()

    with active_streams_lock:
        is_active = stream_id in active_streams
    if is_active and not stop_stream_process(stream_id):
        return jsonify({'error': 'Failed to stop the stream before deletion'}), 500

    old_count = len(config.get('streams', []))
    config['streams'] = [
        item for item in config.get('streams', [])
        if item.get('id') != stream_id
    ]
    if len(config['streams']) == old_count:
        return jsonify({'error': 'Stream not found'}), 404
    if not save_config(config):
        return jsonify({'error': 'Failed to save config'}), 500

    return jsonify({'status': 'deleted', 'stream_id': stream_id})


@app.route('/api/streams/<int:stream_id>/logs')
def api_stream_logs(stream_id):
    log_file = os.path.join(LOG_DIR, f'stream_{stream_id}.log')
    if not os.path.exists(log_file):
        return jsonify({'error': 'Log file not found'}), 404

    try:
        with open(log_file, 'rb') as file:
            file.seek(0, os.SEEK_END)
            size = file.tell()
            file.seek(max(0, size - LOG_TAIL_BYTES))
            content = file.read(LOG_TAIL_BYTES).decode(
                'utf-8', errors='replace'
            )
        lines = content.splitlines(keepends=True)
        return jsonify({
            'stream_id': stream_id,
            'logs': ''.join(lines[-150:]),
        })
    except Exception as exc:
        app.logger.error(f"Error reading logs for stream {stream_id}: {exc}")
        return jsonify({'error': 'Failed to read log file'}), 500





def load_component_status():
    try:
        with open(COMPONENT_STATUS_PATH, 'r', encoding='utf-8') as file:
            data = json.load(file)
        if not isinstance(data, dict):
            raise ValueError('component status must be an object')
    except FileNotFoundError:
        data = {}
    except Exception as exc:
        app.logger.warning(f'Could not read component status: {exc}')
        data = {'error': str(exc)}

    components = data.setdefault('components', {})
    for name, service in COMPONENT_UPDATE_SERVICES.items():
        try:
            result = subprocess.run(
                ['systemctl', 'is-active', service],
                stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
                text=True, timeout=3, check=False,
            )
            components.setdefault(name, {})['updating'] = result.stdout.strip() in {'active', 'activating'}
        except Exception:
            components.setdefault(name, {})['updating'] = False
    try:
        result = subprocess.run(
            ['systemctl', 'is-active', COMPONENT_CHECK_SERVICE],
            stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,
            text=True, timeout=3, check=False,
        )
        data['checking'] = result.stdout.strip() in {'active', 'activating'}
    except Exception:
        data['checking'] = False
    return data


def start_system_service(service_name):
    result = subprocess.run(
        ['sudo', '/bin/systemctl', 'start', '--no-block', service_name],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=10, check=False,
    )
    if result.returncode != 0:
        message = (result.stderr or result.stdout or 'systemctl failed').strip()
        raise RuntimeError(message[-500:])


@app.route('/api/components')
def api_components():
    return jsonify(load_component_status())


@app.route('/api/components/check', methods=['POST'])
def api_components_check():
    try:
        start_system_service(COMPONENT_CHECK_SERVICE)
        return jsonify({'status': 'started'}), 202
    except Exception as exc:
        app.logger.error(f'Component check failed to start: {exc}')
        return jsonify({'error': str(exc)}), 500


@app.route('/api/components/update/<component>', methods=['POST'])
def api_component_update(component):
    service = COMPONENT_UPDATE_SERVICES.get(component)
    if not service:
        return jsonify({'error': 'Unsupported component'}), 404

    data = request.get_json(silent=True) or {}
    force = data.get('force') is True
    with download_jobs_lock:
        downloads_running = any(
            job.get('status') in {'queued', 'running'}
            for job in download_jobs.values()
        )
    if downloads_running:
        return jsonify({'error': 'Сначала дождитесь завершения текущей загрузки файла.'}), 409

    with active_streams_lock:
        active_count = sum(
            1 for info in active_streams.values()
            if info.get('proc') and info['proc'].poll() is None
        )
    if active_count and not force:
        return jsonify({
            'error': 'Есть активные трансляции. Обновление перезапустит backend и кратковременно остановит рестримы.',
            'requires_confirmation': True,
            'active_streams': active_count,
        }), 409

    try:
        start_system_service(service)
        return jsonify({'status': 'started', 'component': component}), 202
    except Exception as exc:
        app.logger.error(f'Component update failed to start: {exc}')
        return jsonify({'error': str(exc)}), 500


@app.route('/downloads/')
def downloads_page():
    return render_template('downloads.html')


def sanitize_download_name(value, fallback='video', max_length=140):
    value = unicodedata.normalize('NFKC', str(value or '')).strip()
    value = re.sub(r'[\x00-\x1f\x7f]+', ' ', value)
    value = re.sub(r'[<>:"/\\|?*]+', '_', value)
    value = re.sub(r'\s+', ' ', value).strip(' ._-')
    if not value:
        value = fallback
    if len(value) > max_length:
        value = value[:max_length].rstrip(' ._-')
    return value or fallback


def download_sidecar_path(job_id):
    return os.path.join(DOWNLOAD_DIR, f'{job_id}.job.json')


def public_download_job(job):
    hidden = {'file_path', 'pid', 'created_ts', 'source_url'}
    return {key: value for key, value in job.items() if key not in hidden}


def save_download_job(job_id):
    with download_jobs_lock:
        job = download_jobs.get(job_id)
        if not job:
            return
        serializable = {
            key: value for key, value in job.items()
            if key not in {'pid'}
        }
    path = download_sidecar_path(job_id)
    temp_path = path + '.tmp'
    try:
        with open(temp_path, 'w', encoding='utf-8') as file:
            json.dump(serializable, file, ensure_ascii=False, indent=2)
            file.write('\n')
        os.replace(temp_path, path)
    except Exception as exc:
        app.logger.warning(f'Could not persist download job {job_id}: {exc}')
        try:
            os.remove(temp_path)
        except OSError:
            pass


def remove_download_artifacts(job_id, keep_sidecar=False):
    prefix = f'{job_id}.'
    for item in Path(DOWNLOAD_DIR).iterdir():
        if not item.is_file() or not item.name.startswith(prefix):
            continue
        if keep_sidecar and item.name == f'{job_id}.job.json':
            continue
        try:
            item.unlink()
        except OSError as exc:
            app.logger.warning(f'Could not remove download artifact {item}: {exc}')


def probe_download_metadata(source_url):
    ytdlp_bin = os.path.join(BIN_DIR, 'yt-dlp')
    args = [
        ytdlp_bin,
        '--no-playlist',
        '--no-warnings',
        '--dump-single-json',
        '--skip-download',
        source_url,
    ]
    result = subprocess.run(
        args,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=45,
        check=False,
    )
    if result.returncode != 0:
        message = (result.stderr or result.stdout or 'Не удалось получить сведения о ролике').strip()
        raise RuntimeError(message[-700:])
    try:
        metadata = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f'yt-dlp вернул некорректные метаданные: {exc}') from exc

    title = sanitize_download_name(metadata.get('title'), 'video')
    uploader = sanitize_download_name(
        metadata.get('uploader') or metadata.get('channel') or '',
        '',
        100,
    )
    duration = metadata.get('duration')
    filesize = metadata.get('filesize') or metadata.get('filesize_approx')
    thumbnail = metadata.get('thumbnail')
    if thumbnail and not str(thumbnail).startswith(('http://', 'https://')):
        thumbnail = None
    return {
        'title': title,
        'uploader': uploader or None,
        'duration': int(duration) if isinstance(duration, (int, float)) else None,
        'estimated_size': int(filesize) if isinstance(filesize, (int, float)) else None,
        'thumbnail': thumbnail,
        'extractor': metadata.get('extractor_key') or metadata.get('extractor'),
    }


def ffprobe_media(path):
    ffprobe_bin = os.path.join(BIN_DIR, 'ffprobe')
    if not os.path.isfile(ffprobe_bin):
        ffprobe_bin = '/usr/bin/ffprobe'
    result = subprocess.run(
        [
            ffprobe_bin, '-v', 'error',
            '-show_entries', 'stream=codec_type,codec_name,width,height:format=duration,size,format_name',
            '-of', 'json',
            path,
        ],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        timeout=30,
        check=False,
    )
    if result.returncode != 0:
        raise RuntimeError((result.stderr or 'ffprobe не смог проверить файл').strip()[-500:])
    try:
        data = json.loads(result.stdout)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f'ffprobe вернул некорректный результат: {exc}') from exc
    streams = data.get('streams') or []
    video_stream = next((item for item in streams if item.get('codec_type') == 'video'), None)
    audio_stream = next((item for item in streams if item.get('codec_type') == 'audio'), None)
    if not video_stream:
        raise RuntimeError('Полученный файл не содержит видеодорожку')
    return {
        'video_codec': video_stream.get('codec_name'),
        'audio_codec': (audio_stream or {}).get('codec_name'),
        'width': video_stream.get('width'),
        'height': video_stream.get('height'),
        'has_audio': bool(audio_stream),
    }


def choose_download_file(job_id):
    ignored_suffixes = {'.part', '.ytdl', '.json', '.tmp'}
    candidates = []
    for item in Path(DOWNLOAD_DIR).glob(f'{job_id}.*'):
        if not item.is_file() or item.name.endswith('.job.json'):
            continue
        if item.suffix.lower() in ignored_suffixes or '.part' in item.name:
            continue
        try:
            media = ffprobe_media(str(item))
            candidates.append((item.stat().st_size, item, media))
        except Exception as exc:
            app.logger.warning(f'Ignoring unsuitable download artifact {item.name}: {exc}')
    if not candidates:
        raise RuntimeError('Готовый видеофайл не найден; возможно, источник отдал только аудио')
    candidates.sort(key=lambda row: row[0], reverse=True)
    _size, path, media = candidates[0]
    return path, media


def download_disk_info():
    stat = os.statvfs(DOWNLOAD_DIR)
    return {
        'total': stat.f_blocks * stat.f_frsize,
        'free': stat.f_bavail * stat.f_frsize,
        'used': (stat.f_blocks - stat.f_bfree) * stat.f_frsize,
    }


def download_worker(job_id, source_url, quality):
    ytdlp_bin = os.path.join(BIN_DIR, 'yt-dlp')
    output_template = os.path.join(DOWNLOAD_DIR, f'{job_id}.%(ext)s')
    if quality == 'best':
        format_selector = 'bv*[vcodec!=none]+ba[acodec!=none]/b[vcodec!=none][acodec!=none]'
    else:
        format_selector = (
            f'bv*[vcodec!=none][height<={quality}]+ba[acodec!=none]/'
            f'b[vcodec!=none][acodec!=none][height<={quality}]'
        )

    with download_jobs_lock:
        job = download_jobs[job_id]
        job.update(
            status='running',
            started_at=datetime.now().isoformat(),
            progress='Получение сведений о ролике...',
        )
    save_download_job(job_id)

    try:
        metadata = probe_download_metadata(source_url)
        disk = download_disk_info()
        estimated = metadata.get('estimated_size')
        if estimated and estimated > max(0, disk['free'] - 2 * 1024**3):
            raise RuntimeError('Недостаточно свободного места для предполагаемого размера ролика')

        with download_jobs_lock:
            download_jobs[job_id].update(metadata)
            download_jobs[job_id]['progress'] = 'Подготовка загрузки видео и аудио...'
        save_download_job(job_id)

        args = [
            ytdlp_bin,
            '--no-playlist',
            '--newline',
            '--merge-output-format', 'mp4/mkv',
            '--max-filesize', '8G',
            '--socket-timeout', '20',
            '--retries', '10',
            '--fragment-retries', '10',
            '-f', format_selector,
            '-S', 'res,vcodec:h264,acodec:aac,ext:mp4:m4a',
            '-o', output_template,
            source_url,
        ]
        proc = subprocess.Popen(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        with download_jobs_lock:
            download_jobs[job_id]['pid'] = proc.pid
        for line in proc.stdout or []:
            clean = line.strip()
            if clean:
                with download_jobs_lock:
                    download_jobs[job_id]['progress'] = clean[-400:]
        code = proc.wait()
        if code != 0:
            raise RuntimeError(f'yt-dlp завершился с кодом {code}')

        file_path, media = choose_download_file(job_id)
        extension = sanitize_download_name(
            file_path.suffix.lstrip('.'), 'mp4', 10
        ).lower()
        friendly_filename = f"{metadata['title']}.{extension}"
        with download_jobs_lock:
            download_jobs[job_id].update(
                status='ready',
                file_path=str(file_path),
                filename=friendly_filename,
                size=file_path.stat().st_size,
                finished_at=datetime.now().isoformat(),
                finished_ts=time.time(),
                expires_at=datetime.fromtimestamp(
                    time.time() + DOWNLOAD_MAX_AGE_SECONDS
                ).isoformat(),
                progress='Готово',
                **media,
            )
            download_jobs[job_id].pop('pid', None)
        save_download_job(job_id)
    except Exception as exc:
        remove_download_artifacts(job_id, keep_sidecar=True)
        with download_jobs_lock:
            download_jobs[job_id].update(
                status='error',
                error=str(exc),
                progress='Ошибка',
                finished_at=datetime.now().isoformat(),
                finished_ts=time.time(),
            )
            download_jobs[job_id].pop('pid', None)
        save_download_job(job_id)


def load_download_history():
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    cutoff = time.time() - DOWNLOAD_MAX_AGE_SECONDS
    for sidecar in Path(DOWNLOAD_DIR).glob('*.job.json'):
        try:
            job = json.loads(sidecar.read_text(encoding='utf-8'))
            job_id = str(job.get('id') or sidecar.name.split('.', 1)[0])
            created_ts = float(job.get('created_ts') or 0)
            if not re.fullmatch(r'[a-f0-9]{12}', job_id):
                raise ValueError('invalid job id')
            if created_ts and created_ts < cutoff:
                remove_download_artifacts(job_id)
                continue
            file_path = job.get('file_path')
            if file_path:
                resolved = Path(file_path).resolve()
                root = Path(DOWNLOAD_DIR).resolve()
                if root not in resolved.parents or not resolved.is_file():
                    job['status'] = 'error'
                    job['error'] = 'Файл больше не существует'
                    job.pop('file_path', None)
            if job.get('status') in {'queued', 'running'}:
                job['status'] = 'error'
                job['error'] = 'Загрузка была прервана перезапуском приложения'
                job['progress'] = 'Ошибка'
                job.pop('pid', None)
            with download_jobs_lock:
                download_jobs[job_id] = job
        except Exception as exc:
            app.logger.warning(f'Could not restore download job {sidecar}: {exc}')


def cleanup_downloads_loop():
    while True:
        cutoff = time.time() - DOWNLOAD_MAX_AGE_SECONDS
        expired_ids = []
        with download_jobs_lock:
            for job_id, job in download_jobs.items():
                reference_ts = (
                    job.get('finished_ts')
                    or job.get('created_ts')
                    or 0
                )
                if reference_ts and reference_ts < cutoff:
                    expired_ids.append(job_id)
            for job_id in expired_ids:
                download_jobs.pop(job_id, None)
        for job_id in expired_ids:
            remove_download_artifacts(job_id)

        for item in Path(DOWNLOAD_DIR).iterdir():
            try:
                if item.is_file() and item.stat().st_mtime < cutoff:
                    if '.part' in item.name or item.suffix in {'.ytdl', '.tmp'}:
                        item.unlink()
            except OSError:
                pass
        time.sleep(300)


@app.route('/api/downloads/probe', methods=['POST'])
def api_probe_download():
    data = request.get_json(silent=True) or {}
    try:
        source_url = normalize_url(
            data.get('source_url'), 'source_url', {'http', 'https'}
        )
        reject_private_download_target(source_url)
        metadata = probe_download_metadata(source_url)
        metadata['disk'] = download_disk_info()
        return jsonify(metadata)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    except Exception as exc:
        return jsonify({'error': str(exc)}), 422


@app.route('/api/downloads', methods=['GET'])
def api_list_downloads():
    with download_jobs_lock:
        jobs = [
            public_download_job(job)
            for job in download_jobs.values()
        ]
    jobs.sort(
        key=lambda item: item.get('created_at') or '',
        reverse=True,
    )
    return jsonify({'jobs': jobs, 'disk': download_disk_info()})


@app.route('/api/downloads', methods=['POST'])
def api_create_download():
    data = request.get_json(silent=True) or {}
    try:
        source_url = normalize_url(
            data.get('source_url'), 'source_url', {'http', 'https'}
        )
        reject_private_download_target(source_url)
    except ValueError as exc:
        return jsonify({'error': str(exc)}), 400
    quality = str(data.get('quality', 'best'))
    if quality not in {'best', '1080', '720', '480'}:
        return jsonify({'error': 'Unsupported quality'}), 400
    disk = download_disk_info()
    if disk['free'] < 2 * 1024**3:
        return jsonify({'error': 'Недостаточно свободного места: требуется минимум 2 ГБ'}), 507
    with download_jobs_lock:
        running = sum(
            1 for job in download_jobs.values()
            if job.get('status') in {'queued', 'running'}
        )
        if running >= DOWNLOAD_MAX_CONCURRENT:
            return jsonify({'error': 'Уже выполняется другое скачивание'}), 409
        job_id = uuid.uuid4().hex[:12]
        download_jobs[job_id] = {
            'id': job_id,
            'status': 'queued',
            'source_url': source_url,
            'quality': quality,
            'created_at': datetime.now().isoformat(),
            'created_ts': time.time(),
            'progress': 'В очереди',
        }
    save_download_job(job_id)
    threading.Thread(
        target=download_worker,
        args=(job_id, source_url, quality),
        daemon=True,
    ).start()
    return jsonify({'id': job_id, 'status': 'queued'}), 202


@app.route('/api/downloads/<job_id>')
def api_download_status(job_id):
    with download_jobs_lock:
        job = download_jobs.get(job_id)
        if not job:
            return jsonify({'error': 'Задание не найдено'}), 404
        public = public_download_job(job)
    return jsonify(public)


@app.route('/api/downloads/<job_id>/file')
def api_download_file(job_id):
    with download_jobs_lock:
        job = download_jobs.get(job_id)
        if not job or job.get('status') != 'ready':
            return jsonify({'error': 'Файл ещё не готов'}), 404
        path = job.get('file_path')
        filename = job.get('filename') or 'video.mp4'
    if not path or not os.path.isfile(path):
        return jsonify({'error': 'Файл не найден'}), 404
    return send_file(path, as_attachment=True, download_name=filename)


@app.route('/api/downloads/<job_id>', methods=['DELETE'])
def api_delete_download(job_id):
    with download_jobs_lock:
        job = download_jobs.get(job_id)
        if not job:
            return jsonify({'error': 'Задание не найдено'}), 404
        if job.get('status') in {'queued', 'running'}:
            return jsonify({'error': 'Нельзя удалить выполняющееся задание'}), 409
        download_jobs.pop(job_id, None)
    remove_download_artifacts(job_id)
    return jsonify({'status': 'deleted', 'id': job_id})


os.makedirs(DOWNLOAD_DIR, exist_ok=True)
load_download_history()
threading.Thread(target=cleanup_downloads_loop, daemon=True).start()


if __name__ == '__main__':
    os.makedirs(LOG_DIR, exist_ok=True)
    app.run(host='127.0.0.1', port=5000, debug=False)
