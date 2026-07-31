# Cricket Stream Lite

Lightweight self-hosted web application for restreaming public live sources to
RTMP destinations and downloading media files. It uses Streamlink, yt-dlp and
FFmpeg without video transcoding during normal restreaming.

> Current status: early release (`v0.1.0`). Deploy behind authentication and
> test with your providers before relying on it for production broadcasts.

## Features

- Restream YouTube, Twitch, Kick and other supported sources to RTMP/RTMPS.
- Automatic Streamlink → yt-dlp fallback or manual engine selection.
- Stream start, stop, restart and automatic recovery after failures.
- Live bitrate, resolution, FPS, uptime and per-stream logs.
- Separate broadcast monitoring and configuration pages.
- Optional visibility of each stream on the monitoring page.
- Media URL analysis, quality selection and video downloads.
- FFprobe validation so audio-only artifacts are not offered as finished video.
- Download history, progress, disk-space information and one-hour cleanup.
- Daily component version checks and controlled Streamlink/yt-dlp updates.
- systemd service, timers and log rotation for Ubuntu.

## How it works

```text
Public source → Streamlink or yt-dlp → FFmpeg stream copy → RTMP destination
```

No GPU is required for regular stream-copy operation. Compatibility fixes may
still cause FFmpeg to re-encode audio for a particular source.

## Requirements

- Ubuntu 24.04 LTS or another recent Debian-based server.
- Root or sudo access.
- A reachable RTMP/RTMPS destination.
- Enough network bandwidth for every concurrent input and output stream.
- Nginx or another reverse proxy with authentication and TLS for remote access.

## Installation

```bash
git clone https://github.com/lazaryants/cricket-stream-lite.git
cd cricket-stream-lite
sudo ./install.sh
```

The installer creates:

- application: `/opt/stream-bridge`;
- Python environment: `/opt/stream-bridge-env`;
- service account: `streambridge`;
- backend listener: `127.0.0.1:5000`;
- systemd services and timers for the application and component checks.

Open the service only through a properly secured reverse proxy. The application
itself currently has no built-in user accounts or authorization layer.

## Pages

| Path | Purpose |
|---|---|
| `/` | Broadcast monitor |
| `/admin/` | Stream configuration and component versions |
| `/downloads/` | Media analysis and downloads |
| `/api/status` | Backend health check |

## Configuration

Runtime configuration is stored at:

```text
/opt/stream-bridge/config/config.json
```

It is intentionally excluded from Git. The initial configuration contains no
streams; add them through the web interface. Back up this file before replacing
or reinstalling a server.

## Operations

```bash
sudo systemctl status stream-bridge --no-pager
sudo systemctl restart stream-bridge
sudo journalctl -u stream-bridge -f
```

Check component versions manually:

```bash
sudo systemctl start cricket-stream-components-check.service
sudo journalctl -u cricket-stream-components-check.service -n 50 --no-pager
```

## Security notes

- Protect every page and API endpoint with TLS and authentication at the proxy.
- RTMP URLs often contain publish keys; treat `config.json` as a secret.
- Do not commit cookie files, downloaded media, logs or component status files.
- Only download or restream material you are authorized to use.
- The media downloader rejects localhost, private and non-global target IPs to
  reduce server-side request forgery risk.
- Component updates run through fixed systemd units rather than arbitrary shell
  commands supplied by the browser.

## Repository layout

```text
app.py                         Flask backend and process manager
templates/                     Monitoring, configuration and downloads UI
static/                        Shared styles
config/config.example.json     Safe initial configuration
scripts/                       Restricted component update helper
systemd/                       Service and timer units
sudoers/                       Narrow systemctl permissions for the web service
logrotate/                     Stream log rotation policy
install.sh                     Ubuntu installer
```

## Limitations

- Single-server application with one Gunicorn worker.
- Runtime state is stored in JSON files rather than a database.
- Provider changes can temporarily break extraction until Streamlink or yt-dlp
  is updated.
- Some protected or DRM-controlled sources are not supported.
- Live-source downloading depends on provider behavior and may not finish like
  an ordinary finite video.

## License

[MIT](LICENSE)
