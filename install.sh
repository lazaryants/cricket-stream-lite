#!/usr/bin/env bash
set -Eeuo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "Run as root: sudo ./install.sh" >&2
  exit 1
fi

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
APP_DIR="/opt/stream-bridge"
VENV_DIR="/opt/stream-bridge-env"
SERVICE_USER="streambridge"

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y --no-install-recommends \
  python3 python3-venv python3-pip ffmpeg curl ca-certificates logrotate sudo

if ! id "${SERVICE_USER}" >/dev/null 2>&1; then
  useradd --system --home-dir "${APP_DIR}" --shell /usr/sbin/nologin "${SERVICE_USER}"
fi

python3 -m venv "${VENV_DIR}"
"${VENV_DIR}/bin/python3" -m pip install --upgrade pip wheel
"${VENV_DIR}/bin/python3" -m pip install -r "${PROJECT_DIR}/requirements.txt"

install -d -o "${SERVICE_USER}" -g "${SERVICE_USER}" \
  "${APP_DIR}" "${APP_DIR}/templates" "${APP_DIR}/static" \
  "${APP_DIR}/config" "${APP_DIR}/logs" "${APP_DIR}/downloads"

install -m 0644 "${PROJECT_DIR}/app.py" "${APP_DIR}/app.py"
cp -a "${PROJECT_DIR}/templates/." "${APP_DIR}/templates/"
cp -a "${PROJECT_DIR}/static/." "${APP_DIR}/static/"

if [[ ! -f "${APP_DIR}/config/config.json" ]]; then
  install -o "${SERVICE_USER}" -g "${SERVICE_USER}" -m 0660 \
    "${PROJECT_DIR}/config/config.example.json" "${APP_DIR}/config/config.json"
fi

chown -R "${SERVICE_USER}:${SERVICE_USER}" \
  "${APP_DIR}/templates" "${APP_DIR}/static" "${APP_DIR}/config" \
  "${APP_DIR}/logs" "${APP_DIR}/downloads"

install -m 0755 "${PROJECT_DIR}/scripts/cricket-stream-component-manager" \
  /usr/local/sbin/cricket-stream-component-manager
install -m 0440 "${PROJECT_DIR}/sudoers/streambridge-components" \
  /etc/sudoers.d/streambridge-components
visudo -cf /etc/sudoers.d/streambridge-components

for unit in "${PROJECT_DIR}"/systemd/*.service "${PROJECT_DIR}"/systemd/*.timer; do
  install -m 0644 "${unit}" "/etc/systemd/system/$(basename "${unit}")"
done
install -m 0644 "${PROJECT_DIR}/logrotate/stream-bridge" /etc/logrotate.d/stream-bridge

"${VENV_DIR}/bin/python3" -m py_compile "${APP_DIR}/app.py"
/usr/local/sbin/cricket-stream-component-manager check

systemctl daemon-reload
systemctl enable --now stream-bridge-logrotate.timer
systemctl enable --now cricket-stream-components-check.timer
systemctl enable stream-bridge.service
systemctl restart stream-bridge.service

for _attempt in {1..20}; do
  if curl -fsS http://127.0.0.1:5000/api/status >/dev/null; then
    echo "Cricket Stream Lite installed successfully."
    echo "Backend: http://127.0.0.1:5000"
    exit 0
  fi
  sleep 1
done

echo "Installation finished, but the health check failed." >&2
systemctl status stream-bridge.service --no-pager -l >&2 || true
exit 1
