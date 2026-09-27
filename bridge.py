import asyncio
import json
import os
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import requests
import websockets

# ============================================================
# НАСТРОЙКИ ИЗ ПЕРЕМЕННЫХ ОКРУЖЕНИЯ
# Секреты задаются в Easypanel, здесь только безопасные значения
# ============================================================
MM_URL = os.getenv("MM_URL", "").rstrip("/")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
BOT_USER_ID = os.getenv("BOT_USER_ID", "")
N8N_WEBHOOK_URL = os.getenv("N8N_WEBHOOK_URL", "")

# Обрабатывать только личку и упоминания
ONLY_DM_AND_MENTIONS = os.getenv("ONLY_DM_AND_MENTIONS", "true").lower() == "true"

# Порт сервера здоровья для Easypanel
HEALTH_PORT = 3000


class HealthHandler(BaseHTTPRequestHandler):
    """Отвечает 200 OK на любой запрос — для проверки здоровья Easypanel."""

    def do_GET(self):
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"ok")

    def log_message(self, *args):
        pass


def start_health_server():
    HTTPServer(("0.0.0.0", HEALTH_PORT), HealthHandler).serve_forever()


def download_file(file_id):
    url = f"{MM_URL}/api/v4/files/{file_id}"
    headers = {"Authorization": f"Bearer {BOT_TOKEN}"}
    response = requests.get(url, headers=headers, timeout=60)
    response.raise_for_status()
    return response.content


def get_file_info(file_id):
    url = f"{MM_URL}/api/v4/files/{file_id}/info"
    headers = {"Authorization": f"Bearer {BOT_TOKEN}"}
    response = requests.get(url, headers=headers, timeout=30)
    response.raise_for_status()
    return response.json()


def send_to_n8n(post, files):
    payload = {
        "text": post.get("message", ""),
        "user_id": post.get("user_id", ""),
        "channel_id": post.get("channel_id", ""),
        "post_id": post.get("id", ""),
        "file_count": len(files),
    }
    file_tuples = [
        (f"file_{i}", (f["name"], f["content"], f["mime"]))
        for i, f in enumerate(files)
    ]
    response = requests.post(N8N_WEBHOOK_URL, data=payload, files=file_tuples, timeout=120)
    response.raise_for_status()


def should_process(data, post):
    if post.get("user_id") == BOT_USER_ID:
        return False
    if not ONLY_DM_AND_MENTIONS:
        return True
    if data.get("channel_type") == "D":
        return True
    mentions = data.get("mentions") or []
    if isinstance(mentions, str):
        try:
            mentions = json.loads(mentions)
        except json.JSONDecodeError:
            mentions = []
    return BOT_USER_ID in mentions


async def listen():
    ws_url = MM_URL.replace("https://", "wss://", 1).replace("http://", "ws://", 1)
    ws_url += "/api/v4/websocket"

    async with websockets.connect(ws_url, ping_interval=30) as ws:
        await ws.send(json.dumps({
            "seq": 1,
            "action": "authentication_challenge",
            "data": {"token": BOT_TOKEN},
        }))
        print("Подключено к Mattermost WebSocket", flush=True)

        async for raw in ws:
            event = json.loads(raw)
            if event.get("event") != "posted":
                continue

            data = event.get("data", {})
            post = data.get("post")
            if isinstance(post, str):
                try:
                    post = json.loads(post)
                except json.JSONDecodeError:
                    continue
            if not post:
                continue

            if not should_process(data, post):
                continue

            files = []
            for fid in post.get("file_ids", []):
                try:
                    info = get_file_info(fid)
                    files.append({
                        "name": info.get("name", f"{fid}.bin"),
                        "mime": info.get("mime_type", "application/octet-stream"),
                        "content": download_file(fid),
                    })
                except requests.RequestException as e:
                    print(f"Не удалось скачать файл {fid}: {e}", flush=True)

            try:
                send_to_n8n(post, files)
                print(f"Отправлено в n8n: post={post.get('id')} files={len(files)}", flush=True)
            except requests.RequestException as e:
                print(f"Не удалось отправить в n8n: {e}", flush=True)


async def main():
    while True:
        try:
            await listen()
        except Exception as e:
            print(f"Соединение прервано: {e}. Переподключение через 5 сек...", flush=True)
            await asyncio.sleep(5)


if __name__ == "__main__":
    threading.Thread(target=start_health_server, daemon=True).start()
    print("Сервер здоровья слушает порт 3000", flush=True)
    asyncio.run(main())
