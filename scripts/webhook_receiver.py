#!/usr/bin/env python3
"""Минимальный приёмник webhook-ов для ручной проверки сервиса (только stdlib).

    python scripts/webhook_receiver.py                    # всегда отвечает 200
    python scripts/webhook_receiver.py --status 500       # всегда 500: retry x3, затем DLQ
    python scripts/webhook_receiver.py --fail-rate 0.5    # половина запросов получает 500

Из контейнера consumer приёмник на хосте доступен как http://host.docker.internal:9000/webhook
"""

import argparse
import json
import random
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


def build_handler(status: int, fail_rate: float) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:  # noqa: N802 - имя задаёт http.server
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length)
            try:
                body = json.dumps(json.loads(raw), ensure_ascii=False, indent=2)
            except ValueError:
                body = raw.decode(errors="replace")

            code = 500 if random.random() < fail_rate else status
            print(f"--> POST {self.path} -> HTTP {code}\n{body}\n", flush=True)
            self.send_response(code)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, *args: object) -> None:
            """Подавляем стандартный access-лог: полезное печатаем сами."""

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument("--status", type=int, default=200, help="HTTP-статус ответа (по умолчанию 200)")
    parser.add_argument("--fail-rate", type=float, default=0.0, help="доля запросов, на которые отвечаем 500")
    args = parser.parse_args()

    server = ThreadingHTTPServer(("0.0.0.0", args.port), build_handler(args.status, args.fail_rate))
    print(f"Listening on :{args.port} (status={args.status}, fail_rate={args.fail_rate}). Ctrl+C to stop.")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
