#!/usr/bin/env python3
"""Verify real prior/current hub receipts replay across upgrade and rollback."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import sqlite3
import subprocess
import tempfile
import time
import urllib.request


def stop(process):
    if process.poll() is None:
        os.killpg(process.pid, signal.SIGTERM)
        try:
            process.wait(timeout=6)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=3)


def snapshot(path):
    with sqlite3.connect(path) as db:
        return (
            db.execute("SELECT actor,nonce,fingerprint,response FROM receipts ORDER BY actor,nonce").fetchall(),
            db.execute("SELECT count(*) FROM messages").fetchone()[0],
            db.execute("SELECT * FROM holds ORDER BY name").fetchall(),
        )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--current", type=Path, required=True)
    parser.add_argument("--previous", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="cam-receipt-compat-") as directory:
        root = Path(directory)
        db = root / "hub.db"
        process = None
        url = None

        def request(path, body=None, nonce=None):
            headers = {"agent-hub-protocol": "1"}
            if body is not None:
                headers["content-type"] = "application/json"
            if nonce is not None:
                headers["agent-hub-nonce"] = nonce
            query = urllib.request.Request(url + path, data=body, headers=headers)
            with urllib.request.urlopen(query, timeout=5) as response:
                return response.read()

        def start(binary):
            nonlocal process, url
            if process is not None:
                stop(process)
            with socket.socket() as reservation:
                reservation.bind(("127.0.0.1", 0))
                port = reservation.getsockname()[1]
            url = f"http://127.0.0.1:{port}"
            with (root / "hub.log").open("ab") as log:
                process = subprocess.Popen(
                    [str(binary.resolve()), "serve", "--no-supervisor", "--db", str(db), "--port", str(port)],
                    stdout=log, stderr=log, start_new_session=True,
                )
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                try:
                    request("/api/hub")
                    return
                except OSError:
                    if process.poll() is not None:
                        break
                    time.sleep(0.025)
            raise AssertionError((root / "hub.log").read_text())

        def fixtures(prefix):
            operations = [
                ("send", "/api/messages", {"from": "a", "to": "b", "body": prefix, "key": None}),
                ("broadcast", "/api/channels/messages", {"from": "a", "to": "#everyone", "body": prefix, "key": None}),
                ("take", "/api/holds", {"name": "check", "holder": "a", "minutes": 30}),
                ("release", "/api/holds/release", {"name": "check", "holder": "a"}),
            ]
            result = []
            for name, path, value in operations:
                body = json.dumps(value, separators=(",", ":")).encode()
                nonce = f"{prefix}-{name}"
                response = request(path, body, nonce)
                result.append((name, path, body, nonce, response))
            return result

        def replay(fixtures, expected, label):
            for name, path, body, nonce, response in fixtures:
                assert request(path, body, nonce) == response, f"{label}: {name} changed response"
            assert snapshot(db) == expected, f"{label}: replay changed receipts, messages or holds"
            print(f"PASS: {label}, send/broadcast/take/release replay byte-for-byte without effects")

        try:
            start(args.previous)
            for name in ["a", "b"]:
                request("/api/agents", json.dumps({"name": name, "machine": "fixture", "reconnect": False}).encode())
            old = fixtures("previous")
            before = snapshot(db)
            start(args.current)
            replay(old, before, "previous receipts read after upgrade")
            new = fixtures("current")
            after = snapshot(db)
            start(args.previous)
            replay(new, after, "current receipts read after rollback")
        finally:
            if process is not None:
                stop(process)


if __name__ == "__main__":
    main()
