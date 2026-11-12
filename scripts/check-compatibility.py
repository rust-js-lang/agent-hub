#!/usr/bin/env python3
"""Exercise each client against the other release's real hub, on private temp ports."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import tempfile
import time
import urllib.request


def wait_for(predicate, timeout=15):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            if predicate():
                return
        except (OSError, ValueError):
            pass
        time.sleep(0.05)
    raise RuntimeError("compatibility hub/watch did not become ready")


def check(server, client):
    with tempfile.TemporaryDirectory(prefix="cam-compat-") as directory:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        args = [server, "serve", "--port", str(port), "--db", str(Path(directory) / "hub.db")]
        help_text = subprocess.check_output([server, "serve", "--help"], text=True)
        if "--no-supervisor" in help_text:
            args += ["--no-supervisor"]
        process = subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, start_new_session=True)
        watcher = None
        env = {**os.environ, "CROSS_AGENT_HUB": url, "CROSS_AGENT_STATE_DIR": directory}
        def run(*command):
            return subprocess.check_output([client, *command], env=env, text=True, timeout=15)
        def agents():
            with urllib.request.urlopen(url + "/api/agents", timeout=2) as response:
                return json.load(response)
        try:
            wait_for(lambda: agents() is not None)
            run("agents", "--as", "compat-a")
            run("agents", "--as", "compat-b")
            run("send", "--as", "compat-a", "compat-b", "version-skew message")
            assert "version-skew message" in run("inbox", "--as", "compat-b")
            assert "You hold" in run("hold", "--as", "compat-a", "compat-check")
            run("release", "--as", "compat-a", "compat-check")
            watcher = subprocess.Popen([client, "watch", "--as", "compat-b"], env=env, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
            wait_for(lambda: any(a["name"] == "compat-b" and a["watching"] for a in agents()))
            run("send", "--as", "compat-a", "compat-b", "mail with a watch connected")
            assert "mail with a watch connected" in run("inbox", "--as", "compat-b")
            assert watcher.poll() is None
            print(f"PASS: {Path(client).parent.name} client -> {Path(server).parent.name} hub")
        finally:
            if watcher:
                watcher.terminate()
                watcher.wait(timeout=5)
            os.killpg(process.pid, signal.SIGTERM)
            try:
                process.wait(timeout=6)
            except subprocess.TimeoutExpired:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=5)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--current", required=True)
    parser.add_argument("--previous", required=True)
    options = parser.parse_args()
    check(str(Path(options.current).resolve()), str(Path(options.previous).resolve()))
    check(str(Path(options.previous).resolve()), str(Path(options.current).resolve()))
