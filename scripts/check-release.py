#!/usr/bin/env python3
"""Verify the exact downloadable archive without a source checkout beside it."""
import hashlib
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.request


archive = Path(sys.argv[1]).resolve()
sums = archive.with_name("SHA256SUMS").read_text().splitlines()
expected = next(line.split()[0] for line in sums if line.split()[-1] == archive.name)
assert hashlib.sha256(archive.read_bytes()).hexdigest() == expected
with tempfile.TemporaryDirectory(prefix="cam-release-") as directory:
    root = Path(directory)
    binary = root / "cross-agent-message"
    with tarfile.open(archive) as package:
        members = package.getmembers()
        assert len(members) == 1 and members[0].name == binary.name and members[0].isfile()
        binary.write_bytes(package.extractfile(members[0]).read())
    binary.chmod(0o755)
    info = json.loads(subprocess.check_output([binary, "build-info"], cwd=root))
    assert info["target"] == "aarch64-apple-darwin" and info["protocol"] == 1
    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]
    env = {**os.environ, "CROSS_AGENT_STATE_DIR": directory}
    with (root / "log").open("w+") as log:
        process = subprocess.Popen(
            [binary, "serve", "--port", str(port), "--db", root / "hub.db"],
            cwd=root, env=env, stdout=log, stderr=log, start_new_session=True,
        )
        url = f"http://127.0.0.1:{port}"
        try:
            deadline = time.monotonic() + 15
            while True:
                try:
                    with urllib.request.urlopen(url + "/api/hub", timeout=2) as response:
                        hub = json.load(response)
                    break
                except OSError:
                    if process.poll() is not None or time.monotonic() > deadline:
                        log.seek(0)
                        raise RuntimeError("extracted hub failed to start:\n" + log.read())
                    time.sleep(0.05)
            assert hub["supervised"] and hub["build"] == info["build"]
            with urllib.request.urlopen(url + "/", timeout=2) as response:
                html = response.read().decode()
            assets = re.findall(r'(?:src|href)="(/assets/[^\"]+)"', html)
            assert assets, "embedded dashboard has no bundled assets"
            for asset in assets:
                with urllib.request.urlopen(url + asset, timeout=2) as response:
                    assert response.status == 200 and response.read()
            def run(*args):
                return subprocess.check_output([binary, "--hub", url, *args], cwd=root, env=env, text=True, timeout=15)
            run("agents", "--as", "release-a")
            run("agents", "--as", "release-b")
            run("send", "--as", "release-a", "release-b", "downloaded release works")
            assert "downloaded release works" in run("inbox", "--as", "release-b")
            print(f"PASS: archive checksum, target, supervised hub, embedded dashboard and delivery ({info['build']})")
        finally:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGTERM)
                try:
                    process.wait(timeout=6)
                except subprocess.TimeoutExpired:
                    os.killpg(process.pid, signal.SIGKILL)
                    process.wait(timeout=5)
