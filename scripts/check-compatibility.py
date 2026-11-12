#!/usr/bin/env python3
"""Exercise each client against the other release's real hub, on private temp ports."""
import argparse
import json
import os
from pathlib import Path
import signal
import shutil
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


def stop_process(process):
    if process.poll() is not None:
        return
    os.killpg(process.pid, signal.SIGTERM)
    try:
        process.wait(timeout=6)
    except subprocess.TimeoutExpired:
        os.killpg(process.pid, signal.SIGKILL)
        process.wait(timeout=5)


def start_process(server, directory, port):
    args = [server, "serve", "--port", str(port), "--db", str(Path(directory) / "hub.db")]
    help_text = subprocess.check_output([server, "serve", "--help"], text=True)
    if "--no-supervisor" in help_text:
        args += ["--no-supervisor"]
    return subprocess.Popen(args, stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, start_new_session=True)


def check_migrated_data(current, previous):
    with tempfile.TemporaryDirectory(prefix="cam-schema-") as directory:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        env = {**os.environ, "CROSS_AGENT_HUB": url, "CROSS_AGENT_STATE_DIR": directory}
        process = start_process(current, directory, port)
        def ready():
            with urllib.request.urlopen(url + "/api/hub", timeout=2) as response:
                return response.status == 200
        def run(binary, *args):
            return subprocess.check_output([binary, *args], env=env, text=True, timeout=15)
        try:
            wait_for(ready)
            run(current, "agents", "--as", "schema-a")
            run(current, "agents", "--as", "schema-b")
            run(current, "send", "--as", "schema-a", "schema-b", "written by the new release")
            run(current, "hold", "--as", "schema-a", "schema-check")
            stop_process(process)
            # A fresh port avoids relying on the previous release's cold-bind
            # policy; the database and all acknowledged data stay the same.
            with socket.socket() as probe:
                probe.bind(("127.0.0.1", 0))
                port = probe.getsockname()[1]
            url = f"http://127.0.0.1:{port}"
            env["CROSS_AGENT_HUB"] = url
            process = start_process(previous, directory, port)
            wait_for(ready)
            assert "written by the new release" in run(previous, "inbox", "--as", "schema-b")
            assert "schema-check" in run(previous, "holds", "--as", "schema-a")
            print("PASS: previous hub reads the schema, messages and holds written by current")
        finally:
            stop_process(process)


def check(server, client, label):
    with tempfile.TemporaryDirectory(prefix="cam-compat-") as directory:
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        process = start_process(server, directory, port)
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
            print(f"PASS: {label}")
        finally:
            if watcher:
                watcher.terminate()
                watcher.wait(timeout=5)
            stop_process(process)


def check_supervised_upgrades(current, previous):
    help_text = subprocess.check_output([previous, "--help"], text=True)
    if not any(line.strip().startswith("upgrade ") for line in help_text.splitlines()):
        print("SKIP: previous release predates supervised upgrade commands")
        return
    with tempfile.TemporaryDirectory(prefix="cam-mix-", dir="/tmp") as directory:
        root = Path(directory)
        installed = root / "cross-agent-message"
        shutil.copy2(current, installed)
        database = root / "hub.db"
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        env = {**os.environ, "CROSS_AGENT_HUB": url, "CROSS_AGENT_STATE_DIR": directory}
        process = subprocess.Popen([installed, "serve", "--db", database, "--port", str(port)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=env, start_new_session=True)
        def info():
            with urllib.request.urlopen(url + "/api/hub", timeout=2) as response:
                return json.load(response)
        def run(binary, *args):
            result = subprocess.run([binary, *args], env=env, text=True, capture_output=True, timeout=30)
            assert result.returncode == 0, result.stderr
            assert "could not announce upgrade" not in result.stderr, result.stderr
            return result.stdout
        def upgrade(client, image):
            expected = json.loads(subprocess.check_output([image, "build-info"], text=True))
            run(client, "upgrade", image, "--db", str(database))
            actual = info()
            assert actual["build"] == expected["build"] and actual["version"] == expected["version"]
            config = json.loads(database.with_suffix(".worker.json").read_text())
            assert Path(config["worker_path"]) == installed.resolve()
        try:
            wait_for(lambda: info().get("supervised"))
            current_cooperative = "cooperative-handoff" in info().get("features", [])
            run(previous, "agents", "--as", "skew-a")
            run(previous, "agents", "--as", "skew-b")
            run(previous, "send", "--as", "skew-a", "skew-b", "supervised version-skew message")
            run(previous, "hold", "--as", "skew-a", "skew-check")
            upgrade(previous, current)  # Previous client uses the short legacy socket aliases.
            upgrade(current, previous)  # Previous worker reports its older readiness format.
            upgrade(current, current)
            assert run(previous, "inbox", "--as", "skew-b").count("supervised version-skew message") == 1
            assert "skew-check" in run(current, "holds", "--as", "skew-a")
            print("PASS: previous client upgrades current supervisor; previous/current workers round-trip at one installed path")
        finally:
            stop_process(process)

    # Snapshot-path parents must be refused. A stable-path parent may run the
    # new worker, but cannot gain a newer handoff guarantee just by doing so.
    with tempfile.TemporaryDirectory(prefix="cam-old-sup-", dir="/tmp") as directory:
        root = Path(directory)
        installed = root / "cross-agent-message"
        shutil.copy2(previous, installed)
        database = root / "hub.db"
        with socket.socket() as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        url = f"http://127.0.0.1:{port}"
        env = {**os.environ, "CROSS_AGENT_HUB": url, "CROSS_AGENT_STATE_DIR": directory}
        process = subprocess.Popen([installed, "serve", "--db", database, "--port", str(port)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, env=env, start_new_session=True)
        def info():
            with urllib.request.urlopen(url + "/api/hub", timeout=2) as response:
                return json.load(response)
        try:
            wait_for(lambda: info().get("supervised"))
            before = info()
            parent = json.loads(database.with_suffix(".worker.json").read_text())
            attempted = subprocess.run([previous, "upgrade", current, "--db", database], env=env, text=True, capture_output=True, timeout=30)
            if not parent.get("worker_path"):
                assert attempted.returncode != 0 and "serve --replace" in attempted.stderr, attempted.stderr
                assert info()["version"] == before["version"] and info()["build"] == before["build"]
                print("PASS: old snapshot supervisor refuses the new worker with cold-migration instructions and rolls back")
            else:
                assert attempted.returncode == 0, attempted.stderr
                expected = json.loads(subprocess.check_output([current, "build-info"], text=True))
                actual = info()
                assert (actual["version"], actual["build"]) == (expected["version"], expected["build"])
                if current_cooperative:
                    parent_cooperative = parent.get("handoff_protocol", 0) == 1
                    assert ("cooperative-handoff" in actual.get("features", [])) == parent_cooperative
                    if not parent_cooperative:
                        refused = subprocess.run([current, "upgrade", current, "--db", database], env=env, text=True, capture_output=True, timeout=30)
                        assert refused.returncode != 0 and "serve --replace" in refused.stderr, refused.stderr
                        assert info()["build"] == actual["build"]
                print("PASS: previous stable-path supervisor runs current worker without inventing parent capabilities")
        finally:
            stop_process(process)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--current", required=True)
    parser.add_argument("--previous", required=True)
    options = parser.parse_args()
    current, previous = str(Path(options.current).resolve()), str(Path(options.previous).resolve())
    check(current, previous, "previous client -> current hub")
    check(previous, current, "current client -> previous hub")
    check_migrated_data(current, previous)
    check_supervised_upgrades(current, previous)
