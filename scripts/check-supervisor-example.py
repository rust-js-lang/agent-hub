#!/usr/bin/env python3
"""Exercise a second application's real process handoff, without the hub."""
import argparse
import json
import os
from pathlib import Path
import shutil
import signal
import socket
import subprocess
import tempfile
import time


def children(pid):
    result = subprocess.run(["pgrep", "-P", str(pid)], capture_output=True, text=True)
    return {int(value) for value in result.stdout.split()}


def wait_until(check, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.025)
    raise AssertionError("condition did not become true within timeout")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--binary", type=Path, required=True)
    args = parser.parse_args()
    source = args.binary.resolve()
    with tempfile.TemporaryDirectory(prefix="ps-echo-") as directory:
        root = Path(directory)
        installed = root / "echo"
        shutil.copy2(source, installed)
        state = root / "state"
        with socket.socket() as reservation:
            reservation.bind(("127.0.0.1", 0))
            port = reservation.getsockname()[1]

        def answers():
            try:
                with socket.create_connection(("127.0.0.1", port), timeout=1) as connection:
                    connection.settimeout(1)
                    data = b""
                    while chunk := connection.recv(4096):
                        data += chunk
                    return data == b"hello echo-v1\n"
            except OSError:
                return False

        with (root / "supervisor.log").open("w+") as log:
            supervisor = subprocess.Popen(
                [str(installed), "serve", str(state), f"127.0.0.1:{port}"],
                stdout=log, stderr=log, start_new_session=True,
            )
            try:
                wait_until(answers)
                before = children(supervisor.pid)
                assert len(before) == 1, before
                result = subprocess.run(
                    [str(source), "upgrade", str(state), str(source)],
                    capture_output=True, text=True, timeout=20, check=True,
                )
                reply = json.loads(result.stdout)
                assert not reply["rolled_back"] and not reply["deferred"], reply
                during = children(supervisor.pid)
                assert len(during) == 2 and before < during, (before, during)
                assert answers()
                wait_until(lambda: len(children(supervisor.pid)) == 1, timeout=8)
                after = children(supervisor.pid)
                assert after.isdisjoint(before), (before, after)
                assert answers()
                subprocess.run([str(source), "stop", str(state)], timeout=10, check=True)
                assert supervisor.wait(timeout=10) == 0
                assert not children(supervisor.pid)
                for pid in during:
                    try:
                        os.kill(pid, 0)
                    except ProcessLookupError:
                        continue
                    raise AssertionError(f"worker {pid} survived stop")
                print("Independent TCP service: startup, handoff, parked predecessor, retirement, delivery and clean stop PASS")
            except BaseException:
                log.flush()
                print((root / "supervisor.log").read_text())
                raise
            finally:
                if supervisor.poll() is None:
                    os.killpg(supervisor.pid, signal.SIGTERM)
                    try:
                        supervisor.wait(timeout=7)
                    except subprocess.TimeoutExpired:
                        os.killpg(supervisor.pid, signal.SIGKILL)
                        supervisor.wait(timeout=5)


if __name__ == "__main__":
    main()
