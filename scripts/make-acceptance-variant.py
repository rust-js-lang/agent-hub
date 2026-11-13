#!/usr/bin/env python3
"""Make a distinct, same-identifier Mach-O for the remote upgrade trial.

Only LC_UUID (debug identity) changes. Code is unchanged; the new file is
ad-hoc signed again before it is executed. Never replace the official asset.
"""
import argparse
import hashlib
import json
from pathlib import Path
import struct
import subprocess
import sys
import uuid

IDENTIFIER = "io.rust-js.agent-hub"


def signature(path):
    subprocess.run(["codesign", "--verify", "--strict", str(path)], check=True)
    description = subprocess.check_output(
        ["codesign", "-d", "--verbose=2", str(path)], stderr=subprocess.STDOUT, text=True,
    )
    assert f"Identifier={IDENTIFIER}\n" in description, description
    assert "Signature=adhoc\n" in description, description


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument("destination", type=Path)
    options = parser.parse_args()
    if sys.platform != "darwin":
        parser.error("the acceptance fixture requires macOS codesign")
    source = options.source.resolve()
    destination = options.destination.absolute()
    if destination.resolve() == source or destination.exists():
        parser.error("destination must be a new file, separate from the official executable")
    signature(source)
    original = source.read_bytes()
    image = bytearray(original)
    assert len(image) >= 32 and struct.unpack_from("<I", image)[0] == 0xFEEDFACF, "expected a thin little-endian Mach-O 64 binary"
    assert struct.unpack_from("<I", image, 4)[0] == 0x0100000C, "expected Apple Silicon (arm64)"
    count, commands_size = struct.unpack_from("<II", image, 16)
    end = 32 + commands_size
    assert end <= len(image), "invalid load command bounds"
    offset = 32
    changed = False
    for _ in range(count):
        assert offset + 8 <= end, "truncated load command"
        command, size = struct.unpack_from("<II", image, offset)
        assert size >= 8 and offset + size <= end, "invalid load command size"
        if command == 0x1B:  # LC_UUID: header followed by 16 bytes of debug identity.
            assert size == 24 and not changed, "invalid or duplicate UUID command"
            image[offset + 8:offset + 24] = uuid.uuid4().bytes
            changed = True
        offset += size
    assert changed and offset == end, "UUID command missing or malformed"
    with destination.open("xb") as output:
        output.write(image)
    destination.chmod(0o755)
    try:
        subprocess.run(["codesign", "--force", "--sign", "-", "--identifier", IDENTIFIER, str(destination)], check=True)
        signature(destination)
        before = json.loads(subprocess.check_output([source, "build-info"], text=True))
        after = json.loads(subprocess.check_output([destination, "build-info"], text=True))
        assert before == after, "variant changed executable metadata"
        source_hash = hashlib.sha256(original).hexdigest()
        variant_hash = hashlib.sha256(destination.read_bytes()).hexdigest()
        assert source_hash != variant_hash, "variant must have different signed bytes"
        print(json.dumps({"identifier": IDENTIFIER, "source_sha256": source_hash,
            "variant_sha256": variant_hash, "build_info": after}))
    except BaseException:
        destination.unlink(missing_ok=True)
        raise


if __name__ == "__main__":
    main()
