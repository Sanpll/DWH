"""Regenerate the fixed fixture and byte-compare it with supplied input files."""
from __future__ import annotations

import hashlib
import subprocess
import sys
import tempfile
from pathlib import Path


def digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    project = Path(__file__).resolve().parents[1]
    source = project / "data" / "fixture"
    generator = project / "scripts" / "generate_data.py"
    files = ("catalogue.jsonl", "events.jsonl", "profile_changes.jsonl", "manifest.json")
    with tempfile.TemporaryDirectory(prefix="fixture-check-") as directory:
        generated = Path(directory) / "generated"
        subprocess.run([
            sys.executable, str(generator), "--mode", "fixture", "--output", str(generated)
        ], check=True)
        mismatches = []
        for name in files:
            equal = digest(source / name) == digest(generated / name)
            print(f"{name}: {'MATCH' if equal else 'DIFFER'}")
            if not equal:
                mismatches.append(name)
    if mismatches:
        raise SystemExit("Fixture differs from generator output: " + ", ".join(mismatches))
    print("Provided fixture is byte-identical to generate_data.py --mode fixture")


if __name__ == "__main__":
    main()
