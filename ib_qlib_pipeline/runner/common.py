from __future__ import annotations

import subprocess
from pathlib import Path


def log(message: str, console_lines: list[str]) -> None:
    print(message)
    console_lines.append(message)


def run_cmd(cmd: list[str], cwd: Path, console_lines: list[str], env: dict[str, str] | None = None) -> None:
    rendered = " ".join(cmd)
    log(f"[run] {rendered}", console_lines)
    proc = subprocess.Popen(
        cmd,
        cwd=str(cwd),
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    assert proc.stdout is not None
    for line in proc.stdout:
        line = line.rstrip("\n")
        print(line)
        console_lines.append(line)
    rc = proc.wait()
    if rc != 0:
        raise SystemExit(rc)
