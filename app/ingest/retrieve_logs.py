# app/ingest/pull_from_router.py
import subprocess
from pathlib import Path

ROUTER_IP = "192.168.8.1"
REMOTE = "/etc/AdGuardHome/data/querylog.json"
LOCAL = Path("data/querylog.json")
IDENTITY = Path.home() / ".ssh" / "id_ed25519"

TAIL_LINES = 20000

def pull_logs():
    cmd = [
        "ssh",
        "-i", str(IDENTITY),
        f"root@{ROUTER_IP}",
        f"busybox tail -n 20000 {REMOTE}"
    ]

    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"SSH tail failed: {result.stderr}")

    LOCAL.parent.mkdir(parents=True, exist_ok=True)
    LOCAL.write_text(result.stdout)

    return LOCAL
