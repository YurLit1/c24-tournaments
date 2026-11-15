import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

FAULT_DIR = os.getenv("FAULT_DIR", "/faults")


def fault(place: str) -> None:
    if os.getenv("FAULTS") != "on":
        return
    try:
        (Path(FAULT_DIR) / place).unlink()
    except FileNotFoundError:
        return
    sys.stdout.write(json.dumps({"time": datetime.now(timezone.utc).isoformat(), "event": "fault.hit",
                                 "place": place, "pid": os.getpid()}) + "\n")
    sys.stdout.flush()
    os._exit(1)
