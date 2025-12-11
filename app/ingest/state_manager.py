import json
from pathlib import Path
import pandas as pd


STATE_PATH = Path("data/state.json")


def load_state():
    """
    {
        "last_ingested_time": pandas.Timestamp or None
    }
    """
    if not STATE_PATH.exists():
        return {}
    try:
        with open(STATE_PATH, "r") as f:
            raw_state = json.load(f)
    except json.JSONDecodeError:
        return {}

    state = {}

    ts = raw_state.get("last_ingested_time")
    if ts:
        state["last_ingested_time"] = pd.to_datetime(ts)
    else:
        state["last_ingested_time"] = None

    return state

def save_state(state: dict):
    ts = state.get("last_ingested_time")
    if ts is not None:
        ts = pd.to_datetime(ts, errors="coerce")
        to_write = {"last_ingested_time": str(ts)}
    else:
        to_write = {"last_ingested_time": None}

    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(to_write, f, indent=2)