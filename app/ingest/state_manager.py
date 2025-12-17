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
            raw = json.load(f)
    except json.JSONDecodeError:
        return {}

    state = {}

    for key in ("last_ingested_time", "last_window_minute"):
        ts = raw.get(key)
        state[key] = pd.to_datetime(ts) if ts else None

    # domain_first_seen
    dfs = raw.get("domain_first_seen", {})
    parsed = {}
    for device, domains in dfs.items():
        parsed[device] = {
            domain: pd.to_datetime(t)
            for domain, t in domains.items()
        }

    state["domain_first_seen"] = parsed

    return state

def save_state(state: dict):
    out = {}

    for key in ("last_ingested_time", "last_window_minute"):
        ts = state.get(key)
        out[key] = ts.isoformat() if ts is not None else None

    dfs = state.get("domain_first_seen", {})
    out["domain_first_seen"] = {
        device: {
            domain: t.isoformat()
            for domain, t in domains.items()
        }
        for device, domains in dfs.items()
    }

    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_PATH, "w") as f:
        json.dump(out, f, indent=2)