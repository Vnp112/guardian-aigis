import json
import pandas as pd
from pathlib import Path
from .state_manager import load_state, save_state

def parse_querylog(path=Path("data/querylog.json")):
    rows = []

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                obj = json.loads(line)
            except:
                continue

            rows.append({
                "time": obj.get("T"),
                "client_ip": obj.get("IP"),
                "domain": obj.get("QH"),
                "qtype": obj.get("QT")
            })

    df = pd.DataFrame(rows)
    df["time"] = pd.to_datetime(df["time"], errors="coerce")
    df = df.dropna(subset=["time", "client_ip", "domain"])
    return df

def write_csv(in_path=Path("data/querylog.json"), out_path=Path("data/sample_dns.csv")):

    df_new = parse_querylog(in_path).sort_values("time")
    state = load_state()
    last_timestamp = state.get("last_ingested_time")
    if last_timestamp is not None:
        last_timestamp = pd.to_datetime(last_timestamp)
    if not out_path.exists() or last_timestamp is None:
        df_new.to_csv(out_path, index=False)
        if not df_new.empty:
            state["last_ingested_time"] = df_new["time"].max()
            save_state(state)
        return out_path
    df_new = df_new[df_new["time"] > last_timestamp]

    if df_new.empty:
        return out_path
    df_new.to_csv(out_path, mode="a", header=False, index=False)
    new_last = df_new["time"].max()
    state["last_ingested_time"] = new_last
    save_state(state)

    return out_path
