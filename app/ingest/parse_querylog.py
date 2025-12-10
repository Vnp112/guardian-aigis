import json
import pandas as pd
from pathlib import Path

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
    df = parse_querylog(in_path)
    df.to_csv(out_path, index=False)
    return out_path