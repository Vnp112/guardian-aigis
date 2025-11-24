from fastapi import FastAPI
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import json
import requests

from app.features.build_features import build_windows
from app.models.detector import detect
from app.ingest.adguard_ingest import adguard_ingest_from_file
from app.ingest.retrieve_logs import pull_logs

app = FastAPI()

def load_file(file):
    if not file.exists():
        return pd.DataFrame()
    return pd.read_csv(file)

def parse_since(s: str):
    if s.endswith("m"):
        return timedelta(minutes=int(s[:-1]))
    if s.endswith("h"):
        return timedelta(hours=int(s[:-1]))
    if s.endswith("d"):
        return timedelta(days=int(s[:-1]))
    return None

@app.get("/")
async def root():
    return {"message": "Hello World"}

@app.get("/items/{item_id}")
async def read_item(item_id: int):
    return {"item_id": item_id}

@app.get("/ping")
async def get_ping():
    return {"status": "ok"}

@app.get("/hello/{name}")
def hello(name: str):
    return {"message": f"Hello, {name}"}


@app.get("/alerts")
def get_alerts():
    path = Path("data/alerts.csv")
    # if not path.exists():
    #     return {"alerts": []}
    # alerts_csv = pd.read_csv(path)
    alerts_df = load_file(path)
    # Iterative method
    # rows = []
    # for idx, row in alerts_csv.iterrows():
    #     rows.append({"client_ip": row["client_ip"],
    #                  "minute": row["minute"],
    #                  "qpm": row["qpm"],
    #                  "uniq": row["uniq"],
    #                  "avg_len": row["avg_len"],
    #                  "score": row["score"]})
    
    # Pandas built in conversion
    rows = alerts_df.to_dict(orient="records")
    return {"alerts": rows}

@app.get("/devices")
def get_devices():
    path = Path("data/features.csv")
    features_df = load_file(path)
    rows = features_df.to_dict(orient="records")
    devices = sorted(set([row["client_ip"] for row in rows]))
    return {"devices": devices}
    
@app.get("/devices/{ip}/history")
def get_device_history(ip: str, since: str | None = None):
    path = Path("data/features.csv")
    if not path.exists():
        return {"ip": ip, "history": []}
    features_df = load_file(path)
    features_df["minute"] = pd.to_datetime(features_df["minute"]).dt.tz_localize(None)
    ip_data = features_df[features_df["client_ip"] == ip].sort_values("minute")
    if since:
        delta = parse_since(since)
        if delta:
            latest = ip_data["minute"].max()
            #cutoff = datetime.utcnow() - delta
            cutoff = latest - delta
            ip_data = ip_data[ip_data["minute"] >= cutoff]
    ip_data = ip_data.to_dict(orient="records")
    return{"ip": ip, "history": ip_data}
    
@app.get("/features")
def get_features():
    path = Path("data/features.csv")
    if not path.exists():
        return {"features": []}
    features_df = load_file(path)
    features = features_df.to_dict(orient="records")
    return {"features": features}

@app.post("/refresh")
def refresh():
    pull_logs()
    adguard_ingest_from_file()
    build_windows()
    detect()
    
    # Make dir and text file to record last refresh timestamp
    path = Path("data")
    path.mkdir(parents=True, exist_ok=True)
    last_refresh = path/"last_refresh.txt"
    with open(last_refresh, "a") as f:
        f.write(datetime.utcnow().isoformat() + "\n")
    
    return {"status": "ok"}

@app.get("/status")
def get_status():
    alerts_csv = Path("data/alerts.csv")
    features_csv = Path("data/features.csv")
    last_refresh_txt = Path("data/last_refresh.txt")
    
    num_devices = 0
    num_feature_rows = 0
    num_alert_rows = 0
    last_feature_time = None
    last_alert_time = None
    last_refresh_timestamp = None
    highest_anomaly_score = 0.0

    
    if features_csv.exists():
        feats = pd.read_csv(features_csv, parse_dates=["minute"])
        if not feats.empty:
            num_feature_rows = len(feats)
            num_devices = feats["client_ip"].nunique()
            last_feature_time = feats["minute"].max().isoformat()
    if alerts_csv.exists():
        alerts = pd.read_csv(alerts_csv, parse_dates=["minute"])
        if not alerts.empty:
            num_alert_rows = len(alerts)
            last_alert_time = alerts["minute"].max().isoformat()
            highest_anomaly_score = alerts["score"].max()
    
    if last_refresh_txt.exists():
        with open(last_refresh_txt, "r") as f:
            last_refresh_timestamp = f.readlines()[-1]
    
    return {
        "num_devices": num_devices,
        "num_feature_rows": num_feature_rows,
        "num_alert_rows": num_alert_rows,
        "last_feature_time": last_feature_time,
        "last_alert_time": last_alert_time,
        "last_refresh_timestamp": last_refresh_timestamp,
        "highest_anomaly_score": highest_anomaly_score
    }
        

# @app.post("/control/login")
# def login():
#     s = requests.Session()
#     s.auth = ('admin', 'pword')
#     r = s.get('http://192.168.8.1:3000/control/login')
#     return {"name": "admin", "password": "pword"}