import streamlit as st, pandas as pd
import requests
from pathlib import Path
from app.features.build_features import build_windows
from app.models.detector import detect
from app.ingest.adguard_ingest import adguard_ingest_from_file

URL = "http://127.0.0.1:8000"

def get_json(path: str):
    r = requests.get(f"{URL}{path}", timeout=5)
    r.raise_for_status()
    return r.json()

alerts_resp = get_json("/alerts")
features_resp = get_json("/features")
devices_resp = get_json("/devices")

alerts = pd.DataFrame(alerts_resp["alerts"])
feats = pd.DataFrame(features_resp["features"])

st.set_page_config(page_title="Guardian AIGIS", layout="wide")

if st.button("Refresh (Ingest → Build → Detect)"):
    adguard_ingest_from_file()
    build_windows()
    detect()

num_devices = 0
highest_anomaly_score = 0.0
last_time = None
ip_list = []

if not alerts.empty:
    # alerts = pd.read_csv(alerts_p, parse_dates=["minute"])
    alerts["minute"] = pd.to_datetime(alerts["minute"])
    num_devices = alerts["client_ip"].nunique() if not alerts.empty else 0
    highest_anomaly_score = alerts["score"].max() if not alerts.empty else 0.0
    last_time = str(alerts["minute"].max()) if not alerts.empty else None
else:
    st.info("No alerts yet. Click Refresh above.")

if not feats.empty:
    feats["minute"] = pd.to_datetime(feats["minute"])
    ip_list = devices_resp["devices"]
else:
    st.info("No features yet. Click Refresh above.")

st.title("Guardian AIGIS  ·  Phase 1 MVP")

dev, max_time, last_timestamp = st.columns(3)
dev.metric(label="Devices", value=num_devices, border=True)
max_time.metric(label="Highest Anomaly Score", value=highest_anomaly_score, border=True)
last_timestamp.metric(label="Last Time", value=last_time, border=True)

if len(ip_list) > 0 and not alerts.empty:
    dropdown = st.selectbox(label="Pick Device", options=ip_list, index=None, placeholder="Select Device")
    if dropdown is not None:
        history_resp = get_json(f"/devices/{dropdown}/history")
        history = pd.DataFrame(history_resp["history"])

        if not history.empty:
            history["minute"] = pd.to_datetime(history["minute"])
            avg_len, qpm_chart, uniq_chart = st.columns(3)

            avg_len.subheader("Average Query Length")
            avg_len.line_chart(data=history.set_index("minute")["avg_len"])

            qpm_chart.subheader("Queries Per Minute")
            qpm_chart.line_chart(data=history.set_index("minute")["qpm"])

            uniq_chart.subheader("Unique Domains")
            uniq_chart.line_chart(data=history.set_index("minute")["uniq"])
else:
    st.info("No alerts and devices to plot.")

st.subheader("Top anomalies (latest minute)")
st.dataframe(alerts[["client_ip","minute","qpm","uniq","avg_len","score"]])

st.subheader("Recent feature windows")
st.dataframe(feats.tail(50))


