import streamlit as st, pandas as pd
import requests
from pathlib import Path

URL = "http://127.0.0.1:8000"

def get_json(path: str):
    r = requests.get(f"{URL}{path}", timeout=10)
    r.raise_for_status()
    return r.json()

st.set_page_config(page_title="Guardian AIGIS", layout="wide")

if st.button("Refresh (Ingest → Build → Detect)"):
    # 1. Kick off Celery job
    resp = requests.post(f"{URL}/refresh", timeout=10)
    task_id = resp.json().get("task_id")

    if task_id:
        with st.spinner("Refreshing logs, building features, running detector..."):
            import time
            while True:
                status = requests.get(f"{URL}/task-status/{task_id}", timeout=5).json()
                state = status.get("state")

                if state in ("SUCCESS", "FAILURE", "REVOKED"):
                    break

                time.sleep(2)

        st.success("Refresh complete!")
        st.rerun()

    
alerts_resp = get_json("/alerts")
features_resp = get_json("/features")
devices_resp = get_json("/devices")
status_resp = get_json("/status")

alerts = pd.DataFrame(alerts_resp["alerts"])
feats = pd.DataFrame(features_resp["features"])

# num_devices = 0
highest_anomaly_score = 0.0
# last_time = None
ip_list = []

if not alerts.empty:
    # alerts = pd.read_csv(alerts_p, parse_dates=["minute"])
    alerts["minute"] = pd.to_datetime(alerts["minute"]).dt.tz_localize(None)
    num_devices = status_resp.get("num_devices", 0)
    highest_anomaly_score = status_resp.get("highest_anomaly_score", 0.0)
    last_time = status_resp.get("last_feature_time", None)
else:
    st.info("No alerts yet. Click Refresh above.")

if not feats.empty:
    #feats["minute"] = pd.to_datetime(feats["minute"])
    ip_list = devices_resp["devices"]
else:
    st.info("No features yet. Click Refresh above.")

st.title("Guardian AIGIS")

dev, max_time, last_timestamp, last_refresh_timestamp = st.columns(4)
dev.metric(label="Devices", value=num_devices, border=True)
max_time.metric(label="Highest Combined Anomaly Score", value=highest_anomaly_score, border=True)
last_timestamp.metric(label="Last Time", value=last_time, border=True)
last_refresh_timestamp.metric(label="Last Refresh Time", value=status_resp.get("last_refresh_timestamp"), border=True)

if len(ip_list) > 0 and not alerts.empty:
    dropdown = st.selectbox(label="Pick Device", options=ip_list, index=None, placeholder="Select Device")
    time_slider = st.select_slider(label="History Slider", options=["0", "1m", "5m", "10m", "30m", "1h", "2h", "3h", "4h", "5h", "6h", "1d", "2d"], value="0")
    if dropdown is not None:
        history_resp = get_json(f"/devices/{dropdown}/history?since={time_slider}")
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
            
            
            
            pc1_col, pc2_col, pca_scatter = st.columns(3)
            
            pc1_col.subheader("PC1 Score Over Time")
            pc1_col.line_chart(history.set_index("minute")["pc1"])
            
            pc2_col.subheader("PC2 Score Over Time")
            pc2_col.line_chart(history.set_index("minute")["pc2"])
            
            pca_scatter.subheader("PC1 vs PC2 Scatter Plot")
            pca_scatter.scatter_chart(history[["pc1", "pc2"]].rename(columns={"pc1": "x", "pc2": "y"}))
            
else:
    st.info("No alerts and devices to plot.")

st.subheader("Top anomalies (latest minute)")
score_threshold = st.slider(label="Select Anomaly Filter Value", min_value = 0.0, max_value=1.0, value=0.0, step=0.01)
filtered_alerts = alerts[alerts["combined_score"] >= score_threshold]
st.dataframe(filtered_alerts[["client_ip","minute","qpm","uniq","avg_len","score", "Mahalanobis", "norm_score", "norm_Mahalanobis", "pc1", "pc2", "combined_score"]])

st.subheader("Recent feature windows")
st.dataframe(feats)


