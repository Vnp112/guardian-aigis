import streamlit as st, pandas as pd
from pathlib import Path
from app.features.build_features import build_windows
from app.models.detector import detect
from app.ingest.adguard_ingest import adguard_ingest_from_file

alerts_p = Path("data/alerts.csv")
feats_p  = Path("data/features.csv")

st.set_page_config(page_title="Guardian AIGIS — Phase 1", layout="wide")

if st.button("Refresh (Build → Detect)"):
    build_windows()
    detect()

num_devices = 0
highest_anomaly_score = 0.0
last_time = None
ip_list = []
alerts = pd.DataFrame()
feats = pd.DataFrame()

if alerts_p.exists():
    alerts = pd.read_csv(alerts_p, parse_dates=["minute"])
    num_devices = alerts["client_ip"].nunique() if not alerts.empty else 0
    highest_anomaly_score = alerts["score"].max() if not alerts.empty else 0.0
    last_time = str(alerts["minute"].max()) if not alerts.empty else None
else:
    st.info("No alerts yet. Click Refresh above.")

if feats_p.exists():
    feats = pd.read_csv(feats_p, parse_dates=["minute"])
    ip_list = feats["client_ip"].unique() if not feats.empty else []
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
        device_alerts = alerts[alerts["client_ip"] == dropdown].sort_values("minute")
        if not device_alerts.empty:
            score_chart, qpm_chart, uniq_chart = st.columns(3)

            score_chart.subheader("Anomaly Score")
            score_chart.line_chart(data=device_alerts.set_index("minute")["score"])

            qpm_chart.subheader("Queries Per Minute")
            qpm_chart.line_chart(data=device_alerts.set_index("minute")["qpm"])

            uniq_chart.subheader("Unique Domains")
            uniq_chart.line_chart(data=device_alerts.set_index("minute")["uniq"])
else:
    st.info("No alerts and devices to plot.")

st.subheader("Top anomalies (latest minute)")
st.dataframe(alerts[["client_ip","minute","qpm","uniq","avg_len","score"]])

st.subheader("Recent feature windows")
st.dataframe(feats.tail(50))


