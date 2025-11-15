import streamlit as st, pandas as pd
from pathlib import Path
from app.features.build_features import build_windows
from app.models.detector import detect

alerts_p = Path("data/alerts.csv")
feats_p  = Path("data/features.csv")

if alerts_p.exists():
    alerts = pd.read_csv(alerts_p, parse_dates=["minute"])
    st.dataframe(alerts[["client_ip","minute","qpm","uniq","avg_len","score"]])
    num_devices = alerts["client_ip"].nunique() if not alerts.empty else 0
    highest_anomaly_score = alerts["score"].max() if not alerts.empty else 0.0
    last_time = str(alerts["minute"].max()) if not alerts.empty else None
else:
    st.info("No alerts yet. Click Refresh above.")


st.set_page_config(page_title="Guardian AIGIS — Phase 1", layout="wide")
st.title("Guardian AIGIS  ·  Phase 1 MVP")

if st.button("Refresh (Build → Detect)"):
    build_windows()
    detect()

st.subheader("Top anomalies (latest minute)")

st.subheader("Recent feature windows")
if feats_p.exists():
    feats = pd.read_csv(feats_p, parse_dates=["minute"])
    st.dataframe(feats.tail(50))

dev, max_time, last_timestamp = st.columns(3)
dev.metric(label="Devices", value=num_devices)
max_time.metric(label="Highest Anomaly Score", value=highest_anomaly_score)
last_timestamp.metric(label="Last Time", value=last_time)
