import streamlit as st, pandas as pd
from pathlib import Path
from app.features.build_features import build_windows
from app.models.detector import detect

st.set_page_config(page_title="Guardian AIGIS — Phase 1", layout="wide")
st.title("Guardian AIGIS  ·  Phase 1 MVP")

if st.button("Refresh (Build → Detect)"):
    build_windows()
    detect()

alerts_p = Path("data/alerts.csv")
feats_p  = Path("data/features.csv")

st.subheader("Top anomalies (latest minute)")
if alerts_p.exists():
    alerts = pd.read_csv(alerts_p, parse_dates=["minute"])
    st.dataframe(alerts[["client_ip","minute","qpm","uniq","avg_len","score"]])
else:
    st.info("No alerts yet. Click Refresh above.")

st.subheader("Recent feature windows")
if feats_p.exists():
    feats = pd.read_csv(feats_p, parse_dates=["minute"])
    st.dataframe(feats.tail(50))
