import pandas as pd
from sklearn.ensemble import IsolationForest
from pathlib import Path

FEAT = Path("data/features.csv")
ALERTS = Path("data/alerts.csv")
MIN_HISTORY = 2  # was 5; lower for sample data

def detect(features_path=FEAT):
    if not Path(features_path).exists():
        print("No features file found. Run the feature builder first.")
        return pd.DataFrame()

    df = pd.read_csv(features_path, parse_dates=["minute"])
    if df.empty:
        print("Features file is empty.")
        return pd.DataFrame(columns=["client_ip","minute","qpm","uniq","avg_len","score"])

    rows = []
    for dev, grp in df.groupby("client_ip"):
        if len(grp) < MIN_HISTORY:
            continue

        X = grp[["qpm","uniq","avg_len"]]
        model = IsolationForest(contamination="auto", random_state=0).fit(X)
        grp = grp.copy()
        grp["score"] = -model.score_samples(X)  # higher = more anomalous
        rows.append(grp.iloc[-1][["client_ip","minute","qpm","uniq","avg_len","score"]])

    if not rows:
        print(f"No devices had at least {MIN_HISTORY} rows of history.")
        out = pd.DataFrame(columns=["client_ip","minute","qpm","uniq","avg_len","score"])
    else:
        out = pd.DataFrame(rows).sort_values("score", ascending=False)

    # write alerts (even if empty) so the dashboard doesn't break
    out.to_csv(ALERTS, index=False)
    return out

if __name__ == "__main__":
    print(detect())
