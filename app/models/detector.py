import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from pathlib import Path

FEAT = Path("data/features.csv")
ALERTS = Path("data/alerts.csv")
MIN_HISTORY = 2  # was 5; lower for sample data

def Mahalanobis_dist(grp: pd.DataFrame):
    #print(grp)
    grp = grp.drop(['client_ip', 'minute'], axis=1).to_numpy()
    #print(grp)
    data = grp[:-1]
    last_row = grp[-1]
    mu = data.mean(axis=0)
    cov = np.cov(data, rowvar=False)
    inv_cov = np.linalg.pinv(cov)
    diff = last_row - mu
    d2 = diff.T @ inv_cov @ diff
    dist = float(np.sqrt(max(d2, 0.0)))
    return dist

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
        mahalanobis_distance = Mahalanobis_dist(grp)
        grp.loc[grp.index[-1], "Mahalanobis"] = mahalanobis_distance
        rows.append(grp.iloc[-1][["client_ip","minute","qpm","uniq","avg_len","score", "Mahalanobis"]])

    if not rows:
        print(f"No devices had at least {MIN_HISTORY} rows of history.")
        out = pd.DataFrame(columns=["client_ip","minute","qpm","uniq","avg_len","score"])
    else:
        out = pd.DataFrame(rows).sort_values("score", ascending=False)

    # write alerts (even if empty) so the dashboard doesn't break
    out.to_csv(ALERTS, index=False)
    return out

if __name__ == "__main__":
    grp = pd.read_csv(FEAT)
    Mahalanobis_dist(grp)
