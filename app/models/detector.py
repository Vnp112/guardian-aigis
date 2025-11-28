import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.decomposition import PCA
from pathlib import Path

FEAT = Path("data/features.csv")
ALERTS = Path("data/alerts.csv")
MIN_HISTORY = 2  # was 5; lower for sample data

def Mahalanobis_dist(grp: pd.DataFrame):
    #print(grp)
    feat_cols = ["qpm", "uniq", "avg_len"]
    X = grp[feat_cols].to_numpy()
    #print(grp)
    #data = X[:-1]
    if len(X) < 2:
        return pd.Series([0.0] * len(grp), index=grp.index)
    #last_row = X[-1]
    mu = X.mean(axis=0)
    cov = np.cov(X, rowvar=False)
    inv_cov = np.linalg.pinv(cov)
    
    distances = []
    for x in X:
        diff = x - mu
        d2 = diff.T @ inv_cov @ diff
        dist = float(np.sqrt(max(d2, 0.0)))
        distances.append(dist)
    return pd.Series(distances, index=grp.index)

def pca_features(history_df: pd.DataFrame, feat_cols: list[str]):
    pca = PCA(n_components=2, random_state=0)
    X = history_df[feat_cols].to_numpy()
    pcs = pca.fit_transform(X)
    history_df["pc1"] = pcs[:, 0]
    history_df["pc2"] = pcs[:, 1]

def detect(features_path=FEAT):
    if not Path(features_path).exists():
        print("No features file found. Run the feature builder first.")
        return pd.DataFrame()

    df = pd.read_csv(features_path, parse_dates=["minute"])
    if df.empty:
        print("Features file is empty.")
        return pd.DataFrame(columns=["client_ip","minute","qpm","uniq","avg_len","score", "Mahalanobis"])
    
    history_parts = []
    
    for dev, grp in df.groupby("client_ip"):
        if len(grp) < MIN_HISTORY:
            continue

        X = grp[["qpm","uniq","avg_len"]]
        model = IsolationForest(contamination="auto", random_state=0).fit(X)
        grp = grp.copy()
        grp["score"] = -model.score_samples(X)  # higher = more anomalous
        grp["Mahalanobis"] = Mahalanobis_dist(grp)
        history_parts.append(grp)
       
    if not history_parts:
        print(f"No devices had at least {MIN_HISTORY} rows of history.")
        # write empty files so API/UI don't break
        empty_cols = ["client_ip","minute","qpm","uniq","avg_len","score","Mahalanobis",
                    "norm_score","norm_Mahalanobis","combined_score"]
        empty_history = pd.DataFrame(columns=empty_cols)
        empty_history.to_csv("data/anomaly_history.csv", index=False)
        empty_alerts = empty_history.copy()
        empty_alerts.to_csv(ALERTS, index=False)
        return empty_alerts

    history_df = pd.concat(history_parts, ignore_index=True)
    
    # Mahalanobis Distance
    eps = 1e-9

    s_min, s_max = history_df["score"].min(), history_df["score"].max()
    history_df["norm_score"] = (history_df["score"] - s_min) / (s_max - s_min + eps)

    m_min, m_max = history_df["Mahalanobis"].min(), history_df["Mahalanobis"].max()
    history_df["norm_Mahalanobis"] = (history_df["Mahalanobis"] - m_min) / (m_max - m_min + eps)
    
    # PCA
    feat_cols = ["qpm", "uniq", "avg_len"]
    pca_features(history_df, feat_cols)
    
    # Combined score calculation (Isolation forest, Mahalanobis Distance)
    history_df["combined_score"] = 0.5 * history_df["norm_score"] + 0.5 * history_df["norm_Mahalanobis"]

    alerts_df = (history_df.sort_values("minute").groupby("client_ip").tail(1).sort_values("combined_score", ascending=False))
    # write alerts (even if empty) so the dashboard doesn't break
    history_df.to_csv("data/anomaly_history.csv", index=False)
    alerts_df.to_csv(ALERTS, index=False)
    return alerts_df


if __name__ == "__main__":
    df = pd.read_csv(FEAT, parse_dates=["minute"])
    history_parts = []
    for dev, grp in df.groupby("client_ip"):
        X = grp[["qpm","uniq","avg_len"]]
        model = IsolationForest(contamination="auto", random_state=0).fit(X)
        grp = grp.copy()
        grp["score"] = -model.score_samples(X)
        grp["Mahalanobis"] = Mahalanobis_dist(grp)
        history_parts.append(grp)
    history_df = pd.concat(history_parts, ignore_index=True)

    feat_cols = ["qpm", "uniq", "avg_len"]
    pca_features(history_df, feat_cols)
    print(history_df[["client_ip","minute","pc1","pc2"]].head())
