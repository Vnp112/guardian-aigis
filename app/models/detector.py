import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.decomposition import PCA
from pathlib import Path

FEAT = Path("data/features.csv")
ALERTS = Path("data/alerts.csv")
MIN_HISTORY = 2

FEAT_COLS = [
    "qpm",
    "uniq",
    "avg_len",
    "len_std",
    "top_domain_ratio",
    "shannon_entropy",
    "new_domain_ratio",
    "KL_divergence",
]

def Mahalanobis_dist(grp: pd.DataFrame, feat_cols: list[str]):
    X = grp[feat_cols].to_numpy()
    if len(X) < 2:
        return pd.Series(0.0, index=grp.index)

    mu = X.mean(axis=0)
    cov = np.cov(X, rowvar=False)
    inv_cov = np.linalg.pinv(cov)

    dists = []
    for x in X:
        diff = x - mu
        d2 = diff.T @ inv_cov @ diff
        dists.append(float(np.sqrt(max(d2, 0.0))))

    return pd.Series(dists, index=grp.index)


def add_pca(history_df: pd.DataFrame, feat_cols: list[str]):
    pca = PCA(n_components=2, random_state=0)
    X = history_df[feat_cols].to_numpy()
    pcs = pca.fit_transform(X)
    history_df["pc1"] = pcs[:, 0]
    history_df["pc2"] = pcs[:, 1]


def detect(features_path=FEAT):
    if not features_path.exists():
        return pd.DataFrame()

    df = pd.read_csv(features_path, parse_dates=["minute"])
    if df.empty:
        return pd.DataFrame()

    history_parts = []

    for device, grp in df.groupby("client_ip"):
        if len(grp) < MIN_HISTORY:
            continue

        grp = grp.sort_values("minute").copy()
        X = grp[FEAT_COLS].astype(float).to_numpy()

        model = IsolationForest(
            contamination="auto",
            random_state=0
        ).fit(X)

        grp["score"] = -model.score_samples(X)
        grp["Mahalanobis"] = Mahalanobis_dist(grp, FEAT_COLS)

        history_parts.append(grp)

    if not history_parts:
        return pd.DataFrame()

    history_df = pd.concat(history_parts, ignore_index=True)


    eps = 1e-9

    s_min, s_max = history_df["score"].min(), history_df["score"].max()
    m_min, m_max = history_df["Mahalanobis"].min(), history_df["Mahalanobis"].max()

    history_df["norm_score"] = (history_df["score"] - s_min) / (s_max - s_min + eps)
    history_df["norm_Mahalanobis"] = (history_df["Mahalanobis"] - m_min) / (m_max - m_min + eps)

    history_df["combined_score"] = (
        0.5 * history_df["norm_score"]
        + 0.5 * history_df["norm_Mahalanobis"]
    )

    add_pca(history_df, FEAT_COLS)

    alerts_df = (
        history_df
        .sort_values("minute")
        .groupby("client_ip")
        .tail(1)
        .sort_values("combined_score", ascending=False)
    )

    history_df.to_csv("data/anomaly_history.csv", index=False)
    alerts_df.to_csv(ALERTS, index=False)

    return alerts_df
