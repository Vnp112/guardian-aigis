import pandas as pd
import numpy as np
from pathlib import Path
from app.ingest.parse_querylog import parse_querylog

SRC = Path("data/querylog.json")
OUT = Path("data/features.csv")

def top_domain_ratio_calc(domains: pd.Series):
    counts = domains.value_counts()
    if counts.sum() == 0:
        return 0
    else:
        return counts.max() / counts.sum()
    
def shannon_entropy_calc(domains: pd.Series):
    counts = domains.value_counts()
    if counts.sum() == 0:
        return 0
    else:
        probs = counts / counts.sum()
        entropy = -(probs * np.log2(probs)).sum()
        return entropy

# def KL_divergence_calc(df):
#     device_baseline = {}
#     for device in df["client_ip"].unique():
#         df_device = df[df.client_ip == device]
#         counts = df_device["domain"].value_counts()
#         total = counts.sum()
#         P_base = (counts / total).astype(float)
#         device_baseline[device] = P_base.to_dict()
#     return device_baseline

def compute_baseline_probs(df: pd.DataFrame):
    """
    Computes P(domain | device) using all historical data.
    Returns a Series indexed by (client_ip, domain).
    """
    base = (
        df.groupby(["client_ip", "domain"])
          .size()
          .groupby(level=0)
          .apply(lambda s: s / s.sum())
    )
    return base


def compute_window_probs(df: pd.DataFrame):
    """
    Computes P(domain | device, minute) for each time window.
    Returns a Series indexed by (client_ip, minute, domain).
    """
    counts = (
        df.groupby(["client_ip", "minute", "domain"])
          .size()
          .rename("count")
    )
    probs = counts.groupby(level=[0, 1]).apply(lambda s: s / s.sum())
    return probs


def compute_KL_vectorized(window_probs, baseline_probs, epsilon=1e-7):
    """
    Fully vectorized KL divergence:
    KL(P_t || P_base) = sum P_t * log(P_t / P_base)
    """
    # Align baselines so they match the window index structure
    baseline_aligned = baseline_probs.reindex(window_probs.index, fill_value=0)

    p_t = window_probs + epsilon
    p_b = baseline_aligned + epsilon

    KL_vals = (p_t * np.log(p_t / p_b))

    # Collapse domain index → per (device, minute)
    KL_per_window = KL_vals.groupby(level=[0, 1]).sum()

    return KL_per_window
    
    
def build_windows(src_path=SRC, freq="1min"):
    df = parse_querylog(SRC)
    if df.empty:
        OUT.write_text("") 
        return pd.DataFrame()
    
    df["minute"] = df["time"].dt.floor(freq)
    df["first_seen"] = (df.groupby(["client_ip", "domain"])["time"].transform("min"))
    df["is_new_domain"] = df["time"] == df["first_seen"]
    
    baseline_probs = compute_baseline_probs(df)
    window_probs = compute_window_probs(df)
    KL_series = compute_KL_vectorized(window_probs, baseline_probs)
    
    g = (df.groupby(["client_ip","minute"]).agg(
                qpm=("domain","count"),
                uniq=("domain","nunique"),
                avg_len=("domain", lambda s: s.str.len().mean()),
                len_std=("domain", lambda s: s.str.len().std()),
                top_domain_ratio=("domain", top_domain_ratio_calc),
                shannon_entropy=("domain", shannon_entropy_calc),
                new_domain_ratio=("is_new_domain", "mean")
                )
           .reset_index().fillna(0)
        )
    g = g.merge(
        KL_series.rename("KL_divergence"),
        on=["client_ip", "minute"],
        how="left"
    )
    g["KL_divergence"] = g["KL_divergence"].fillna(0)
    g.to_csv(OUT, index=False)
    return g

if __name__ == "__main__":
    print(build_windows().tail())
