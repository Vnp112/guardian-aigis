import pandas as pd
import numpy as np
from pathlib import Path

SRC = Path("data/sample_dns.csv")
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

def KL_divergence_calc(df):
    device_baseline = {}
    for device in df["client_ip"].unique():
        df_device = df[df.client_ip == device]
        counts = df_device["domain"].value_counts()
        total = counts.sum()
        P_base = (counts / total).astype(float)
        device_baseline[device] = P_base.to_dict()
    return device_baseline
    
    
def build_windows(src_path=SRC, freq="1min"):
    df = pd.read_csv(src_path, parse_dates=["time"])
    if df.empty:
        OUT.write_text(""); return pd.DataFrame()
    df["minute"] = df["time"].dt.floor(freq)
    df["first_seen"] = (df.groupby(["client_ip", "domain"])["time"].transform("min"))
    df["is_new_domain"] = df["time"] == df["first_seen"]
    device_baseline = KL_divergence_calc(df)
    g = (df.groupby(["client_ip","minute"]).agg(
                qpm=("domain","count"),
                uniq=("domain","nunique"),
                avg_len=("domain", lambda s: s.str.len().mean()),
                len_std=("domain", lambda s: s.str.len().std()),
                top_domain_ratio=("domain", top_domain_ratio_calc),
                shannon_entropy=("domain", shannon_entropy_calc),
                new_domain_ratio=("is_new_domain", "mean")
                )
           .reset_index().fillna(0))
    KL_vals = []
    epsilon = 1e-7
    for idx, row in g.iterrows():
        device = row["client_ip"]
        min = row["minute"]
        window_df = df[(df.client_ip == device) & (df.minute == min)]
        counts = window_df["domain"].value_counts()
        total = counts.sum()

        P_t = (counts/total).astype(float)
        P_base = device_baseline[device]
        domains = set(P_t.keys()).union(set(P_base.keys()))
        KL = 0
        
        for domain in domains:
            p_t = P_t.get(domain, 0) + epsilon
            p_base = P_base.get(domain, 0) + epsilon
            KL += p_t * np.log(p_t / p_base)
 
            
        KL_vals.append(KL)
    g["KL_divergence"] = KL_vals
    g.to_csv(OUT, index=False)
    return g

if __name__ == "__main__":
    print(build_windows().tail())
