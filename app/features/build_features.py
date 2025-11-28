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

def build_windows(src_path=SRC, freq="1min"):
    df = pd.read_csv(src_path, parse_dates=["time"])
    if df.empty:
        OUT.write_text(""); return pd.DataFrame()
    df["minute"] = df["time"].dt.floor(freq)
    g = (df.groupby(["client_ip","minute"]).agg(
                qpm=("domain","count"),
                uniq=("domain","nunique"),
                avg_len=("domain", lambda s: s.str.len().mean()),
                len_std=("domain", lambda s: s.str.len().std()),
                top_domain_ratio=("domain", top_domain_ratio_calc),
                shannon_entropy=("domain", shannon_entropy_calc)
                )
           .reset_index().fillna(0))
    g.to_csv(OUT, index=False)
    return g

if __name__ == "__main__":
    print(build_windows().tail())
