import pandas as pd
from pathlib import Path

SRC = Path("data/sample_dns.csv")
OUT = Path("data/features.csv")

def build_windows(src_path=SRC, freq="1min"):
    df = pd.read_csv(src_path, parse_dates=["time"])
    if df.empty:
        OUT.write_text(""); return pd.DataFrame()
    df["minute"] = df["time"].dt.floor(freq)
    g = (df.groupby(["client_ip","minute"])
           .agg(qpm=("domain","count"),
                uniq=("domain","nunique"),
                avg_len=("domain", lambda s: s.str.len().mean()))
           .reset_index().fillna(0))
    g.to_csv(OUT, index=False)
    return g

if __name__ == "__main__":
    print(build_windows().tail())
