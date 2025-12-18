# input: features.csv, using client_ip, minute, FEAT_COLS columns
import pandas as pd
import numpy as np

PATH = ("data/features.csv")

client_ip = ["client_ip"]
minute = ["minute"]

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

