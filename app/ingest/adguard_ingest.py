import requests
import pandas as pd
from pathlib import Path

ADGUARD_URL = "http://192.168.8.1/control/querylog?limit=1000"
OUT = Path("data/sample_dns.csv")

resp = requests.get(ADGUARD_URL)
print(resp.status_code)
print(resp.text[:200])
#data = resp.json()


def adguard_fetch(out_path: Path = OUT):
    """
    Fetch DNS logs from AdGuard Home on the Flint2 and write
    a normalized CSV with columns: time, client_ip, domain, qtype.
    """
    # 1. GET request to ADGUARD_URL (with timeout)
    # 2. resp.json() -> dict; pull entries = dict["data"]
    # 3. Build a list of dicts with keys: time, client_ip, domain, qtype
    # 4. Create DataFrame from that list
    # 5. Save to CSV at out_path
    # 6. Return the DataFrame so you can inspect it if needed
    resp = requests.get(ADGUARD_URL)
    data = resp.json()
    print(resp.status.code)
    print(resp.text[:200])
