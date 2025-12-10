from .celery_app import celery_app
from .ingest.retrieve_logs import pull_logs
from .ingest.parse_querylog import write_csv
from .features.build_features import build_windows
from .models.detector import detect
from pathlib import Path

@celery_app.task(bind=True)
def run_refresh(self):
    pull_logs()
    csv_path = write_csv(
        in_path=Path("data/querylog.json"),
        out_path=Path("data/sample_dns.csv")
    )
    build_windows(src_path=csv_path)
    detect()
    return "done"
