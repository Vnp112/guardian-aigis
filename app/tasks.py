from app.celery_app import celery_app
from app.ingest.retrieve_logs import pull_logs
from app.features.build_features import build_windows
from app.models.detector import detect

@celery_app.task(bind=True)
def run_refresh(self):
    pull_logs()
    build_windows()
    detect()
    return "done"
