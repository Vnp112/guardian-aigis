from celery import Celery

celery_app = Celery(
    "guardian_aigis",
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/1",
    include=["app.tasks"],  
)

celery_app.conf.update(
    task_track_started=True,
    task_time_limit=600,
    result_expires=3600,
)
