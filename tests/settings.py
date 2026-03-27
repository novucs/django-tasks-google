SECRET_KEY = "tests-only-secret-key"
DEBUG = True
USE_TZ = True
TIME_ZONE = "UTC"

DATABASES = {
    "default": {
        "ENGINE": "django.db.backends.sqlite3",
        "NAME": ":memory:",
    }
}

INSTALLED_APPS = [
    "django.contrib.admin",
    "django.contrib.auth",
    "django.contrib.contenttypes",
    "django_tasks_google",
]

MIDDLEWARE = []
ROOT_URLCONF = "django_tasks_google.urls"

TASKS = {
    "default": {
        "BACKEND": "django_tasks_google.backends.CloudTasksBackend",
        "QUEUES": ["default"],
        "OPTIONS": {
            "project_id": "test-project",
            "location": "us-central1",
            "base_url": "https://example.com/tasks/",
            "oidc_service_account": "worker@example.iam.gserviceaccount.com",
            "heartbeat_enabled": True,
            "heartbeat_interval_seconds": 1,
            "heartbeat_timeout_seconds": 3,
            "heartbeat_join_timeout_seconds": 1,
        },
    }
}

# The existing migration file is stale relative to the models; disable migrations for
# tests so Django builds tables directly from model definitions.
MIGRATION_MODULES = {"django_tasks_google": None}
