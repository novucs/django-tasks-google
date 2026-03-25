from django.urls import path

from django_tasks_google.views import execute_task_view

app_name = "django_tasks_google"

urlpatterns = [
    path("execute/", execute_task_view, name="execute_task"),
]
