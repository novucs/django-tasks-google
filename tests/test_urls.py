from django.urls import resolve, reverse

from django_tasks_google.views import execute_task_view, schedule_task_view


def test_execute_url_resolves_to_execute_task_view():
    match = resolve("/execute/")
    assert match.func == execute_task_view
    assert reverse("execute_task") == "/execute/"


def test_schedule_url_resolves_to_schedule_task_view():
    match = resolve("/schedule/")
    assert match.func == schedule_task_view
    assert reverse("schedule_task") == "/schedule/"
