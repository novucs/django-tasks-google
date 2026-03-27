import pytest
from django.core.exceptions import ValidationError

from django_tasks_google.forms import (
    ExecuteTaskForm,
    ScheduledTaskAdminForm,
    ScheduleTaskForm,
    validate_backend,
)

pytestmark = pytest.mark.django_db


def _admin_form_data(**overrides):
    data = {
        "name": "task_name",
        "description": "",
        "schedule": "*/5 * * * *",
        "time_zone": "UTC",
        "state": "enabled",
        "module_path": "tests.fake_tasks.sample_task",
        "task_selector": "",
        "backend_alias": "default",
        "queue_name": "default",
        "run_after": "",
        "takes_context": "",
        "priority": "",
        "args": "[]",
        "kwargs": "{}",
        "cloud_scheduler_job_name": "",
        "idempotency_key": "",
    }
    data.update(overrides)
    return data


def test_validate_backend_returns_backend_instance():
    backend = validate_backend("default")
    assert backend.alias == "default"


def test_validate_backend_raises_for_invalid_backend():
    with pytest.raises(ValidationError, match="Invalid backend alias"):
        validate_backend("missing-backend")


def test_execute_task_form_validates_backend():
    form = ExecuteTaskForm(data={"execution_id": "123", "backend": "default"})
    assert form.is_valid()
    assert form.cleaned_data["backend"].alias == "default"


def test_schedule_task_form_validates_backend():
    form = ScheduleTaskForm(
        data={"task_id": "123", "backend": "default", "idempotency_key": "abc"}
    )
    assert form.is_valid()
    assert form.cleaned_data["backend"].alias == "default"


def test_schedule_task_form_rejects_invalid_backend():
    form = ScheduleTaskForm(
        data={"task_id": "123", "backend": "missing", "idempotency_key": "abc"}
    )
    assert not form.is_valid()
    assert "backend" in form.errors


def test_scheduled_task_admin_form_requires_selector_or_module_path():
    form = ScheduledTaskAdminForm(data=_admin_form_data(module_path=""))
    assert not form.is_valid()
    assert "__all__" in form.errors
    assert "Either task selector or module path must be set" in str(
        form.errors["__all__"]
    )


def test_scheduled_task_admin_form_rejects_invalid_name():
    form = ScheduledTaskAdminForm(data=_admin_form_data(name="bad name"))
    assert not form.is_valid()
    assert "name" in form.errors


def test_scheduled_task_admin_form_rejects_non_list_args():
    form = ScheduledTaskAdminForm(data=_admin_form_data(args='{"x": 1}'))
    assert not form.is_valid()
    assert "args" in form.errors


def test_scheduled_task_admin_form_rejects_non_dict_kwargs():
    form = ScheduledTaskAdminForm(data=_admin_form_data(kwargs='["x"]'))
    assert not form.is_valid()
    assert "kwargs" in form.errors
