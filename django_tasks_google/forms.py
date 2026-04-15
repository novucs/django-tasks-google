import importlib
import inspect
import re

from django import forms
from django.apps import apps
from django.core.exceptions import ValidationError
from django.tasks import InvalidTaskBackend, task_backends
from django.utils.module_loading import import_string

from django_tasks_google.models import ScheduledTask


def validate_backend(backend: str):
    try:
        return task_backends[backend]
    except InvalidTaskBackend as err:
        raise forms.ValidationError("Invalid backend alias") from err


def _is_task(obj) -> bool:
    # Match the detection used by get_task_choices(): a Task instance created
    # by django.tasks' @task decorator.
    return (
        hasattr(obj, "task") or hasattr(obj, "_is_task") or type(obj).__name__ == "Task"
    )


class ExecuteTaskForm(forms.Form):
    execution_id = forms.CharField()
    backend = forms.CharField()

    def clean_backend(self):
        return validate_backend(self.cleaned_data["backend"])


class ScheduleTaskForm(forms.Form):
    task_id = forms.CharField()
    backend = forms.CharField()

    def clean_backend(self):
        return validate_backend(self.cleaned_data["backend"])


class EnqueueTaskForm(forms.Form):
    task_path = forms.CharField()
    backend = forms.CharField(required=False)
    args = forms.JSONField(required=False)
    kwargs = forms.JSONField(required=False)
    callback_url = forms.URLField(required=False)
    queue_name = forms.CharField(required=False)

    def clean_task_path(self):
        path = self.cleaned_data["task_path"]
        try:
            obj = import_string(path)
        except (ImportError, AttributeError) as err:
            raise forms.ValidationError("Unknown task path") from err
        if not _is_task(obj):
            raise forms.ValidationError(
                "Path does not point to a @task-decorated callable"
            )
        return obj

    def clean_backend(self):
        alias = self.cleaned_data.get("backend") or ""
        if not alias:
            return None
        return validate_backend(alias)

    def clean_args(self):
        data = self.cleaned_data.get("args")
        if data in (None, ""):
            return []
        if not isinstance(data, list):
            raise forms.ValidationError("args must be a JSON list")
        return data

    def clean_kwargs(self):
        data = self.cleaned_data.get("kwargs")
        if data in (None, ""):
            return {}
        if not isinstance(data, dict):
            raise forms.ValidationError("kwargs must be a JSON object")
        return data


class ScheduledTaskAdminForm(forms.ModelForm):
    name = forms.CharField(
        help_text=(
            "Name can only contain alphanumeric characters, hyphens '-' and "
            "underscores '_'"
        )
    )
    task_selector = forms.ChoiceField(
        choices=[],
        required=False,
        label="Select Task (Optional)",
        help_text="Pick a task here OR type a custom path below.",
    )
    schedule = forms.CharField(
        help_text=(
            "Schedules are specified using unix-cron format. "
            'E.g. every minute: "* * * * *", every 3 hours: "0 */3 * * *", '
            'every Monday at 9:00: "0 9 * * 1".'
        )
    )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields["task_selector"].choices = [
            ("", "--- Manual Entry ---")
        ] + get_task_choices()
        self.fields["module_path"].required = False
        self.fields["cloud_scheduler_job_name"].required = False

    class Meta:
        model = ScheduledTask
        fields = "__all__"
        widgets = {
            "name": forms.TextInput(
                attrs={"style": "width: 400px;", "placeholder": "task-name-here"}
            ),
            "module_path": forms.TextInput(attrs={"style": "width: 400px;"}),
            "backend_alias": forms.TextInput(attrs={"style": "width: 200px;"}),
            "schedule": forms.TextInput(attrs={"placeholder": "*/5 * * * *"}),
            "time_zone": forms.TextInput(attrs={"placeholder": "UTC"}),
            "cloud_scheduler_job_name": forms.TextInput(
                attrs={"style": "width: 400px;"}
            ),
            "description": forms.Textarea(attrs={"rows": 3, "cols": 40}),
            "args": forms.Textarea(attrs={"rows": 3, "cols": 40}),
            "kwargs": forms.Textarea(attrs={"rows": 3, "cols": 40}),
        }

    def clean(self):
        cleaned_data = super().clean()
        selector_val = cleaned_data.get("task_selector")
        if selector_val:
            cleaned_data["module_path"] = selector_val
        if not cleaned_data.get("module_path"):
            raise ValidationError("Either task selector or module path must be set")
        return cleaned_data

    def clean_name(self):
        name = self.cleaned_data.get("name")
        if not re.match(r"^[a-zA-Z0-9_-]+$", name):
            raise ValidationError(
                "Name can only contain alphanumeric characters, hyphens '-', "
                "and underscores '_'."
            )
        return name

    def clean_args(self):
        data = self.cleaned_data.get("args")
        if data in [None, ""]:
            return []
        if not isinstance(data, list):
            raise ValidationError(
                "Arguments must be a valid JSON list (e.g., [1, 'test'])."
            )
        return data

    def clean_kwargs(self):
        data = self.cleaned_data.get("kwargs")
        if data in [None, ""]:
            return {}
        if not isinstance(data, dict):
            raise ValidationError(
                "Keyword arguments must be a valid JSON object "
                '(e.g., {"key": "value"}).'
            )
        return data


def get_task_choices():
    choices = []
    for app_config in apps.get_app_configs():
        try:
            module = importlib.import_module(f"{app_config.name}.tasks")
            for name, obj in inspect.getmembers(module):
                is_task_decorated = (
                    hasattr(obj, "task")
                    or hasattr(obj, "_is_task")
                    or type(obj).__name__ == "Task"
                )
                if is_task_decorated:
                    path = f"{app_config.name}.tasks.{name}"
                    choices.append((path, path))
        except ImportError:
            continue
    return [("", "---------")] + sorted(choices)
