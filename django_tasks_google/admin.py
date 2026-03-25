import importlib
import inspect
import re

from django import forms
from django.apps import apps
from django.contrib import admin, messages
from django.core.exceptions import ValidationError

from django_tasks_google.models import ScheduledTask
from django_tasks_google.scheduler import delete_remote_scheduled_task


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


class ScheduledTaskAdminForm(forms.ModelForm):
    name = forms.CharField(
        help_text="Name can only contain alphanumeric characters, hyphens '-' and underscores '_'"
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
            'E.g. every minute: "* * * * *", every 3 hours: "0 */3 * * *", every Monday at 9:00: "0 9 * * 1".'
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
            "backend": forms.TextInput(attrs={"style": "width: 200px;"}),
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
        if not cleaned_data["module_path"]:
            raise ValidationError("Either task selector or module path must be set")
        return cleaned_data

    def clean_name(self):
        name = self.cleaned_data.get("name")
        if not re.match(r"^[a-zA-Z0-9_-]+$", name):
            raise ValidationError(
                "Name can only contain alphanumeric characters, hyphens '-', and underscores '_'."
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
                'Keyword arguments must be a valid JSON object (e.g., {"key": "value"}).'
            )
        return data


@admin.register(ScheduledTask)
class ScheduledTaskAdmin(admin.ModelAdmin):
    form = ScheduledTaskAdminForm

    list_display = ("name", "state", "schedule", "time_zone", "backend")
    list_filter = ("state", "backend", "time_zone")
    search_fields = ("name", "module_path", "description")
    fieldsets = (
        ("General Info", {"fields": ("name", "description", "state")}),
        (
            "Execution Details",
            {"fields": ("task_selector", "module_path", "backend", "takes_context")},
        ),
        (
            "Parameters",
            {
                "fields": ("args", "kwargs"),
                "description": "JSON formatted arguments for the task.",
            },
        ),
        (
            "Scheduling",
            {"fields": ("schedule", "time_zone", "cloud_scheduler_job_name")},
        ),
    )
    actions = ["sync_tasks"]

    @admin.action(description="Sync selected tasks with Cloud Scheduler")
    def sync_tasks(self, request, queryset):
        for task in queryset:
            try:
                task.sync()
                self.message_user(
                    request, f"Successfully synced '{task.name}'", messages.SUCCESS
                )
            except Exception as e:
                self.message_user(
                    request, f"Failed to sync '{task.name}': {str(e)}", messages.ERROR
                )

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)
        try:
            obj.sync()
        except Exception as e:
            self.message_user(
                request, f"Model saved but sync failed: {e}", messages.WARNING
            )

    def delete_model(self, request, obj):
        self._cleanup_cloud_scheduler(request, obj)
        super().delete_model(request, obj)

    def delete_queryset(self, request, queryset):
        for obj in queryset:
            self._cleanup_cloud_scheduler(request, obj)
        super().delete_queryset(request, queryset)

    def _cleanup_cloud_scheduler(self, request, task):
        try:
            delete_remote_scheduled_task(task)
        except Exception as e:
            self.message_user(
                request,
                f"Cloud Scheduler deletion failed for {task.name}: {e}",
                messages.WARNING,
            )
