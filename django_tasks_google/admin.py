import logging

from django.contrib import admin, messages

from django_tasks_google.forms import ScheduledTaskAdminForm
from django_tasks_google.models import ScheduledTask
from django_tasks_google.scheduler import delete_cloud_scheduler_job_if_exists

logger = logging.getLogger("django_tasks_google")


@admin.register(ScheduledTask)
class ScheduledTaskAdmin(admin.ModelAdmin):
    form = ScheduledTaskAdminForm

    list_display = ("name", "state", "schedule", "time_zone", "backend_alias")
    list_filter = ("state", "backend_alias", "time_zone")
    search_fields = ("name", "module_path", "description")
    fieldsets = (
        ("General Info", {"fields": ("name", "description", "state")}),
        (
            "Execution Details",
            {
                "fields": (
                    "task_selector",
                    "module_path",
                    "backend_alias",
                    "takes_context",
                )
            },
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
            except Exception as err:
                logger.exception(
                    "Failed syncing scheduled task name=%s id=%s",
                    task.name,
                    task.pk,
                )
                self.message_user(
                    request, f"Failed to sync '{task.name}': {err}", messages.ERROR
                )

    def save_model(self, request, obj, form, change):
        super().save_model(request, obj, form, change)
        try:
            obj.sync()
        except Exception as err:
            logger.exception(
                "Sync failed after saving scheduled task name=%s id=%s",
                obj.name,
                obj.pk,
            )
            self.message_user(
                request, f"Model saved but sync failed: {err}", messages.WARNING
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
            delete_cloud_scheduler_job_if_exists(task.cloud_scheduler_job_name)
        except Exception as err:
            logger.exception(
                "Cloud Scheduler cleanup failed name=%s id=%s job_name=%s",
                task.name,
                task.pk,
                task.cloud_scheduler_job_name,
            )
            self.message_user(
                request,
                f"Cloud Scheduler deletion failed for {task.name}: {err}",
                messages.WARNING,
            )
