import logging

from django.contrib import admin, messages
from django.utils.html import format_html_join

from django_tasks_google.forms import ScheduledTaskAdminForm
from django_tasks_google.models import ScheduledTask, TaskExecution
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


@admin.register(TaskExecution)
class TaskExecutionAdmin(admin.ModelAdmin):
    list_display = (
        "id",
        "status",
        "module_path",
        "backend_alias",
        "queue_name",
        "enqueued_at",
        "started_at",
        "finished_at",
    )
    list_filter = ("status", "backend_alias", "queue_name")
    search_fields = ("module_path", "cloud_task_name", "cloud_run_job_execution_name")
    date_hierarchy = "enqueued_at"
    ordering = ("-enqueued_at",)
    actions = ["cancel_executions"]
    fieldsets = (
        (
            "Task",
            {
                "fields": (
                    "module_path",
                    "priority",
                    "backend_alias",
                    "queue_name",
                    "takes_context",
                    "args",
                    "kwargs",
                )
            },
        ),
        (
            "State",
            {
                "fields": (
                    "status",
                    "enqueued_at",
                    "started_at",
                    "finished_at",
                    "last_attempted_at",
                    "cancelled_at",
                    "force_cancel",
                    "max_attempts",
                )
            },
        ),
        ("Result", {"fields": ("return_value", "formatted_errors")}),
        (
            "Workers",
            {"fields": ("worker_ids", "lease_worker_id", "lease_expires_at")},
        ),
        (
            "Google Cloud",
            {"fields": ("cloud_task_name", "cloud_run_job_execution_name")},
        ),
    )

    def has_add_permission(self, request):
        return False

    def get_readonly_fields(self, request, obj=None):
        return tuple(f.name for f in self.model._meta.fields) + ("formatted_errors",)

    @admin.display(description="Errors")
    def formatted_errors(self, obj):
        if not obj.errors:
            return "No errors"
        return format_html_join(
            "",
            "<p><strong>{}</strong><pre>{}</pre></p>",
            (
                (error.get("exception_class_path", ""), error.get("traceback", ""))
                for error in obj.errors
            ),
        )

    @admin.action(description="Cancel selected executions")
    def cancel_executions(self, request, queryset):
        cancelled = skipped = 0
        failed = []
        for execution in queryset:
            if execution.is_finished:
                skipped += 1
                continue
            try:
                execution.cancel()
                cancelled += 1
            except Exception:
                logger.exception(
                    "Failed cancelling task execution id=%s path=%s",
                    execution.pk,
                    execution.module_path,
                )
                failed.append(execution.pk)

        if cancelled:
            self.message_user(
                request, f"Cancelled {cancelled} execution(s)", messages.SUCCESS
            )
        if skipped:
            self.message_user(
                request,
                f"Skipped {skipped} already-finished execution(s)",
                messages.WARNING,
            )
        if failed:
            ids = ", ".join(str(pk) for pk in failed)
            self.message_user(
                request, f"Failed to cancel execution(s): {ids}", messages.ERROR
            )
