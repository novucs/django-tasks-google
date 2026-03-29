import os

from django.core.management.base import BaseCommand, CommandError

from django_tasks_google.executor import execute_task


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("execution_id", type=str)

    def handle(self, *args, **options):
        execution_id = options["execution_id"]

        try:
            # CLOUD_RUN_TASK_ATTEMPT is 0-indexed.
            attempt = int(os.environ.get("CLOUD_RUN_TASK_ATTEMPT")) + 1
        except (TypeError, ValueError) as e:
            raise CommandError("CLOUD_RUN_TASK_ATTEMPT must be an integer") from e

        should_retry = execute_task(execution_id, attempt)
        if should_retry:
            raise CommandError(f"Task id={execution_id} retry requested")
