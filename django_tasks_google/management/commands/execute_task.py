from django.core.management.base import BaseCommand, CommandError

from django_tasks_google.executor import execute_task


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("execution_id", type=str)

    def handle(self, *args, **options):
        execution_id = options["execution_id"]
        should_retry = execute_task(execution_id)
        if should_retry:
            raise CommandError(
                f"Task execution retry requested for execution_id={execution_id}"
            )
