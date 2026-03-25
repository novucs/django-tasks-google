from django.core.management.base import BaseCommand, CommandError

from django_tasks_google.executor import execute_task
from django_tasks_google.models import TaskExecution


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("task_execution_id", type=str)

    def handle(self, *args, **options):
        execution = TaskExecution.objects.get(pk=options["task_execution_id"])
        if not execute_task(execution):
            raise CommandError("Task execution failed")
