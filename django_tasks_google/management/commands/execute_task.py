from django.core.management.base import BaseCommand, CommandError

from django_tasks_google.executor import execute_task
from django_tasks_google.models import TaskExecution


class Command(BaseCommand):
    def add_arguments(self, parser):
        parser.add_argument("execution_id", type=str)

    def handle(self, *args, **options):
        if not execute_task(options["execution_id"]):
            raise CommandError("Task execution failed")
