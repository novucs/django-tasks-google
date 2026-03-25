from django.core.management.base import BaseCommand

from django_tasks_google.scheduler import sync_scheduled_tasks


class Command(BaseCommand):
    def handle(self, *args, **options):
        sync_scheduled_tasks()
