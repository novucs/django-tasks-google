import logging
from datetime import timedelta

from django.core.management.base import BaseCommand
from django.tasks.base import TaskResultStatus
from django.utils import timezone

from django_tasks_google.executor import _deliver_callback_if_needed
from django_tasks_google.models import TaskExecution

logger = logging.getLogger("django_tasks_google")


class Command(BaseCommand):
    help = "Retry delivery of failed workflow callbacks"

    def add_arguments(self, parser):
        parser.add_argument(
            "--max-age-hours",
            type=int,
            default=24,
            help="Only retry callbacks for executions finished within this many hours (default: 24)",
        )
        parser.add_argument(
            "--limit",
            type=int,
            default=100,
            help="Maximum number of callbacks to retry per invocation (default: 100)",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="List executions that would be retried without actually retrying",
        )

    def handle(self, *args, **options):
        cutoff = timezone.now() - timedelta(hours=options["max_age_hours"])

        execution_ids = list(
            TaskExecution.objects.filter(
                callback_url__isnull=False,
                callback_delivered_at__isnull=True,
                status__in=[TaskResultStatus.SUCCESSFUL, TaskResultStatus.FAILED],
                finished_at__isnull=False,
                finished_at__gte=cutoff,
            )
            .order_by("finished_at")
            .values_list("pk", flat=True)[: options["limit"]]
        )

        if not execution_ids:
            self.stdout.write("No pending callbacks found")
            return

        retried = 0
        delivered = 0
        for execution_id in execution_ids:
            if options["dry_run"]:
                self.stdout.write(f"Would retry execution_id={execution_id}")
                retried += 1
                continue

            _deliver_callback_if_needed(execution_id)
            retried += 1

            execution = TaskExecution.objects.filter(pk=execution_id).first()
            if execution and execution.callback_delivered_at:
                delivered += 1

        if options["dry_run"]:
            self.stdout.write(f"Dry run: {retried} callbacks would be retried")
        else:
            self.stdout.write(
                f"Retried {retried} callbacks, {delivered} delivered successfully"
            )
