from django.db import migrations, models


class Migration(migrations.Migration):
    initial = True

    dependencies = []

    operations = [
        migrations.CreateModel(
            name="ScheduledTask",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("name", models.TextField(unique=True)),
                ("description", models.TextField(blank=True, default="")),
                ("module_path", models.TextField()),
                ("backend", models.TextField(default="scheduler")),
                ("takes_context", models.BooleanField(default=False)),
                ("args", models.JSONField(blank=True, default=list)),
                ("kwargs", models.JSONField(blank=True, default=dict)),
                ("schedule", models.TextField()),
                ("time_zone", models.TextField(default="UTC")),
                ("cloud_scheduler_job_name", models.TextField(null=True, unique=True)),
                (
                    "state",
                    models.TextField(
                        choices=[("enabled", "Enabled"), ("disabled", "Disabled")],
                        default="enabled",
                    ),
                ),
            ],
        ),
        migrations.CreateModel(
            name="TaskExecution",
            fields=[
                (
                    "id",
                    models.BigAutoField(
                        auto_created=True,
                        primary_key=True,
                        serialize=False,
                        verbose_name="ID",
                    ),
                ),
                ("priority", models.IntegerField(default=0)),
                ("module_path", models.TextField()),
                ("backend", models.TextField(default="default")),
                ("queue_name", models.TextField(default="default")),
                ("run_after", models.DateTimeField(null=True)),
                ("takes_context", models.BooleanField(default=False)),
                ("args", models.JSONField(default=list)),
                ("kwargs", models.JSONField(default=dict)),
                (
                    "status",
                    models.TextField(
                        choices=[
                            ("READY", "Ready"),
                            ("RUNNING", "Running"),
                            ("FAILED", "Failed"),
                            ("SUCCESSFUL", "Successful"),
                        ],
                        default="READY",
                    ),
                ),
                ("enqueued_at", models.DateTimeField(auto_now_add=True)),
                ("started_at", models.DateTimeField(null=True)),
                ("finished_at", models.DateTimeField(null=True)),
                ("last_attempted_at", models.DateTimeField(null=True)),
                ("errors", models.JSONField(default=list)),
                ("worker_ids", models.JSONField(default=list)),
                ("return_value", models.JSONField(null=True)),
                ("cancelled_at", models.DateTimeField(null=True)),
                (
                    "cloud_run_job_execution_name",
                    models.TextField(null=True, unique=True),
                ),
                ("cloud_task_name", models.TextField(null=True, unique=True)),
                (
                    "cloud_scheduler_idempotency_key",
                    models.TextField(null=True, unique=True),
                ),
            ],
        ),
    ]
