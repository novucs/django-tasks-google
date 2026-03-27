from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("django_tasks_google", "0001_initial"),
    ]

    operations = [
        migrations.RenameField(
            model_name="scheduledtask",
            old_name="backend",
            new_name="backend_alias",
        ),
        migrations.AlterField(
            model_name="scheduledtask",
            name="backend_alias",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AlterField(
            model_name="scheduledtask",
            name="takes_context",
            field=models.BooleanField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="scheduledtask",
            name="priority",
            field=models.IntegerField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="scheduledtask",
            name="queue_name",
            field=models.TextField(blank=True, default=""),
        ),
        migrations.AddField(
            model_name="scheduledtask",
            name="run_after",
            field=models.DateTimeField(blank=True, null=True),
        ),
        migrations.AddField(
            model_name="scheduledtask",
            name="idempotency_key",
            field=models.TextField(null=True),
        ),
        migrations.RenameField(
            model_name="taskexecution",
            old_name="backend",
            new_name="backend_alias",
        ),
        migrations.RemoveField(
            model_name="taskexecution",
            name="cloud_scheduler_idempotency_key",
        ),
        migrations.AddField(
            model_name="taskexecution",
            name="lease_expires_at",
            field=models.DateTimeField(null=True),
        ),
        migrations.AddField(
            model_name="taskexecution",
            name="lease_worker_id",
            field=models.TextField(null=True),
        ),
    ]
