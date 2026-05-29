from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("django_tasks_google", "0003_taskexecution_max_attempts"),
    ]

    operations = [
        migrations.AddField(
            model_name="taskexecution",
            name="force_cancel",
            field=models.BooleanField(default=False),
        ),
    ]
