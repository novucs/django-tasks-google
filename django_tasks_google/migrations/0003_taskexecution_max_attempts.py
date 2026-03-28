from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("django_tasks_google", "0002_update_models_to_current_schema"),
    ]

    operations = [
        migrations.AddField(
            model_name="taskexecution",
            name="max_attempts",
            field=models.IntegerField(null=True),
        ),
    ]
