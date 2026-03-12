# Generated manually for Celery integration

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('reconciliation', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='reconciliationjob',
            name='task_id',
            field=models.CharField(blank=True, max_length=255, null=True),
        ),
    ]