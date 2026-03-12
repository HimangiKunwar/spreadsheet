# Generated migration for adding dataset field to SavedRule

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('datasets', '0001_initial'),
        ('bulk_operations', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='savedrule',
            name='dataset',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, to='datasets.dataset'),
        ),
    ]