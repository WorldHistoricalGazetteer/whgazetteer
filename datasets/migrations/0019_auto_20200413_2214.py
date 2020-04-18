# Generated by Django 2.2.10 on 2020-04-13 22:14

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('datasets', '0018_auto_20200310_1437'),
    ]

    operations = [
        migrations.AlterField(
            model_name='dataset',
            name='ds_status',
            field=models.CharField(blank=True, choices=[('format_error', 'Invalid format'), ('format_ok', 'Valid format'), ('in_database', 'Inserted to database'), ('uploaded', 'File uploaded'), ('ready', 'Ready for submittal'), ('accessioning', 'Accessioning'), ('accessioned', 'Accessioned')], max_length=12, null=True),
        ),
        migrations.AlterField(
            model_name='datasetfile',
            name='df_status',
            field=models.CharField(blank=True, choices=[('format_error', 'Invalid format'), ('format_ok', 'Valid format'), ('in_database', 'Inserted to database'), ('uploaded', 'File uploaded'), ('ready', 'Ready for submittal'), ('accessioning', 'Accessioning'), ('accessioned', 'Accessioned')], max_length=12, null=True),
        ),
    ]