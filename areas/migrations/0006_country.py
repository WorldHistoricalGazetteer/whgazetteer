# Generated by Django 2.2.13 on 2020-12-17 23:13

import django.contrib.gis.db.models.fields
import django.contrib.postgres.fields.jsonb
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('areas', '0005_delete_country'),
    ]

    operations = [
        migrations.CreateModel(
            name='Country',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('tgnid', models.IntegerField(blank=True, null=True, verbose_name='Getty TGN id')),
                ('tgnlabel', models.CharField(blank=True, max_length=255, null=True, verbose_name='Getty TGN preferred name')),
                ('iso', models.CharField(max_length=2, verbose_name='2-character code')),
                ('gnlabel', models.CharField(max_length=255, verbose_name='geonames label')),
                ('geonameid', models.IntegerField(verbose_name='geonames id')),
                ('un', models.CharField(blank=True, max_length=3, null=True, verbose_name='UN name')),
                ('variants', django.contrib.postgres.fields.jsonb.JSONField(blank=True, null=True)),
                ('poly', django.contrib.gis.db.models.fields.PolygonField(srid=4326)),
            ],
        ),
    ]
