from django.db import models
from django.contrib.auth.models import User
from django.contrib.postgres.fields import JSONField, ArrayField
from django.core.validators import URLValidator
from main.choices import TRACETYPES, TRACERELATIONS

# FK dataset >> Dataset
class Trace(models.Model):
    # auto id
    title = models.CharField(max_length=255)
    src_id = models.CharField(max_length=2044)
    dataset = models.ForeignKey('datasets.Dataset', db_column='dataset',
        to_field='label', related_name='traces', on_delete=models.CASCADE)
    context = JSONField(blank=True, null=True)
    uri = models.TextField(validators=[URLValidator()])
    trace_type = models.CharField(max_length=20, default='Annotation')
    creator = JSONField(blank=True, null=True)
    created = models.DateTimeField(null=True, auto_now_add=True)
    motivation = models.CharField(max_length=20)

    def __str__(self):
        # return str(self.id)
        return '%s:%d' % (self.dataset, self.id)

    class Meta:
        managed = True
        db_table = 'traces'
        indexes = [
            models.Index(fields=['dataset']),
        ]
# # defer,target_id,source,uri,title,trace_type,trace_datatype,description,format,language,selector_type,selector_value 
# FK trace_id >> Trace
class TraceTarget(models.Model):
    # auto id
    trace_id = models.ForeignKey(Trace, related_name='targets',
        default=-1, on_delete=models.CASCADE)
    uri = models.TextField(validators=[URLValidator()])
    title = models.CharField(max_length=255)
    types = ArrayField(models.CharField(max_length=30,choices=TRACETYPES), null=True, blank=True)
    description = models.TextField()

    class Meta:
        managed = True
        db_table = 'trace_target'
    
# FK trace_id >> Trace
class TraceBody(models.Model):    
    # auto id
    trace_id = models.ForeignKey(Trace, related_name='bodies',
        default=-1, on_delete=models.CASCADE)
    uri = models.TextField(validators=[URLValidator()])
    title = models.CharField(max_length=255)
    when = JSONField(blank=True, null=True)
    relation = models.CharField(max_length=20, choices=TRACERELATIONS)
    
    class Meta:
        managed = True
        db_table = 'trace_body'
    