#from django.db import models
#from django.contrib.auth.models import User

#import json

#class Trace(models.Model):
    ## let id be auto-maintained, as Django decrees/prefers
    #title = models.CharField(max_length=255)
    #src_id = models.CharField(max_length=2044)
    #dataset = models.ForeignKey('datasets.Dataset', db_column='dataset',
        #to_field='label', related_name='places', on_delete=models.CASCADE)
    #ccodes = ArrayField(models.CharField(max_length=2))

    #def __str__(self):
        ## return str(self.id)
        #return '%s:%d' % (self.dataset, self.id)

    #class Meta:
        #managed = True
        #db_table = 'places'
        #indexes = [
            #models.Index(fields=['src_id', 'dataset']),
        #]

