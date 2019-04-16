from django.db import models
from django.conf import settings
from django.contrib.auth.models import User
from django.urls import reverse
from places.models import Place

from main.choices import COMMENT_TAGS

class Comment(models.Model):
    user = models.ForeignKey(settings.AUTH_USER_MODEL,
        related_name='comments', on_delete=models.CASCADE)
    place_id = models.ForeignKey(Place, on_delete=models.CASCADE)
    #place_id = models.IntegerField()
    tag = models.CharField(max_length=20, choices=COMMENT_TAGS,default="other")
    note = models.CharField(max_length=2044,null=True, blank=True)
    created = models.DateTimeField(null=True, auto_now_add=True)
    
    class Meta:
        managed = True
        db_table = 'comments'
    
    
