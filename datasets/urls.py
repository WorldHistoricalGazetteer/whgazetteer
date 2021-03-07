# datasets.urls

from django.urls import path, include
from django.conf.urls import url
from django.conf.urls.static import static
from django.conf import settings

from . import views
from datasets.utils import download_file, download_augmented, download_gis, UpdateCountsView

# dataset actions
app_name='datasets'
urlpatterns = [
    
    ### 
    # try refactoring, ea. tab w/its own view, template
    # templates include ds_tabs.html
    ###
    path('<int:id>/summary', views.DatasetSummaryView.as_view(), name='ds_summary'),
    path('<int:id>/log', views.DatasetLogView.as_view(), name='ds_log'),
    
    
    ###
    # in use
    ###
    path('create/', views.DatasetCreateView.as_view(), name='dataset-create'),
    
    # upload excel
    path('xl/', views.xl_upload, name='xl-upload'),
    
    path('compare/', views.ds_compare, name='dataset-compare'),
    path('update/', views.ds_update, name='dataset-update'),
    
    path('<int:id>/delete', views.DatasetDeleteView.as_view(), name='dataset-delete'),

    # TODO: single download url w/format variable
    # download current upload file revision, unchanged
    path('<int:id>/file/', download_file, name="dl-file"), # 

    # download augmented dataset
    path('<int:id>/augmented/<str:format>', download_augmented, name="dl-aug"), # 

    # download flattened geojson data
    path('<int:id>/gis/', download_gis, name="dl-gis"), # 

    # also handles update for name, description fields
    path('<int:id>/detail', views.DatasetDetailView.as_view(), name='dataset-detail'),

    # insert validated delimited (csv for short) file data to db
    path('<int:pk>/insert_tsv/', views.ds_insert_tsv, name="ds_insert_tsv"),

    # insert validated lpf file data to db
    path('<int:pk>/insert_lpf/', views.ds_insert_lpf, name="ds_insert_lpf"),

    # initiate reconciliation
    path('<int:pk>/recon/', views.ds_recon, name="ds_recon"), # form submit

    # review, validate hits
    path('<int:pk>/review/<str:tid>/<str:passnum>', views.review, name="review"),

    # accept any unreviewed wikidata pass0 hits from given task
    path('wd_pass0/<str:tid>', views.write_wd_pass0, name="wd_pass0"),

    # list places in a dataset; for physical geog layers
    path('<str:label>/places/', views.ds_list, name='ds_list'),

    # delete TaskResult & associated hits
    path('task-delete/<str:tid>/<str:scope>', views.task_delete, name="task-delete"),
    
    # add DatasetUser collaborator
    path('collab-add/<int:dsid>', views.collab_add, name="collab-add"),
    
    # delete DatasetUser collaborator
    path('collab-delete/<int:uid>/<int:dsid>', views.collab_delete, name="collab-delete"),
    
    # undo last save in review
    path('match-undo/<int:ds>/<str:tid>/<int:pid>', views.match_undo, name="match-undo"),
    
    path('updatecounts/', UpdateCountsView.as_view(), name='update_counts') #     
    
] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)
