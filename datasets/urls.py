# datasets.urls

from django.urls import path
from django.conf.urls import url
from django.conf.urls.static import static
from django.conf import settings

from . import views
from datasets.utils import download_file, download_augmented, download_gis, UpdateCountsView

# dataset actions
app_name='datasets'
urlpatterns = [
    
    path('create/', views.DatasetCreateView.as_view(), name='dataset-create'),
    path('<int:id>/delete', views.DatasetDeleteView.as_view(), name='dataset-delete'),

    # insert validated delimited file data to db (LP-TSV)
    path('<int:pk>/insert_tsv/', views.ds_insert_tsv, name="ds_insert_tsv"),

    # insert validated lpf file data to db
    path('<int:pk>/insert_lpf/', views.ds_insert_lpf, name="ds_insert_lpf"),

    # dataset owner views
    path('<int:id>/summary', views.DatasetSummaryView.as_view(), name='ds_summary'),
    path('<int:id>/browse', views.DatasetBrowseView.as_view(), name='ds_browse'),
    path('<int:id>/reconcile', views.DatasetReconcileView.as_view(), name='ds_reconcile'),
    path('<int:id>/collab', views.DatasetCollabView.as_view(), name='ds_collab'),
    path('<int:id>/addtask', views.DatasetAddTaskView.as_view(), name='ds_addtask'),
    path('<int:id>/log', views.DatasetLogView.as_view(), name='ds_log'),

    # update-related
    path('compare/', views.ds_compare, name='dataset-compare'),
    path('update/', views.ds_update, name='dataset-update'),
        
    # TODO: single download url w/format variable
    # download latest file, as uploaded
    path('<int:id>/file/', download_file, name="dl-file"),
    
    # download augmented dataset
    path('<int:id>/augmented/<str:format>', download_augmented, name="dl-aug"), # 

    # download flattened geojson data
    path('<int:id>/gis/', download_gis, name="dl-gis"), # 

    # initiate reconciliation task (hands off to align_*())
    path('<int:pk>/recon/', views.ds_recon, name="ds_recon"), # form submit

    # review, validate hits
    path('<int:pk>/review/<str:tid>/<str:passnum>', views.review, name="review"),

    # undo last save in review
    path('match-undo/<int:ds>/<str:tid>/<int:pid>', views.match_undo, name="match-undo"),
        
    # accept any unreviewed wikidata pass0 hits from given task
    path('wd_pass0/<str:tid>', views.write_wd_pass0, name="wd_pass0"),

    # accept any unreviewed whg pass0 hits; create & index child docs
    path('idx_pass0/<str:tid>', views.write_idx_pass0, name="idx_pass0"),

    # delete task (celery_results_taskresult) & associated hits
    path('task-delete/<str:tid>/<str:scope>', views.task_delete, name="task-delete"),
    
    # add DatasetUser collaborator
    path('collab-add/<int:dsid>/<str:v>', views.collab_add, name="collab-add"),
    
    # delete DatasetUser collaborator
    path('collab-delete/<int:uid>/<int:dsid>/<str:v>', views.collab_delete, name="collab-delete"),
    
    # refresh reconciliation counts
    path('updatecounts/', UpdateCountsView.as_view(), name='update_counts'),
    
    #
    # IN USE?
    #
    # upload excel
    path('xl/', views.xl_upload, name='xl-upload'),
    
] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)


#path('<int:id>/detail', views.DatasetDetailView.as_view(), name='dataset-detail'),
    
# list places in a dataset; for physical geog layers
#path('<str:label>/places/', views.ds_list, name='ds_list'),

