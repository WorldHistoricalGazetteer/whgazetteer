# datasets.urls

from django.urls import path#, include
#from django.conf.urls import url
from django.conf.urls.static import static
from django.conf import settings

from . import views
from datasets.utils import download_file, UpdateCountsView, download_augmented, \
  fetch_geojson_ds, downloader, fetch_geojson_flat, downloadLP7

# dataset actions
app_name='datasets'
urlpatterns = [

  ## BASICS: create, delete, insert data
  path('create/', views.DatasetCreateView.as_view(), name='dataset-create'),
  path('<int:id>/delete', views.DatasetDeleteView.as_view(), name='dataset-delete'),

  # insert validated delimited file data to db (csv, tsv, spreadsheet)
  path('<int:pk>/insert_tsv/', views.ds_insert_tsv, name="ds_insert_tsv"),

  # insert validated lpf file data to db
  path('<int:pk>/insert_lpf/', views.ds_insert_lpf, name="ds_insert_lpf"),

  # upload excel
  path('xl/', views.xl_upload, name='xl-upload'),

  ## MANAGE/VIEW
  # dataset owner pages (tabs); names correspond to template names
  path('<int:id>/summary', views.DatasetSummaryView.as_view(), name='ds_summary'),
  path('<int:id>/browse', views.DatasetBrowseView.as_view(), name='ds_browse'),
  path('<int:id>/reconcile', views.DatasetReconcileView.as_view(), name='ds_reconcile'),
  path('<int:id>/collab', views.DatasetCollabView.as_view(), name='ds_collab'),
  path('<int:id>/addtask', views.DatasetAddTaskView.as_view(), name='ds_addtask'),
  path('<int:id>/log', views.DatasetLogView.as_view(), name='ds_log'),

  # public dataset pages (tabs): metadata, browse
  path('<int:pk>', views.DatasetPublicView.as_view(), name='ds_meta'),
  path('<int:id>/places', views.DatasetPlacesView.as_view(), name='ds_places'),

  ## DOWNLOADS
  # one-off for LP7
  path('download_lp7/', downloadLP7, name='download_lp7'),

  # download latest file, as uploaded
  path('<int:id>/file/', download_file, name="dl-file"), #

  # initiate downloads of augmented datasets via celery task (called from ajax)
  path('dlcelery/', downloader, name='dl_celery'),
  ## DEPRECATing download augmented dataset
  path('<int:id>/augmented/<str:format>', download_augmented, name="dl-aug"), #

  ## UPDATES (in progress)
  path('compare/', views.ds_compare, name='dataset-compare'),
  path('update/', views.ds_update, name='dataset-update'),

  ## RECONCILIATION/REVIEW
  # initiate reconciliation
  path('<int:pk>/recon/', views.ds_recon, name="ds_recon"), # form submit

  # review, validate hits
  path('<int:pk>/review/<str:tid>/<str:passnum>', views.review, name="review"),

  # direct load of deferred place to review screen
  path('<int:pk>/review/<str:tid>/<str:pid>', views.review, name="review"),

  # accept any unreviewed wikidata pass0 hits from given task
  path('wd_pass0/<str:tid>', views.write_wd_pass0, name="wd_pass0"),

  # accept any unreviewed whg pass0 hits; create & index child docs
  path('idx_pass0/<str:tid>', views.write_idx_pass0, name="idx_pass0"),

  # delete TaskResult & associated hits
  path('task-delete/<str:tid>/<str:scope>', views.task_delete, name="task-delete"),

  # undo last save in review
  path('match-undo/<int:ds>/<str:tid>/<int:pid>', views.match_undo, name="match-undo"),

  # refresh reconciliation counts (ds.id from $.get)
  path('updatecounts/', UpdateCountsView.as_view(), name='update_counts'),

  ## COLLABORATORS
  # add DatasetUser collaborator
  path('collab-add/<int:dsid>/<str:v>', views.collab_add, name="collab-add"),

  # delete DatasetUser collaborator
  path('collab-delete/<int:uid>/<int:dsid>/<str:v>', views.collab_delete, name="collab-delete"),

  ## GEOMETRY
  path('<int:dsid>/geojson/', fetch_geojson_ds, name="geojson"),
  path('<int:dsid>/geojson_flat/', fetch_geojson_flat, name="geojson-flat"),

  # list places in a dataset; for physical geog layers
  path('<str:label>/places/', views.ds_list, name='ds_list'),


] + static(settings.MEDIA_URL, document_root = settings.MEDIA_ROOT)


#path('<int:id>/detail', views.DatasetDetailView.as_view(), name='dataset-detail'),
