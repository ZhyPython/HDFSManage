from django.urls import path
from .views import get_hdfs_metrics, get_clusters, get_active_namenode, \
                   get_datanodes, dowanload_file_to_host, upload_file, \
                   download_file

urlpatterns = [
    path('get_metrics/', get_hdfs_metrics, name="get_hdfs_metrics"),
    path('get_clusters/', get_clusters, name="get_clusters"),
    path('get_active_namenode/', get_active_namenode, name="get_active_namenode"),
    path('get_datanodes/', get_datanodes, name="get_datanodes"),
    path('dowanload_file_to_host/', dowanload_file_to_host, name="dowanload_file_to_host"),
    path('upload_file/', upload_file, name="upload_file"),
    path('download_file/', download_file, name='download_file')
]
