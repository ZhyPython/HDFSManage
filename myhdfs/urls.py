from django.urls import path
from .views import get_hdfs_metrics, get_clusters, get_active_namenode, \
                   get_datanodes, dowanload_file_to_host, upload_file, \
                   download_file, get_block_info, get_hdfs_dir, mkdir, \
                   delete_file, set_permission, set_owner, set_group, \
                   set_replication

urlpatterns = [
    path('get_metrics/', get_hdfs_metrics, name="get_hdfs_metrics"),
    path('get_clusters/', get_clusters, name="get_clusters"),
    path('get_active_namenode/', get_active_namenode, name="get_active_namenode"),
    path('get_datanodes/', get_datanodes, name="get_datanodes"),
    path('dowanload_file_to_host/', dowanload_file_to_host, name="dowanload_file_to_host"),
    path('upload_file/', upload_file, name="upload_file"),
    path('download_file/', download_file, name='download_file'),
    path('get_block_info/', get_block_info, name='get_block_info'),
    path('get_hdfs_dir/', get_hdfs_dir, name='get_hdfs_dir'),
    path('mkdir/', mkdir, name='mkdir'),
    path('delete_file/', delete_file, name='delete_file'),
    path('set_permission/', set_permission, name='set_permission'),
    path('set_owner/', set_owner, name='set_owner'),
    path('set_group/', set_group, name='set_group'),
    path('set_replication/', set_replication, name='set_replication'),
]
