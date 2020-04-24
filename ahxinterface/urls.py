from django.urls import path

from .views import upload_file, download_file, invoke_agent


urlpatterns = [
    path('Upload/', upload_file, name='Upload'),
    path('DownloadFileAsync/', download_file, name='DownloadFileAsync'),
    path('InvokeAgent/', invoke_agent, name='InvokeAgent')
]
