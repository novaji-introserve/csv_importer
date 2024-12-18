from django.urls import path
from .views import CSVImportView, upload_page

urlpatterns = [
    path('upload/', upload_page, name='upload_page'),
    path('upload-csv/', CSVImportView.as_view(), name='csv-upload'), 
]

