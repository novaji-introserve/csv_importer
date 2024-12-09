from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from django.core.files.storage import FileSystemStorage
from django.conf import settings
from .models import ImportLog
from .tasks import process_csv_import
from drf_yasg.utils import swagger_auto_schema
from drf_yasg import openapi
from rest_framework.request import Request
from typing import Union 

class CSVImportView(APIView):
    @swagger_auto_schema(
        operation_description="Upload CSV file for data import",
        manual_parameters=[
            openapi.Parameter(
                'file',
                openapi.IN_FORM,
                type=openapi.TYPE_FILE,
                required=True,
                description="CSV file to import (max 2GB)"
            ),
            openapi.Parameter(
                'table_name',
                openapi.IN_FORM,
                type=openapi.TYPE_STRING,
                required=True,
                enum=['civil_servant', 'repayment', 'loan_details'],
                description="Target table for import"
            )
        ],
        responses={
            200: openapi.Response(
                description="Import initiated successfully",
                schema=openapi.Schema(
                    type=openapi.TYPE_OBJECT,
                    properties={
                        'import_id': openapi.Schema(type=openapi.TYPE_INTEGER),
                        'message': openapi.Schema(type=openapi.TYPE_STRING)
                    }
                )
            ),
            400: "Invalid request",
            413: "File too large",
            500: "Server error"
        }
    )
    def post(self, request: Request) -> Response:
        try:
            # Validate file presence
            if 'file' not in request.FILES:
                return Response(
                    {'error': 'No file provided'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            file = request.FILES['file']
            table_name: Union[str, None] = request.data.get('table_name')

            # Validate table_name
            if table_name not in ['civil_servant', 'repayment', 'loan_details']:
                return Response(
                    {'error': 'Invalid or missing table_name'},
                    status=status.HTTP_400_BAD_REQUEST
                )

            # Validate file size
            if file.size > settings.MAX_UPLOAD_SIZE:
                return Response(
                    {'error': 'File size exceeds 2GB limit'},
                    status=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE
                )

            # Save file
            fs = FileSystemStorage()
            filename = fs.save(f'imports/{file.name}', file)
            file_path = fs.path(filename)

            # Create import log
            import_log = ImportLog.objects.create(
                file_name=file.name,
                table_name=table_name,  # table_name is now guaranteed to be a valid string
                created_by=request.user.id,
                total_records=0
            )

            # Queue processing task
            process_csv_import.delay(file_path, table_name, import_log.id)

            return Response({
                'import_id': import_log.id,
                'message': 'Import initiated successfully'
            })

        except Exception as e:
            return Response(
                {'error': str(e)},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )
