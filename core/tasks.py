from celery import shared_task
from .services.csv_processor import CSVProcessor
from .models import ImportLog
from django.utils import timezone
import logging
from typing import Any

logger = logging.getLogger(__name__)

@shared_task(
    bind=True,
    max_retries=3,
    rate_limit='2/h',
    time_limit=1800  # 30 minutes
)
def process_csv_import(self: Any, file_path: str, table_name: str, import_log_id: int) -> bool:
    """Celery task for processing CSV imports with rate limiting and retries"""
    try:
        import_log = ImportLog.objects.get(id=import_log_id)
        import_log.status = 'processing'
        import_log.save()

        processor = CSVProcessor(table_name)
        
        # Validate table schema
        if not processor.validate_table_schema():
            raise ValueError(f"Invalid table schema for {table_name}")
        
        # Process file
        success = processor.process_file(file_path, import_log_id)
        
        # Update import log
        import_log.status = 'completed' if success else 'failed'
        import_log.completed_at = timezone.now()
        import_log.save()
        
        return success

    except Exception as e:
        logger.error(f"Import task error: {str(e)}")
        if self.request.retries < self.max_retries:
            self.retry(exc=e, countdown=300)  # Retry after 5 minutes
        raise
