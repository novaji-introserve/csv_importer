# from celery import shared_task
# from .services.csv_processor import CSVProcessor
# from .models import ImportLog
# from django.utils import timezone
# import logging
# from typing import Any
# from django.core.exceptions import ObjectDoesNotExist

# logger = logging.getLogger(__name__)


# @shared_task(
#     bind=True,
#     max_retries=3,
#     rate_limit='2/h',
#     time_limit=1800  # 30 minutes
# )
# def process_csv_import(self: Any, file_path: str, table_name: str, import_log_id: int) -> bool:
#     """Celery task for processing CSV imports with rate limiting and retries"""
#     try:
#         try:
#             import_log = ImportLog.objects.get(id=import_log_id)
#         except ImportLog.DoesNotExist:
#             logger.error(f"ImportLog with id {import_log_id} does not exist.")
#             raise ValueError(f"ImportLog with id {import_log_id} does not exist.")
        
#         # Proceed with task if ImportLog exists
#         import_log.status = 'processing'
#         import_log.save()

#         processor = CSVProcessor(table_name)

#         # Try reading the file with different encodings
#         try:
#             # processor.read_file(file_path)
#             data = processor.read_file(file_path)
#             logger.info(f"Columns in the file for {table_name}: {list(data.columns)}")
#         except UnicodeDecodeError as e:
#             logger.error(f"Error reading file: {str(e)}")
#             logger.error(f"File encoding error: {str(e)}")
#             import_log.status = 'failed'
#             import_log.error_message = f"File encoding error: {str(e)}"
#             import_log.save()
#             raise e

#         # Validate table schema
#         if not processor.validate_table_schema():
#             raise ValueError(f"Invalid table schema for {table_name}")

#         # Process file
#         success = processor.process_file(file_path, import_log_id)

#         # Update import log
#         import_log.status = 'completed' if success else 'failed'
#         import_log.completed_at = timezone.now()
#         import_log.save()

#         return success

#     except Exception as e:
#         logger.error(f"Import task error: {str(e)}")
#         if self.request.retries < self.max_retries:
#             self.retry(exc=e, countdown=300)  # Retry after 5 minutes
#         # Save the error message properly
#         if import_log:
#             import_log.status = 'failed'
#             import_log.error_message = f"Error processing file: {str(e)}"
#             import_log.save()
#         raise

import os
from celery import shared_task
from django.db import transaction, DatabaseError
from .services.csv_processor import CSVProcessor
from .models import ImportLog
from django.utils import timezone
import logging
from typing import Any
from django.core.exceptions import ObjectDoesNotExist
from celery.exceptions import SoftTimeLimitExceeded

logger = logging.getLogger(__name__)


@shared_task(
    bind=True,
    max_retries=3,
    rate_limit='10/m', 
    time_limit=1800,
    soft_time_limit=1700,
    acks_late=True,
    reject_on_worker_lost=True,
    autoretry_for=(DatabaseError, OSError),
    retry_backoff=True,
    retry_backoff_max=300, 
    retry_jitter=True
)
def process_csv_import(self, file_path: str, table_name: str, import_log_id: int) -> bool:
    logger.info(f"Starting import task for file: {file_path}, table: {table_name}, log_id: {import_log_id}")
    
    try:
        with transaction.atomic():
            # Set a timeout for the lock acquisition
            import_log = ImportLog.objects.select_for_update(nowait=True).get(id=import_log_id)
            
            # Early check for file existence
            if not os.path.exists(file_path):
                logger.error(f"File not found: {file_path}")
                import_log.status = 'failed'
                import_log.error_message = 'File not found'
                import_log.save()
                return False
            
            if import_log.status == 'processing':
                logger.warning(f"Import {import_log_id} is already being processed")
                return False
            
            import_log.status = 'processing'
            import_log.started_at = timezone.now()
            import_log.save()
            
            processor = CSVProcessor(table_name)
            success = False
            
            try:
                data = processor.read_file(file_path)
                if processor.validate_table_schema():
                    success = processor.process_file(file_path, import_log_id)
                else:
                    import_log.error_message = f"Invalid table schema for {table_name}"
            except Exception as e:
                import_log.error_message = f"Processing error: {str(e)}"
                raise
            finally:
                # Always update the import log status
                import_log.status = 'completed' if success else 'failed'
                import_log.completed_at = timezone.now()
                import_log.save()
                
                # Cleanup temporary file
                try:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                except OSError as e:
                    logger.warning(f"Could not remove file {file_path}: {e}")
            
            return success
            
    except SoftTimeLimitExceeded:
        logger.error(f"Task timed out for import {import_log_id}")
        if 'import_log' in locals():
            import_log.status = 'failed'
            import_log.error_message = 'Task timed out'
            import_log.save()
        raise
        
    except Exception as e:
        logger.error(f"Import task error: {str(e)}")
        if hasattr(self, 'request') and self.request.retries < self.max_retries:
            raise self.retry(exc=e, countdown=60)  # Reduced retry delay
        if 'import_log' in locals():
            import_log.status = 'failed'
            import_log.error_message = f"Error: {str(e)}"
            import_log.save()
        return False
    