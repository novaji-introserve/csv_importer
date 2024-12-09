import pandas as pd
from sqlalchemy import create_engine, text
import logging
from django.conf import settings
from datetime import datetime

logger = logging.getLogger(__name__)

class CSVProcessor:
    CHUNK_SIZE = 10000
    DEFAULT_COLUMNS = {
        'create_date': datetime.now(),
        'write_date': datetime.now(),
        'create_uid': 1,
        'write_uid': 1
    }
    
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.engine = create_engine(
            f'postgresql://{settings.DB_CONFIG["user"]}:{settings.DB_CONFIG["password"]}'
            f'@{settings.DB_CONFIG["host"]}/{settings.DB_CONFIG["database"]}'
        )
        
    def validate_table_schema(self) -> bool:
        """Verify table exists and has required columns"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(
                    f"SELECT column_name FROM information_schema.columns "
                    f"WHERE table_name = '{self.table_name}'"
                ))
                columns = [row[0] for row in result]
                required_columns = ['create_date', 'write_date', 'create_uid', 'write_uid']
                return all(col in columns for col in required_columns)
        except Exception as e:
            logger.error(f"Schema validation error: {str(e)}")
            return False

    def process_file(self, file_path: str, import_log_id: int) -> bool:
        """Process CSV file in chunks with error handling and logging"""
        try:
            total_processed = 0
            
            for chunk in pd.read_csv(file_path, chunksize=self.CHUNK_SIZE):
                # Add default columns
                for col, value in self.DEFAULT_COLUMNS.items():
                    chunk[col] = value
                
                # Clean data
                chunk = self._clean_chunk(chunk)
                
                # Bulk insert
                chunk.to_sql(
                    self.table_name,
                    self.engine,
                    if_exists='append',
                    index=False,
                    method='multi'
                )
                
                total_processed += len(chunk)
                self._update_progress(import_log_id, total_processed)
                
            return True
            
        except Exception as e:
            logger.error(f"File processing error: {str(e)}")
            self._update_error(import_log_id, str(e))
            return False

    def _clean_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Clean and validate data chunk"""
        # Remove leading/trailing whitespace
        for col in chunk.select_dtypes(['object']):
            chunk[col] = chunk[col].str.strip()
        
        # Convert empty strings to None
        chunk = chunk.replace(r'^\s*$', None, regex=True)
        
        return chunk

    def _update_progress(self, import_log_id: int, processed_records: int) -> None:
        """Update import progress in database"""
        with self.engine.connect() as conn:
            conn.execute(text(
                f"UPDATE core_importlog "
                f"SET successful_records = {processed_records} "
                f"WHERE id = {import_log_id}"
            ))
            conn.commit()

    def _update_error(self, import_log_id: int, error_message: str) -> None:
        """Update error status in database"""
        with self.engine.connect() as conn:
            conn.execute(text(
                f"UPDATE core_importlog "
                f"SET status = 'failed', error_message = '{error_message}' "
                f"WHERE id = {import_log_id}"
            ))
            conn.commit()
            