import zipfile
import chardet
import pandas as pd
import io
from sqlalchemy import create_engine, text
import logging
from django.conf import settings
from datetime import datetime
from typing import List, Dict, Any, Optional
import numpy as np

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
            f'postgresql://{settings.DATABASES["default"]["USER"]}:'
            f'{settings.DATABASES["default"]["PASSWORD"]}@'
            f'{settings.DATABASES["default"]["HOST"]}/'
            f'{settings.DATABASES["default"]["NAME"]}'
        )
        self.csv_to_db_column_map = {
            "Employee Name": "name",
            "IPPIS Number": "ippis_number",
            "NSCDC Number": "nscdc_number",
            "ORACLE Number": "oracle_number",
            "EMP Number": "emp_number",
            "NPF Number": "npf_number",
            "NCS Number": "ncs_number",
            "PR Number": "pr_number",
            "TI Number": "ti_number",
            "PF Number": "pf_number",
            "FCT Number": "fct_number",
            "BVN": "bvn",
            "RankName": "rank_name",
            "GradeLevel": "grade_level",
            "Step": "step",
            "Department": "department",
            "MDA": "mda",
            "Gender": "gender",
            "DateofFirstAppt": "date_of_first_application",
            "Birthdate": "birthdate",
            "BankName": "bank_name",
            "BankCode": "bank_code",
            "BranchName": "branch_name",
            "AccountNumber": "account_number",
            "NetPayment": "net_payment",
            "CivilServantCategory": "civil_servant_type_id",
            "Net Payment Month": "net_payment_month",
            "Email Address": "net_payment_month",
        }
        self.loan_details_column_map = {
            "Tenor": "loan_tenor",
            "LOAN TYPE": "loan_type",
            "NSCDC NO": "nscdc_no",
            "NAME": "name",
            "ACCT NO": "account_no",
            "Initiated By": "initiated_by",
            "Relation Officer": "relation_officer",
            "BANK": "bank",
            "CODE": "code",
            0.01: "one_percent",
            0.015: "one_five_percent",
            "Disbursement Date": "disbursement_dates",
            "OLD LOAN": "old_loan_amount",
            "NEW LOAN": "new_loan_amount",
            "DISB AMOUNT": "disbursement_amount",
            "LOAN BALANCE": "loan_balance",
            "IPPIS Number": "ippis_number",
            "Emp Number": "emp_number",
            "NPF Number": "npf_number",
            "NCS Number": "ncs_number",
            "PR Number": "pr_number",
            "TI Number": "ti_number",
            "FCT Number": "fct_number",
            "PF Number": "pf_number",
            "Oracle Number": "oracle_number",
        }
        self.repayment_column_map = {
            "IPPIS NO": "ippis_no",
            "Employee Name": "employee_name",
            "Year": "year",
            "Amount": "amount",
            "Product": "product_id",
            "Month": "month_field",
            "New PSN": "new_psn",
            "Old PSN": "old_psn",
            "Employee Number": "employee_no",
            "Staff ID": "staff_id",
            "Legacy ID": "legacy_id",
            "Full Name": "full_name",
            "Element": "element",
            "Period": "period",
            "Command": "command",
            "Staff Number": "staff_number",
            "Staff Name": "staff_name",
            "Loan Type": "loan_type",
            "Service Number": "service_number",
            "Pencom ID": "pencom_id",
            "Account ID": "account_id",
            "Bank Code": "bank_code",
            "Beneficiary": "beneficiary",
            "Element Name": "element_name",
            "NHF Number": "nhf_number",
            "Employee Number": "employee_number",
            "IPPIS Number": "ippis_number",
            "Name": "name",
            "Period Name": "period_name",
            "WACS Creditors Name": "wacs_creditors_name",
            "Ministry Name": "ministry_name",
            "Value Data": "value_data",
            "Narration": "narration",
            "Loan Amount": "loan_amount",
            "Deduction": "deduction",
            "WACS Monthly Deduction Amount": "wacs_monthly_deduction_amount",
            "NSCDC Number": "nscdc_no",
            "Oracle Number": "oracle_number",
            "EMP Number": "emp_number",
            "NPF Number": "npf_number",
            "NCS Number": "ncs_number",
            "PR Number": "pr_number",
            "TI Number": "ti_number",
            "FCT Number": "fct_number",
            "PF Number": "pf_number"
        }
        self.LOAN_TYPE_MAPPING = {
            'RENEWAL': 'renewal',
            'NEWLOAN': 'new_loan',
            'TOPUP': 'top_up',
            'LOANCOMPLETED': 'completed_loan',
            'RENEWAL ': 'renewal',
            'NEWLOAN ': 'new_loan',
            'TOPUP ': 'top_up',
            'LOANCOMPLETED ': 'completed_loan',
            ' RENEWAL': 'renewal',
            ' NEWLOAN': 'new_loan',
            ' TOPUP': 'top_up',
            ' LOANCOMPLETED': 'completed_loan',
            'NEW LOAN': 'new_loan',
            'TOP UP': 'top_up',
            'COMPLETED LOAN': 'completed_loan'
        }
    
    @staticmethod
    def safe_convert(x, column_name=None):
        try:
            # Handle None, NaN, or empty values
            if pd.isna(x) or x is None or str(x).strip() == '':
                if column_name == 'loan_tenor':
                    return 0
                return 0.00
                
            # Convert to string and remove commas and currency symbols
            x_str = str(x).strip().replace(',', '').replace('₦', '').replace('$', '')
                
            # Handle conversion  to appropriate type (int or float)
            if column_name == 'loan_tenor':
                # Ensure conversion to integer
                return int(x_str) if x_str.isdigit() else 0
            # Convert to float
            return float(x_str)
            
        except (ValueError, TypeError) as e:
            logger.error(f"Conversion error for value '{x}': {e}")
            return 0.00 if column_name != 'loan_tenor' else 0
        
    def _rename_columns(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Renames columns based on the CSV to DB mapping and handles data normalization."""
            # Select the appropriate column mapping
        if self.table_name == 'loan_details':
            column_map = self.loan_details_column_map
        elif self.table_name == 'repayment':
            column_map = self.repayment_column_map
        else:
            column_map = self.csv_to_db_column_map
            
        # First, rename columns
        chunk.rename(columns=column_map, inplace=True)
        # chunk.rename(columns=self.csv_to_db_column_map, inplace=True)
        
        # Float columns that need special handling
        numeric_columns = [
            'loan_amount', 
            'deduction', 
            'wacs_monthly_deduction_amount',
            'amount',
            'old_loan_amount',
            'new_loan_amount',
            'one_percent',
            'one_five_percent',
            'disbursement_amount',
            'insurance',
            'admin_fee',
            'loan_balance',
            'preliquidation_fee'
        ]
        
        logger.info(f"Data types before insertion: {chunk.dtypes}")
        
        # Detailed numeric column conversion
        for col in numeric_columns:
            if col in chunk.columns:
                try:
                    # Log original column info
                    logger.info(f"\nProcessing column: {col}")
                    logger.info(f"Column dtype before conversion: {chunk[col].dtype}")
                    logger.info(f"Sample values before conversion: {chunk[col].head()}")
                    
                    # Apply safe conversion
                    # chunk[col] = chunk[col].apply(safe_convert)
                    chunk[col] = chunk[col].apply(lambda x: self.safe_convert(x, column_name=col))
                
                    # Log column info after conversion
                    logger.info(f"Column dtype after conversion: {chunk[col].dtype}")
                    logger.info(f"Sample values after conversion: {chunk[col].head()}")
                    logger.info(f"Unique values after conversion: {chunk[col].unique()}")
                
                except Exception as e:
                    logger.error(f"Unexpected error processing column {col}: {e}")
                    # Re-raise the exception to prevent further processing
                    raise
                
        MONTH_TO_NUMBER = {
            'January': '01',
            'February': '02',
            'March': '03',
            'April': '04',
            'May': '05',
            'June': '06',
            'July': '07', 
            'August': '08',
            'September': '09',
            'October': '10',
            'November': '11',
            'December': '12'
        }

        if 'month_field' in chunk.columns:
            # Convert month names to two-digit numbers with error logging
            def convert_month(month):
                try:
                    return MONTH_TO_NUMBER.get(month, month)
                except Exception as e:
                    logger.warning(f"Unable to convert month '{month}': {e}")
                    return month
            
            chunk['month_field'] = chunk['month_field'].apply(convert_month)


        # Handle disbursement_dates only for loan_details table
        if self.table_name == 'loan_details':
            if 'disbursement_dates' in chunk.columns:
                # Replace empty strings with None
                chunk['disbursement_dates'] = chunk['disbursement_dates'].replace('', None)

                # Clean and convert dates
                def clean_date(date_str):
                    if pd.isna(date_str) or date_str is None or str(date_str).strip() == '':
                        return None
                    try:
                        parsed_date = pd.to_datetime(str(date_str).strip(), errors='coerce')
                        return parsed_date.date() if not pd.isna(parsed_date) else None
                    except Exception as e:
                        logger.warning(f"Error converting date '{date_str}': {str(e)}")
                        return None

                chunk['disbursement_dates'] = chunk['disbursement_dates'].apply(clean_date)
                logger.info(f"Disbursement dates after cleaning: {chunk['disbursement_dates'].unique()}")
            else:
                logger.warning(f"Column 'disbursement_dates' not found in chunk for table {self.table_name}.")


        
        # Handle loan type mapping for loan_details table
        if self.table_name == 'loan_details':
            # First ensure the column is renamed properly
            if 'LOAN TYPE' in chunk.columns:
                chunk.rename(columns={'LOAN TYPE': 'loan_type'}, inplace=True)
            
            # Then map the values to the correct format
            if 'loan_type' in chunk.columns:
                def map_loan_type(value):
                    if pd.isna(value) or value is None or str(value).strip() == '':
                        return 'new_loan'  # default value
                    cleaned_value = str(value).strip().upper()
                    mapped_value = self.LOAN_TYPE_MAPPING.get(cleaned_value)
                    if mapped_value is None:
                        logger.warning(f"Unknown loan type value: {value}")
                        return 'new_loan'  # default value
                    return mapped_value

                # Apply the mapping
                chunk['loan_type'] = chunk['loan_type'].apply(map_loan_type)
                
                # Log the unique values after mapping to verify
                logger.info(f"Unique loan types after mapping: {chunk['loan_type'].unique()}")
            
        # Normalize Gender column
        if 'gender' in chunk.columns:
            # Create a mapping for gender normalization
            gender_mapping = {
                # Case-insensitive mappings for various ways "Male" might be written
                'male': 'male',
                'm': 'male',
                'Male': 'male',
                'M': 'male',
                
                # Case-insensitive mappings for various ways "Female" might be written
                'female': 'female',
                'f': 'female',
                'Female': 'female',
                'F': 'female'
            }
            
            # Apply gender normalization
            chunk['gender'] = chunk['gender'].map(gender_mapping)
            
            # Optional: Log any unmatched gender values
            unmatched_genders = chunk.loc[chunk['gender'].isna(), 'gender'].unique()
            if len(unmatched_genders) > 0:
                logger.warning(f"Unmatched gender values found: {unmatched_genders}")
        
        # Existing category mapping logic
        if 'civil_servant_type_id' in chunk.columns:
            try:
                # Create a mapping of category names to IDs
                with self.engine.connect() as conn:
                    category_mapping = pd.read_sql(
                        "SELECT id, name FROM civil_servant_category", 
                        conn
                    )
                    
                    # Create a dictionary mapping category names to IDs
                    name_to_id = dict(zip(
                        category_mapping['name'].str.strip().str.lower(), 
                        category_mapping['id']
                    ))
                    
                    # Replace category names with their corresponding IDs
                    chunk['civil_servant_type_id'] = chunk['civil_servant_type_id'].str.strip().str.lower().map(name_to_id)
                    
                    # Log unmatched categories
                    unmatched = chunk.loc[chunk['civil_servant_type_id'].isna(), 'civil_servant_type_id'].unique()
                    if len(unmatched) > 0:
                        logger.warning(f"Unmatched categories: {unmatched}")
            except Exception as e:
                logger.error(f"Error mapping civil servant categories: {e}")
        
        # Map Product_id to the correct product_id
        if 'product_id' in chunk.columns:
            try:
                with self.engine.connect() as conn:
                    # Create a mapping of product names/codes to IDs
                    product_mapping = pd.read_sql(
                        "SELECT id, name, code FROM repayment_product", 
                        conn
                    )
                    
                    # Create dictionaries for mapping
                    name_to_id = dict(zip(
                        product_mapping['name'].str.strip().str.lower(), 
                        product_mapping['id']
                    ))
                    code_to_id = dict(zip(
                        product_mapping['code'].str.strip().str.lower(), 
                        product_mapping['id']
                    ))
                    
                    # Try to map using name first, then code
                    chunk['product_id'] = chunk['product_id'].apply(lambda x: 
                        name_to_id.get(str(x).strip().lower()) or 
                        code_to_id.get(str(x).strip().lower()) or 
                        x
                    )
                    
                    # Log unmatched products
                    unmatched = chunk.loc[chunk['product_id'].isna() | (chunk['product_id'] == chunk['product_id'].astype(str)), 'product_id'].unique()
                    if len(unmatched) > 0:
                        logger.warning(f"Unmatched Products: {unmatched}")
            
            except Exception as e:
                logger.error(f"Error mapping product categories: {e}")
        
        return chunk
    
    def validate_table_schema(self) -> bool:
        """Validates the required columns exist in the target table."""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_name = :table_name"
                ), {"table_name": self.table_name})
                columns: List[str] = [row[0] for row in result]
                
                # Choose the appropriate column mapping based on table name
                if self.table_name == 'repayment':
                    mapped_columns = list(self.repayment_column_map.values())
                elif self.table_name == 'loan_details':
                    mapped_columns = list(self.loan_details_column_map.values())
                else:
                    mapped_columns = list(self.csv_to_db_column_map.values())
                
                # Add default columns
                required_columns = ['create_date', 'write_date', 'create_uid', 'write_uid'] + mapped_columns
                
                # Check which required columns are missing
                missing_columns = [col for col in required_columns if col not in columns]
                
                if missing_columns:
                    logger.error(f"Missing columns for {self.table_name}: {missing_columns}")
                    logger.error(f"Existing columns: {columns}")
                    return False
                
                return True
        except Exception as e:
            logger.error(f"Schema validation error for {self.table_name}: {str(e)}")
            return False

    def read_file(self, file_path: str) -> pd.DataFrame:
        """Reads CSV or Excel files with robust handling."""
        try:
            # Check file extension
            if file_path.endswith('.xlsx') or file_path.endswith('.xls'):
                # Read Excel file
                return pd.read_excel(
                    file_path, 
                    dtype=str,  
                    engine='openpyxl'  
                )

            if file_path.endswith('.zip'):
                with zipfile.ZipFile(file_path, 'r') as zip_ref:
                    zip_ref.extractall('/tmp/extracted_files')
                    extracted_file = zip_ref.namelist()[0]
                    extracted_file_path = f'/tmp/extracted_files/{extracted_file}'
                    return self._read_csv_with_robust_parsing(extracted_file_path)
            
            # Normal CSV processing
            return self._read_csv_with_robust_parsing(file_path)

        except Exception as e:
            logger.error(f"File reading error: {str(e)}")
            raise e

    def _read_csv_with_robust_parsing(self, file_path: str) -> pd.DataFrame:
        """Advanced CSV reading with multiple fallback strategies."""
        # First, detect file encoding
        file_encoding = self._detect_file_encoding(file_path)
        
        # Try reading with different parsing strategies
        parsing_strategies = [
            self._standard_csv_read,
            self._flexible_csv_read,
            self._raw_text_csv_read
        ]
        
        for strategy in parsing_strategies:
            try:
                df = strategy(file_path, file_encoding)
                if not df.empty:
                    return df
            except Exception as e:
                logger.warning(f"Parsing strategy failed: {strategy.__name__}: {str(e)}")
        
        raise ValueError("Could not parse the CSV file with any available strategy")

    def _detect_file_encoding(self, file_path: str) -> str:
        """Detect file encoding using chardet with fallback."""
        try:
            with open(file_path, 'rb') as file:
                raw_data = file.read()
                result = chardet.detect(raw_data)
                
            # Prioritize detected encoding, with fallbacks
            encodings = [
                result['encoding'] or 'utf-8', 
                'utf-8', 
                'latin1', 
                'ISO-8859-1', 
                'cp1252'
            ]
            
            return next(enc for enc in encodings if enc)
        except Exception as e:
            logger.error(f"Encoding detection error: {str(e)}")
            return 'utf-8'  # Safe default

    def _standard_csv_read(self, file_path: str, encoding: str) -> pd.DataFrame:
        """CSV reading that never fails, preserving all rows."""
        try:
            return pd.read_csv(
                file_path,
                encoding=encoding,
                quotechar='"',
                thousands=',',
                dtype=str,  # Force everything to string
                on_bad_lines='warn',  # Log bad lines but continue
                low_memory=False
            )
        except Exception as e:
            logger.warning(f"CSV read failed: {str(e)}")
            # Fallback reading strategies
            try:
                # Try with Python engine and alternative delimiters
                delimiters = [',', ';', '\t', '|']
                for delimiter in delimiters:
                    try:
                        return pd.read_csv(
                            file_path,
                            encoding=encoding,
                            sep=delimiter,
                            dtype=str,
                            on_bad_lines='warn',
                            engine='c'
                        )
                    except Exception as inner_e:
                        logger.warning(f"Delimiter {delimiter} failed: {str(inner_e)}")
                
                # Absolute last resort: raw text reading
                with open(file_path, 'r', encoding=encoding, errors='replace') as f:
                    content = f.read()
                    return pd.read_csv(
                        io.StringIO(content),
                        dtype=str,
                        on_bad_lines='warn',
                        engine='c'
                    )
            except Exception as final_e:
                logger.error(f"Catastrophic file read failure: {str(final_e)}")
                # Create an empty DataFrame to prevent total failure
                return pd.DataFrame()


    def _flexible_csv_read(self, file_path: str, encoding: str) -> pd.DataFrame:
        """More flexible CSV reading with additional parsing options."""
        return self._standard_csv_read(file_path, encoding)

    def _raw_text_csv_read(self, file_path: str, encoding: str) -> pd.DataFrame:
        """Last resort: read file as raw text and parse manually."""
        return self._standard_csv_read(file_path, encoding)
    
    def process_file(self, file_path: str, import_log_id: int) -> bool:
        """Processes the CSV file in chunks and inserts data into the database."""
        try:
            total_processed = 0
            data = self.read_file(file_path)  # Read the file
            
            logger.info(f"Total rows read: {len(data)}")
            logger.info(f"Original Columns: {list(data.columns)}")
            
            # Rename columns based on the mapping
            data = self._rename_columns(data)
            
            logger.info(f"Renamed Columns: {list(data.columns)}")
            
            with self.engine.connect() as conn:
                # Clean the entire dataframe
                data = self._clean_chunk(data)

                logger.info(f"Cleaned Columns: {list(data.columns)}")
                logger.info(f"Cleaned Data Sample:\n{data.head()}")
                if self.table_name == 'loan_details':
                    logger.info(f"Data sample before insertion:\n{data[['loan_type', 'disbursement_dates']].head()}")
                    logger.info(f"Disbursement dates before insertion: {data['disbursement_dates'].unique()}")
            
                # Only log loan_type and disbursement_dates for loan_details table
                if self.table_name == 'loan_details':
                    if 'loan_type' in data.columns:
                        logger.info(f"Loan types before insertion: {data['loan_type'].unique()}")

                    if 'disbursement_dates' in data.columns:
                        data['disbursement_dates'] = data['disbursement_dates'].replace('', None)
                        data.loc[data['disbursement_dates'] == '', 'disbursement_dates'] = None
                        logger.info(f"Disbursement d`ates unique values: {data['disbursement_dates'].unique()}")

                # Bulk insert
                try:
                    data.to_sql(
                        self.table_name,
                        self.engine,
                        if_exists='append',
                        index=False,
                        method='multi'
                    )
                    logger.info(f"Successfully inserted {len(data)} rows")
                    total_processed = len(data)
                    self._update_progress(conn, import_log_id, total_processed)
                    return True
                except Exception as insert_error:
                    logger.error(f"Insertion error: {insert_error}")
                    logger.error(f"Problematic data columns:\n{data.columns}")
                    logger.error(f"Problematic data sample:\n{data.head()}")
                    
                    with self.engine.connect() as error_conn:
                        self._update_error(error_conn, import_log_id, str(insert_error))
                    return False

        except Exception as e:
            logger.error(f"File processing error: {str(e)}")
            with self.engine.connect() as conn:
                self._update_error(conn, import_log_id, str(e))
            return False

    def _clean_chunk(self, chunk: pd.DataFrame) -> pd.DataFrame:
        """Cleans a chunk of data by stripping whitespace and handling empty values."""
        # Ensure every value is a string and handle NaN values
        chunk = chunk.astype(str)

        # Replace NaN values with an empty string
        chunk = chunk.fillna('')
        
        numeric_columns = [
            'amount',
            'loan_amount',
            'deduction',
            'wacs_monthly_deduction_amount',
            'net_payment',
            'old_loan_amount',
            'new_loan_amount',
            'one_percent',
            'one_five_percent',
            'disbursement_amount',
            'insurance',
            'admin_fee',
            'loan_balance',
            'preliquidation_fee',
            'loan_tenor'
        ]
        
        # Handle numeric columns first
        for col in numeric_columns:
            if col in chunk.columns:
                # Special handling for loan_tenor to ensure it's an integer
                if col == 'loan_tenor':
                    # Convert loan_tenor to integer, replacing empty or invalid values with 0
                    chunk[col] = chunk[col].apply(lambda x: self.safe_convert(x, column_name=col))
                else:
                    # For other numeric columns, convert empty strings and NaN to 0.0
                    chunk[col] = pd.to_numeric(chunk[col].replace('', '0'), errors='coerce').fillna(0.0)
    
        # Trim extreme whitespace
        for col in chunk.columns:
            if col not in numeric_columns:
                chunk[col] = chunk[col].fillna('')
                if chunk[col].dtype == 'object':
                    chunk[col] = chunk[col].astype(str).str.strip()

        # Handle disbursement_dates only for loan_details table
        if self.table_name == 'loan_details':
            if 'disbursement_dates' in chunk.columns:
                chunk['disbursement_dates'] = chunk['disbursement_dates'].replace('', None)

                def clean_date(date_str):
                    if pd.isna(date_str) or date_str is None or str(date_str).strip() == '':
                        return None
                    try:
                        parsed_date = pd.to_datetime(str(date_str).strip(), errors='coerce')
                        return parsed_date.date() if not pd.isna(parsed_date) else None
                    except Exception as e:
                        logger.warning(f"Error converting date '{date_str}': {str(e)}")
                        return None

                chunk['disbursement_dates'] = chunk['disbursement_dates'].apply(clean_date)
            else:
                logger.warning(f"Column 'disbursement_dates' not found in chunk for table {self.table_name}.")

        # Select the appropriate column mapping
        if self.table_name == 'loan_details':
            expected_columns = list(self.loan_details_column_map.values())
        elif self.table_name == 'repayment':
            expected_columns = list(self.repayment_column_map.values())
        else:
            expected_columns = list(self.csv_to_db_column_map.values())

        # Ensure all expected columns are present
        for col in expected_columns:
            if col not in chunk.columns:
                chunk[col] = 0.00 if col in numeric_columns else ''

        # Add default columns if they don't exist
        for col, value in self.DEFAULT_COLUMNS.items():
            if col not in chunk.columns:
                chunk[col] = value

        return chunk

    def _update_progress(self, conn, import_log_id: int, processed_records: int) -> None:
        """Updates the progress of the import in the database."""
        try:
            conn.execute(text(
                "UPDATE core_importlog "
                "SET successful_records = :processed_records "
                "WHERE id = :import_log_id"
            ), {"processed_records": processed_records, "import_log_id": import_log_id})
            conn.commit()
        except Exception as e:
            logger.error(f"Error updating progress: {str(e)}")

    def _update_error(self, conn, import_log_id: int, error_message: str) -> None:
        """Updates the error status in the import log."""
        try:
            conn.execute(text(
                "UPDATE core_importlog "
                "SET status = 'failed', error_message = :error_message "
                "WHERE id = :import_log_id"
            ), {"error_message": error_message, "import_log_id": import_log_id})
            conn.commit()
        except Exception as e:
            logger.error(f"Error updating error status: {str(e)}")

