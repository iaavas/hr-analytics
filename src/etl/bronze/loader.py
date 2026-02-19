"""Bronze loader - writes raw data to bronze tables with minimal transformation."""

import logging
from typing import Optional, Dict, Any
from datetime import datetime
import pandas as pd
from sqlalchemy.orm import Session

from src.db.models.bronze import EmployeeBronze, TimesheetBronze
from src.app.database import SessionLocal

logger = logging.getLogger(__name__)


class BronzeLoader:

    def __init__(self, db_session: Optional[Session] = None):
        self.db_session = db_session or SessionLocal()
        self._session_owner = db_session is None

    def load_employee_data(
        self,
        df: pd.DataFrame,
        source_file: str,
        batch_size: int = 1000,
    ) -> int:
     
        try:
            records_loaded = 0
            
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                bronze_records = []
                
                for _, row in batch.iterrows():
                    bronze_record = EmployeeBronze(
                        client_employee_id=str(row.get("client_employee_id", "")),
                        first_name=str(row.get("first_name", "")),
                        middle_name=str(row.get("middle_name", "")),
                        last_name=str(row.get("last_name", "")),
                        preferred_name=str(row.get("preferred_name", "")),
                        job_code=str(row.get("job_code", "")),
                        job_title=str(row.get("job_title", "")),
                        job_start_date=str(row.get("job_start_date", "")),
                        organization_id=str(row.get("organization_id", "")),
                        organization_name=str(row.get("organization_name", "")),
                        department_id=str(row.get("department_id", "")),
                        department_name=str(row.get("department_name", "")),
                        dob=str(row.get("dob", "")),
                        hire_date=str(row.get("hire_date", "")),
                        recent_hire_date=str(row.get("recent_hire_date", "")),
                        anniversary_date=str(row.get("anniversary_date", "")),
                        term_date=str(row.get("term_date", "")),
                        years_of_experience=str(row.get("years_of_experience", "")),
                        work_email=str(row.get("work_email", "")),
                        address=str(row.get("address", "")),
                        city=str(row.get("city", "")),
                        state=str(row.get("state", "")),
                        zip=str(row.get("zip", "")),
                        country=str(row.get("country", "")),
                        manager_employee_id=str(row.get("manager_employee_id", "")),
                        manager_employee_name=str(row.get("manager_employee_name", "")),
                        fte_status=str(row.get("fte_status", "")),
                        is_per_deim=str(row.get("is_per_deim", "")),
                        cell_phone=str(row.get("cell_phone", "")),
                        work_phone=str(row.get("work_phone", "")),
                        scheduled_weekly_hour=str(row.get("scheduled_weekly_hour", "")),
                        active_status=str(row.get("active_status", "")),
                        termination_reason=str(row.get("termination_reason", "")),
                        clinical_level=str(row.get("clinical_level", "")),
                        source_file=source_file,
                        created_at=datetime.utcnow(),
                    )
                    bronze_records.append(bronze_record)
                
                self.db_session.bulk_save_objects(bronze_records)
                records_loaded += len(bronze_records)
                
                logger.info(
                    f"Loaded batch {i//batch_size + 1}: {len(bronze_records)} employee records"
                )
            
            self.db_session.commit()
            logger.info(f"Successfully loaded {records_loaded} employee records from {source_file}")
            return records_loaded
            
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"Error loading employee data: {e}", exc_info=True)
            raise

    def load_timesheet_data(
        self,
        df: pd.DataFrame,
        source_file: str,
        batch_size: int = 1000,
    ) -> int:
    
        try:
            records_loaded = 0
            
            for i in range(0, len(df), batch_size):
                batch = df.iloc[i:i + batch_size]
                bronze_records = []
                
                for _, row in batch.iterrows():
                    hours_worked = row.get("hours_worked")
                    if pd.isna(hours_worked) or hours_worked == "":
                        hours_worked = None
                    else:
                        try:
                            hours_worked = float(hours_worked)
                        except (ValueError, TypeError):
                            hours_worked = None
                    
                    bronze_record = TimesheetBronze(
                        client_employee_id=str(row.get("client_employee_id", "")),
                        department_id=str(row.get("department_id", "")),
                        department_name=str(row.get("department_name", "")),
                        home_department_id=str(row.get("home_department_id", "")),
                        home_department_name=str(row.get("home_department_name", "")),
                        pay_code=str(row.get("pay_code", "")),
                        punch_in_comment=str(row.get("punch_in_comment", "")),
                        punch_out_comment=str(row.get("punch_out_comment", "")),
                        hours_worked=hours_worked,
                        punch_apply_date=str(row.get("punch_apply_date", "")),
                        punch_in_datetime=str(row.get("punch_in_datetime", "")),
                        punch_out_datetime=str(row.get("punch_out_datetime", "")),
                        scheduled_start_datetime=str(row.get("scheduled_start_datetime", "")),
                        scheduled_end_datetime=str(row.get("scheduled_end_datetime", "")),
                        source_file=source_file,
                        created_at=datetime.utcnow(),
                    )
                    bronze_records.append(bronze_record)
                
                self.db_session.bulk_save_objects(bronze_records)
                records_loaded += len(bronze_records)
                
                logger.info(
                    f"Loaded batch {i//batch_size + 1}: {len(bronze_records)} timesheet records"
                )
            
            self.db_session.commit()
            logger.info(f"Successfully loaded {records_loaded} timesheet records from {source_file}")
            return records_loaded
            
        except Exception as e:
            self.db_session.rollback()
            logger.error(f"Error loading timesheet data: {e}", exc_info=True)
            raise

    def close(self):
        if self._session_owner and self.db_session:
            self.db_session.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
