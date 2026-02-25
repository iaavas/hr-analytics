from typing import List, Optional

from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.orm import Session

from src.app.core.exceptions import NotFoundError, ServiceError
from src.app.schemas.employee_schema import EmployeeCreate, EmployeeUpdate
from src.db.models.silver import Employee


def get_employee_by_id(db: Session, employee_id: str) -> Optional[Employee]:
    try:
        return db.query(Employee).filter(Employee.client_employee_id == employee_id).first()
    except SQLAlchemyError as e:
        raise ServiceError(f"Failed to fetch employee: {e!s}") from e


def get_employee_or_raise(db: Session, employee_id: str) -> Employee:
    employee = get_employee_by_id(db, employee_id)
    if not employee:
        raise NotFoundError("Employee", employee_id)
    return employee


def create_employee(db: Session, employee_data: EmployeeCreate) -> Employee:
    try:
        employee = Employee(**employee_data.model_dump())
        db.add(employee)
        db.commit()
        db.refresh(employee)
        return employee
    except SQLAlchemyError as e:
        db.rollback()
        raise ServiceError(f"Failed to create employee: {e!s}") from e


def get_all_employees(db: Session, skip: int = 0, limit: int = 100) -> List[Employee]:
    try:
        return db.query(Employee).offset(skip).limit(limit).all()
    except SQLAlchemyError as e:
        raise ServiceError(f"Failed to list employees: {e!s}") from e


def update_employee(
    db: Session, employee_id: str, employee_data: EmployeeUpdate
) -> Employee:
    employee = get_employee_or_raise(db, employee_id)
    try:
        update_data = employee_data.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            setattr(employee, field, value)
        db.commit()
        db.refresh(employee)
        return employee
    except SQLAlchemyError as e:
        db.rollback()
        raise ServiceError(f"Failed to update employee: {e!s}") from e


def delete_employee(db: Session, employee_id: str) -> None:
    employee = get_employee_or_raise(db, employee_id)
    try:
        db.delete(employee)
        db.commit()
    except SQLAlchemyError as e:
        db.rollback()
        raise ServiceError(f"Failed to delete employee: {e!s}") from e
