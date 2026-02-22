from typing import List, Optional

from sqlalchemy.orm import Session

from src.app.schemas.employee_schema import EmployeeCreate, EmployeeUpdate
from src.db.models.silver import Employee


def create_employee(db: Session, employee_data: EmployeeCreate) -> Employee:
    employee = Employee(**employee_data.model_dump())
    db.add(employee)
    db.commit()
    db.refresh(employee)
    return employee


def get_all_employees(db: Session, skip: int = 0, limit: int = 100) -> List[Employee]:
    return db.query(Employee).offset(skip).limit(limit).all()


def get_employee_by_id(db: Session, employee_id: str) -> Optional[Employee]:
    return db.query(Employee).filter(Employee.client_employee_id == employee_id).first()


def update_employee(
    db: Session, employee_id: str, employee_data: EmployeeUpdate
) -> Optional[Employee]:
    employee = get_employee_by_id(db, employee_id)
    if not employee:
        return None

    update_data = employee_data.model_dump(exclude_unset=True)
    for field, value in update_data.items():
        setattr(employee, field, value)

    db.commit()
    db.refresh(employee)
    return employee


def delete_employee(db: Session, employee_id: str) -> bool:
    employee = get_employee_by_id(db, employee_id)
    if not employee:
        return False
    db.delete(employee)
    db.commit()
    return True
