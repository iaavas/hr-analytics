from typing import List

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from src.app.database import get_db
from src.app.core.security import get_current_user
from src.app.schemas.employee_schema import (
    EmployeeCreate,
    EmployeeRead,
    EmployeeUpdate,
    LoginRequest,
    TokenResponse,
)
from src.app.services import employee_service

router = APIRouter(prefix="/employees", tags=["employees"])


@router.post("", response_model=EmployeeRead, status_code=status.HTTP_201_CREATED)
def create_employee(
    employee: EmployeeCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    return employee_service.create_employee(db, employee)


@router.get("", response_model=List[EmployeeRead])
def get_employees(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    return employee_service.get_all_employees(db, skip=skip, limit=limit)


@router.get("/{employee_id}", response_model=EmployeeRead)
def get_employee(
    employee_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    employee = employee_service.get_employee_by_id(db, employee_id)
    if not employee:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee {employee_id} not found",
        )
    return employee


@router.patch("/{employee_id}", response_model=EmployeeRead)
def update_employee(
    employee_id: str,
    employee: EmployeeUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    updated = employee_service.update_employee(db, employee_id, employee)
    if not updated:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee {employee_id} not found",
        )
    return updated


@router.delete("/{employee_id}", status_code=status.HTTP_204_NO_CONTENT)
def delete_employee(
    employee_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    success = employee_service.delete_employee(db, employee_id)
    if not success:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"Employee {employee_id} not found",
        )
