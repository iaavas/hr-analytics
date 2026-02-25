
from fastapi import APIRouter, Depends, status
from sqlalchemy.orm import Session

from src.app.database import get_db
from src.app.core.security import get_current_user, require_roles, ROLE_ADMIN
from src.app.schemas.employee_schema import (
    EmployeeCreate,
    EmployeeRead,
    EmployeeUpdate,
)
from src.app.schemas.response_schema import ApiResponse
from src.app.services import employee_service

router = APIRouter(prefix="/employees", tags=["employees"])


@router.post("", response_model=ApiResponse, status_code=status.HTTP_201_CREATED)
def create_employee(
    employee: EmployeeCreate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_roles([ROLE_ADMIN])),
):
    """Create a new employee. Requires admin role."""
    created = employee_service.create_employee(db, employee)
    return ApiResponse.ok(
        data=EmployeeRead.model_validate(created).model_dump(),
        message="Employee created successfully.",
    )


@router.get("", response_model=ApiResponse)
def get_employees(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """List employees with optional pagination (skip, limit)."""
    employees = employee_service.get_all_employees(db, skip=skip, limit=limit)
    data = [EmployeeRead.model_validate(e).model_dump() for e in employees]
    return ApiResponse.ok(data=data, message="Retrieved list of employees.")


@router.get("/{employee_id}", response_model=ApiResponse)
def get_employee(
    employee_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(get_current_user),
):
    """Get a single employee by client_employee_id. Returns 404 if not found."""
    employee = employee_service.get_employee_or_raise(db, employee_id)
    return ApiResponse.ok(
        data=EmployeeRead.model_validate(employee).model_dump(),
        message="Employee retrieved successfully.",
    )


@router.patch("/{employee_id}", response_model=ApiResponse)
def update_employee(
    employee_id: str,
    employee: EmployeeUpdate,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_roles([ROLE_ADMIN])),
):
    """Update an existing employee by client_employee_id. Partial updates supported. Requires admin role."""
    updated = employee_service.update_employee(db, employee_id, employee)
    return ApiResponse.ok(
        data=EmployeeRead.model_validate(updated).model_dump(),
        message="Employee updated successfully.",
    )


@router.delete("/{employee_id}", response_model=ApiResponse)
def delete_employee(
    employee_id: str,
    db: Session = Depends(get_db),
    current_user: dict = Depends(require_roles([ROLE_ADMIN])),
):
    """Delete an employee by client_employee_id. Requires admin role."""
    employee_service.delete_employee(db, employee_id)
    return ApiResponse.ok(data=None, message="Employee removed successfully.")
