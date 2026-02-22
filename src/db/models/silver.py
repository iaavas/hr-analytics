import sqlalchemy as sa
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func

from src.app.database import Base


class Organization(Base):
    __tablename__ = "organization"
    __table_args__ = {"schema": "silver"}

    organization_id = sa.Column(sa.String(50), primary_key=True)
    organization_name = sa.Column(sa.String(255), nullable=False)

    employees = relationship("Employee", back_populates="organization")
    departments = relationship("Department", back_populates="organization")


class Department(Base):
    __tablename__ = "department"
    __table_args__ = (
        sa.Index("ix_department_org", "organization_id"),
        {"schema": "silver"},
    )

    department_id = sa.Column(sa.String(50), primary_key=True)
    department_name = sa.Column(sa.String(255), nullable=False)

    organization_id = sa.Column(
        sa.String(50),
        sa.ForeignKey("silver.organization.organization_id"),
        nullable=True,
    )

    organization = relationship("Organization", back_populates="departments")
    employees = relationship("Employee", back_populates="department")


class Employee(Base):
    __tablename__ = "employee"
    __table_args__ = (
        sa.Index("ix_employee_department", "department_id"),
        sa.Index("ix_employee_org", "organization_id"),
        {"schema": "silver"},
    )

    client_employee_id = sa.Column(sa.String(50), primary_key=True)

    first_name = sa.Column(sa.String(100), nullable=False)
    middle_name = sa.Column(sa.String(100))
    last_name = sa.Column(sa.String(100), nullable=False)
    preferred_name = sa.Column(sa.String(100))

    job_code = sa.Column(sa.String(50))
    job_title = sa.Column(sa.String(255))

    organization_id = sa.Column(
        sa.String(50),
        sa.ForeignKey("silver.organization.organization_id"),
        nullable=True,
    )

    department_id = sa.Column(
        sa.String(50),
        sa.ForeignKey("silver.department.department_id"),
        nullable=True,
    )

    manager_employee_id = sa.Column(
        sa.String(50),
        sa.ForeignKey(
            "silver.employee.client_employee_id",
            deferrable=True,
            initially="DEFERRED",
        ),
        nullable=True,
    )

    organization = relationship("Organization", back_populates="employees")
    department = relationship("Department", back_populates="employees")

    manager = relationship(
        "Employee",
        remote_side=[client_employee_id],
        backref="direct_reports",
    )

    hire_date = sa.Column(sa.Date)
    term_date = sa.Column(sa.Date)
    dob = sa.Column(sa.Date)

    years_of_experience = sa.Column(sa.Float)
    scheduled_weekly_hour = sa.Column(sa.Float)

    is_active = sa.Column(sa.Boolean)
    is_per_diem = sa.Column(sa.Boolean)

    source_file = sa.Column(sa.String(255))

    created_at = sa.Column(sa.DateTime, server_default=func.now())
    updated_at = sa.Column(
        sa.DateTime,
        server_default=func.now(),
        onupdate=func.now(),
    )


class Timesheet(Base):
    __tablename__ = "timesheet"
    __table_args__ = (
        sa.Index("ix_timesheet_emp_date", "client_employee_id", "work_date"),
        sa.UniqueConstraint(
            "client_employee_id",
            "punch_in_datetime",
            "punch_out_datetime",
            name="uq_timesheet_shift",
        ),
        {"schema": "silver"},
    )

    id = sa.Column(sa.Integer, primary_key=True, autoincrement=True)

    client_employee_id = sa.Column(
        sa.String(50),
        sa.ForeignKey("silver.employee.client_employee_id"),
        nullable=False,
    )

    department_id = sa.Column(
        sa.String(50),
        sa.ForeignKey("silver.department.department_id"),
        nullable=True,
    )

    employee = relationship("Employee", backref="timesheets")

    punch_apply_date = sa.Column(sa.Date)
    punch_in_datetime = sa.Column(sa.DateTime)
    punch_out_datetime = sa.Column(sa.DateTime)

    scheduled_start_datetime = sa.Column(sa.DateTime)
    scheduled_end_datetime = sa.Column(sa.DateTime)

    worked_minutes = sa.Column(sa.Float)
    scheduled_minutes = sa.Column(sa.Float)
    hours_worked = sa.Column(sa.Float)

    work_date = sa.Column(sa.Date)

    pay_code = sa.Column(sa.String(100))
    punch_in_comment = sa.Column(sa.String(500))
    punch_out_comment = sa.Column(sa.String(500))

    source_file = sa.Column(sa.String(255))

    created_at = sa.Column(sa.DateTime, server_default=func.now())
    updated_at = sa.Column(
        sa.DateTime,
        server_default=func.now(),
        onupdate=func.now(),
    )
