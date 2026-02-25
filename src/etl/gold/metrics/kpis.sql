
-- 1) Active Headcount Over Time
SELECT
    year,
    month,
    active_headcount
FROM gold.headcount_trend
ORDER BY year, month;

-- 2) Turnover Trend (terminations per month)
SELECT
    year,
    month,
    terminations AS termination_count,
    ROUND(terminations::numeric / NULLIF(active_headcount, 0) * 100, 2) AS turnover_rate_pct
FROM gold.headcount_trend
ORDER BY year, month;

-- 3) Average Tenure by Department (in days)
SELECT
    department_name,
    year,
    month,
    avg_tenure_days
FROM gold.department_monthly_metrics
ORDER BY department_name, year, month;

-- 4) Average Working Hours per Employee (monthly)
SELECT
    client_employee_id,
    year,
    month,
    total_hours_worked,
    avg_hours_per_day,
    avg_hours_per_week
FROM gold.employee_attendance_metrics
ORDER BY year, month, client_employee_id;

-- 5) Late Arrival Frequency (count per employee per month)
SELECT
    client_employee_id,
    year,
    month,
    late_arrival_count,
    late_arrival_rate
FROM gold.employee_attendance_metrics
ORDER BY year, month, client_employee_id;

-- 6) Early Departure Count
SELECT
    client_employee_id,
    year,
    month,
    early_departure_count,
    early_departure_rate
FROM gold.employee_attendance_metrics
ORDER BY year, month, client_employee_id;

-- 7) Total Overtime Count
SELECT
    client_employee_id,
    year,
    month,
    overtime_count,
    overtime_rate
FROM gold.employee_attendance_metrics
ORDER BY year, month, client_employee_id;

-- 8) Rolling Average Working Hours (last 4 weeks as stored by ETL)
SELECT
    client_employee_id,
    year,
    month,
    rolling_avg_hours_4w
FROM gold.employee_attendance_metrics
ORDER BY year, month, client_employee_id;

-- 9) Early Attrition Rate (company-level)
SELECT
    year,
    month,
    early_attrition_count,
    early_attrition_rate
FROM gold.headcount_trend
ORDER BY year, month;
