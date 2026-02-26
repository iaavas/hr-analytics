
from __future__ import annotations

import argparse
import os
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine

BASE_DIR = Path(__file__).resolve().parent
OUTPUT_DIR = BASE_DIR / "output"


def _first_of_month(year: int, month: int) -> pd.Timestamp:
    return pd.to_datetime(f"{year}-{month:02d}-01")


def _safe_save(fig: go.Figure, name: str):
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    outfile = OUTPUT_DIR / name
    fig.write_html(outfile, include_plotlyjs="cdn")
    print(f"Wrote {outfile}")


def _top_categories(series: pd.Series, top: int = 6) -> List[str]:
    return series.value_counts().head(top).index.tolist()


def load_headcount(engine) -> pd.DataFrame:
    q = """
    SELECT year, month, active_headcount, new_hires, terminations,
           early_attrition_count, early_attrition_rate
    FROM gold.headcount_trend
    ORDER BY year, month;
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return df
    df["date"] = [_first_of_month(y, m) for y, m in zip(df.year, df.month)]
    rate = df["terminations"] / df["active_headcount"].replace(0, pd.NA) * 100
    df["turnover_rate"] = pd.to_numeric(rate, errors="coerce").round(2)
    return df


def load_department_metrics(engine) -> pd.DataFrame:
    q = """
    SELECT department_id, department_name, year, month,
           active_headcount, total_terminations, turnover_rate
    FROM gold.department_monthly_metrics
    ORDER BY year, month;
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return df
    df["date"] = [_first_of_month(y, m) for y, m in zip(df.year, df.month)]
    return df


def load_employee_snapshots(engine) -> pd.DataFrame:
    q = """
    SELECT client_employee_id, department_id, department_name, job_title,
           year, month, is_active
    FROM gold.employee_monthly_snapshot
    ORDER BY year, month;
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return df
    df["date"] = [_first_of_month(y, m) for y, m in zip(df.year, df.month)]
    return df


def load_attendance(engine) -> pd.DataFrame:
    q = """
    SELECT client_employee_id, year, month, total_hours_worked,
           avg_hours_per_day, avg_hours_per_week,
           late_arrival_count, early_departure_count, overtime_count,
           avg_variance_minutes, rolling_avg_hours_4w
    FROM gold.employee_attendance_metrics
    ORDER BY year, month;
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return df
    df["date"] = [_first_of_month(y, m) for y, m in zip(df.year, df.month)]
    return df


def load_timesheet_daily(engine) -> pd.DataFrame:
    q = """
    SELECT client_employee_id, department_id, work_date,
           total_hours_worked, late_arrival_count,
           early_departure_count, overtime_count
    FROM gold.timesheet_daily_summary
    ORDER BY work_date;
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return df
    df["work_date"] = pd.to_datetime(df["work_date"])
    return df


def load_org_metrics(engine) -> pd.DataFrame:
    q = """
    SELECT organization_name, year, month, active_employees,
           turnover_rate
    FROM gold.organization_metrics
    ORDER BY year, month;
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return df
    df["date"] = [_first_of_month(y, m) for y, m in zip(df.year, df.month)]
    return df


def workforce_dashboard(
    headcount: pd.DataFrame,
    dept_metrics: pd.DataFrame,
    snapshots: pd.DataFrame,
    org_metrics: pd.DataFrame,
) -> go.Figure:
    """Headcount, turnover, early attrition – with simple scope filters."""

    if headcount.empty:
        raise ValueError("No headcount data found; run gold ETL first.")

    base = headcount.copy()
    base["label"] = "Company"

    if dept_metrics.empty:
        dept_traces = pd.DataFrame()
    else:
        dept_top = _top_categories(dept_metrics["department_name"].dropna())
        dept_traces = (
            dept_metrics[dept_metrics["department_name"].isin(dept_top)]
            .groupby(["department_name", "date"], as_index=False)
            .agg(
                active_headcount=("active_headcount", "sum"),
                terminations=("total_terminations", "sum"),
                turnover_rate=("turnover_rate", "mean"),
            )
        )

    if snapshots.empty:
        job_traces = pd.DataFrame()
    else:
        job_top = _top_categories(snapshots["job_title"].dropna())
        job_traces = (
            snapshots[(snapshots["job_title"].isin(job_top))
                      & (snapshots["is_active"] == True)]
            .groupby(["job_title", "date"], as_index=False)
            .agg(active_headcount=("client_employee_id", "count"))
        )

    # Organization-level (if multiple orgs exist)
    if org_metrics.empty:
        org_traces = pd.DataFrame()
    else:
        org_traces = (
            org_metrics.groupby(["organization_name", "date"], as_index=False)
            .agg(
                active_headcount=("active_employees", "sum"),
                turnover_rate=("turnover_rate", "mean"),
            )
        )

    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        specs=[[{}], [{"secondary_y": True}], [{}]],
        subplot_titles=(
            "Active Headcount Over Time",
            "Turnover Trend (terminations + rate)",
            "Early Attrition Rate",
        ),
    )

    def _termination_series(df: pd.DataFrame):
        if "terminations" in df:
            return df["terminations"]
        if "total_terminations" in df:
            return df["total_terminations"]
        if "termination_count" in df:
            return df["termination_count"]
        return pd.Series([None] * len(df))

    def _turnover_series(df: pd.DataFrame):
        if "turnover_rate" in df:
            return df["turnover_rate"]
        if "turnover_rate_pct" in df:
            return df["turnover_rate_pct"]
        return pd.Series([None] * len(df))

    def _quarterize_company(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        tmp = df.copy()
        tmp["period"] = tmp["date"].dt.to_period("Q")
        agg = (
            tmp.groupby("period", as_index=False)
            .agg(
                active_headcount=("active_headcount", "last"),
                terminations=("terminations", "sum"),
                new_hires=("new_hires", "sum"),
                early_attrition_count=("early_attrition_count", "sum"),
            )
        )
        rate_series = (
            agg["terminations"] /
            agg["active_headcount"].replace(0, pd.NA) * 100
        )
        agg["turnover_rate"] = pd.to_numeric(
            rate_series, errors="coerce").round(2)
        agg["early_attrition_rate"] = (
            agg["early_attrition_count"] /
            agg["new_hires"].replace(0, pd.NA) * 100
        ).fillna(0).round(2)
        agg["date"] = agg["period"].dt.to_timestamp(how="end")
        return agg.drop(columns=["period"])

    def _quarterize_group(df: pd.DataFrame, group_col: str, has_turnover: bool = True) -> pd.DataFrame:
        if df.empty:
            return df
        tmp = df.copy()
        tmp["period"] = tmp["date"].dt.to_period("Q")
        agg = tmp.groupby([group_col, "period"], as_index=False).agg(
            active_headcount=("active_headcount", "last"),
        )
        if "terminations" in tmp:
            term = tmp.groupby([group_col, "period"], as_index=False)[
                "terminations"].sum()
            agg = agg.merge(term, on=[group_col, "period"], how="left")
            rate_series = (
                agg["terminations"] /
                agg["active_headcount"].replace(0, pd.NA) * 100
            )
            agg["turnover_rate"] = pd.to_numeric(
                rate_series, errors="coerce").round(2)
        elif has_turnover and "turnover_rate" in tmp:
            rate = tmp.groupby([group_col, "period"], as_index=False)[
                "turnover_rate"].mean()
            agg = agg.merge(rate, on=[group_col, "period"], how="left")
        agg["date"] = agg["period"].dt.to_timestamp(how="end")
        return agg.drop(columns=["period"])

    base_q = _quarterize_company(base)
    dept_q = _quarterize_group(dept_traces, "department_name")
    job_q = _quarterize_group(job_traces, "job_title", has_turnover=False)
    org_q = _quarterize_group(org_traces, "organization_name")

    agg_monthly_x: list[list] = []
    agg_monthly_y: list[list] = []
    agg_quarterly_x: list[list] = []
    agg_quarterly_y: list[list] = []

    company_indices: list[int] = []
    dept_indices: list[int] = []
    job_indices: list[int] = []
    org_indices: list[int] = []

    def add_headcount_trace(df_m: pd.DataFrame, df_q: pd.DataFrame, name: str, visible: bool):
        fig.add_trace(
            go.Scatter(
                x=df_m["date"],
                y=df_m["active_headcount"],
                mode="lines+markers",
                name=f"Headcount – {name}",
                hovertemplate="%{x|%Y-%m-%d}<br>Headcount=%{y}",
                visible=visible,
            ),
            row=1,
            col=1,
        )
        agg_monthly_x.append(df_m["date"].tolist())
        agg_monthly_y.append(df_m["active_headcount"].tolist())
        if df_q is not None and not df_q.empty:
            agg_quarterly_x.append(df_q["date"].tolist())
            agg_quarterly_y.append(df_q["active_headcount"].tolist())
        else:
            agg_quarterly_x.append(df_m["date"].tolist())
            agg_quarterly_y.append(df_m["active_headcount"].tolist())

    def add_turnover_trace(df_m: pd.DataFrame, df_q: pd.DataFrame, name: str, visible: bool):
        term_m = _termination_series(df_m)
        rate_m = _turnover_series(df_m)

        fig.add_trace(
            go.Bar(
                x=df_m["date"],
                y=term_m,
                name=f"Terminations – {name}",
                marker_color="#4C78A8",
                hovertemplate="%{x|%Y-%m-%d}<br>Terminations=%{y}",
                opacity=0.8,
                visible=visible,
            ),
            row=2,
            col=1,
        )
        fig.add_trace(
            go.Scatter(
                x=df_m["date"],
                y=rate_m,
                mode="lines+markers",
                name=f"Turnover % – {name}",
                marker_color="#E45756",
                hovertemplate="%{x|%Y-%m-%d}<br>Turnover=%{y:.2f}%",
                visible=visible,
            ),
            row=2,
            col=1,
            secondary_y=True,
        )

        agg_monthly_x.extend([df_m["date"].tolist(), df_m["date"].tolist()])
        agg_monthly_y.extend([term_m.tolist(), rate_m.tolist()])

        if df_q is not None and not df_q.empty:
            term_q = _termination_series(df_q)
            rate_q = _turnover_series(df_q)
            agg_quarterly_x.extend(
                [df_q["date"].tolist(), df_q["date"].tolist()])
            agg_quarterly_y.extend([term_q.tolist(), rate_q.tolist()])
        else:
            agg_quarterly_x.extend(
                [df_m["date"].tolist(), df_m["date"].tolist()])
            agg_quarterly_y.extend([term_m.tolist(), rate_m.tolist()])

    def add_attrition_trace(df_m: pd.DataFrame, df_q: pd.DataFrame, name: str, visible: bool):
        fig.add_trace(
            go.Scatter(
                x=df_m["date"],
                y=df_m["early_attrition_rate"],
                mode="lines+markers",
                name=f"Early Attrition % – {name}",
                marker_color="#72B7B2",
                hovertemplate="%{x|%Y-%m-%d}<br>Attrition=%{y:.2f}%",
                visible=visible,
            ),
            row=3,
            col=1,
        )
        agg_monthly_x.append(df_m["date"].tolist())
        agg_monthly_y.append(df_m["early_attrition_rate"].tolist())
        if df_q is not None and not df_q.empty and "early_attrition_rate" in df_q:
            agg_quarterly_x.append(df_q["date"].tolist())
            agg_quarterly_y.append(df_q["early_attrition_rate"].tolist())
        else:
            agg_quarterly_x.append(df_m["date"].tolist())
            agg_quarterly_y.append(df_m["early_attrition_rate"].tolist())

    start = len(fig.data)
    add_headcount_trace(base, base_q, "Company", True)
    add_turnover_trace(base, base_q, "Company", True)
    add_attrition_trace(base, base_q, "Company", True)
    company_indices = list(range(start, len(fig.data)))

    for dept in dept_traces["department_name"].unique() if len(dept_traces) else []:
        start = len(fig.data)
        df_m = dept_traces[dept_traces["department_name"] == dept]
        df_q = dept_q[dept_q["department_name"] == dept]
        add_headcount_trace(df_m, df_q, dept, False)
        add_turnover_trace(df_m, df_q, dept, False)
        dept_indices.extend(range(start, len(fig.data)))

    for title in job_traces["job_title"].unique() if len(job_traces) else []:
        start = len(fig.data)
        df_m = job_traces[job_traces["job_title"] == title]
        df_q = job_q[job_q["job_title"] == title]
        add_headcount_trace(df_m, df_q, f"Job: {title}", False)
        add_turnover_trace(
            base, base_q, f"Job: {title} (company turnover)", False)
        add_attrition_trace(
            base, base_q, f"Job: {title} (company attrition)", False)
        job_indices.extend(range(start, len(fig.data)))

    for org in org_traces["organization_name"].unique() if len(org_traces) else []:
        start = len(fig.data)
        df_m = org_traces[org_traces["organization_name"] == org]
        df_q = org_q[org_q["organization_name"] == org]
        add_headcount_trace(df_m, df_q, org, False)
        add_turnover_trace(df_m, df_q, org, False)
        add_attrition_trace(base, base_q, org, False)
        org_indices.extend(range(start, len(fig.data)))

    trace_count = len(fig.data)
    company_vis = [False] * trace_count
    dept_vis = [False] * trace_count
    job_vis = [False] * trace_count
    org_vis = [False] * trace_count

    for idx in company_indices:
        company_vis[idx] = True
    for idx in dept_indices:
        dept_vis[idx] = True
    for idx in job_indices:
        job_vis[idx] = True
    for idx in org_indices:
        org_vis[idx] = True

    fig.update_layout(
        title="Workforce Trend Dashboard",
        legend_title="",
        hovermode="x unified",
        height=900,
        updatemenus=[
            {
                "buttons": [
                    {"label": "Company", "method": "update",
                        "args": [{"visible": company_vis}]},
                    {"label": "Top Departments", "method": "update",
                        "args": [{"visible": dept_vis}]},
                    {"label": "Top Job Titles", "method": "update",
                        "args": [{"visible": job_vis}]},
                    {"label": "Organizations", "method": "update",
                        "args": [{"visible": org_vis}]},
                ],
                "direction": "down",
                "x": 0,
                "y": 1.15,
                "showactive": True,
                "pad": {"r": 10, "t": 10},
            },
            {
                "buttons": [
                    {"label": "Monthly", "method": "update", "args": [
                        {"x": agg_monthly_x, "y": agg_monthly_y}]},
                    {"label": "Quarterly", "method": "update", "args": [
                        {"x": agg_quarterly_x, "y": agg_quarterly_y}]},
                ],
                "direction": "down",
                "x": 0.18,
                "y": 1.15,
                "showactive": True,
                "pad": {"r": 10, "t": 10},
            },
        ],
    )

    fig.update_xaxes(rangeslider_visible=True, row=3, col=1)
    fig.update_yaxes(title_text="Headcount", row=1, col=1)
    fig.update_yaxes(title_text="Terminations", row=2, col=1)
    fig.update_yaxes(title_text="Turnover %", secondary_y=True, row=2, col=1)
    fig.update_yaxes(title_text="Early Attrition %", row=3, col=1)

    return fig


def work_hours_dashboard(
    attendance: pd.DataFrame, daily: pd.DataFrame, snapshots: pd.DataFrame
) -> go.Figure:
    if attendance.empty or daily.empty:
        raise ValueError(
            "Attendance or daily timesheet data missing; run gold ETL first.")

    if snapshots.empty:
        box_df = attendance.copy()
        box_df["department"] = "Unknown"
    else:
        dept_lookup = snapshots[
            ["client_employee_id", "department_id", "department_name", "date"]
        ].drop_duplicates()
        box_df = attendance.merge(
            dept_lookup,
            on=["client_employee_id", "date"],
            how="left",
        )
        box_df["department"] = (
            box_df["department_name"]
            .fillna(box_df["department_id"])
            .fillna("Unknown")
        )

    daily_mean = (
        daily.groupby("work_date", as_index=False)
        .agg(
            avg_hours=("total_hours_worked", "mean"),
            overtime_count=("overtime_count", "sum"),
        )
    )
    daily_mean["roll_7"] = daily_mean["avg_hours"].rolling(7).mean()
    daily_mean["roll_14"] = daily_mean["avg_hours"].rolling(14).mean()
    daily_mean["roll_30"] = daily_mean["avg_hours"].rolling(30).mean()

    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=False,
        vertical_spacing=0.08,
        subplot_titles=(
            "Average Working Hours per Employee (box by department)",
            "Rolling Average Working Hours",
            "Overtime Count Trend",
        ),
    )

    fig.add_trace(
        go.Box(
            x=box_df["department"],
            y=box_df["avg_hours_per_day"],
            name="Avg hours/day",
            boxmean=True,
            hovertemplate="Dept=%{x}<br>Avg Hours=%{y:.2f}",
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=daily_mean["work_date"],
            y=daily_mean["roll_7"],
            name="7-day rolling",
            mode="lines",
            line=dict(color="#4C78A8"),
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=daily_mean["work_date"],
            y=daily_mean["roll_14"],
            name="14-day rolling",
            mode="lines",
            visible=False,
            line=dict(color="#F58518"),
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=daily_mean["work_date"],
            y=daily_mean["roll_30"],
            name="30-day rolling",
            mode="lines",
            visible=False,
            line=dict(color="#54A24B"),
        ),
        row=2,
        col=1,
    )

    fig.add_trace(
        go.Bar(
            x=daily_mean["work_date"],
            y=daily_mean["overtime_count"],
            name="Overtime count",
            marker_color="#E45756",
            opacity=0.8,
        ),
        row=3,
        col=1,
    )

    buttons = [
        {
            "label": "Rolling 7d",
            "method": "update",
            "args": [
                {"visible": [True, True, False, False, True]},
            ],
        },
        {
            "label": "Rolling 14d",
            "method": "update",
            "args": [
                {"visible": [True, False, True, False, True]},
            ],
        },
        {
            "label": "Rolling 30d",
            "method": "update",
            "args": [
                {"visible": [True, False, False, True, True]},
            ],
        },
    ]

    fig.update_layout(
        title="Work Hours & Overtime Pattern",
        height=900,
        showlegend=True,
        updatemenus=[
            {
                "buttons": buttons,
                "direction": "down",
                "x": 0,
                "y": 1.12,
                "showactive": True,
            }
        ],
    )

    fig.update_xaxes(rangeslider_visible=True, row=2, col=1)
    fig.update_xaxes(rangeslider_visible=True, row=3, col=1)
    fig.update_yaxes(title_text="Avg hours/day", row=1, col=1)
    fig.update_yaxes(title_text="Rolling avg hours", row=2, col=1)
    fig.update_yaxes(title_text="Overtime count", row=3, col=1)

    return fig


def attendance_discipline_dashboard(daily: pd.DataFrame) -> go.Figure:
    if daily.empty:
        raise ValueError("Timesheet daily summary is empty.")

    dept = (
        daily.groupby("department_id", as_index=False)
        .agg(
            late_arrivals=("late_arrival_count", "sum"),
            early_departures=("early_departure_count", "sum"),
        )
        .sort_values("late_arrivals", ascending=False)
    )

    heat = (
        daily.copy()
    )
    heat["weekday"] = heat["work_date"].dt.day_name()
    heat_day = (
        heat.groupby(["work_date", "weekday"], as_index=False)
        .agg(late_arrivals=("late_arrival_count", "sum"))
    )

    offenders = (
        daily.groupby("client_employee_id", as_index=False)
        .agg(late_arrivals=("late_arrival_count", "sum"))
        .sort_values("late_arrivals", ascending=False)
        .head(10)
    )

    fig = make_subplots(
        rows=3,
        cols=1,
        vertical_spacing=0.1,
        subplot_titles=(
            "Late Arrival Frequency by Department",
            "Early Departure Count by Department",
            "Top 10 Late Arrival Offenders",
        ),
    )

    fig.add_trace(
        go.Bar(
            x=dept["department_id"].fillna("Unknown"),
            y=dept["late_arrivals"],
            name="Late arrivals",
            marker_color="#4C78A8",
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Bar(
            x=dept["department_id"].fillna("Unknown"),
            y=dept["early_departures"],
            name="Early departures",
            marker_color="#F58518",
        ),
        row=2,
        col=1,
    )

    fig.add_trace(
        go.Bar(
            x=offenders["client_employee_id"],
            y=offenders["late_arrivals"],
            name="Late arrivals",
            marker_color="#E45756",
        ),
        row=3,
        col=1,
    )

    heat_fig = px.density_heatmap(
        heat_day,
        x="weekday",
        y="work_date",
        z="late_arrivals",
        color_continuous_scale="Blues",
        title="Late arrivals calendar heatmap",
    )
    heat_html = OUTPUT_DIR / "attendance_heatmap.html"
    heat_fig.write_html(heat_html, include_plotlyjs="cdn")
    print(f"Wrote {heat_html} (heatmap) — kept separate for clarity.")

    fig.update_layout(
        title="Attendance Discipline",
        height=900,
        bargap=0.2,
    )
    fig.update_yaxes(title_text="Late arrivals", row=1, col=1)
    fig.update_yaxes(title_text="Early departures", row=2, col=1)
    fig.update_yaxes(title_text="Count", row=3, col=1)

    return fig


def main():
    parser = argparse.ArgumentParser(
        description="Generate HR Insights ")
    parser.add_argument(
        "--db",
        dest="db_url",
        default=os.environ.get(
            "DATABASE_URL", "postgresql://hr_insights:hr_insights@localhost:5432/hr_insights"),
        help="Database URL; defaults to DATABASE_URL env or local dev value.",
    )
    args = parser.parse_args()

    engine = create_engine(args.db_url)

    headcount = load_headcount(engine)
    dept_metrics = load_department_metrics(engine)
    snapshots = load_employee_snapshots(engine)
    attendance = load_attendance(engine)
    daily = load_timesheet_daily(engine)
    org_metrics = load_org_metrics(engine)

    wf_fig = workforce_dashboard(
        headcount, dept_metrics, snapshots, org_metrics)
    _safe_save(wf_fig, "workforce_trend.html")

    hours_fig = work_hours_dashboard(attendance, daily, snapshots)
    _safe_save(hours_fig, "work_hours_overtime.html")

    discipline_fig = attendance_discipline_dashboard(daily)
    _safe_save(discipline_fig, "attendance_discipline.html")


if __name__ == "__main__":
    main()
