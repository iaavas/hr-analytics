
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
    SELECT
        s.client_employee_id,
        s.department_id,
        s.department_name,
        s.job_title,
        s.hire_date,
        s.term_date,
        e.organization_id,
        COALESCE(o.organization_name, e.organization_id) AS organization_name,
        s.year,
        s.month,
        s.is_active
    FROM gold.employee_monthly_snapshot s
    LEFT JOIN silver.employee e
        ON s.client_employee_id = e.client_employee_id
    LEFT JOIN silver.organization o
        ON e.organization_id = o.organization_id
    ORDER BY s.year, s.month;
    """
    df = pd.read_sql(q, engine)
    if df.empty:
        return df
    df["date"] = [_first_of_month(y, m) for y, m in zip(df.year, df.month)]
    df["hire_date"] = pd.to_datetime(df["hire_date"], errors="coerce")
    df["term_date"] = pd.to_datetime(df["term_date"], errors="coerce")
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
    SELECT
        t.client_employee_id,
        NULLIF(
            TRIM(
                COALESCE(NULLIF(e.preferred_name, ''), NULLIF(e.first_name, ''))
                || ' ' ||
                COALESCE(NULLIF(e.last_name, ''), '')
            ),
            ''
        ) AS employee_name,
        t.department_id,
        d.department_name,
        t.work_date,
        t.total_hours_worked,
        t.late_arrival_count,
        t.early_departure_count,
        t.overtime_count
    FROM gold.timesheet_daily_summary t
    LEFT JOIN silver.employee e
        ON t.client_employee_id = e.client_employee_id
    LEFT JOIN silver.department d
        ON t.department_id = d.department_id
    ORDER BY t.work_date;
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
    """Headcount, turnover, early attrition with real org/department/job scopes."""

    del headcount, dept_metrics, org_metrics

    if snapshots.empty:
        raise ValueError(
            "Employee monthly snapshots are empty; run gold ETL first.")

    scoped = snapshots.copy()
    scoped["department_label"] = (
        scoped["department_name"]
        .fillna(scoped["department_id"])
        .fillna("Unknown Department")
    )
    scoped["job_label"] = scoped["job_title"].fillna("Unknown Job Title")
    scoped["organization_label"] = (
        scoped["organization_name"]
        .fillna(scoped["organization_id"])
        .fillna("Unknown Organization")
    )

    def _build_snapshot_metrics(group_col: str, top: Optional[int] = None) -> pd.DataFrame:
        if group_col not in scoped.columns:
            return pd.DataFrame()

        df = scoped[scoped[group_col].notna()].copy()
        if df.empty:
            return df

        if top is not None:
            top_groups = _top_categories(
                df.loc[df["is_active"] == True, group_col].dropna(),
                top=top,
            )
            df = df[df[group_col].isin(top_groups)]
            if df.empty:
                return df

        snapshot_month = df["date"].dt.to_period("M")
        hire_month = df["hire_date"].dt.to_period("M")
        term_month = df["term_date"].dt.to_period("M")
        tenure_days = (df["term_date"] - df["hire_date"]).dt.days

        df["is_active_flag"] = df["is_active"].fillna(False).astype(int)
        df["is_new_hire_month"] = (
            hire_month == snapshot_month).fillna(False).astype(int)
        df["is_termination_month"] = (
            term_month == snapshot_month).fillna(False).astype(int)
        df["is_early_attrition_month"] = (
            (term_month == snapshot_month)
            & tenure_days.notna()
            & (tenure_days <= 90)
        ).fillna(False).astype(int)

        agg = (
            df.groupby([group_col, "date"], as_index=False)
            .agg(
                active_headcount=("is_active_flag", "sum"),
                new_hires=("is_new_hire_month", "sum"),
                terminations=("is_termination_month", "sum"),
                early_attrition_count=("is_early_attrition_month", "sum"),
            )
            .sort_values(["date", group_col])
        )

        turnover = (
            agg["terminations"] /
            agg["active_headcount"].replace(0, pd.NA) * 100
        )
        attrition = (
            agg["early_attrition_count"] /
            agg["terminations"].replace(0, pd.NA) * 100
        )
        agg["turnover_rate"] = pd.to_numeric(
            turnover, errors="coerce").round(2)
        agg["early_attrition_rate"] = (
            pd.to_numeric(attrition, errors="coerce")
            .fillna(0)
            .round(2)
        )
        return agg

    dept_traces = _build_snapshot_metrics("department_label", top=6)
    job_traces = _build_snapshot_metrics("job_label", top=6)
    org_traces = _build_snapshot_metrics("organization_label")

    if dept_traces.empty and job_traces.empty and org_traces.empty:
        raise ValueError(
            "No workforce trend groupings found in employee snapshots.")

    fig = make_subplots(
        rows=3,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.12,
        row_heights=[0.34, 0.34, 0.32],
        specs=[[{}], [{"secondary_y": True}], [{}]],
        subplot_titles=(
            "Active Headcount Over Time",
            "Turnover Trend (terminations + rate)",
            "Early Attrition Rate",
        ),
    )

    def _termination_series(df: pd.DataFrame):
        for col in ("terminations", "total_terminations", "termination_count"):
            if col in df:
                return df[col]
        return pd.Series([None] * len(df))

    def _turnover_series(df: pd.DataFrame):
        for col in ("turnover_rate", "turnover_rate_pct"):
            if col in df:
                return df[col]
        return pd.Series([None] * len(df))

    def _quarterize_group(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
        if df.empty:
            return df
        tmp = df.copy()
        tmp["period"] = tmp["date"].dt.to_period("Q")
        agg = (
            tmp.groupby([group_col, "period"], as_index=False)
            .agg(
                active_headcount=("active_headcount", "last"),
                new_hires=("new_hires", "sum"),
                terminations=("terminations", "sum"),
                early_attrition_count=("early_attrition_count", "sum"),
            )
        )
        turnover = (
            agg["terminations"] /
            agg["active_headcount"].replace(0, pd.NA) * 100
        )
        attrition = (
            agg["early_attrition_count"] /
            agg["terminations"].replace(0, pd.NA) * 100
        )
        agg["turnover_rate"] = pd.to_numeric(
            turnover, errors="coerce").round(2)
        agg["early_attrition_rate"] = (
            pd.to_numeric(attrition, errors="coerce")
            .fillna(0)
            .round(2)
        )
        agg["date"] = agg["period"].dt.to_timestamp(how="end")
        return agg.drop(columns=["period"])

    dept_q = _quarterize_group(dept_traces, "department_label")
    job_q = _quarterize_group(job_traces, "job_label")
    org_q = _quarterize_group(org_traces, "organization_label")

    agg_monthly_x:   list[list] = []
    agg_monthly_y:   list[list] = []
    agg_quarterly_x: list[list] = []
    agg_quarterly_y: list[list] = []

    dept_indices:    list[int] = []
    job_indices:     list[int] = []
    org_indices:     list[int] = []

    # ── Trace adders ──────────────────────────────────────────────────────────
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
            row=1, col=1,
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
            row=2, col=1,
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
            row=2, col=1,
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
            row=3, col=1,
        )
        agg_monthly_x.append(df_m["date"].tolist())
        agg_monthly_y.append(df_m["early_attrition_rate"].tolist())
        if df_q is not None and not df_q.empty and "early_attrition_rate" in df_q:
            agg_quarterly_x.append(df_q["date"].tolist())
            agg_quarterly_y.append(df_q["early_attrition_rate"].tolist())
        else:
            agg_quarterly_x.append(df_m["date"].tolist())
            agg_quarterly_y.append(df_m["early_attrition_rate"].tolist())

    default_scope = (
        "org" if len(org_traces)
        else "dept" if len(dept_traces)
        else "job"
    )

    for dept in (dept_traces["department_label"].unique() if len(dept_traces) else []):
        start = len(fig.data)
        df_m = dept_traces[dept_traces["department_label"] == dept]
        df_q = dept_q[dept_q["department_label"] == dept]
        add_headcount_trace(df_m, df_q, dept, default_scope == "dept")
        add_turnover_trace(df_m, df_q, dept, default_scope == "dept")
        add_attrition_trace(df_m, df_q, dept, default_scope == "dept")
        dept_indices.extend(range(start, len(fig.data)))

    for title in (job_traces["job_label"].unique() if len(job_traces) else []):
        start = len(fig.data)
        df_m = job_traces[job_traces["job_label"] == title]
        df_q = job_q[job_q["job_label"] == title]
        add_headcount_trace(
            df_m, df_q, f"Job: {title}", default_scope == "job")
        add_turnover_trace(df_m, df_q, f"Job: {title}", default_scope == "job")
        add_attrition_trace(
            df_m, df_q, f"Job: {title}", default_scope == "job")
        job_indices.extend(range(start, len(fig.data)))

    for org in (org_traces["organization_label"].unique() if len(org_traces) else []):
        start = len(fig.data)
        df_m = org_traces[org_traces["organization_label"] == org]
        df_q = org_q[org_q["organization_label"] == org]
        add_headcount_trace(df_m, df_q, org, default_scope == "org")
        add_turnover_trace(df_m, df_q, org, default_scope == "org")
        add_attrition_trace(df_m, df_q, org, default_scope == "org")
        org_indices.extend(range(start, len(fig.data)))

    trace_count = len(fig.data)
    dept_vis = [False] * trace_count
    job_vis = [False] * trace_count
    org_vis = [False] * trace_count

    for idx in dept_indices:
        dept_vis[idx] = True
    for idx in job_indices:
        job_vis[idx] = True
    for idx in org_indices:
        org_vis[idx] = True

    view_buttons = []
    if any(org_vis):
        view_buttons.append(
            {"label": "Organizations", "method": "update",
                "args": [{"visible": org_vis}]}
        )
    if any(dept_vis):
        view_buttons.append(
            {"label": "Departments", "method": "update",
                "args": [{"visible": dept_vis}]}
        )
    if any(job_vis):
        view_buttons.append(
            {"label": "Job Titles", "method": "update",
                "args": [{"visible": job_vis}]}
        )

    fig.update_layout(
        # Title sits HIGH, well above the button row
        title=dict(
            text="Workforce Trend Dashboard",
            x=0.5,
            xanchor="center",
            y=0.99,
            font=dict(size=18),
        ),
        hovermode="x unified",
        height=1200,
        # t=180 carves room for title + buttons
        margin=dict(t=180, b=60, l=80, r=160),
        legend=dict(
            orientation="v",
            x=1.02,
            y=1.0,
            xanchor="left",
            yanchor="top",
            bgcolor="rgba(255,255,255,0.85)",
            bordercolor="#CCCCCC",
            borderwidth=1,
        ),
        plot_bgcolor="white",
        paper_bgcolor="white",
        updatemenus=[
            dict(
                buttons=view_buttons,
                direction="down",
                x=0.0,
                xanchor="left",
                y=1.10,
                yanchor="top",
                showactive=True,
                bgcolor="#F4F4F4",
                bordercolor="#CCCCCC",
                borderwidth=1,
                font=dict(size=13),
                pad=dict(l=8, r=12, t=8, b=8),
            ),
            dict(
                buttons=[
                    {"label": "Monthly",   "method": "update",
                     "args": [{"x": agg_monthly_x,   "y": agg_monthly_y}]},
                    {"label": "Quarterly", "method": "update",
                     "args": [{"x": agg_quarterly_x, "y": agg_quarterly_y}]},
                ],
                direction="down",
                x=0.30,
                xanchor="left",
                y=1.10,
                yanchor="top",
                showactive=True,
                bgcolor="#F4F4F4",
                bordercolor="#CCCCCC",
                borderwidth=1,
                font=dict(size=13),
                pad=dict(l=8, r=12, t=8, b=8),
            ),
        ],
        annotations=[
            dict(
                text="<b>View by</b>",
                x=0.0, xref="paper",
                y=1.135, yref="paper",
                showarrow=False,
                font=dict(size=11),
                xanchor="left",
            ),
            dict(
                text="<b>Granularity</b>",
                x=0.30, xref="paper",
                y=1.135, yref="paper",
                showarrow=False,
                font=dict(size=11),
                xanchor="left",
            ),
        ],
    )

    fig.update_yaxes(title_text="Headcount",
                     showgrid=True, gridcolor="#EEEEEE", row=1, col=1)
    fig.update_yaxes(title_text="Terminations",
                     showgrid=True, gridcolor="#EEEEEE", row=2, col=1)
    fig.update_yaxes(title_text="Turnover %",        showgrid=True,
                     gridcolor="#EEEEEE", secondary_y=True, row=2, col=1)
    fig.update_yaxes(title_text="Early Attrition %",
                     showgrid=True, gridcolor="#EEEEEE", row=3, col=1)

    fig.update_xaxes(rangeslider_visible=False, row=1, col=1)
    fig.update_xaxes(rangeslider_visible=False, row=2, col=1)
    fig.update_xaxes(
        rangeslider_visible=True,
        rangeslider=dict(thickness=0.04),
        showgrid=True,
        gridcolor="#EEEEEE",
        row=3, col=1,
    )

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
        vertical_spacing=0.22,
        row_heights=[0.35, 0.35, 0.30],
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
            marker_color="#4C78A8",
            line_color="#4C78A8",
            hovertemplate="Dept=%{x}<br>Avg Hours=%{y:.2f}",
        ),
        row=1, col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=daily_mean["work_date"],
            y=daily_mean["roll_7"],
            name="7-day rolling",
            mode="lines",
            line=dict(color="#4C78A8", width=2),
        ),
        row=2, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=daily_mean["work_date"],
            y=daily_mean["roll_14"],
            name="14-day rolling",
            mode="lines",
            visible=False,
            line=dict(color="#F58518", width=2),
        ),
        row=2, col=1,
    )
    fig.add_trace(
        go.Scatter(
            x=daily_mean["work_date"],
            y=daily_mean["roll_30"],
            name="30-day rolling",
            mode="lines",
            visible=False,
            line=dict(color="#54A24B", width=2),
        ),
        row=2, col=1,
    )

    fig.add_trace(
        go.Bar(
            x=daily_mean["work_date"],
            y=daily_mean["overtime_count"],
            name="Overtime count",
            marker_color="#E45756",
            opacity=0.8,
        ),
        row=3, col=1,
    )

    buttons = [
        {
            "label": "Rolling 7d",
            "method": "update",
            "args": [{"visible": [True, True, False, False, True]}],
        },
        {
            "label": "Rolling 14d",
            "method": "update",
            "args": [{"visible": [True, False, True, False, True]}],
        },
        {
            "label": "Rolling 30d",
            "method": "update",
            "args": [{"visible": [True, False, False, True, True]}],
        },
    ]

    fig.update_layout(
        title=dict(
            text="Work Hours & Overtime Pattern",
            x=0.5,
            xanchor="center",
            font=dict(size=18),
            y=0.99,
        ),
        height=1300,
        showlegend=True,
        legend=dict(
            orientation="v",
            x=1.02,
            y=1.0,
            xanchor="left",
            yanchor="top",
            bgcolor="rgba(255,255,255,0.8)",
            bordercolor="#CCCCCC",
            borderwidth=1,
        ),
        margin=dict(t=150, b=80, l=80, r=160),
        plot_bgcolor="white",
        paper_bgcolor="white",
        updatemenus=[
            dict(
                buttons=buttons,
                direction="down",
                x=0.0,
                xanchor="left",
                y=1.065,
                yanchor="top",
                showactive=True,
                bgcolor="#F4F4F4",
                bordercolor="#CCCCCC",
                borderwidth=1,
                font=dict(size=13),
                pad=dict(r=12, t=8, b=8, l=8),
            ),
        ],
        annotations=[
            dict(
                text="<b>Rolling window</b>",
                x=0.0, xref="paper",
                y=1.095, yref="paper",
                showarrow=False,
                font=dict(size=12),
                xanchor="left",
            ),
        ],
    )

    fig.update_xaxes(
        tickangle=-35,
        tickfont=dict(size=10),
        showgrid=False,
        row=1, col=1,
    )
    fig.update_yaxes(
        title_text="Avg hours/day",
        showgrid=True,
        gridcolor="#EEEEEE",
        row=1, col=1,
    )

    fig.update_xaxes(
        rangeslider_visible=True,
        rangeslider=dict(thickness=0.04),
        showgrid=True,
        gridcolor="#EEEEEE",
        row=2, col=1,
    )
    fig.update_yaxes(
        title_text="Rolling avg hours",
        showgrid=True,
        gridcolor="#EEEEEE",
        row=2, col=1,
    )

    fig.update_xaxes(
        rangeslider_visible=False,
        showgrid=True,
        gridcolor="#EEEEEE",
        row=3, col=1,
    )
    fig.update_yaxes(
        title_text="Overtime count",
        showgrid=True,
        gridcolor="#EEEEEE",
        row=3, col=1,
    )

    return fig


def attendance_discipline_dashboard(daily: pd.DataFrame) -> go.Figure:
    if daily.empty:
        raise ValueError("Timesheet daily summary is empty.")

    dept = (
        daily.groupby(["department_id", "department_name"],
                      as_index=False, dropna=False)
        .agg(
            late_arrivals=("late_arrival_count", "sum"),
            early_departures=("early_departure_count", "sum"),
        )
    )
    dept["department_label"] = (
        dept["department_name"]
        .fillna(dept["department_id"])
        .fillna("Unknown")
    )
    late_dept = (
        dept.loc[dept["late_arrivals"] > 0, [
            "department_label", "late_arrivals"]]
        .sort_values("late_arrivals", ascending=True)
        .reset_index(drop=True)
    )
    early_dept = (
        dept.loc[dept["early_departures"] > 0, [
            "department_label", "early_departures"]]
        .sort_values("early_departures", ascending=True)
        .reset_index(drop=True)
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
        daily.groupby(
            ["client_employee_id", "employee_name"],
            as_index=False,
            dropna=False,
        )
        .agg(late_arrivals=("late_arrival_count", "sum"))
    )
    offenders["employee_label"] = (
        offenders["employee_name"]
        .fillna(offenders["client_employee_id"])
        .fillna("Unknown")
    )
    offenders = (
        offenders.loc[offenders["late_arrivals"] >
                      0, ["employee_label", "late_arrivals"]]
        .sort_values("late_arrivals", ascending=False)
        .head(10)
        .sort_values("late_arrivals", ascending=True)
        .reset_index(drop=True)
    )

    fig = make_subplots(
        rows=3,
        cols=1,
        vertical_spacing=0.16,
        subplot_titles=(
            "Late Arrival Frequency by Department",
            "Early Departure Count by Department",
            "Top Late Arrival Offenders",
        ),
    )

    fig.add_trace(
        go.Bar(
            x=late_dept["late_arrivals"],
            y=late_dept["department_label"],
            name="Late arrivals",
            marker_color="#4C78A8",
            orientation="h",
        ),
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Bar(
            x=early_dept["early_departures"],
            y=early_dept["department_label"],
            name="Early departures",
            marker_color="#F58518",
            orientation="h",
        ),
        row=2,
        col=1,
    )

    fig.add_trace(
        go.Bar(
            x=offenders["late_arrivals"],
            y=offenders["employee_label"],
            name="Late arrivals",
            marker_color="#E45756",
            orientation="h",
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
    print(f"Wrote {heat_html} (heatmap)")

    fig.update_layout(
        title="Attendance Discipline",
        height=1040,
        bargap=0.2,
        margin=dict(t=90, b=70, l=60, r=30),
    )
    fig.update_xaxes(title_text="Late arrivals", row=1, col=1)
    fig.update_xaxes(title_text="Early departures", row=2, col=1)
    fig.update_xaxes(title_text="Count", row=3, col=1)
    fig.update_yaxes(automargin=True, row=1, col=1)
    fig.update_yaxes(automargin=True, row=2, col=1)
    fig.update_yaxes(automargin=True, row=3, col=1)

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
