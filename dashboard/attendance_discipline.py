from __future__ import annotations

import numpy as np
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from style import (
    PALETTE,
    insights_annotation,
    theme_axes,
    theme_layout,
)
from insights import discipline_insights


def attendance_discipline_dashboard(daily: pd.DataFrame) -> go.Figure:
    if daily.empty:
        raise ValueError("Timesheet daily summary is empty.")

    dept = (
        daily.groupby(
            ["department_id", "department_label"],
            as_index=False,
            dropna=False,
        )
        .agg(
            late_arrivals=("late_arrival_count", "sum"),
            early_departures=("early_departure_count", "sum"),
            total_late_min=("late_minutes_total", "sum"),
            days=("work_date", "nunique"),
        )
    )
    dept["avg_late_min"] = pd.to_numeric(
        dept["total_late_min"] / dept["late_arrivals"].replace(0, pd.NA),
        errors="coerce",
    ).round(1)
    late_dept = (
        dept.loc[dept["late_arrivals"] > 0]
        .sort_values("late_arrivals", ascending=True)
        .reset_index(drop=True)
    )
    early_dept = (
        dept.loc[dept["early_departures"] > 0]
        .sort_values("early_departures", ascending=True)
        .reset_index(drop=True)
    )

    weekly = daily.copy()
    weekly["week_start"] = weekly["work_date"] - pd.to_timedelta(
        weekly["work_date"].dt.dayofweek, unit="D"
    )
    weekly_agg = weekly.groupby("week_start", as_index=False).agg(
        late_arrivals=("late_arrival_count", "sum"),
        early_departures=("early_departure_count", "sum"),
    )

    offenders = (
        daily.groupby(
            ["client_employee_id", "employee_name"],
            as_index=False,
            dropna=False,
        )
        .agg(
            late_arrivals=("late_arrival_count", "sum"),
            total_late_min=("late_minutes_total", "sum"),
            days_tracked=("work_date", "nunique"),
        )
    )
    offenders["employee_label"] = (
        offenders["employee_name"]
        .fillna(offenders["client_employee_id"])
        .fillna("Unknown")
    )
    offenders["late_rate"] = pd.to_numeric(
        offenders["late_arrivals"]
        / offenders["days_tracked"].replace(0, 1)
        * 100,
        errors="coerce",
    ).round(1)
    offenders = (
        offenders.loc[offenders["late_arrivals"] > 0]
        .sort_values("late_arrivals", ascending=False)
        .head(15)
        .sort_values("late_arrivals", ascending=True)
        .reset_index(drop=True)
    )

    dow = daily.copy()
    dow["dow_name"] = dow["work_date"].dt.day_name()
    dow["dow"] = dow["work_date"].dt.dayofweek
    dow_agg = (
        dow.groupby(["dow", "dow_name"], as_index=False)
        .agg(
            total_late=("late_arrival_count", "sum"),
            total_early=("early_departure_count", "sum"),
        )
        .sort_values("dow")
    )

    fig = make_subplots(
        rows=4,
        cols=1,
        vertical_spacing=0.10,
        row_heights=[0.25, 0.25, 0.25, 0.25],
        subplot_titles=(
            "Discipline Issues by Department",
            "Weekly Trend — Late Arrivals & Early Departures",
            "Day-of-Week Patterns",
            "Top Late Arrival Employees (Risk List)",
        ),
    )

    if not late_dept.empty:
        fig.add_trace(
            go.Bar(
                y=late_dept["department_label"],
                x=late_dept["late_arrivals"],
                name="Late arrivals",
                marker_color=PALETTE["danger"],
                orientation="h",
                text=late_dept["late_arrivals"],
                textposition="auto",
                hovertemplate=(
                    "<b>%{y}</b><br>Late: %{x}<br>"
                    "Avg late min: %{customdata:.0f}<extra></extra>"
                ),
                customdata=late_dept["avg_late_min"].fillna(0),
            ),
            row=1,
            col=1,
        )
    if not early_dept.empty:
        fig.add_trace(
            go.Bar(
                y=early_dept["department_label"],
                x=early_dept["early_departures"],
                name="Early departures",
                marker_color=PALETTE["warning"],
                orientation="h",
                text=early_dept["early_departures"],
                textposition="auto",
                hovertemplate="<b>%{y}</b><br>Early dep: %{x}<extra></extra>",
            ),
            row=1,
            col=1,
        )

    fig.add_trace(
        go.Bar(
            x=weekly_agg["week_start"],
            y=weekly_agg["late_arrivals"],
            name="Weekly late",
            marker_color=PALETTE["danger_light"],
            opacity=0.6,
            hovertemplate="Wk %{x|%b %d}<br>Late: %{y}<extra></extra>",
        ),
        row=2,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=weekly_agg["week_start"],
            y=weekly_agg["early_departures"],
            name="Weekly early dep",
            marker_color=PALETTE["warning"],
            opacity=0.6,
            hovertemplate="Wk %{x|%b %d}<br>Early: %{y}<extra></extra>",
        ),
        row=2,
        col=1,
    )
    if len(weekly_agg) >= 4:
        roll_late = weekly_agg["late_arrivals"].rolling(
            4, min_periods=1
        ).mean()
        fig.add_trace(
            go.Scatter(
                x=weekly_agg["week_start"],
                y=roll_late,
                mode="lines",
                name="4-wk avg (late)",
                line=dict(color=PALETTE["danger"], width=2),
                hovertemplate="%{x|%b %d}<br>4-wk avg: %{y:.1f}<extra></extra>",
            ),
            row=2,
            col=1,
        )

    fig.add_trace(
        go.Bar(
            x=dow_agg["dow_name"],
            y=dow_agg["total_late"],
            name="Late by day",
            marker_color=PALETTE["danger"],
            text=dow_agg["total_late"],
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Late: %{y}<extra></extra>",
        ),
        row=3,
        col=1,
    )
    fig.add_trace(
        go.Bar(
            x=dow_agg["dow_name"],
            y=dow_agg["total_early"],
            name="Early dep by day",
            marker_color=PALETTE["warning"],
            text=dow_agg["total_early"],
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Early: %{y}<extra></extra>",
        ),
        row=3,
        col=1,
    )

    if not offenders.empty:
        fig.add_trace(
            go.Bar(
                x=offenders["late_arrivals"],
                y=offenders["employee_label"],
                name="Late arrivals",
                marker=dict(
                    color=offenders["late_rate"],
                    colorscale=[
                        [0, PALETTE["warning"]],
                        [1, PALETTE["danger"]],
                    ],
                    showscale=True,
                    colorbar=dict(title="Late %", len=0.2, y=0.12),
                ),
                orientation="h",
                text=[f"{r:.0f}%" for r in offenders["late_rate"]],
                textposition="auto",
                hovertemplate=(
                    "<b>%{y}</b><br>Late: %{x}<br>"
                    "Days: %{customdata[0]}<extra></extra>"
                ),
                customdata=np.column_stack(
                    [
                        offenders["days_tracked"],
                        offenders["total_late_min"].fillna(0),
                    ]
                ),
            ),
            row=4,
            col=1,
        )

    all_depts = sorted(daily["department_label"].dropna().unique())
    if len(all_depts) > 1:
        dept_filter_buttons = [
            {
                "label": "All Departments",
                "method": "update",
                "args": [{"visible": [True] * len(fig.data)}],
            }
        ]
        _ = dept_filter_buttons  # kept for potential future UI extensions

    insights = discipline_insights(daily)
    extra_annotations = []
    insight_ann = insights_annotation(insights, x=0.5, y=1.1)
    if insight_ann:
        extra_annotations.append(insight_ann)

    fig.update_layout(
        **theme_layout(),
        title=dict(
            text=(
                "<b>Attendance Discipline</b><br>"
                "<span style='font-size:13px;color:#6B7280;'>"
                "Late arrivals, early departures &amp; risk identification"
                "</span>"
            ),
            x=0.5,
            xanchor="center",
            y=0.99,
        ),
        height=1500,
        barmode="group",
        margin=dict(t=200, b=80, l=160, r=80),
        annotations=list(fig.layout.annotations or []) + extra_annotations,
    )

    fig.update_xaxes(title_text="Count", row=1, col=1)
    fig.update_yaxes(automargin=True, row=1, col=1)
    fig.update_yaxes(title_text="Count", row=2, col=1)
    fig.update_yaxes(title_text="Count", row=3, col=1)
    fig.update_xaxes(title_text="Count", row=4, col=1)
    fig.update_yaxes(automargin=True, row=4, col=1)
    fig.update_xaxes(
        rangeslider_visible=True,
        rangeslider=dict(thickness=0.03),
        row=2,
        col=1,
    )

    theme_axes(fig, rows=4)
    return fig

