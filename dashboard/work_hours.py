from __future__ import annotations

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots

from style import (
    MENU_STYLE,
    PALETTE,
    SERIES_COLORS,
    insights_annotation,
    theme_axes,
    theme_layout,
)
from insights import hours_insights


def work_hours_dashboard(
    attendance: pd.DataFrame,
    daily: pd.DataFrame,
    snapshots: pd.DataFrame,
) -> go.Figure:
    if attendance.empty or daily.empty:
        raise ValueError(
            "Attendance or daily timesheet data missing; run gold ETL first."
        )

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
            late_count=("late_arrival_count", "sum"),
        )
    )
    daily_mean["roll_7"] = daily_mean["avg_hours"].rolling(
        7, min_periods=1
    ).mean()
    daily_mean["roll_14"] = daily_mean["avg_hours"].rolling(
        14, min_periods=1
    ).mean()
    daily_mean["roll_30"] = daily_mean["avg_hours"].rolling(
        30, min_periods=1
    ).mean()

    dow_heat = daily.copy()
    dow_heat["dow"] = dow_heat["work_date"].dt.dayofweek
    dow_heat["dow_name"] = dow_heat["work_date"].dt.day_name()
    dow_agg = (
        dow_heat.groupby(["dow", "dow_name"], as_index=False)
        .agg(avg_hours=("total_hours_worked", "mean"))
        .sort_values("dow")
    )

    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=False,
        vertical_spacing=0.10,
        row_heights=[0.20, 0.30, 0.25, 0.25],
        subplot_titles=(
            "Hours Distribution by Department",
            "Rolling Average Working Hours",
            "Day-of-Week Patterns",
            "Overtime Trend with Anomaly Detection",
        ),
    )

    top_depts = (
        box_df.groupby("department")["avg_hours_per_day"]
        .count()
        .nlargest(10)
        .index
    )
    for i, dept in enumerate(top_depts):
        dept_data = box_df[box_df["department"] == dept]
        fig.add_trace(
            go.Box(
                y=dept_data["avg_hours_per_day"],
                name=dept[:20],
                boxmean=True,
                marker_color=SERIES_COLORS[i % len(SERIES_COLORS)],
                hovertemplate="<b>%{x}</b><br>Hours: %{y:.1f}<extra></extra>",
            ),
            row=1,
            col=1,
        )
    fig.add_hline(
        y=8,
        line_dash="dash",
        line_color=PALETTE["neutral_500"],
        annotation_text="8h target",
        row=1,
        col=1,
    )

    fig.add_trace(
        go.Scatter(
            x=daily_mean["work_date"],
            y=daily_mean["avg_hours"],
            mode="markers",
            name="Daily avg",
            marker=dict(color=PALETTE["neutral_300"], size=3),
            hovertemplate="%{x|%b %d}<br>Avg: %{y:.1f}h<extra></extra>",
        ),
        row=2,
        col=1,
    )

    rolling_traces_start = len(fig.data)
    for window, col_name, color, label in [
        (7, "roll_7", PALETTE["primary"], "7-day rolling"),
        (14, "roll_14", PALETTE["secondary"], "14-day rolling"),
        (30, "roll_30", PALETTE["success"], "30-day rolling"),
    ]:
        fig.add_trace(
            go.Scatter(
                x=daily_mean["work_date"],
                y=daily_mean[col_name],
                mode="lines",
                name=label,
                line=dict(color=color, width=2.5),
                visible=True if window == 7 else False,
                hovertemplate=(
                    f"%{{x|%b %d}}<br>{label}: %{{y:.2f}}h<extra></extra>"
                ),
            ),
            row=2,
            col=1,
        )
    fig.add_hline(
        y=8,
        line_dash="dash",
        line_color=PALETTE["neutral_500"],
        annotation_text="8h target",
        row=2,
        col=1,
    )

    fig.add_trace(
        go.Bar(
            x=dow_agg["dow_name"],
            y=dow_agg["avg_hours"],
            name="Avg hours by day",
            marker_color=[
                PALETTE["primary"] if h >= 8 else PALETTE["warning"]
                for h in dow_agg["avg_hours"]
            ],
            text=dow_agg["avg_hours"].round(1).astype(str) + "h",
            textposition="outside",
            hovertemplate="<b>%{x}</b><br>Avg Hours: %{y:.2f}<extra></extra>",
        ),
        row=3,
        col=1,
    )

    ot_mean = daily_mean["overtime_count"].mean()
    ot_std = daily_mean["overtime_count"].std()
    threshold = ot_mean + 2 * ot_std
    daily_mean["ot_anomaly"] = daily_mean["overtime_count"] > threshold
    fig.add_trace(
        go.Bar(
            x=daily_mean["work_date"],
            y=daily_mean["overtime_count"],
            name="Overtime events",
            marker_color=[
                PALETTE["danger"] if a else PALETTE["warning"]
                for a in daily_mean["ot_anomaly"]
            ],
            opacity=0.7,
            hovertemplate="%{x|%b %d}<br>OT: %{y}<extra></extra>",
        ),
        row=4,
        col=1,
    )
    fig.add_hline(
        y=threshold,
        line_dash="dot",
        line_color=PALETTE["danger"],
        annotation_text=f"Anomaly ({threshold:.0f})",
        row=4,
        col=1,
    )
    ot_roll = daily_mean["overtime_count"].rolling(14, min_periods=1).mean()
    fig.add_trace(
        go.Scatter(
            x=daily_mean["work_date"],
            y=ot_roll,
            mode="lines",
            name="OT 14-day avg",
            line=dict(color=PALETTE["danger"], width=2),
            hovertemplate="%{x|%b %d}<br>14d avg: %{y:.1f}<extra></extra>",
        ),
        row=4,
        col=1,
    )

    total_traces = len(fig.data)
    base_vis = [True] * total_traces
    for i in range(rolling_traces_start, rolling_traces_start + 3):
        base_vis[i] = i == rolling_traces_start

    def _rolling_vis(active_offset: int) -> list[bool]:
        vis = list(base_vis)
        for i in range(3):
            vis[rolling_traces_start + i] = i == active_offset
        return vis

    rolling_buttons = [
        {
            "label": "7-day",
            "method": "update",
            "args": [{"visible": _rolling_vis(0)}],
        },
        {
            "label": "14-day",
            "method": "update",
            "args": [{"visible": _rolling_vis(1)}],
        },
        {
            "label": "30-day",
            "method": "update",
            "args": [{"visible": _rolling_vis(2)}],
        },
    ]

    insights = hours_insights(attendance, daily)
    extra_annotations = [
        dict(
            text="<b>Rolling window</b>",
            x=0.0,
            xref="paper",
            y=1.06,
            yref="paper",
            showarrow=False,
            font=dict(size=10, color=PALETTE["neutral_500"]),
            xanchor="left",
        ),
    ]
    insight_ann = insights_annotation(insights, x=0.5, y=1.09)
    if insight_ann:
        extra_annotations.append(insight_ann)

    fig.update_layout(
        **theme_layout(),
        title=dict(
            text=(
                "<b>Work Hours &amp; Overtime</b><br>"
                "<span style='font-size:13px;color:#6B7280;'>"
                "Patterns, anomalies, and day-of-week analysis"
                "</span>"
            ),
            x=0.5,
            xanchor="center",
            y=0.99,
        ),
        height=1500,
        margin=dict(t=200, b=80, l=80, r=80),
        updatemenus=[
            dict(
                **MENU_STYLE,
                buttons=rolling_buttons,
                x=0.0,
                xanchor="left",
                y=1.04,
                yanchor="top",
            ),
        ],
        annotations=list(fig.layout.annotations or []) + extra_annotations,
    )

    fig.update_yaxes(title_text="Avg hours/day", row=1, col=1)
    fig.update_yaxes(title_text="Rolling avg hours", row=2, col=1)
    fig.update_yaxes(title_text="Avg hours", row=3, col=1)
    fig.update_yaxes(title_text="OT count", row=4, col=1)
    fig.update_xaxes(tickangle=-35, tickfont=dict(size=10), row=1, col=1)
    fig.update_xaxes(
        rangeslider_visible=True,
        rangeslider=dict(thickness=0.03),
        row=2,
        col=1,
    )

    theme_axes(fig, rows=4)
    return fig

