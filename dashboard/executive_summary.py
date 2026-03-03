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
from insights import (
    fmt_delta,
    fmt_pct,
    hours_insights,
    workforce_insights,
)


def executive_summary_dashboard(
    headcount: pd.DataFrame,
    dept_metrics: pd.DataFrame,
    org_metrics: pd.DataFrame,
    attendance: pd.DataFrame,
    daily: pd.DataFrame,
) -> go.Figure:
    """Top-level KPI overview with cross-domain charts."""

    fig = make_subplots(
        rows=3,
        cols=2,
        vertical_spacing=0.12,
        horizontal_spacing=0.10,
        row_heights=[0.35, 0.35, 0.30],
        subplot_titles=(
            "Headcount & Net Change",
            "Turnover Rate Trend",
            "Avg Hours/Day Distribution",
            "Overtime Events Over Time",
            "Department Health Scorecard",
            "Tenure by Department",
        ),
    )

    if not headcount.empty:
        hc = headcount.sort_values("date")
        fig.add_trace(
            go.Scatter(
                x=hc["date"],
                y=hc["active_headcount"],
                mode="lines+markers",
                name="Headcount",
                line=dict(color=PALETTE["primary"], width=2.5),
                marker=dict(size=4),
                hovertemplate="%{x|%b %Y}<br>Headcount: %{y:,}<extra></extra>",
            ),
            row=1,
            col=1,
        )
        fig.add_trace(
            go.Bar(
                x=hc["date"],
                y=hc["net_change"],
                name="Net Change",
                marker_color=[
                    PALETTE["success"] if v >= 0 else PALETTE["danger"]
                    for v in hc["net_change"]
                ],
                opacity=0.6,
                hovertemplate="%{x|%b %Y}<br>Net: %{y:+d}<extra></extra>",
            ),
            row=1,
            col=1,
        )

    if not headcount.empty:
        hc = headcount.sort_values("date")
        fig.add_trace(
            go.Scatter(
                x=hc["date"],
                y=hc["turnover_rate"],
                mode="lines+markers",
                name="Turnover %",
                line=dict(color=PALETTE["danger"], width=2),
                marker=dict(size=4),
                hovertemplate="%{x|%b %Y}<br>Turnover: %{y:.1f}%<extra></extra>",
            ),
            row=1,
            col=2,
        )
        if len(hc) >= 3:
            z = np.polyfit(range(len(hc)), hc["turnover_rate"].fillna(0), 1)
            trend_y = np.polyval(z, range(len(hc)))
            fig.add_trace(
                go.Scatter(
                    x=hc["date"],
                    y=trend_y,
                    mode="lines",
                    name="Trendline",
                    line=dict(
                        color=PALETTE["danger_light"], width=1.5, dash="dash"
                    ),
                    showlegend=False,
                    hoverinfo="skip",
                ),
                row=1,
                col=2,
            )

    if not attendance.empty:
        latest_date = attendance["date"].max()
        att_latest = attendance[attendance["date"] == latest_date]
        fig.add_trace(
            go.Violin(
                y=att_latest["avg_hours_per_day"].dropna(),
                name="Hours/Day",
                box_visible=True,
                meanline_visible=True,
                fillcolor=PALETTE["primary_light"],
                line_color=PALETTE["primary"],
                opacity=0.7,
                hovertemplate="Hours: %{y:.1f}<extra></extra>",
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

    if not daily.empty:
        ot_trend = daily.groupby("work_date", as_index=False)["overtime_count"].sum()
        ot_trend["roll_14"] = ot_trend["overtime_count"].rolling(
            14, min_periods=1
        ).mean()
        fig.add_trace(
            go.Bar(
                x=ot_trend["work_date"],
                y=ot_trend["overtime_count"],
                name="Daily OT",
                marker_color=PALETTE["warning"],
                opacity=0.4,
                hovertemplate="%{x|%b %d}<br>OT: %{y}<extra></extra>",
            ),
            row=2,
            col=2,
        )
        fig.add_trace(
            go.Scatter(
                x=ot_trend["work_date"],
                y=ot_trend["roll_14"],
                mode="lines",
                name="14-day avg",
                line=dict(color=PALETTE["danger"], width=2),
                hovertemplate="%{x|%b %d}<br>14d avg: %{y:.1f}<extra></extra>",
            ),
            row=2,
            col=2,
        )

    if not dept_metrics.empty:
        latest_dm = dept_metrics[
            dept_metrics["date"] == dept_metrics["date"].max()
        ]
        if not latest_dm.empty:
            fig.add_trace(
                go.Scatter(
                    x=latest_dm["turnover_rate"].fillna(0),
                    y=latest_dm["avg_weekly_hours"].fillna(0),
                    text=latest_dm["department_label"],
                    mode="markers+text",
                    textposition="top center",
                    textfont=dict(size=9),
                    name="Departments",
                    marker=dict(
                        size=latest_dm["active_headcount"]
                        .fillna(5)
                        .clip(lower=5)
                        * 1.5,
                        color=latest_dm["turnover_rate"].fillna(0),
                        colorscale=[
                            [0, PALETTE["success"]],
                            [0.5, PALETTE["warning"]],
                            [1, PALETTE["danger"]],
                        ],
                        showscale=True,
                        colorbar=dict(title="Turnover %", len=0.25, y=0.15),
                        line=dict(width=1, color=PALETTE["white"]),
                    ),
                    hovertemplate="<b>%{text}</b><br>Turnover: %{x:.1f}%<br>Avg Wkly Hrs: %{y:.1f}<extra></extra>",
                ),
                row=3,
                col=1,
            )

    if not dept_metrics.empty:
        latest_dm = dept_metrics[
            dept_metrics["date"] == dept_metrics["date"].max()
        ]
        top_depts = latest_dm.nlargest(8, "active_headcount")
        if not top_depts.empty:
            fig.add_trace(
                go.Bar(
                    x=top_depts["department_label"],
                    y=top_depts["avg_tenure_months"],
                    name="Avg Tenure (mo)",
                    marker_color=PALETTE["secondary"],
                    text=top_depts["avg_tenure_months"].apply(
                        lambda v: f"{v:.0f}mo" if pd.notna(v) else ""
                    ),
                    textposition="outside",
                    hovertemplate="<b>%{x}</b><br>Tenure: %{y:.1f} months<extra></extra>",
                ),
                row=3,
                col=2,
            )

    kpi_parts: list[str] = []
    if not headcount.empty:
        hc = headcount.sort_values("date")
        latest = hc.iloc[-1]
        prev = hc.iloc[-2] if len(hc) >= 2 else latest
        hc_delta, _ = fmt_delta(
            latest["active_headcount"], prev["active_headcount"]
        )
        kpi_parts.append(
            f"<b>Headcount:</b> {int(latest['active_headcount']):,} (MoM {hc_delta})"
        )
        kpi_parts.append(
            f"<b>Turnover:</b> {fmt_pct(latest.get('turnover_rate', 0), signed=False)}"
        )
        kpi_parts.append(
            f"<b>Early Attrition:</b> {fmt_pct(latest.get('early_attrition_rate', 0), signed=False)}"
        )
        net_3m = int(hc.tail(3)["net_change"].sum())
        kpi_parts.append(f"<b>Net 3-mo:</b> {net_3m:+d}")
    kpi_text = "    |    ".join(kpi_parts) if kpi_parts else ""

    insights = workforce_insights(headcount) + hours_insights(attendance, daily)
    extra_annotations = []
    if kpi_text:
        extra_annotations.append(
            dict(
                text=kpi_text,
                x=0.5,
                xref="paper",
                y=1.08,
                yref="paper",
                showarrow=False,
                font=dict(size=13, color=PALETTE["neutral_900"]),
                xanchor="center",
                align="center",
                bgcolor="rgba(255,255,255,0.95)",
                bordercolor=PALETTE["neutral_300"],
                borderwidth=1,
                borderpad=10,
            )
        )
    insight_ann = insights_annotation(insights, x=0.5, y=1.14)
    if insight_ann:
        extra_annotations.append(insight_ann)

    fig.update_layout(
        **theme_layout(),
        title=dict(
            text=(
                "<b>Executive Summary</b><br>"
                "<span style='font-size:13px;color:#6B7280;'>"
                "Workforce health at a glance"
                "</span>"
            ),
            x=0.5,
            xanchor="center",
            y=0.99,
        ),
        height=1400,
        margin=dict(t=200, b=100, l=80, r=80),
        showlegend=True,
        annotations=list(fig.layout.annotations or []) + extra_annotations,
    )

    fig.update_yaxes(title_text="Employees", row=1, col=1)
    fig.update_yaxes(title_text="Turnover %", row=1, col=2)
    fig.update_yaxes(title_text="Hours/Day", row=2, col=1)
    fig.update_yaxes(title_text="OT Count", row=2, col=2)
    fig.update_xaxes(title_text="Turnover %", row=3, col=1)
    fig.update_yaxes(title_text="Avg Weekly Hrs", row=3, col=1)
    fig.update_xaxes(tickangle=-30, row=3, col=2)
    fig.update_yaxes(title_text="Months", row=3, col=2)

    theme_axes(fig, rows=3, cols=2)
    return fig

