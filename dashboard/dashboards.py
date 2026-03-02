from __future__ import annotations

from typing import Optional

import numpy as np
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
from insights import (
    discipline_insights,
    fmt_delta,
    fmt_pct,
    hours_insights,
    workforce_insights,
)


def _top_categories(series: pd.Series, top: int = 6) -> list[str]:
    return series.value_counts().head(top).index.tolist()


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
        ot_trend = daily.groupby("work_date", as_index=False)[
            "overtime_count"].sum()
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

    insights = workforce_insights(
        headcount) + hours_insights(attendance, daily)
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


def workforce_dashboard(
    headcount: pd.DataFrame,
    dept_metrics: pd.DataFrame,
    snapshots: pd.DataFrame,
    org_metrics: pd.DataFrame,
) -> go.Figure:
    """Headcount, turnover, early attrition with org/department/job drill-downs."""

    if snapshots.empty:
        raise ValueError(
            "Employee monthly snapshots are empty; run gold ETL first."
        )

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

    def _build_snapshot_metrics(
        group_col: str, top: Optional[int] = None
    ) -> pd.DataFrame:
        if group_col not in scoped.columns:
            return pd.DataFrame()
        df = scoped[scoped[group_col].notna()].copy()
        if df.empty:
            return df
        if top is not None:
            top_groups = _top_categories(
                df.loc[df["is_active"] == True, group_col].dropna(), top=top  # noqa: E712
            )
            df = df[df[group_col].isin(top_groups)]
            if df.empty:
                return df

        snapshot_month = df["date"].dt.to_period("M")
        hire_month = df["hire_date"].dt.to_period("M")
        term_month = df["term_date"].dt.to_period("M")
        tenure_days = (df["term_date"] - df["hire_date"]).dt.days

        df["is_active_flag"] = df["is_active"].fillna(False).astype(int)
        df["is_new_hire_month"] = (hire_month == snapshot_month).fillna(
            False
        ).astype(int)
        df["is_termination_month"] = (term_month == snapshot_month).fillna(
            False
        ).astype(int)
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
        turnover = agg["terminations"] / agg["active_headcount"].replace(
            0, pd.NA
        ) * 100
        attrition = agg["early_attrition_count"] / agg[
            "terminations"
        ].replace(0, pd.NA) * 100
        agg["turnover_rate"] = pd.to_numeric(
            turnover, errors="coerce").round(2)
        agg["early_attrition_rate"] = (
            pd.to_numeric(attrition, errors="coerce").fillna(0).round(2)
        )
        return agg

    dept_traces = _build_snapshot_metrics("department_label", top=6)
    job_traces = _build_snapshot_metrics("job_label", top=6)
    org_traces = _build_snapshot_metrics("organization_label")

    if dept_traces.empty and job_traces.empty and org_traces.empty:
        raise ValueError("No workforce trend groupings found.")

    fig = make_subplots(
        rows=4,
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.08,
        row_heights=[0.28, 0.28, 0.22, 0.22],
        specs=[[{}], [{"secondary_y": True}], [{}], [{}]],
        subplot_titles=(
            "Active Headcount Over Time",
            "Turnover Trend (terminations + rate)",
            "Early Attrition Rate",
            "Hire vs. Termination Balance",
        ),
    )

    def _termination_series(df: pd.DataFrame) -> pd.Series:
        for col in ("terminations", "total_terminations"):
            if col in df:
                return df[col]
        return pd.Series([None] * len(df))

    def _turnover_series(df: pd.DataFrame) -> pd.Series:
        if "turnover_rate" in df:
            return df["turnover_rate"]
        return pd.Series([None] * len(df))

    def _quarterize_group(df: pd.DataFrame, group_col: str) -> pd.DataFrame:
        if df.empty:
            return df
        tmp = df.copy()
        tmp["period"] = tmp["date"].dt.to_period("Q")
        agg = tmp.groupby([group_col, "period"], as_index=False).agg(
            active_headcount=("active_headcount", "last"),
            new_hires=("new_hires", "sum"),
            terminations=("terminations", "sum"),
            early_attrition_count=("early_attrition_count", "sum"),
        )
        turnover = agg["terminations"] / agg["active_headcount"].replace(
            0, pd.NA
        ) * 100
        attrition = agg["early_attrition_count"] / agg[
            "terminations"
        ].replace(0, pd.NA) * 100
        agg["turnover_rate"] = pd.to_numeric(
            turnover, errors="coerce").round(2)
        agg["early_attrition_rate"] = (
            pd.to_numeric(attrition, errors="coerce").fillna(0).round(2)
        )
        agg["date"] = agg["period"].dt.to_timestamp(how="end")
        return agg.drop(columns=["period"])

    dept_q = _quarterize_group(dept_traces, "department_label")
    job_q = _quarterize_group(job_traces, "job_label")
    org_q = _quarterize_group(org_traces, "organization_label")

    agg_monthly_x: list[list] = []
    agg_monthly_y: list[list] = []
    agg_quarterly_x: list[list] = []
    agg_quarterly_y: list[list] = []

    dept_indices: list[int] = []
    job_indices: list[int] = []
    org_indices: list[int] = []

    color_idx = 0

    def _next_color() -> str:
        nonlocal color_idx
        c = SERIES_COLORS[color_idx % len(SERIES_COLORS)]
        color_idx += 1
        return c

    def _append_agg(
        monthly_x,
        monthly_y,
        quarterly_df: Optional[pd.DataFrame],
        quarterly_col: str,
    ):
        agg_monthly_x.append(monthly_x)
        agg_monthly_y.append(monthly_y)
        if (
            quarterly_df is not None
            and not quarterly_df.empty
            and quarterly_col in quarterly_df
        ):
            agg_quarterly_x.append(quarterly_df["date"].tolist())
            agg_quarterly_y.append(quarterly_df[quarterly_col].tolist())
        else:
            agg_quarterly_x.append(monthly_x)
            agg_quarterly_y.append(monthly_y)

    def add_headcount_trace(
        df_m: pd.DataFrame,
        df_q: pd.DataFrame,
        name: str,
        visible: bool,
    ):
        c = _next_color()
        fig.add_trace(
            go.Scatter(
                x=df_m["date"],
                y=df_m["active_headcount"],
                mode="lines+markers",
                name=f"HC – {name}",
                line=dict(color=c, width=2),
                marker=dict(size=4),
                hovertemplate="%{x|%b %Y}<br>HC: %{y:,}<extra></extra>",
                visible=visible,
            ),
            row=1,
            col=1,
        )
        _append_agg(
            df_m["date"].tolist(),
            df_m["active_headcount"].tolist(),
            df_q,
            "active_headcount",
        )

    def add_turnover_trace(
        df_m: pd.DataFrame,
        df_q: pd.DataFrame,
        name: str,
        visible: bool,
    ):
        c = _next_color()
        term_m = _termination_series(df_m)
        rate_m = _turnover_series(df_m)
        fig.add_trace(
            go.Bar(
                x=df_m["date"],
                y=term_m,
                name=f"Terms – {name}",
                marker_color=c,
                opacity=0.7,
                visible=visible,
                hovertemplate="%{x|%b %Y}<br>Terms: %{y}<extra></extra>",
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
                line=dict(color=PALETTE["danger"], width=2),
                marker=dict(size=4),
                visible=visible,
                hovertemplate="%{x|%b %Y}<br>Turnover: %{y:.2f}%<extra></extra>",
            ),
            row=2,
            col=1,
            secondary_y=True,
        )
        agg_monthly_x.extend(
            [df_m["date"].tolist(), df_m["date"].tolist()]
        )
        agg_monthly_y.extend(
            [term_m.tolist(), rate_m.tolist()]
        )
        if df_q is not None and not df_q.empty:
            term_q = _termination_series(df_q)
            rate_q = _turnover_series(df_q)
            agg_quarterly_x.extend(
                [df_q["date"].tolist(), df_q["date"].tolist()]
            )
            agg_quarterly_y.extend(
                [term_q.tolist(), rate_q.tolist()]
            )
        else:
            agg_quarterly_x.extend(
                [df_m["date"].tolist(), df_m["date"].tolist()]
            )
            agg_quarterly_y.extend(
                [term_m.tolist(), rate_m.tolist()]
            )

    def add_attrition_trace(
        df_m: pd.DataFrame,
        df_q: pd.DataFrame,
        name: str,
        visible: bool,
    ):
        fig.add_trace(
            go.Scatter(
                x=df_m["date"],
                y=df_m["early_attrition_rate"],
                mode="lines+markers",
                name=f"Attrition % – {name}",
                line=dict(color=PALETTE["warning"], width=2),
                marker=dict(size=4),
                visible=visible,
                hovertemplate="%{x|%b %Y}<br>Attrition: %{y:.1f}%<extra></extra>",
            ),
            row=3,
            col=1,
        )
        _append_agg(
            df_m["date"].tolist(),
            df_m["early_attrition_rate"].tolist(),
            df_q,
            "early_attrition_rate",
        )

    def add_balance_trace(
        df_m: pd.DataFrame,
        df_q: pd.DataFrame,
        name: str,
        visible: bool,
    ):
        fig.add_trace(
            go.Bar(
                x=df_m["date"],
                y=df_m["new_hires"],
                name=f"Hires – {name}",
                marker_color=PALETTE["success"],
                opacity=0.7,
                visible=visible,
                hovertemplate="%{x|%b %Y}<br>Hires: %{y}<extra></extra>",
            ),
            row=4,
            col=1,
        )
        fig.add_trace(
            go.Bar(
                x=df_m["date"],
                y=-df_m["terminations"],
                name=f"Terms – {name}",
                marker_color=PALETTE["danger_light"],
                opacity=0.7,
                visible=visible,
                hovertemplate="%{x|%b %Y}<br>Terms: %{customdata}<extra></extra>",
                customdata=df_m["terminations"],
            ),
            row=4,
            col=1,
        )
        agg_monthly_x.extend(
            [df_m["date"].tolist(), df_m["date"].tolist()]
        )
        agg_monthly_y.extend(
            [
                df_m["new_hires"].tolist(),
                (-df_m["terminations"]).tolist(),
            ]
        )
        if df_q is not None and not df_q.empty:
            agg_quarterly_x.extend(
                [df_q["date"].tolist(), df_q["date"].tolist()]
            )
            agg_quarterly_y.extend(
                [
                    df_q["new_hires"].tolist(),
                    (-df_q["terminations"]).tolist(),
                ]
            )
        else:
            agg_quarterly_x.extend(
                [df_m["date"].tolist(), df_m["date"].tolist()]
            )
            agg_quarterly_y.extend(
                [
                    df_m["new_hires"].tolist(),
                    (-df_m["terminations"]).tolist(),
                ]
            )

    default_scope = "org" if len(
        org_traces) else "dept" if len(dept_traces) else "job"
    drill_groups: list[tuple[str, list[int]]] = []

    for dept in (
        dept_traces["department_label"].unique() if len(dept_traces) else []
    ):
        start = len(fig.data)
        df_m = dept_traces[dept_traces["department_label"] == dept]
        df_q = dept_q[dept_q["department_label"] == dept]
        vis = default_scope == "dept"
        add_headcount_trace(df_m, df_q, dept, vis)
        add_turnover_trace(df_m, df_q, dept, vis)
        add_attrition_trace(df_m, df_q, dept, vis)
        add_balance_trace(df_m, df_q, dept, vis)
        idx_range = list(range(start, len(fig.data)))
        dept_indices.extend(idx_range)
        drill_groups.append((f"Dept: {dept}", idx_range))

    for title in (
        job_traces["job_label"].unique() if len(job_traces) else []
    ):
        start = len(fig.data)
        df_m = job_traces[job_traces["job_label"] == title]
        df_q = job_q[job_q["job_label"] == title]
        vis = default_scope == "job"
        add_headcount_trace(df_m, df_q, f"Job: {title}", vis)
        add_turnover_trace(df_m, df_q, f"Job: {title}", vis)
        add_attrition_trace(df_m, df_q, f"Job: {title}", vis)
        add_balance_trace(df_m, df_q, f"Job: {title}", vis)
        idx_range = list(range(start, len(fig.data)))
        job_indices.extend(idx_range)
        drill_groups.append((f"Job: {title}", idx_range))

    for org in (
        org_traces["organization_label"].unique() if len(org_traces) else []
    ):
        start = len(fig.data)
        df_m = org_traces[org_traces["organization_label"] == org]
        df_q = org_q[org_q["organization_label"] == org]
        vis = default_scope == "org"
        add_headcount_trace(df_m, df_q, org, vis)
        add_turnover_trace(df_m, df_q, org, vis)
        add_attrition_trace(df_m, df_q, org, vis)
        add_balance_trace(df_m, df_q, org, vis)
        idx_range = list(range(start, len(fig.data)))
        org_indices.extend(idx_range)
        drill_groups.append((f"Org: {org}", idx_range))

    trace_count = len(fig.data)
    default_visible = [
        (tr.visible if tr.visible is not None else True) for tr in fig.data
    ]

    dept_vis = [i in dept_indices for i in range(trace_count)]
    job_vis = [i in job_indices for i in range(trace_count)]
    org_vis = [i in org_indices for i in range(trace_count)]

    view_buttons = []
    if any(org_vis):
        view_buttons.append(
            {
                "label": "By Organization",
                "method": "update",
                "args": [{"visible": org_vis}],
            }
        )
    if any(dept_vis):
        view_buttons.append(
            {
                "label": "By Department",
                "method": "update",
                "args": [{"visible": dept_vis}],
            }
        )
    if any(job_vis):
        view_buttons.append(
            {
                "label": "By Job Title",
                "method": "update",
                "args": [{"visible": job_vis}],
            }
        )

    drill_buttons = [
        {
            "label": "All (scope)",
            "method": "update",
            "args": [{"visible": default_visible}],
        }
    ]
    for label, idxs in drill_groups:
        vis = [i in idxs for i in range(trace_count)]
        drill_buttons.append(
            {"label": label[:36], "method": "update",
                "args": [{"visible": vis}]}
        )

    insights = workforce_insights(headcount)
    extra_annotations = [
        dict(
            text="<b>View by</b>",
            x=0.0,
            xref="paper",
            y=1.09,
            yref="paper",
            showarrow=False,
            font=dict(size=10, color=PALETTE["neutral_500"]),
            xanchor="left",
        ),
        dict(
            text="<b>Granularity</b>",
            x=0.30,
            xref="paper",
            y=1.09,
            yref="paper",
            showarrow=False,
            font=dict(size=10, color=PALETTE["neutral_500"]),
            xanchor="left",
        ),
        dict(
            text="<b>Drill down</b>",
            x=0.55,
            xref="paper",
            y=1.09,
            yref="paper",
            showarrow=False,
            font=dict(size=10, color=PALETTE["neutral_500"]),
            xanchor="left",
        ),
    ]
    insight_ann = insights_annotation(insights, x=0.5, y=1.15)
    if insight_ann:
        extra_annotations.append(insight_ann)

    fig.update_layout(
        **theme_layout(),
        title=dict(
            text=(
                "<b>Workforce Trends</b><br>"
                "<span style='font-size:13px;color:#6B7280;'>"
                "Headcount, turnover, attrition &amp; hiring balance"
                "</span>"
            ),
            x=0.5,
            xanchor="center",
            y=0.99,
        ),
        height=1500,
        barmode="relative",
        margin=dict(t=240, b=80, l=80, r=80),
        updatemenus=[
            dict(
                **MENU_STYLE,
                buttons=view_buttons,
                x=0.0,
                xanchor="left",
                y=1.07,
                yanchor="top",
            ),
            dict(
                **MENU_STYLE,
                buttons=[
                    {
                        "label": "Monthly",
                        "method": "update",
                        "args": [
                            {"x": agg_monthly_x, "y": agg_monthly_y}
                        ],
                    },
                    {
                        "label": "Quarterly",
                        "method": "update",
                        "args": [
                            {"x": agg_quarterly_x, "y": agg_quarterly_y}
                        ],
                    },
                ],
                x=0.30,
                xanchor="left",
                y=1.07,
                yanchor="top",
            ),
            dict(
                **MENU_STYLE,
                buttons=drill_buttons,
                x=0.55,
                xanchor="left",
                y=1.07,
                yanchor="top",
            ),
        ],
        annotations=list(fig.layout.annotations or []) + extra_annotations,
    )

    fig.update_yaxes(title_text="Headcount", row=1, col=1)
    fig.update_yaxes(title_text="Terminations", row=2, col=1)
    fig.update_yaxes(title_text="Turnover %", secondary_y=True, row=2, col=1)
    fig.update_yaxes(title_text="Early Attrition %", row=3, col=1)
    fig.update_yaxes(title_text="Hires / Terms", row=4, col=1)
    fig.add_hline(
        y=0,
        line_dash="solid",
        line_color=PALETTE["neutral_300"],
        row=4,
        col=1,
    )

    for r in range(1, 4):
        fig.update_xaxes(rangeslider_visible=False, row=r, col=1)

    theme_axes(fig, rows=4)
    return fig


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
