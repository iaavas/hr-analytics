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
from insights import workforce_insights


def _top_categories(series: pd.Series, top: int = 6) -> list[str]:
    return series.value_counts().head(top).index.tolist()


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
            turnover, errors="coerce"
        ).round(2)
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
            turnover, errors="coerce"
        ).round(2)
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

