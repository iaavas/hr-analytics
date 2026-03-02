from __future__ import annotations

import plotly.graph_objects as go


PALETTE = {
    "primary": "#2563EB",
    "primary_light": "#60A5FA",
    "secondary": "#7C3AED",
    "success": "#059669",
    "warning": "#D97706",
    "danger": "#DC2626",
    "danger_light": "#F87171",
    "neutral_900": "#111827",
    "neutral_700": "#374151",
    "neutral_500": "#6B7280",
    "neutral_300": "#D1D5DB",
    "neutral_100": "#F3F4F6",
    "neutral_50": "#F9FAFB",
    "white": "#FFFFFF",
    "grid": "#E5E7EB",
}

SERIES_COLORS = [
    "#2563EB",
    "#7C3AED",
    "#059669",
    "#D97706",
    "#DC2626",
    "#0891B2",
    "#DB2777",
    "#4F46E5",
    "#65A30D",
    "#EA580C",
]

FONT = dict(family="Inter, system-ui, -apple-system, sans-serif")

MENU_STYLE = dict(
    direction="down",
    showactive=True,
    bgcolor=PALETTE["white"],
    bordercolor=PALETTE["neutral_300"],
    borderwidth=1,
    font=dict(size=12),
    pad=dict(l=8, r=12, t=6, b=6),
)


def theme_layout() -> dict:
    return dict(
        font=FONT,
        plot_bgcolor=PALETTE["white"],
        paper_bgcolor=PALETTE["neutral_50"],
        hovermode="x unified",
        hoverlabel=dict(
            bgcolor=PALETTE["white"],
            font_size=12,
            font_family=FONT["family"],
            bordercolor=PALETTE["neutral_300"],
        ),
        legend=dict(
            orientation="h",
            x=0.5,
            xanchor="center",
            y=-0.05,
            yanchor="top",
            bgcolor="rgba(255,255,255,0.9)",
            bordercolor=PALETTE["neutral_300"],
            borderwidth=1,
            font=dict(size=11),
        ),
    )


def theme_axes(fig: go.Figure, rows: int = 1, cols: int = 1) -> None:
    for r in range(1, rows + 1):
        for c in range(1, cols + 1):
            fig.update_xaxes(
                showgrid=True,
                gridcolor=PALETTE["grid"],
                gridwidth=1,
                zeroline=False,
                showline=True,
                linecolor=PALETTE["neutral_300"],
                tickfont=dict(size=10, color=PALETTE["neutral_500"]),
                row=r,
                col=c,
            )
            fig.update_yaxes(
                showgrid=True,
                gridcolor=PALETTE["grid"],
                gridwidth=1,
                zeroline=False,
                showline=True,
                linecolor=PALETTE["neutral_300"],
                tickfont=dict(size=10, color=PALETTE["neutral_500"]),
                row=r,
                col=c,
            )


def insights_annotation(insights: list[str], x: float = 0.5, y: float = 1.0) -> dict:
    """Build a Plotly-safe annotation using only <b>, <br>, <span>."""
    if not insights:
        return {}
    text = "<br>".join(insights)
    return dict(
        text=f"<b>Key Insights</b><br>{text}",
        x=x,
        xref="paper",
        y=y,
        yref="paper",
        showarrow=False,
        font=dict(size=11, color=PALETTE["neutral_700"]),
        xanchor="center",
        align="left",
        bgcolor="rgba(255,255,255,0.95)",
        bordercolor=PALETTE["neutral_300"],
        borderwidth=1,
        borderpad=8,
    )

