from __future__ import annotations

import pandas as pd

from style import PALETTE


def fmt_pct(n: float, signed: bool = True) -> str:
    if pd.isna(n):
        return "n/a"
    sign = "+" if signed and n >= 0 else ""
    return f"{sign}{n:.1f}%"


def fmt_delta(current: float, previous: float) -> tuple[str, str]:
    if pd.isna(current) or pd.isna(previous) or previous == 0:
        return ("n/a", PALETTE["neutral_500"])
    pct = (current - previous) / abs(previous) * 100
    sign = "+" if pct >= 0 else ""
    if pct > 5:
        color = PALETTE["danger"]
    elif pct < -2:
        color = PALETTE["success"]
    else:
        color = PALETTE["neutral_700"]
    return (f"{sign}{pct:.1f}%", color)


def trend_direction(series: pd.Series, window: int = 3) -> str:
    if len(series) < window:
        return "flat"
    recent = series.tail(window).mean()
    if len(series) >= 2 * window:
        prior = series.iloc[-(2 * window) : -window].mean()
    else:
        prior = series.head(window).mean()
    if pd.isna(recent) or pd.isna(prior):
        return "flat"
    diff_pct = (recent - prior) / abs(prior) * 100 if prior != 0 else 0
    if diff_pct > 3:
        return "rising"
    if diff_pct < -3:
        return "falling"
    return "stable"


def workforce_insights(headcount: pd.DataFrame) -> list[str]:
    if headcount.empty:
        return []
    df = headcount.sort_values("date")
    turnover_trend = trend_direction(df["turnover_rate"].dropna())
    hc_trend = trend_direction(df["active_headcount"].dropna())
    latest = df.iloc[-1]
    early_attr = latest.get("early_attrition_rate", pd.NA)
    hires_3m = int(df.tail(3)["new_hires"].sum())
    terms_3m = int(df.tail(3)["terminations"].sum())
    net_3m = hires_3m - terms_3m

    insights: list[str] = []
    if turnover_trend == "rising":
        insights.append(
            "Turnover trending upward — investigate high-turnover departments."
        )
    elif turnover_trend == "falling":
        insights.append("Turnover declining — retention efforts appear effective.")
    if not pd.isna(early_attr) and early_attr > 30:
        insights.append(
            f"Early attrition at {early_attr:.0f}% — onboarding may need attention."
        )
    if net_3m < 0:
        insights.append(f"Net negative headcount ({net_3m:+d}) over 3 months.")
    if hc_trend == "rising":
        insights.append("Headcount growth is strong and accelerating.")
    return insights


def hours_insights(attendance: pd.DataFrame, daily: pd.DataFrame) -> list[str]:
    if attendance.empty:
        return []
    latest_month = attendance["date"].max()
    latest = attendance[attendance["date"] == latest_month]
    avg_hours = latest["avg_hours_per_day"].mean()
    total_employees = max(latest["client_employee_id"].nunique(), 1)
    avg_overtime = latest["overtime_count"].sum()
    ot_rate = avg_overtime / total_employees

    insights: list[str] = []
    if avg_hours > 9:
        insights.append(f"Avg {avg_hours:.1f}h/day — potential burnout risk.")
    elif avg_hours < 7:
        insights.append(f"Avg {avg_hours:.1f}h/day — check for underutilization.")
    if ot_rate > 3:
        insights.append(f"High overtime ({ot_rate:.1f} OT/employee). Review staffing.")
    if not daily.empty:
        dc = daily.copy()
        dc["dow"] = dc["work_date"].dt.dayofweek
        if dc[dc["dow"] == 4]["overtime_count"].mean() > 0.5:
            insights.append("Friday overtime spikes detected.")
        if dc[dc["dow"] == 0]["late_arrival_count"].mean() > 0.3:
            insights.append("Monday late arrivals elevated.")
    return insights


def discipline_insights(daily: pd.DataFrame) -> list[str]:
    if daily.empty:
        return []
    total_late = int(daily["late_arrival_count"].sum())
    total_early = int(daily["early_departure_count"].sum())
    total_days = max(daily["work_date"].nunique(), 1)
    late_per_day = total_late / total_days

    chronic_late = daily.groupby("client_employee_id")["late_arrival_count"].sum()
    chronic_count = (
        int((chronic_late > chronic_late.quantile(0.9)).sum())
        if len(chronic_late) > 0
        else 0
    )

    dept_late = daily.groupby("department_label")["late_arrival_count"].sum()
    worst_dept = dept_late.idxmax() if not dept_late.empty else "Unknown"

    insights: list[str] = []
    if late_per_day > 5:
        insights.append(f"Avg {late_per_day:.1f} late arrivals/day — systemic issue.")
    if chronic_count > 0:
        insights.append(
            f"{chronic_count} chronic late arrivers (top 10th pctile)."
        )
    if not dept_late.empty:
        insights.append(f"'{worst_dept}' leads in late arrivals.")
    if total_early > total_late:
        insights.append("Early departures exceed late arrivals.")
    return insights

