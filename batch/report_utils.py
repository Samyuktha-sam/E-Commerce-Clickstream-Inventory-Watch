from __future__ import annotations

from typing import Iterable, Mapping


def classify_user_segment(views: int, purchases: int) -> str:
    if purchases > 0:
        return "Buyer"
    if views >= 5:
        return "Window Shopper"
    return "Casual Visitor"


def build_summary_text(
    report_date: str,
    top_products: Iterable[Mapping[str, object]],
    segment_counts: Iterable[Mapping[str, object]],
    conversion_rates: Iterable[Mapping[str, object]],
) -> str:
    lines = [
        f"Daily Clickstream Summary - {report_date}",
        "",
        "Top 5 most viewed products:",
    ]
    lines.extend(
        f"- {item['product_id']}: {item['view_count']} views" for item in top_products
    )
    lines.append("")
    lines.append("User segments:")
    lines.extend(
        f"- {item['segment']}: {item['user_count']} users" for item in segment_counts
    )
    lines.append("")
    lines.append("Conversion rate by category:")
    lines.extend(
        "- {category}: {conversion_rate:.2%} ({purchase_count}/{view_count})".format(**item)
        for item in conversion_rates
    )
    return "\n".join(lines) + "\n"
