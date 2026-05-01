from collections import defaultdict
from streaming.src.alert_logic import check_flash_sale
from streaming.src.metrics import build_product_summary

views = defaultdict(int)
purchases = defaultdict(int)
add_to_cart = defaultdict(int)


def process_event(event):
    product_id = event["product_id"]
    event_type = event["event_type"]

    if event_type == "view":
        views[product_id] += 1
    elif event_type == "add_to_cart":
        add_to_cart[product_id] += 1
    elif event_type == "purchase":
        purchases[product_id] += 1

    summary = build_product_summary(
        product_id,
        views[product_id],
        purchases[product_id],
        add_to_cart[product_id]
    )

    alert = check_flash_sale(
        product_id,
        views[product_id],
        purchases[product_id]
    )

    return summary, alert