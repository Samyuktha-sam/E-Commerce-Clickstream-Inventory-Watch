from collections import defaultdict
from alert_logic import check_flash_sale

views = defaultdict(int)
purchases = defaultdict(int)

def process_event(event):
    product_id = event["product_id"]
    event_type = event["event_type"]

    if event_type == "view":
        views[product_id] += 1
    elif event_type == "purchase":
        purchases[product_id] += 1

    alert = check_flash_sale(product_id, views[product_id], purchases[product_id])

    return alert