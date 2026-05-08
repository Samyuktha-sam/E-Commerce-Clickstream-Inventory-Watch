import json
from collections import defaultdict

EVENT_FILE = "storage/raw/events.jsonl"

product_views = defaultdict(int)

user_views = defaultdict(int)
user_purchases = defaultdict(int)

with open(EVENT_FILE, "r", encoding="utf-8") as file:
    for line in file:
        event = json.loads(line)

        product_id = event["product_id"]
        user_id = event["user_id"]
        event_type = event["event_type"]

        # -----------------------------
        # Product view aggregation
        # -----------------------------
        if event_type == "view":
            product_views[product_id] += 1
            user_views[user_id] += 1

        elif event_type == "purchase":
            user_purchases[user_id] += 1

# ==================================================
# 1. TOP 5 MOST VIEWED PRODUCTS
# ==================================================

top_products = sorted(
    product_views.items(),
    key=lambda x: x[1],
    reverse=True
)[:5]

print("\n===== TOP 5 MOST VIEWED PRODUCTS =====")

for rank, (product, views) in enumerate(top_products, start=1):
    print(f"{rank}. {product} -> {views} views")

# ==================================================
# 2. DAILY USER SEGMENTATION
# ==================================================

buyers = []
window_shoppers = []

for user in user_views:

    views = user_views[user]
    purchases = user_purchases[user]

    # Buyer
    if purchases > 0:
        buyers.append(user)

    # Window shopper
    elif views >= 5 and purchases == 0:
        window_shoppers.append(user)

print("\n===== DAILY USER SEGMENTATION =====")

print(f"\nBuyers ({len(buyers)} users)")
for user in buyers:
    print(user)

print(f"\nWindow Shoppers ({len(window_shoppers)} users)")
for user in window_shoppers:
    print(user)

# ==================================================
# SAVE REPORT
# ==================================================

with open("storage/reports/daily_report.txt", "w", encoding="utf-8") as report:

    report.write("TOP 5 MOST VIEWED PRODUCTS\n")

    for rank, (product, views) in enumerate(top_products, start=1):
        report.write(f"{rank}. {product} -> {views} views\n")

    report.write("\nDAILY USER SEGMENTATION\n")

    report.write(f"\nBuyers ({len(buyers)} users)\n")
    for user in buyers:
        report.write(user + "\n")

    report.write(f"\nWindow Shoppers ({len(window_shoppers)} users)\n")
    for user in window_shoppers:
        report.write(user + "\n")

print("\nDaily batch report generated")