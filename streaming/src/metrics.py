def calculate_conversion_rate(views, purchases):
    if views == 0:
        return 0
    return round((purchases / views) * 100, 2)


def build_product_summary(product_id, views, purchases, add_to_cart):
    return {
        "product_id": product_id,
        "views": views,
        "add_to_cart": add_to_cart,
        "purchases": purchases,
        "conversion_rate": calculate_conversion_rate(views, purchases)
    }