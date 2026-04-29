def check_flash_sale(product_id, views, purchases):
    if views > 100 and purchases < 5:
        return {
            "product_id": product_id,
            "views": views,
            "purchases": purchases,
            "message": "High interest but low conversion ,Trigger Flash Sale!"
        }
    return None