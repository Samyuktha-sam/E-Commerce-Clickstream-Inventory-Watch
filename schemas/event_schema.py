VALID_EVENT_TYPES = {"view", "add_to_cart", "purchase"}

REQUIRED_FIELDS = {
    "event_id",
    "user_id",
    "session_id",
    "product_id",
    "category",
    "event_type",
    "price",
    "timestamp"
}New-Item streaming\src\metrics.py -ItemType File