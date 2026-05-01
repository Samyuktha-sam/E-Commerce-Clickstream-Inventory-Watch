from schemas.event_schema import REQUIRED_FIELDS, VALID_EVENT_TYPES


def validate_event(event: dict) -> bool:
    if not isinstance(event, dict):
        return False

    missing_fields = REQUIRED_FIELDS - event.keys()
    if missing_fields:
        return False

    if event["event_type"] not in VALID_EVENT_TYPES:
        return False

    if not isinstance(event["price"], (int, float)) or event["price"] < 0:
        return False

    return True