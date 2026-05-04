"""
Clickstream event simulator for generating realistic e-commerce events.
"""

import random
from datetime import datetime
from typing import Dict, List


class ClickstreamSimulator:
    """Generates simulated clickstream events with configurable parameters."""

    DEFAULT_PRODUCTS = ["P100", "P101", "P102", "P103", "P104"]
    DEFAULT_EVENTS = ["view", "add_to_cart", "purchase"]
    DEFAULT_EVENT_WEIGHTS = [70, 20, 10]  # Percentages for view, add_to_cart, purchase

    def __init__(
        self,
        products: List[str] = None,
        events: List[str] = None,
        event_weights: List[int] = None,
        user_id_range: tuple = (1000, 1100),
    ):
        """
        Initialize the clickstream simulator.

        Args:
            products: List of product IDs (default: DEFAULT_PRODUCTS)
            events: List of event types (default: DEFAULT_EVENTS)
            event_weights: Weights for event distribution (default: DEFAULT_EVENT_WEIGHTS)
            user_id_range: Tuple of (min, max) for random user IDs
        """
        self.products = products or self.DEFAULT_PRODUCTS
        self.events = events or self.DEFAULT_EVENTS
        self.event_weights = event_weights or self.DEFAULT_EVENT_WEIGHTS
        self.user_id_min, self.user_id_max = user_id_range

    def generate_event(self) -> Dict:
        """
        Generate a single clickstream event.

        Returns:
            Dictionary containing event data with keys:
            - user_id: Randomly generated user ID
            - product_id: Randomly selected product
            - event_type: Event type based on configured weights
            - timestamp: Current UTC timestamp in ISO format
        """
        event = {
            "user_id": random.randint(self.user_id_min, self.user_id_max),
            "product_id": random.choice(self.products),
            "event_type": random.choices(self.events, weights=self.event_weights)[0],
            "timestamp": datetime.utcnow().isoformat(),
        }
        return event

    def generate_batch(self, count: int) -> List[Dict]:
        """
        Generate a batch of clickstream events.

        Args:
            count: Number of events to generate

        Returns:
            List of event dictionaries
        """
        return [self.generate_event() for _ in range(count)]
