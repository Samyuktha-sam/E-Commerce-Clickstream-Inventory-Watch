"""
Clickstream event simulator for generating realistic e-commerce events.
"""

import random
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class ClickstreamSimulator:
    """Generates simulated clickstream events with configurable parameters."""

    DEFAULT_PRODUCTS = [
        "E100",
        "E101",
        "E102",
        "E103",
        "E104",
        "C200",
        "C201",
        "C202",
        "C203",
        "C204",
        "B300",
        "B301",
        "B302",
        "B303",
        "B304",
        "H400",
        "H401",
        "H402",
        "H403",
        "H404",
        "S500",
        "S501",
        "S502",
        "S503",
        "S504",
    ]
    DEFAULT_EVENTS = ["view", "add_to_cart", "purchase"]
    DEFAULT_EVENT_WEIGHTS = [70, 25, 5]  # Percentages for view, add_to_cart, purchase
    DEFAULT_CATEGORIES = ["Electronics", "Clothing", "Books", "Home", "Sports"]
    DEFAULT_PRODUCT_CATEGORY_MAP = {
        "E100": "Electronics",
        "E101": "Electronics",
        "E102": "Electronics",
        "E103": "Electronics",
        "E104": "Electronics",
        "C200": "Clothing",
        "C201": "Clothing",
        "C202": "Clothing",
        "C203": "Clothing",
        "C204": "Clothing",
        "B300": "Books",
        "B301": "Books",
        "B302": "Books",
        "B303": "Books",
        "B304": "Books",
        "H400": "Home",
        "H401": "Home",
        "H402": "Home",
        "H403": "Home",
        "H404": "Home",
        "S500": "Sports",
        "S501": "Sports",
        "S502": "Sports",
        "S503": "Sports",
        "S504": "Sports",
    }

    # Scenario configurations
    SCENARIOS = {
        "normal": {"weights": [70, 25, 5]},
        "peak_hour": {"weights": [60, 30, 10]},  # Higher conversion during peak
        "off_peak": {"weights": [80, 15, 5]},  # More browsing during off-peak
        "impulse": {"weights": [50, 40, 10]},  # Higher cart additions
        "browsing": {"weights": [85, 10, 5]},  # Mostly views
    }

    def __init__(
        self,
        products: List[str] = None,
        events: List[str] = None,
        event_weights: List[int] = None,
        user_id_range: tuple = (1000, 1100),
        categories: List[str] = None,
        scenario: str = "normal",
        product_category_map: Dict[str, str] = None,
    ):
        """
        Initialize the clickstream simulator.

        Args:
            products: List of product IDs (default: DEFAULT_PRODUCTS)
            events: List of event types (default: DEFAULT_EVENTS)
            event_weights: Weights for event distribution (default: DEFAULT_EVENT_WEIGHTS)
            user_id_range: Tuple of (min, max) for random user IDs
            categories: List of product categories (default: DEFAULT_CATEGORIES)
            scenario: Scenario type affecting event distribution (default: "normal")
            product_category_map: Mapping of product_id to category
        """
        self.products = products or self.DEFAULT_PRODUCTS
        self.events = events or self.DEFAULT_EVENTS
        self.user_id_min, self.user_id_max = user_id_range
        self.categories = categories or self.DEFAULT_CATEGORIES
        self.scenario = scenario
        self.product_category_map = product_category_map or (
            self.DEFAULT_PRODUCT_CATEGORY_MAP
            if self.products == self.DEFAULT_PRODUCTS
            and self.categories == self.DEFAULT_CATEGORIES
            else {}
        )

        if event_weights is None:
            if scenario in self.SCENARIOS:
                base_weights = self.SCENARIOS[scenario]["weights"]
            else:
                base_weights = self.DEFAULT_EVENT_WEIGHTS

            if len(base_weights) == len(self.events):
                self.event_weights = base_weights
            else:
                self.event_weights = [1] * len(self.events)
        else:
            self.event_weights = event_weights

    def generate_event(self, timestamp: Optional[datetime] = None) -> Dict:
        """
        Generate a single clickstream event.

        Args:
            timestamp: Specific timestamp for the event (default: current time)

        Returns:
            Dictionary containing event data with keys:
            - event_id: Unique UUID for the event
            - user_id: Randomly generated user ID
            - product_id: Randomly selected product
            - event_type: Event type based on configured weights
            - category: Product category
            - timestamp: Event timestamp in ISO format
        """
        event_time = timestamp or datetime.utcnow()
        product_id = random.choice(self.products)
        category = self.product_category_map.get(
            product_id, random.choice(self.categories)
        )
        event = {
            "event_id": str(uuid.uuid4()),
            "user_id": random.randint(self.user_id_min, self.user_id_max),
            "product_id": product_id,
            "event_type": random.choices(self.events, weights=self.event_weights)[0],
            "category": category,
            "timestamp": event_time.isoformat(),
        }
        return event

    def generate_batch(
        self, count: int, time_range: Optional[tuple] = None
    ) -> List[Dict]:
        """
        Generate a batch of clickstream events.

        Args:
            count: Number of events to generate
            time_range: Optional tuple of (start_datetime, end_datetime) to spread events over

        Returns:
            List of event dictionaries
        """
        if time_range:
            start_time, end_time = time_range
            time_diff = (end_time - start_time).total_seconds()
            timestamps = [
                start_time + timedelta(seconds=random.uniform(0, time_diff))
                for _ in range(count)
            ]
            timestamps.sort()  # Sort to maintain chronological order
            return [self.generate_event(ts) for ts in timestamps]
        else:
            return [self.generate_event() for _ in range(count)]

    def generate_user_session(
        self,
        user_id: Optional[int] = None,
        start_time: Optional[datetime] = None,
        session_length: int = 5,
    ) -> List[Dict]:
        """
        Generate a user session with multiple events in sequence.

        Args:
            user_id: Specific user ID (random if None)
            start_time: Session start time (current time if None)
            session_length: Number of events in the session (default: 5)

        Returns:
            List of events for the user session
        """
        user = user_id or random.randint(self.user_id_min, self.user_id_max)
        session_start = start_time or datetime.utcnow()

        # Session pattern: mostly views, some cart additions, possibly a purchase
        session_events = []
        products_viewed = random.sample(
            self.products, min(session_length, len(self.products))
        )

        current_time = session_start
        for i, product in enumerate(products_viewed):
            # Time between events: 30 seconds to 5 minutes
            if i > 0:
                current_time += timedelta(seconds=random.randint(30, 300))
            event_time = current_time

            # Event type based on position in session
            if (
                i == len(products_viewed) - 1 and random.random() < 0.3
            ):  # 30% chance of purchase at end
                event_type = "purchase"
            elif (
                i >= len(products_viewed) - 2 and random.random() < 0.5
            ):  # 50% chance of cart addition
                event_type = "add_to_cart"
            else:
                event_type = "view"

            category = self.product_category_map.get(
                product, random.choice(self.categories)
            )
            event = {
                "event_id": str(uuid.uuid4()),
                "user_id": user,
                "product_id": product,
                "event_type": event_type,
                "category": category,
                "timestamp": event_time.isoformat(),
            }
            session_events.append(event)

        return session_events

    def generate_mixed_scenarios(
        self, total_events: int, time_range: Optional[tuple] = None
    ) -> List[Dict]:
        """
        Generate events with a mix of different scenarios.

        Args:
            total_events: Total number of events to generate
            time_range: Optional time range to spread events over

        Returns:
            List of events with mixed scenarios
        """
        events = []

        # Mix of scenarios: 40% normal, 30% sessions, 20% peak, 10% browsing
        normal_count = int(total_events * 0.4)
        session_count = int(total_events * 0.3)
        peak_count = int(total_events * 0.2)
        browsing_count = total_events - normal_count - session_count - peak_count

        # Generate normal events
        normal_sim = ClickstreamSimulator(scenario="normal")
        events.extend(normal_sim.generate_batch(normal_count, time_range))

        # Generate peak hour events
        if time_range:
            start_time, end_time = time_range
            peak_start = start_time + (end_time - start_time) * 0.4  # Peak in middle
            peak_end = start_time + (end_time - start_time) * 0.6
            peak_range = (peak_start, peak_end)
        else:
            peak_range = None
        peak_sim = ClickstreamSimulator(scenario="peak_hour")
        events.extend(peak_sim.generate_batch(peak_count, peak_range))

        # Generate browsing events
        browsing_sim = ClickstreamSimulator(scenario="browsing")
        events.extend(browsing_sim.generate_batch(browsing_count, time_range))

        # Generate user sessions
        sessions = []
        while len(sessions) < session_count:
            if time_range:
                session_start = time_range[0] + timedelta(
                    seconds=random.uniform(
                        0, (time_range[1] - time_range[0]).total_seconds()
                    )
                )
            else:
                session_start = None
            session = self.generate_user_session(
                start_time=session_start, session_length=random.randint(3, 8)
            )
            sessions.extend(session)

        events.extend(sessions[:session_count])  # Take only needed number

        # Sort by timestamp
        events.sort(key=lambda x: x["timestamp"])

        return events
