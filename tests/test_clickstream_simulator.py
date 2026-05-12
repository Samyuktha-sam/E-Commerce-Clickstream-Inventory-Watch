"""
Unit tests for the ClickstreamSimulator class.
"""

from datetime import datetime
from producers.clickstream_simulator import ClickstreamSimulator


class TestClickstreamSimulatorInitialization:
    """Tests for ClickstreamSimulator initialization."""

    def test_default_initialization(self):
        """Test simulator with default parameters."""
        simulator = ClickstreamSimulator()

        assert simulator.products == ClickstreamSimulator.DEFAULT_PRODUCTS
        assert simulator.events == ClickstreamSimulator.DEFAULT_EVENTS
        assert simulator.event_weights == ClickstreamSimulator.DEFAULT_EVENT_WEIGHTS
        assert simulator.user_id_min == 1000
        assert simulator.user_id_max == 1100
        assert simulator.categories == ClickstreamSimulator.DEFAULT_CATEGORIES
        assert simulator.scenario == "normal"
        assert simulator.product_category_map == (
            ClickstreamSimulator.DEFAULT_PRODUCT_CATEGORY_MAP
        )

    def test_custom_products(self):
        """Test simulator with custom products."""
        custom_products = ["PROD_A", "PROD_B", "PROD_C"]
        simulator = ClickstreamSimulator(products=custom_products)

        assert simulator.products == custom_products

    def test_custom_events(self):
        """Test simulator with custom events."""
        custom_events = ["click", "impression", "conversion"]
        simulator = ClickstreamSimulator(events=custom_events)

        assert simulator.events == custom_events

    def test_custom_event_weights(self):
        """Test simulator with custom event weights."""
        custom_weights = [50, 30, 20]
        simulator = ClickstreamSimulator(event_weights=custom_weights)

        assert simulator.event_weights == custom_weights

    def test_custom_user_id_range(self):
        """Test simulator with custom user ID range."""
        custom_range = (5000, 5500)
        simulator = ClickstreamSimulator(user_id_range=custom_range)

        assert simulator.user_id_min == 5000
        assert simulator.user_id_max == 5500

    def test_custom_categories(self):
        """Test simulator with custom categories."""
        custom_categories = ["Tech", "Fashion", "Books"]
        simulator = ClickstreamSimulator(categories=custom_categories)

        assert simulator.categories == custom_categories
        assert simulator.product_category_map == {}

    def test_custom_product_category_map(self):
        """Test simulator with custom product-category mapping."""
        custom_products = ["X1", "X2", "Y1"]
        custom_categories = ["CatX", "CatY"]
        custom_map = {"X1": "CatX", "X2": "CatX", "Y1": "CatY"}

        simulator = ClickstreamSimulator(
            products=custom_products,
            categories=custom_categories,
            product_category_map=custom_map,
        )

        assert simulator.product_category_map == custom_map

    def test_scenario_normal(self):
        """Test simulator with normal scenario."""
        simulator = ClickstreamSimulator(scenario="normal")
        assert simulator.scenario == "normal"
        assert simulator.event_weights == [70, 25, 5]

    def test_scenario_peak_hour(self):
        """Test simulator with peak_hour scenario."""
        simulator = ClickstreamSimulator(scenario="peak_hour")
        assert simulator.scenario == "peak_hour"
        assert simulator.event_weights == [60, 30, 10]

    def test_all_custom_parameters(self):
        """Test simulator with all custom parameters."""
        custom_products = ["P_CUSTOM"]
        custom_events = ["custom_event"]
        custom_weights = [100]
        custom_range = (10000, 10100)
        custom_categories = ["CustomCategory"]

        simulator = ClickstreamSimulator(
            products=custom_products,
            events=custom_events,
            event_weights=custom_weights,
            user_id_range=custom_range,
            categories=custom_categories,
            scenario="normal",
        )

        assert simulator.products == custom_products
        assert simulator.events == custom_events
        assert simulator.event_weights == custom_weights
        assert simulator.user_id_min == 10000
        assert simulator.user_id_max == 10100
        assert simulator.categories == custom_categories
        assert simulator.scenario == "normal"


class TestClickstreamSimulatorEventGeneration:
    """Tests for event generation methods."""

    def test_generate_event_structure(self):
        """Test that generated event has correct structure."""
        simulator = ClickstreamSimulator()
        event = simulator.generate_event()

        assert isinstance(event, dict)
        assert "event_id" in event
        assert "user_id" in event
        assert "product_id" in event
        assert "event_type" in event
        assert "category" in event
        assert "timestamp" in event
        assert len(event) == 6

    def test_generate_event_user_id_in_range(self):
        """Test that user_id is within specified range."""
        simulator = ClickstreamSimulator(user_id_range=(1000, 1100))

        for _ in range(100):
            event = simulator.generate_event()
            assert 1000 <= event["user_id"] <= 1100

    def test_generate_event_user_id_custom_range(self):
        """Test user_id with custom range."""
        custom_range = (5000, 5050)
        simulator = ClickstreamSimulator(user_id_range=custom_range)

        for _ in range(50):
            event = simulator.generate_event()
            assert 5000 <= event["user_id"] <= 5050

    def test_generate_event_product_id_valid(self):
        """Test that product_id is from available products."""
        simulator = ClickstreamSimulator()

        for _ in range(50):
            event = simulator.generate_event()
            assert event["product_id"] in simulator.products

    def test_generate_event_category_matches_product(self):
        """Test that category matches product mapping for defaults."""
        simulator = ClickstreamSimulator()

        for _ in range(50):
            event = simulator.generate_event()
            mapped_category = simulator.product_category_map.get(event["product_id"])
            assert event["category"] == mapped_category

    def test_generate_event_custom_products(self):
        """Test event generation with custom products."""
        custom_products = ["CUSTOM_P1", "CUSTOM_P2"]
        simulator = ClickstreamSimulator(products=custom_products)

        for _ in range(50):
            event = simulator.generate_event()
            assert event["product_id"] in custom_products

    def test_generate_event_category_valid(self):
        """Test that category is from available categories."""
        simulator = ClickstreamSimulator()

        for _ in range(50):
            event = simulator.generate_event()
            assert event["category"] in simulator.categories

    def test_generate_event_custom_categories(self):
        """Test event generation with custom categories."""
        custom_categories = ["CustomCat1", "CustomCat2"]
        simulator = ClickstreamSimulator(categories=custom_categories)

        for _ in range(50):
            event = simulator.generate_event()
            assert event["category"] in custom_categories

    def test_generate_event_type_valid(self):
        """Test that event_type is from available events."""
        simulator = ClickstreamSimulator()

        for _ in range(50):
            event = simulator.generate_event()
            assert event["event_type"] in simulator.events

    def test_generate_event_custom_events(self):
        """Test event generation with custom events."""
        custom_events = ["custom_view", "custom_purchase"]
        custom_weights = [60, 40]
        simulator = ClickstreamSimulator(
            events=custom_events, event_weights=custom_weights
        )

        for _ in range(50):
            event = simulator.generate_event()
            assert event["event_type"] in custom_events

    def test_generate_event_timestamp_valid(self):
        """Test that timestamp is valid ISO format."""
        simulator = ClickstreamSimulator()
        event = simulator.generate_event()

        # Should not raise an exception
        timestamp = datetime.fromisoformat(event["timestamp"])
        assert isinstance(timestamp, datetime)

    def test_generate_event_timestamp_recent(self):
        """Test that timestamp is recent (within last second)."""
        simulator = ClickstreamSimulator()
        before = datetime.utcnow()
        event = simulator.generate_event()
        after = datetime.utcnow()

        event_time = datetime.fromisoformat(event["timestamp"])
        assert before <= event_time <= after

    def test_generate_event_id_unique(self):
        """Test that each event has a unique event_id."""
        simulator = ClickstreamSimulator()
        events = [simulator.generate_event() for _ in range(100)]

        event_ids = [e["event_id"] for e in events]
        # All event_ids should be unique
        assert len(set(event_ids)) == len(event_ids)

    def test_generate_event_randomness(self):
        """Test that events are not identical (randomness)."""
        simulator = ClickstreamSimulator()
        events = [simulator.generate_event() for _ in range(10)]

        # Events should have different event_ids
        event_ids = [e["event_id"] for e in events]
        assert len(set(event_ids)) == len(event_ids)  # All unique


class TestClickstreamSimulatorBatchGeneration:
    """Tests for batch event generation."""

    def test_generate_batch_correct_count(self):
        """Test that batch returns correct number of events."""
        simulator = ClickstreamSimulator()

        batch_5 = simulator.generate_batch(5)
        assert len(batch_5) == 5

        batch_100 = simulator.generate_batch(100)
        assert len(batch_100) == 100

    def test_generate_batch_zero_count(self):
        """Test batch generation with zero count."""
        simulator = ClickstreamSimulator()
        batch = simulator.generate_batch(0)

        assert batch == []

    def test_generate_batch_one_count(self):
        """Test batch generation with count of 1."""
        simulator = ClickstreamSimulator()
        batch = simulator.generate_batch(1)

        assert len(batch) == 1
        assert isinstance(batch[0], dict)

    def test_generate_batch_event_validity(self):
        """Test that all events in batch are valid."""
        simulator = ClickstreamSimulator()
        batch = simulator.generate_batch(10)

        for event in batch:
            assert "event_id" in event
            assert "user_id" in event
            assert "product_id" in event
            assert "event_type" in event
            assert "category" in event
            assert "timestamp" in event
            assert event["product_id"] in simulator.products
            assert event["event_type"] in simulator.events
            assert event["category"] in simulator.categories
            assert event["category"] == simulator.product_category_map.get(
                event["product_id"]
            )

    def test_generate_batch_independence(self):
        """Test that batch events are independent."""
        simulator = ClickstreamSimulator()
        batch = simulator.generate_batch(20)

        # Collect all user_ids
        user_ids = [e["user_id"] for e in batch]

        # Should have variation (very unlikely to get same user_id 20 times)
        assert len(set(user_ids)) > 1

    def test_generate_batch_with_custom_parameters(self):
        """Test batch generation with custom parameters."""
        custom_products = ["PROD_X", "PROD_Y"]
        custom_events = ["event_x"]
        custom_weights = [100]
        custom_categories = ["CatX", "CatY"]

        simulator = ClickstreamSimulator(
            products=custom_products,
            events=custom_events,
            event_weights=custom_weights,
            categories=custom_categories,
        )

        batch = simulator.generate_batch(5)

        for event in batch:
            assert event["product_id"] in custom_products
            assert event["event_type"] == "event_x"
            assert event["category"] in custom_categories

    def test_generate_batch_with_time_range(self):
        """Test batch generation with time range."""
        from datetime import datetime, timedelta

        simulator = ClickstreamSimulator()
        start_time = datetime(2026, 5, 12, 10, 0, 0)
        end_time = datetime(2026, 5, 12, 11, 0, 0)

        batch = simulator.generate_batch(5, time_range=(start_time, end_time))

        assert len(batch) == 5
        for event in batch:
            event_time = datetime.fromisoformat(event["timestamp"])
            assert start_time <= event_time <= end_time

        # Check timestamps are sorted
        timestamps = [datetime.fromisoformat(e["timestamp"]) for e in batch]
        assert timestamps == sorted(timestamps)


class TestClickstreamSimulatorEventDistribution:
    """Tests for event type distribution accuracy."""

    def test_event_distribution_default_weights(self):
        """Test that event distribution matches expected weights."""
        simulator = ClickstreamSimulator()
        events = [simulator.generate_event() for _ in range(1000)]

        event_counts = {}
        for event in events:
            event_type = event["event_type"]
            event_counts[event_type] = event_counts.get(event_type, 0) + 1

        # With 70/20/10 weights, view should be most common
        view_count = event_counts.get("view", 0)
        add_to_cart_count = event_counts.get("add_to_cart", 0)
        purchase_count = event_counts.get("purchase", 0)

        assert view_count > add_to_cart_count
        assert add_to_cart_count > purchase_count

    def test_event_distribution_custom_weights(self):
        """Test event distribution with custom weights."""
        custom_events = ["rare", "common"]
        custom_weights = [1, 99]

        simulator = ClickstreamSimulator(
            events=custom_events, event_weights=custom_weights
        )

        events = [simulator.generate_event() for _ in range(200)]

        common_count = sum(1 for e in events if e["event_type"] == "common")
        rare_count = sum(1 for e in events if e["event_type"] == "rare")

        # "common" should be much more frequent than "rare"
        assert common_count > rare_count * 5


class TestClickstreamSimulatorEdgeCases:
    """Tests for edge cases and error handling."""

    def test_single_product(self):
        """Test simulator with single product."""
        simulator = ClickstreamSimulator(products=["SINGLE_PRODUCT"])

        for _ in range(10):
            event = simulator.generate_event()
            assert event["product_id"] == "SINGLE_PRODUCT"

    def test_single_event_type(self):
        """Test simulator with single event type."""
        simulator = ClickstreamSimulator(events=["single_event"], event_weights=[100])

        for _ in range(10):
            event = simulator.generate_event()
            assert event["event_type"] == "single_event"

    def test_large_user_id_range(self):
        """Test simulator with large user ID range."""
        simulator = ClickstreamSimulator(user_id_range=(1, 1000000))

        for _ in range(50):
            event = simulator.generate_event()
            assert 1 <= event["user_id"] <= 1000000

    def test_product_with_special_characters(self):
        """Test simulator with product IDs containing special characters."""
        special_products = ["PROD-001", "PROD_002", "PROD.003"]
        simulator = ClickstreamSimulator(products=special_products)

        for _ in range(10):
            event = simulator.generate_event()
            assert event["product_id"] in special_products

    def test_event_type_with_special_characters(self):
        """Test simulator with event types containing special characters."""
        special_events = ["event-view", "event_purchase", "event.add"]
        simulator = ClickstreamSimulator(events=special_events)

        for _ in range(10):
            event = simulator.generate_event()
            assert event["event_type"] in special_events


class TestClickstreamSimulatorScenarios:
    """Tests for scenario-based event generation."""

    def test_scenario_peak_hour_weights(self):
        """Test that peak_hour scenario has higher purchase rates."""
        simulator = ClickstreamSimulator(scenario="peak_hour")
        events = [simulator.generate_event() for _ in range(1000)]

        purchase_count = sum(1 for e in events if e["event_type"] == "purchase")
        assert purchase_count > 80  # Should be around 100 with 10% weight

    def test_scenario_browsing_weights(self):
        """Test that browsing scenario has mostly views."""
        simulator = ClickstreamSimulator(scenario="browsing")
        events = [simulator.generate_event() for _ in range(1000)]

        view_count = sum(1 for e in events if e["event_type"] == "view")
        assert view_count > 800  # Should be around 850 with 85% weight


class TestClickstreamSimulatorUserSessions:
    """Tests for user session generation."""

    def test_generate_user_session_structure(self):
        """Test that user session generates valid events."""
        simulator = ClickstreamSimulator()
        session = simulator.generate_user_session(session_length=3)

        assert len(session) == 3
        user_id = session[0]["user_id"]
        for event in session:
            assert event["user_id"] == user_id
            assert "event_id" in event
            assert "product_id" in event
            assert "event_type" in event
            assert "category" in event
            assert "timestamp" in event

    def test_generate_user_session_chronological(self):
        """Test that session events are in chronological order."""
        from datetime import datetime

        simulator = ClickstreamSimulator()
        start_time = datetime(2026, 5, 12, 10, 0, 0)
        session = simulator.generate_user_session(
            start_time=start_time, session_length=5
        )

        timestamps = [datetime.fromisoformat(e["timestamp"]) for e in session]
        assert timestamps == sorted(timestamps)
        assert all(ts >= start_time for ts in timestamps)


class TestClickstreamSimulatorMixedScenarios:
    """Tests for mixed scenario generation."""

    def test_generate_mixed_scenarios_total_count(self):
        """Test that mixed scenarios generate correct total events."""
        simulator = ClickstreamSimulator()
        events = simulator.generate_mixed_scenarios(100)

        assert len(events) == 100

    def test_generate_mixed_scenarios_time_range(self):
        """Test mixed scenarios with time range."""
        from datetime import datetime

        simulator = ClickstreamSimulator()
        start_time = datetime(2026, 5, 12, 9, 0, 0)
        end_time = datetime(2026, 5, 12, 17, 0, 0)

        events = simulator.generate_mixed_scenarios(
            50, time_range=(start_time, end_time)
        )

        assert len(events) == 50
        for event in events:
            event_time = datetime.fromisoformat(event["timestamp"])
            assert start_time <= event_time <= end_time

        # Check chronological order
        timestamps = [datetime.fromisoformat(e["timestamp"]) for e in events]
        assert timestamps == sorted(timestamps)
