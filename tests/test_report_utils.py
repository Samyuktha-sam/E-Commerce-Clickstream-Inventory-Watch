from batch.report_utils import build_summary_text, classify_user_segment


def test_classify_user_segment_prefers_buyer():
    assert classify_user_segment(views=10, purchases=1) == "Buyer"


def test_classify_user_segment_marks_window_shopper():
    assert classify_user_segment(views=5, purchases=0) == "Window Shopper"


def test_build_summary_text_contains_key_sections():
    summary = build_summary_text(
        report_date="2026-05-11",
        top_products=[{"product_id": "P100", "view_count": 12}],
        segment_counts=[{"segment": "Buyer", "user_count": 3}],
        conversion_rates=[
            {
                "category": "laptops",
                "conversion_rate": 0.25,
                "purchase_count": 1,
                "view_count": 4,
            }
        ],
    )

    assert "Daily Clickstream Summary - 2026-05-11" in summary
    assert "- P100: 12 views" in summary
    assert "- Buyer: 3 users" in summary
    assert "- laptops: 25.00% (1/4)" in summary
