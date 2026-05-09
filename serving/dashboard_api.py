"""
Serving Layer — Dashboard API
FastAPI application that queries ClickHouse and serves JSON to the BI dashboard.

Endpoints:
  GET /api/top-products          — top viewed products (real-time, last 1h)
  GET /api/funnel                — view→cart→purchase funnel (last 24h)
  GET /api/alerts                — recent flash-sale alerts (last 6h)
  GET /api/user-segments         — segment counts from batch layer (today)
  GET /api/events-timeline       — events per hour (last 24h, for sparkline)
  GET /api/health                — liveness check
"""

import os
from contextlib import asynccontextmanager
from typing import Any

from clickhouse_driver import Client
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "9000"))
CLICKHOUSE_DB   = os.getenv("CLICKHOUSE_DB", "clickstream")

# ---------------------------------------------------------------------------
# Shared ClickHouse client (created once on startup)
# ---------------------------------------------------------------------------
_ch_client: Client | None = None


def get_client() -> Client:
    global _ch_client
    if _ch_client is None:
        _ch_client = Client(
            host=CLICKHOUSE_HOST,
            port=CLICKHOUSE_PORT,
            database=CLICKHOUSE_DB,
        )
    return _ch_client


def query(sql: str, params: dict | None = None) -> list[dict]:
    """Execute a SELECT and return a list of row dicts."""
    client = get_client()
    try:
        rows, columns = client.execute(sql, params or {}, with_column_types=True)
        col_names = [c[0] for c in columns]
        return [dict(zip(col_names, row)) for row in rows]
    except Exception as exc:
        # Attempt reconnect once
        global _ch_client
        _ch_client = None
        raise HTTPException(status_code=503, detail=f"ClickHouse error: {exc}") from exc


# ---------------------------------------------------------------------------
# App
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    get_client()  # warm up connection
    yield


app = FastAPI(
    title="Clickstream BI Dashboard API",
    version="1.0.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET"],
    allow_headers=["*"],
)


# ---------------------------------------------------------------------------
# Endpoints
# ---------------------------------------------------------------------------

@app.get("/api/health")
def health() -> dict:
    try:
        result = query("SELECT 1 AS ok")
        return {"status": "ok", "clickhouse": result[0]["ok"] == 1}
    except Exception as exc:
        return {"status": "degraded", "error": str(exc)}


@app.get("/api/top-products")
def top_products(limit: int = 10, hours: int = 1) -> list[dict]:
    """Top viewed products in the last N hours (speed layer)."""
    sql = """
    SELECT
        product_id,
        sumIf(event_count, event_type = 'view')        AS view_count,
        sumIf(event_count, event_type = 'purchase')    AS purchase_count,
        sumIf(event_count, event_type = 'add_to_cart') AS cart_count
    FROM mv_product_events_hourly
    WHERE hour >= now() - INTERVAL %(hours)s HOUR
    GROUP BY product_id
    ORDER BY view_count DESC
    LIMIT %(limit)s
    """
    return query(sql, {"hours": hours, "limit": limit})


@app.get("/api/funnel")
def funnel(hours: int = 24) -> list[dict]:
    """View → add-to-cart → purchase conversion funnel for the last N hours."""
    sql = """
    SELECT
        event_type,
        sum(event_count) AS total
    FROM mv_product_events_hourly
    WHERE hour >= now() - INTERVAL %(hours)s HOUR
    GROUP BY event_type
    ORDER BY total DESC
    """
    return query(sql, {"hours": hours})


@app.get("/api/alerts")
def alerts(hours: int = 6, limit: int = 50) -> list[dict]:
    """Recent flash-sale alerts from the speed layer."""
    sql = """
    SELECT
        formatDateTime(window_start, '%%Y-%%m-%%d %%H:%%i') AS window_start,
        formatDateTime(window_end,   '%%Y-%%m-%%d %%H:%%i') AS window_end,
        product_id,
        view_count,
        purchase_count,
        action
    FROM flash_sale_alerts
    WHERE window_start >= now() - INTERVAL %(hours)s HOUR
    ORDER BY window_start DESC
    LIMIT %(limit)s
    """
    return query(sql, {"hours": hours, "limit": limit})


@app.get("/api/user-segments")
def user_segments() -> list[dict]:
    """User segment counts from today's batch report."""
    sql = """
    SELECT
        segment,
        count() AS user_count
    FROM batch_user_segments
    WHERE report_date = today()
    GROUP BY segment
    ORDER BY user_count DESC
    """
    rows = query(sql)
    # Fallback: if no batch data today, derive from real-time
    if not rows:
        sql_rt = """
        SELECT
            multiIf(
                purchases > 0,           'Buyer',
                views >= 5,              'Window Shopper',
                'Casual Visitor'
            )                            AS segment,
            count()                      AS user_count
        FROM mv_user_summary_realtime
        WHERE window_day = today()
        GROUP BY segment
        ORDER BY user_count DESC
        """
        rows = query(sql_rt)
    return rows


@app.get("/api/events-timeline")
def events_timeline(hours: int = 24) -> list[dict]:
    """Event counts bucketed by hour for a sparkline chart."""
    sql = """
    SELECT
        formatDateTime(hour, '%%H:%%i') AS hour_label,
        sum(event_count)                AS total_events,
        sumIf(event_count, event_type = 'view')     AS views,
        sumIf(event_count, event_type = 'purchase') AS purchases
    FROM mv_product_events_hourly
    WHERE hour >= now() - INTERVAL %(hours)s HOUR
    GROUP BY hour
    ORDER BY hour ASC
    """
    return query(sql, {"hours": hours})


@app.get("/api/stats-summary")
def stats_summary() -> dict[str, Any]:
    """Quick KPI numbers for the dashboard header cards."""
    sql = """
    SELECT
        countDistinct(user_id)  AS active_users,
        countIf(event_type = 'view')     AS total_views,
        countIf(event_type = 'purchase') AS total_purchases,
        countDistinct(product_id)        AS active_products
    FROM raw_events
    WHERE event_time >= now() - INTERVAL 1 HOUR
    """
    rows = query(sql)
    return rows[0] if rows else {}