from __future__ import annotations

from scripts.etl.web.app import app


def test_stock_pool_routes_registered() -> None:
    routes = {rule.rule: set(rule.methods or []) for rule in app.url_map.iter_rules()}

    assert "/api/stock-pools" in routes
    assert "GET" in routes["/api/stock-pools"]
    assert "POST" in routes["/api/stock-pools"]

    assert "/api/stock-pools/<pool_name>" in routes
    assert "PATCH" in routes["/api/stock-pools/<pool_name>"]
    assert "DELETE" in routes["/api/stock-pools/<pool_name>"]

    assert "/api/stock-pools/<pool_name>/stocks" in routes
    assert {"GET", "POST", "DELETE"}.issubset(routes["/api/stock-pools/<pool_name>/stocks"])
