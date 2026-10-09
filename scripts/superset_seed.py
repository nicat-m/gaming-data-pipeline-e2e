#!/usr/bin/env python3
"""Seed Superset with the gaming dashboard (charts + dashboard layout).

Runs INSIDE the superset container (it has the superset python package and
shares the metadata DB volume):

    docker exec gaming-superset python /app/superset_home/seed.py

Idempotent: every object is looked up by name first; existing objects are
updated in place instead of duplicated.

Charts are built on the curated/serving models that the Phase 1 pipeline
already publishes in ClickHouse (staging.stg_game_events,
curated.fct_in_game_transaction, curated.dim_player,
serving.v_player_engagement_daily).
"""
from __future__ import annotations

import sys

from superset.app import create_app

app = create_app()

with app.app_context():
    from flask_appbuilder.security.sqla.models import User  # noqa: F401
    from superset import db
    from superset.models.core import Database
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice
    from superset.connectors.sqla.models import SqlaTable

    admin = db.session.query(User).filter_by(username="admin").first()
    ch_db = db.session.query(Database).filter_by(database_name="Gaming Lakehouse (ClickHouse)").one()

    # ---------------------------------------------------------------- datasets
    wanted_datasets = [
        ("staging", "stg_game_events"),
        ("curated", "dim_player"),
        ("curated", "fct_in_game_transaction"),
        ("serving", "v_player_engagement_daily"),
    ]
    datasets: dict[str, SqlaTable] = {}
    for schema, table in wanted_datasets:
        ds = (
            db.session.query(SqlaTable)
            .filter_by(database_id=ch_db.id, schema=schema, table_name=table)
            .first()
        )
        if ds is None:
            ds = SqlaTable(
                database_id=ch_db.id,
                schema=schema,
                table_name=table,
                owners=[admin] if admin else [],
            )
            db.session.add(ds)
            db.session.flush()
            ds.fetch_metadata()  # pull real columns from ClickHouse
            db.session.flush()
            print(f"dataset created: {schema}.{table}")
        else:
            if not ds.columns:
                ds.fetch_metadata()
                db.session.flush()
                print(f"dataset re-synced: {schema}.{table} (was stale/empty)")
            else:
                print(f"dataset exists:  {schema}.{table}")
        datasets[f"{schema}.{table}"] = ds

    # ------------------------------------------------------------------ charts
    def make_chart(_slug, name, viz, ds_key, params_overrides: dict):
        chart = db.session.query(Slice).filter_by(slice_name=name).first()
        import json as _json
        params = {
            "datasource": f"{datasets[ds_key].id}__table",
            "viz_type": viz,
            **params_overrides,
        }
        if chart is None:
            chart = Slice(
                slice_name=name,
                viz_type=viz,
                datasource_type="table",
                datasource_id=datasets[ds_key].id,
                params=_json.dumps(params),
                owners=[admin] if admin else [],
            )
            db.session.add(chart)
            db.session.flush()
            print(f"chart created:  {name}")
        else:
            chart.params = _json.dumps(params)
            print(f"chart exists:   {name}")
        return chart

    charts = [
        make_chart(
            "events_over_time",
            "Events over time (last 24h)",
            "echarts_timeseries_line",
            "staging.stg_game_events",
            {
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "event_id"},
                             "aggregate": "COUNT", "label": "events"}],
                "groupby": [],
                "x_axis": "event_time",
                "time_grain_sqla": "PT1H",
                "time_range": "Last 24 hours",
                "adhoc_filters": [],
                "y_axis_format": "SMART_NUMBER",
                "rich_tooltip": True,
                "show_legend": False,
            },
        ),
        make_chart(
            "events_by_type",
            "Events by type (last 24h)",
            "pie",
            "staging.stg_game_events",
            {
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "event_id"},
                             "aggregate": "COUNT", "label": "events"}],
                "groupby": ["event_type"],
                "time_range": "Last 24 hours",
                "row_limit": 10,
                "donut": True,
                "labels_outside": True,
            },
        ),
        make_chart(
            "revenue_by_country",
            "Revenue by country (all time)",
            "echarts_bar_horizontal",
            "serving.v_player_engagement_daily",
            {
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "total_amount_usd"},
                             "aggregate": "SUM", "label": "revenue usd"}],
                "groupby": ["country"],
                "row_limit": 15,
                "y_axis_format": "SMART_NUMBER",
                "sort_by_metric": True,
            },
        ),
        make_chart(
            "transactions_trend",
            "Transactions per hour (last 7 days)",
            "echarts_timeseries_bar",
            "curated.fct_in_game_transaction",
            {
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "event_id"},
                             "aggregate": "COUNT", "label": "transactions"}],
                "x_axis": "event_time",
                "time_grain_sqla": "PT1H",
                "time_range": "Last 7 days",
                "y_axis_format": "SMART_NUMBER",
            },
        ),
        make_chart(
            "players_by_country",
            "Players by country",
            "pie",
            "curated.dim_player",
            {
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "player_id"},
                             "aggregate": "COUNT", "label": "players"}],
                "groupby": ["country"],
                "row_limit": 15,
                "donut": True,
            },
        ),
        make_chart(
            "kpi_numbers",
            "KPI: players / events / revenue",
            "big_number",
            "serving.v_player_engagement_daily",
            {
                "metric": {"expressionType": "SIMPLE", "column": {"column_name": "total_amount_usd"},
                           "aggregate": "SUM", "label": "revenue"},
                "subheader": "total in-game revenue (all time)",
                "y_axis_format": "SMART_NUMBER",
            },
        ),
    ]

    # --------------------------------------------------------------- dashboard
    dash = db.session.query(Dashboard).filter_by(dashboard_title="Gaming Lakehouse — Live Overview").first()
    def _chart_node(chart_id: int, node_id: str, row: int, col: int) -> dict:
        # v2 layout: every CHART node MUST carry a meta dict with chart_id
        # (plus chartSliceId in some versions) - a node without meta makes the
        # React renderer throw and the dashboard stays on the loading spinner.
        return {
            "type": "CHART",
            "id": node_id,
            "chart_id": chart_id,
            "children": [],
            "parents": ["ROOT_ID"],
            "width": 6,
            "height": 24,
            "meta": {
                "chartId": chart_id,
                "chart_id": chart_id,
                "width": 6,
                "height": 24,
            },
        }

    CHART_NODE_IDS = ["0-c0", "0-c1", "1-c0", "1-c1", "2-c0", "2-c1"]
    position_json = {
        # two-column stacked grid, three rows
        "DASHBOARD_VERSION_KEY": "v2",
        "0-c0": _chart_node(charts[0].id, "0-c0", 0, 0),
        "0-c1": _chart_node(charts[1].id, "0-c1", 0, 1),
        "1-c0": _chart_node(charts[3].id, "1-c0", 1, 0),
        "1-c1": _chart_node(charts[4].id, "1-c1", 1, 1),
        "2-c0": _chart_node(charts[2].id, "2-c0", 2, 0),
        "2-c1": _chart_node(charts[5].id, "2-c1", 2, 1),
        "ROOT_ID": {"type": "ROOT_ID", "id": "ROOT_ID", "children": list(CHART_NODE_IDS)},
    }
    if dash is None:
        dash = Dashboard(
            dashboard_title="Gaming Lakehouse — Live Overview",
            slug="gaming-live-overview",
            published=True,
            json_metadata='{"refresh_frequency": 30, "timed_refresh_immune_slices": []}',
        )
        db.session.add(dash)
        print("dashboard created: Gaming Lakehouse — Live Overview")
    else:
        print("dashboard exists:  Gaming Lakehouse — Live Overview")
    dash.slices = charts
    import json as _json
    dash.position_json = _json.dumps(position_json)

    db.session.commit()

    # Safety net: a dataset whose columns are still empty makes every chart on it
    # fail with "Columns missing in dataset" / "Empty query?" - force a metadata
    # refresh for those (idempotent, cheap).
    for key, ds in datasets.items():
        if not ds.columns:
            ds.fetch_metadata()
            print(f"forced metadata refresh: {key}")
    db.session.commit()

    print("\nDone. Open: http://localhost:8088/superset/dashboard/gaming-live-overview/")
    sys.exit(0)
