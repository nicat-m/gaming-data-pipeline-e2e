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

import os
import sys
from urllib.parse import quote_plus

from superset.app import create_app

app = create_app()

with app.app_context():
    from flask_appbuilder.security.sqla.models import User  # noqa: F401
    from superset import db
    from superset.connectors.sqla.models import SqlaTable
    from superset.models.core import Database
    from superset.models.dashboard import Dashboard
    from superset.models.slice import Slice

    admin = db.session.query(User).filter_by(username="admin").first()
    # ClickHouse connection, built from the same env vars as the rest of the stack.
    conn_name = "Gaming Lakehouse (ClickHouse)"
    ch_db = db.session.query(Database).filter_by(database_name=conn_name).first()
    if ch_db is None:
        ch_db = Database(database_name=conn_name)
        db.session.add(ch_db)
    ch_db.set_sqlalchemy_uri(
        "clickhousedb://{u}:{p}@clickhouse:8123/{d}".format(
            u=quote_plus(os.environ["CLICKHOUSE_USER"]),
            p=quote_plus(os.environ["CLICKHOUSE_PASSWORD"]),
            d=os.environ["CLICKHOUSE_DB"],
        )
    )
    db.session.commit()

    # ---------------------------------------------------------------- datasets
    wanted_datasets = [
        ("staging", "stg_game_events"),
        ("curated", "dim_player"),
        ("curated", "fct_in_game_transaction"),
        ("serving", "v_player_engagement_daily"),
        ("serving", "v_game_revenue"),
        ("serving", "v_pipeline_health"),
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
            chart.viz_type = viz
            chart.params = _json.dumps(params)
            print(f"chart exists:   {name}")
        return chart

    def _last(col: str, unit: str, n: int) -> list[dict]:
        # Superset 4.x time filtering = a TEMPORAL_RANGE adhoc filter on the time
        # column. A rolling window anchored on "now": the "Last day"/"Last week"
        # presets are calendar-based (end at midnight today) and would hide
        # today's events.
        window = f'DATEADD(DATETIME("now"), -{n}, {unit}) : DATETIME("now")'
        return [{"expressionType": "SIMPLE", "clause": "WHERE", "subject": col,
                 "operator": "TEMPORAL_RANGE", "comparator": window}]

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
                "adhoc_filters": _last("event_time", "hour", 24),
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
                "metric": {"expressionType": "SIMPLE", "column": {"column_name": "event_id"},
                           "aggregate": "COUNT", "label": "events"},
                "groupby": ["event_type"],
                "adhoc_filters": _last("event_time", "hour", 24),
                "row_limit": 10,
                "donut": True,
                "labels_outside": True,
            },
        ),
        make_chart(
            "revenue_by_country",
            "Revenue by country (all time)",
            "echarts_timeseries_bar",
            "serving.v_player_engagement_daily",
            {
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "total_amount_usd"},
                             "aggregate": "SUM", "label": "revenue usd"}],
                "groupby": [],
                "x_axis": "country",
                "orientation": "horizontal",
                "row_limit": 15,
                "y_axis_format": "SMART_NUMBER",
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
                "adhoc_filters": _last("event_time", "day", 7),
                "y_axis_format": "SMART_NUMBER",
            },
        ),
        make_chart(
            "players_by_country",
            "Players by country",
            "pie",
            "curated.dim_player",
            {
                "metric": {"expressionType": "SIMPLE", "column": {"column_name": "player_id"},
                           "aggregate": "COUNT", "label": "players"},
                "groupby": ["country"],
                "row_limit": 15,
                "donut": True,
            },
        ),
        make_chart(
            "kpi_numbers",
            "KPI: players / events / revenue",
            "big_number_total",
            "serving.v_player_engagement_daily",
            {
                "metric": {"expressionType": "SIMPLE", "column": {"column_name": "total_amount_usd"},
                           "aggregate": "SUM", "label": "revenue"},
                "subheader": "total in-game revenue (all time)",
                "y_axis_format": "SMART_NUMBER",
            },
        ),
        make_chart(
            "revenue_by_genre",
            "Revenue by game genre",
            "echarts_timeseries_bar",
            "serving.v_game_revenue",
            {
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "revenue_usd"},
                             "aggregate": "SUM", "label": "revenue usd"}],
                "groupby": [],
                "x_axis": "genre",
                "x_axis_sort": "revenue usd",
                "x_axis_sort_asc": False,
                "row_limit": 15,
                "y_axis_format": "SMART_NUMBER",
            },
        ),
        make_chart(
            "top_games",
            "Top 10 games by revenue",
            "echarts_timeseries_bar",
            "serving.v_game_revenue",
            {
                "metrics": [{"expressionType": "SIMPLE", "column": {"column_name": "revenue_usd"},
                             "aggregate": "SUM", "label": "revenue usd"}],
                "groupby": [],
                "x_axis": "title",
                "x_axis_sort": "revenue usd",
                "x_axis_sort_asc": False,
                "orientation": "horizontal",
                "row_limit": 10,
                "y_axis_format": "SMART_NUMBER",
            },
        ),
        make_chart(
            "duplicate_rows_kpi",
            "Duplicate rows across all layers (must be 0)",
            "big_number_total",
            "serving.v_pipeline_health",
            {
                "metric": {"expressionType": "SIMPLE", "column": {"column_name": "duplicate_rows"},
                           "aggregate": "SUM", "label": "duplicate rows"},
                "subheader": "idempotency check: rows minus distinct keys",
                "y_axis_format": "SMART_NUMBER",
            },
        ),
        make_chart(
            "layer_counts_table",
            "Rows per layer and logical date",
            "table",
            "serving.v_pipeline_health",
            {
                "query_mode": "aggregate",
                "groupby": ["layer", "logical_date"],
                "metrics": [
                    {"expressionType": "SIMPLE", "column": {"column_name": "row_count"},
                     "aggregate": "SUM", "label": "rows"},
                    {"expressionType": "SIMPLE", "column": {"column_name": "distinct_keys"},
                     "aggregate": "SUM", "label": "distinct keys"},
                    {"expressionType": "SIMPLE", "column": {"column_name": "duplicate_rows"},
                     "aggregate": "SUM", "label": "duplicates"},
                ],
                "all_columns": [],
                "order_desc": False,
                "row_limit": 100,
                "page_length": 20,
            },
        ),
        make_chart(
            "top_players",
            "Top 10 players by spend",
            "table",
            "serving.v_player_engagement_daily",
            {
                "query_mode": "raw",
                "all_columns": ["player_id", "country", "transactions", "total_amount_usd", "last_txn_at"],
                "groupby": [],
                "metrics": [],
                "order_by_cols": ['["total_amount_usd", false]'],
                "row_limit": 10,
                "page_length": 10,
                "include_search": False,
            },
        ),
    ]

    # --------------------------------------------------------------- dashboard
    dash = db.session.query(Dashboard).filter_by(dashboard_title="Gaming Lakehouse — Live Overview").first()

    def _chart_node(chart_id: int, node_id: str, slice_name: str, parents: list) -> dict:
        # v2 layout: CHART nodes live INSIDE ROW nodes. meta carries chartId,
        # sliceName and the col/row dims (width in 12-col grid units, height in
        # 8px units). Without the ROW wrapper the renderer computes dimensions
        # from a missing parent and crashes with "reading 'width' of undefined".
        return {
            "type": "CHART",
            "id": node_id,
            "children": [],
            "parents": parents,
            "meta": {
                "chartId": chart_id,
                "sliceName": slice_name,
                "width": 6,
                "height": 50,
            },
        }

    def _row_node(node_id: str, children: list, parents: list) -> dict:
        return {
            "type": "ROW",
            "id": node_id,
            "children": children,
            "parents": parents,
            "meta": {"background": "BACKGROUND_TRANSPARENT"},
        }

    # Grid: 4 rows x 2 chart columns (12-col grid -> width 6 per chart).
    chart_specs = [
        (charts[0].id, "Events over time (last 24h)"),
        (charts[1].id, "Events by type (last 24h)"),
        (charts[3].id, "Transactions per hour (last 7 days)"),
        (charts[4].id, "Players by country"),
        (charts[2].id, "Revenue by country (all time)"),
        (charts[5].id, "KPI: players / events / revenue"),
        (charts[6].id, "Revenue by game genre"),
        (charts[7].id, "Top 10 games by revenue"),
        (charts[8].id, "Duplicate rows across all layers (must be 0)"),
        (charts[9].id, "Rows per layer and logical date"),
        (charts[10].id, "Top 10 players by spend"),
    ]
    node_by_id: dict[str, dict] = {}
    row_ids: list[str] = []
    for row_idx in range((len(chart_specs) + 1) // 2):
        row_id = f"ROW-{row_idx}"
        chart_ids: list[str] = []
        row_specs = chart_specs[row_idx * 2: row_idx * 2 + 2]
        for col_idx, (chart_id, slice_name) in enumerate(row_specs):
            node_id = f"CHART-{row_idx}-{col_idx}"
            node_by_id[node_id] = _chart_node(
                chart_id, node_id, slice_name,
                ["ROOT_ID", "GRID_ID", row_id],
            )
            if len(row_specs) == 1:  # odd chart count: last chart spans the full row
                node_by_id[node_id]["meta"]["width"] = 12
            chart_ids.append(node_id)
        node_by_id[row_id] = _row_node(row_id, chart_ids, ["ROOT_ID", "GRID_ID"])
        row_ids.append(row_id)

    node_by_id["GRID_ID"] = {
        "type": "GRID",
        "id": "GRID_ID",
        "children": row_ids,
        "parents": ["ROOT_ID"],
    }
    position_json = {
        "DASHBOARD_VERSION_KEY": "v2",
        "ROOT_ID": {"type": "ROOT", "id": "ROOT_ID", "children": ["GRID_ID"]},
        **node_by_id,
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
