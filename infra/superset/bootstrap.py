"""One-shot Superset bootstrap (idempotent): creates the ClickHouse database
connection and registers the dashboard datasets.

Runs inside the superset container (has the Superset python env +
clickhouse-connect driver, installed by the container entrypoint). All
credentials come from environment variables set in docker-compose.yml -
no secret literal is stored in this repo.

    docker exec gaming-superset python /app/bootstrap.py
"""
import os

from superset import create_app  # noqa: E402

CH_USER = os.environ["CLICKHOUSE_USER"]
CH_PASSWORD = os.environ["CLICKHOUSE_PASSWORD"]
CH_DB = os.environ.get("CLICKHOUSE_DB", "gaming_lakehouse")
DB_NAME = "Gaming Lakehouse (ClickHouse)"

DATASETS = [
    ("raw", "game_events", "Raw events as landed from Kafka/RustFS (ReplacingMergeTree)."),
    ("staging", "stg_game_events", "Typed 1:1 staging view; malformed timestamps flagged NULL."),
    ("curated", "dim_player", "Player dimension (one row per player)."),
    ("curated", "fct_in_game_transaction", "In-game transaction fact (one row per purchase)."),
    ("serving", "v_player_engagement_daily", "Serving view: per-player engagement totals."),
]


def main() -> None:
    app = create_app()
    with app.app_context():
        from superset import db
        from superset.connectors.sqla.models import SqlaTable
        from superset.models.core import Database

        database = db.session.query(Database).filter_by(database_name=DB_NAME).first()
        if not database:
            database = Database(database_name=DB_NAME)
            # URI built from env - the password is stored in Superset's own
            # metadata DB (mounted volume), never in the git repo.
            database.sqlalchemy_uri = (
                f"clickhousedb://{CH_USER}:{CH_PASSWORD}@clickhouse:8123/{CH_DB}"
            )
            database.expose_in_sqllab = True
            database.allow_run_async = False
            db.session.add(database)
            db.session.commit()
            print(f"[bootstrap] created database connection '{DB_NAME}'")
        else:
            print(f"[bootstrap] database connection '{DB_NAME}' already exists")

        for schema, table, description in DATASETS:
            existing = (
                db.session.query(SqlaTable)
                .filter_by(database_id=database.id, schema=schema, table_name=table)
                .first()
            )
            if existing:
                print(f"[bootstrap] dataset exists: {schema}.{table}")
                continue
            ds = SqlaTable(
                database_id=database.id,
                schema=schema,
                table_name=table,
                description=description,
            )
            db.session.add(ds)
            db.session.commit()
            # Populate column metadata from the live table.
            ds.fetch_metadata()
            print(f"[bootstrap] registered dataset {schema}.{table}")

        print("[bootstrap] done")


if __name__ == "__main__":
    main()
