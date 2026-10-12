# ADR 0003 — External game-catalog API (FreeToGame) as a second source

Date: 2026-10-12 · Status: accepted · Phase: 1/2

## Context

The only source was the synthetic generator (Postgres → Debezium → Kafka).
It is reproducible but entirely invented, and `game_name`/`game_genre` were
free text inside events. We considered replacing the generator with a public
API from <https://github.com/public-apis/public-apis>.

## Decision

Keep the generator for **player events** and add the **FreeToGame API**
(`https://www.freetogame.com/api/games`, no key, ~420 games) as a second,
real source for a **game dimension**.

Why not replace the generator: public gaming APIs expose catalog/reference
data, not the player-level sessions and purchases the business questions
(revenue per segment, churn) need; and dropping the generator would remove
the CDC path (source Postgres → Debezium → Kafka), a required component.

## Implementation

- `fetch_games_api` (Airflow, retries 3×/1 min): HTTP GET with timeout and
  in-client retry/backoff → `raw/games_catalog/dt=<ds>/games.jsonl` in RustFS
  (`ingestion/api/freetogame.py`).
- `load_games_to_clickhouse`: delete-then-insert of the date partition into
  `raw.games_catalog`.
- dbt: `stg_games` (latest snapshot, typed) → `dim_game`; `fct_in_game_transaction`
  gained `game_id`; serving view `v_game_revenue` (revenue by game/genre/publisher).
- The generator now emits real catalog ids (`540` = Overwatch, ...), so events
  join the dimension.
- 4 more DQ checks (not_null/unique `game_id`, not_null `title`, non-empty
  catalog) — total 8, all gate the run.

## Consequences

- The pipeline depends on an external service. Mitigation: retries, a clear
  failure (`GamesApiError`), URL/timeout configurable via `.env`; to demo an
  API outage set `FREETOGAME_API_URL` to an unreachable host and re-run.
- Events generated before this change carry old ids (`g-001`...) and do not
  join; the smoke report states the matched/unmatched fact counts.
- Dimension is SCD1 (latest snapshot); history is a later-phase TODO.
