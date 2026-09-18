from ingestion.producer.generator import generate_event


def test_generate_event_has_required_fields():
    event = generate_event(dirty_record_rate=0.0, schema_drift_rate=0.0)
    assert "event_id" in event
    assert "event_type" in event
    assert "player" in event
    assert "device" in event
    assert "game" in event


def test_generate_event_can_be_dirty():
    dirty_events = [generate_event(dirty_record_rate=1.0, schema_drift_rate=0.0) for _ in range(20)]
    assert any(e["player"]["player_id"] is None for e in dirty_events) or any(
        e["event_time"] == "not-a-timestamp" for e in dirty_events
    ) or any((e.get("amount_usd") or 0) < 0 for e in dirty_events)


def test_generate_event_can_drift_schema():
    drifted = [generate_event(dirty_record_rate=0.0, schema_drift_rate=1.0) for _ in range(5)]
    assert all("ab_test_cohort" in e for e in drifted)
