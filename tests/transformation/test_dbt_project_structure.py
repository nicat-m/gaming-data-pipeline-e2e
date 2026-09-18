from pathlib import Path

import yaml

DBT_PROJECT_DIR = Path(__file__).resolve().parents[2] / "transformation" / "dbt_gaming"


def test_dbt_project_yml_is_valid_yaml():
    content = yaml.safe_load((DBT_PROJECT_DIR / "dbt_project.yml").read_text())
    assert content["name"] == "dbt_gaming"
    assert content["profile"] == "dbt_gaming"


def test_staging_and_curated_dirs_exist():
    assert (DBT_PROJECT_DIR / "models" / "staging" / "stg_game_events.sql").exists()
    assert (DBT_PROJECT_DIR / "models" / "curated" / "README.md").exists()
