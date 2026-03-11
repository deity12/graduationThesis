from __future__ import annotations

from pathlib import Path

from src.common.config import get_project_root, load_json, load_yaml


PROJECT_ROOT = get_project_root()

REQUIRED_DIRECTORIES = [
    "configs",
    "configs/data",
    "configs/llm",
    "configs/model",
    "configs/experiment",
    "src",
    "tests",
    "scripts",
    "data",
    "data/raw",
    "data/raw/prices",
    "data/raw/universe",
    "data/raw/mapping",
    "data/interim",
    "data/processed",
    "outputs",
    "logs",
]

REQUIRED_CONFIGS = [
    "configs/paths.yaml",
    "configs/data/walk_forward_2016_2023.yaml",
    "configs/llm/runtime_qwen25_32b_awq.yaml",
    "configs/llm/spillover_schema_v2_vllm_compatible.json",
    "configs/model/temporal_gru.yaml",
    "configs/experiment/base.yaml",
    "configs/experiment/stage4_feasibility.yaml",
    "configs/experiment/stage7_temporal_only.yaml",
    "configs/experiment/stage9_full_model.yaml",
]


def test_required_directories_exist() -> None:
    missing = [path for path in REQUIRED_DIRECTORIES if not (PROJECT_ROOT / path).is_dir()]
    assert not missing, f"Missing required directories: {missing}"


def test_required_configs_can_be_loaded() -> None:
    for relative_path in REQUIRED_CONFIGS:
        path = PROJECT_ROOT / relative_path
        assert path.is_file(), f"Missing config file: {relative_path}"
        if path.suffix == ".json":
            payload = load_json(path)
        else:
            payload = load_yaml(path)
        assert payload, f"Config should not be empty: {relative_path}"


def test_paths_config_points_to_existing_directories() -> None:
    paths_config = load_yaml("configs/paths.yaml")
    for key, relative_path in paths_config["paths"].items():
        target = PROJECT_ROOT / relative_path
        assert target.exists(), f"{key} points to a missing path: {relative_path}"


def test_walk_forward_protocol_is_fixed() -> None:
    config = load_yaml("configs/data/walk_forward_2016_2023.yaml")
    protocol = config["protocol"]
    assert protocol["evaluation_start"] == "2016-01-01"
    assert protocol["evaluation_end"] == "2023-12-31"
    assert protocol["warmup_start"] == "2015-09-01"
    assert protocol["warmup_end"] == "2015-12-31"
    assert protocol["warmup_usage"] == "window_initialization_only"
    assert protocol["include_warmup_in_train"] is False
    assert protocol["include_warmup_in_val"] is False
    assert protocol["include_warmup_in_test"] is False
    assert protocol["split_count"] == 4

    expected_splits = {
        "S1": ("2016-01-01", "2018-12-31", "2019-01-01", "2019-12-31", "2020-01-01", "2020-12-31"),
        "S2": ("2017-01-01", "2019-12-31", "2020-01-01", "2020-12-31", "2021-01-01", "2021-12-31"),
        "S3": ("2018-01-01", "2020-12-31", "2021-01-01", "2021-12-31", "2022-01-01", "2022-12-31"),
        "S4": ("2019-01-01", "2021-12-31", "2022-01-01", "2022-12-31", "2023-01-01", "2023-12-31"),
    }

    splits = config["splits"]
    assert [split["name"] for split in splits] == ["S1", "S2", "S3", "S4"]

    for split in splits:
        name = split["name"]
        train_start, train_end, val_start, val_end, test_start, test_end = expected_splits[name]
        assert split["train"]["start"] == train_start
        assert split["train"]["end"] == train_end
        assert split["val"]["start"] == val_start
        assert split["val"]["end"] == val_end
        assert split["test"]["start"] == test_start
        assert split["test"]["end"] == test_end
