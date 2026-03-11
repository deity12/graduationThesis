from __future__ import annotations

from pathlib import Path

import pandas as pd

from src.common.config import load_yaml
from src.llm.stage4_feasibility import prepare_official_sample
from src.llm.structured_decode import decode_structured_output
from src.llm.vllm_client import VLLMClient, VLLMClientConfig


def test_structured_decode_converts_empty_strings_to_none() -> None:
    decoded = decode_structured_output(
        {
            "source": "",
            "targets": ["AAPL", "", "AAPL", "MSFT"],
            "event_type": "",
        }
    )
    assert decoded["source"] is None
    assert decoded["event_type"] is None
    assert decoded["targets"] == ["AAPL", "MSFT"]


def test_structured_decode_keeps_normal_strings_and_filters_empty_targets() -> None:
    decoded = decode_structured_output(
        {
            "source": "Apple Inc.",
            "targets": ["", "Microsoft", None, "NVIDIA"],
            "event_type": "partnership",
        }
    )
    assert decoded["source"] == "Apple Inc."
    assert decoded["event_type"] == "partnership"
    assert decoded["targets"] == ["Microsoft", "NVIDIA"]


def test_guided_json_payload_mode_is_available() -> None:
    schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "source": {"type": "string"},
            "targets": {"type": "array", "items": {"type": "string"}},
            "event_type": {"type": "string", "enum": ["other"]},
        },
        "required": ["source", "targets", "event_type"],
    }
    client = VLLMClient(
        VLLMClientConfig(
            base_url="http://127.0.0.1:8000/v1",
            api_key="-",
            model_name="Qwen/Qwen2.5-32B-Instruct-AWQ",
            request_mode="guided_json",
        )
    )
    payload = client.build_chat_payload(
        messages=[{"role": "user", "content": "test"}],
        schema=schema,
    )

    assert payload["guided_json"] == schema
    assert payload["response_format"] == {"type": "json_object"}


def test_official_sampling_excludes_warmup_from_denominator(tmp_path: Path) -> None:
    frame = pd.DataFrame(
        [
            {
                "news_id": "warmup-1",
                "published_date": "2015-10-02",
                "title": "Warmup row",
                "body": "",
                "summary_lsa": "",
                "summary_luhn": "",
                "summary_textrank": "",
                "summary_lexrank": "",
                "source_ticker": "AAPL",
                "source_company_name": "Apple Inc.",
                "is_mapped": True,
                "is_warmup": True,
                "in_evaluation_window": False,
            },
            {
                "news_id": "official-1",
                "published_date": "2016-01-05",
                "title": "Official row 1",
                "body": "",
                "summary_lsa": "",
                "summary_luhn": "",
                "summary_textrank": "",
                "summary_lexrank": "",
                "source_ticker": "AAPL",
                "source_company_name": "Apple Inc.",
                "is_mapped": True,
                "is_warmup": False,
                "in_evaluation_window": True,
            },
            {
                "news_id": "official-2",
                "published_date": "2016-02-11",
                "title": "Official row 2",
                "body": "",
                "summary_lsa": "",
                "summary_luhn": "",
                "summary_textrank": "",
                "summary_lexrank": "",
                "source_ticker": "MSFT",
                "source_company_name": "Microsoft",
                "is_mapped": True,
                "is_warmup": False,
                "in_evaluation_window": True,
            },
        ]
    )
    news_path = tmp_path / "news_source_mapped.parquet"
    frame.to_parquet(news_path, index=False)

    config = load_yaml("configs/llm/stage4_feasibility_2016.yaml")
    config["stage4"]["inputs"]["news_source_mapped"] = news_path.as_posix()
    config["stage4"]["sampling"]["sample_size"] = 100
    config["stage4"]["sampling"]["batch_size"] = 2

    sample_summary = prepare_official_sample(config)
    sample_frame = sample_summary["official_sample_frame"]

    assert sample_summary["warmup_candidate_count"] == 1
    assert sample_summary["official_candidate_count"] == 2
    assert sample_summary["official_sample_size"] == 2
    assert set(sample_frame["published_date"]) == {"2016-01-05", "2016-02-11"}
