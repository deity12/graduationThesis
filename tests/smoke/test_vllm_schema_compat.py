from __future__ import annotations

from src.common.config import load_yaml
from src.llm.schema_compat import load_stage4_schema, validate_vllm_schema
from src.llm.vllm_client import VLLMClient, VLLMClientConfig


def test_project_schema_is_vllm_compatible() -> None:
    schema = load_stage4_schema("configs/llm/spillover_schema_v2_vllm_compatible.json")
    result = validate_vllm_schema(schema)
    assert result.is_compatible is True
    assert result.errors == []


def test_schema_precheck_rejects_nullable_and_extra_fields() -> None:
    bad_schema = {
        "type": "object",
        "additionalProperties": False,
        "properties": {
            "source": {"type": "string"},
            "targets": {"type": "array", "items": {"type": "string"}},
            "event_type": {"type": "string", "enum": ["other"]},
            "confidence": {"type": "number"},
        },
        "required": ["source", "targets", "event_type", "confidence"],
        "nullable": True,
    }
    result = validate_vllm_schema(bad_schema)
    assert result.is_compatible is False
    assert any("forbidden keyword 'nullable'" in error for error in result.errors)
    assert any("Top-level properties must appear exactly" in error for error in result.errors)


def test_stage4_config_and_payload_defaults_are_loadable() -> None:
    config = load_yaml("configs/llm/stage4_feasibility_2016.yaml")
    execution = config["stage4"]["execution"]
    runtime = load_yaml(config["stage4"]["runtime_config"])["runtime"]
    schema = load_stage4_schema(config["stage4"]["schema_path"])

    client = VLLMClient(
        VLLMClientConfig(
            base_url=execution["base_url"],
            api_key=execution["api_key"],
            model_name=runtime["model_name"],
            timeout_seconds=float(execution["timeout_seconds"]),
            temperature=float(runtime["temperature"]),
            top_p=float(runtime["top_p"]),
            max_output_tokens=int(runtime["max_output_tokens"]),
            request_mode=execution["request_mode"],
        )
    )
    payload = client.build_chat_payload(
        messages=[{"role": "user", "content": "test"}],
        schema=schema,
        seed=7,
    )

    assert payload["model"] == runtime["model_name"]
    assert payload["response_format"]["type"] == "json_schema"
    assert payload["response_format"]["json_schema"]["schema"] == schema
    assert payload["seed"] == 7
