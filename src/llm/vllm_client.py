"""Minimal vLLM OpenAI-compatible client for Stage 4."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import requests


class VLLMClientError(RuntimeError):
    """Raised when a vLLM HTTP call fails."""


@dataclass(frozen=True)
class VLLMClientConfig:
    """HTTP and decoding settings for the Stage 4 vLLM client."""

    base_url: str
    model_name: str
    api_key: str = "-"
    timeout_seconds: float = 30.0
    temperature: float = 0.0
    top_p: float = 1.0
    max_output_tokens: int = 512
    request_mode: str = "structured_outputs"
    verify_tls: bool = True


@dataclass(frozen=True)
class VLLMPreflightResult:
    """Lightweight HTTP preflight result for a local vLLM server."""

    server_reachable: bool
    model_resolved: bool
    structured_output_supported: bool
    hosted_models: list[str] = field(default_factory=list)
    error: str | None = None

    @property
    def ready(self) -> bool:
        return self.server_reachable and self.model_resolved and self.structured_output_supported


class VLLMClient:
    """Thin wrapper around vLLM's OpenAI-compatible chat completions API."""

    def __init__(self, config: VLLMClientConfig, session: requests.Session | None = None) -> None:
        self.config = config
        self.session = session or requests.Session()

    @staticmethod
    def _normalize_api_root(base_url: str) -> str:
        normalized = base_url.rstrip("/")
        if normalized.endswith("/v1"):
            return normalized
        return f"{normalized}/v1"

    @property
    def api_root(self) -> str:
        return self._normalize_api_root(self.config.base_url)

    def _headers(self) -> dict[str, str]:
        return {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }

    def build_chat_payload(
        self,
        messages: list[dict[str, str]],
        schema: dict[str, Any],
        seed: int | None = None,
        request_mode: str | None = None,
    ) -> dict[str, Any]:
        mode = request_mode or self.config.request_mode
        payload: dict[str, Any] = {
            "model": self.config.model_name,
            "messages": messages,
            "temperature": self.config.temperature,
            "top_p": self.config.top_p,
            "max_tokens": self.config.max_output_tokens,
        }
        if seed is not None:
            payload["seed"] = seed

        if mode == "structured_outputs":
            # vLLM's OpenAI-compatible endpoint supports OpenAI-style JSON schema response_format.
            # Keep the stage-4 schema vLLM-compatible (no nullable/anyOf/oneOf/null).
            payload["response_format"] = {
                "type": "json_schema",
                "json_schema": {
                    "name": "stage4_spillover",
                    "schema": schema,
                    "strict": True,
                },
            }
        elif mode == "guided_json":
            payload["response_format"] = {"type": "json_object"}
            payload["guided_json"] = schema
        else:
            raise ValueError(f"Unsupported request_mode: {mode}")
        return payload

    def list_models(self) -> list[str]:
        url = f"{self.api_root}/models"
        try:
            response = self.session.get(
                url,
                headers=self._headers(),
                timeout=self.config.timeout_seconds,
                verify=self.config.verify_tls,
            )
            response.raise_for_status()
        except requests.RequestException as exc:
            raise VLLMClientError(f"Failed to reach vLLM models endpoint at {url}: {exc}") from exc

        payload = response.json()
        data = payload.get("data", [])
        return [str(item.get("id", "")) for item in data if item.get("id")]

    def preflight(self) -> VLLMPreflightResult:
        try:
            hosted_models = self.list_models()
        except VLLMClientError as exc:
            return VLLMPreflightResult(
                server_reachable=False,
                model_resolved=False,
                structured_output_supported=False,
                hosted_models=[],
                error=str(exc),
            )

        model_resolved = self.config.model_name in hosted_models if hosted_models else False
        return VLLMPreflightResult(
            server_reachable=True,
            model_resolved=model_resolved,
            structured_output_supported=True,
            hosted_models=hosted_models,
            error=None if model_resolved else f"Configured model not served: {self.config.model_name}",
        )

    def run_minimal_structured_output_check(self, schema: dict[str, Any]) -> tuple[bool, str | None]:
        """Probe whether the server can execute one minimal structured-output call."""
        messages = [
            {
                "role": "user",
                "content": (
                    "Return a JSON object with source as an empty string, targets as an empty list, "
                    'and event_type as "other".'
                ),
            }
        ]
        try:
            self.generate_structured_json(messages=messages, schema=schema, seed=0)
        except VLLMClientError as exc:
            return False, str(exc)
        return True, None

    def generate_structured_json(
        self,
        messages: list[dict[str, str]],
        schema: dict[str, Any],
        seed: int | None = None,
    ) -> dict[str, Any]:
        errors: list[str] = []
        if self.config.request_mode == "auto":
            request_modes = ["structured_outputs", "guided_json"]
        else:
            request_modes = [self.config.request_mode]

        url = f"{self.api_root}/chat/completions"
        for request_mode in request_modes:
            payload = self.build_chat_payload(messages=messages, schema=schema, seed=seed, request_mode=request_mode)
            try:
                response = self.session.post(
                    url,
                    headers=self._headers(),
                    json=payload,
                    timeout=self.config.timeout_seconds,
                    verify=self.config.verify_tls,
                )
                response.raise_for_status()
            except requests.RequestException as exc:
                errors.append(f"{request_mode}: {exc}")
                continue

            body = response.json()
            choices = body.get("choices", [])
            if not choices:
                raise VLLMClientError("vLLM chat completion returned no choices.")
            message = choices[0].get("message", {})
            content = message.get("content", "")
            if isinstance(content, list):
                text_parts = [str(item.get("text", "")) for item in content if isinstance(item, dict)]
                content = "".join(text_parts)
            if not isinstance(content, str):
                raise VLLMClientError(f"Unsupported chat completion content type: {type(content).__name__}")
            return {
                "raw_content": content,
                "request_mode": request_mode,
                "response": body,
            }

        joined = "; ".join(errors) if errors else "no request modes attempted"
        raise VLLMClientError(f"All vLLM structured-output requests failed: {joined}")
