"""Databricks Model Serving client (Foundation Model API).

Calls a chat-task serving endpoint via its OpenAI-compatible
``invocations`` URL. We use this rather than the OpenAI SDK so the
runtime dependency stays at ``httpx`` (already in use for the volume
client) and the auth path matches the rest of the backend
(``DATABRICKS_HOST`` + ``DATABRICKS_TOKEN``).

Endpoint contract:
    POST {host}/serving-endpoints/{endpoint_name}/invocations
    Authorization: Bearer <token>
    Body: { messages: [...], temperature, max_tokens, response_format }
"""
from __future__ import annotations

import json
import logging
import os
from typing import Any

import httpx

logger = logging.getLogger(__name__)


def _coalesce_content(content: object) -> str:
    """Reasoning models (e.g. gpt-oss) return ``content`` as a list of
    ``{type, text|reasoning_content|...}`` items rather than a plain
    string. Concatenate only the user-facing text parts and drop the
    chain-of-thought, which we never want to surface.
    """
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if not isinstance(item, dict):
                continue
            t = item.get("type")
            if t in ("text", "output_text"):
                txt = item.get("text") or ""
                if txt:
                    parts.append(str(txt))
            # Skip 'reasoning', 'thinking', etc. on purpose.
        return "".join(parts).strip()
    # Anything else: stringify defensively.
    return str(content) if content is not None else ""


class LLMConfigError(RuntimeError):
    """Missing endpoint / host / token."""


class LLMCallError(RuntimeError):
    """Serving endpoint returned a non-2xx or unparseable body."""


class DatabricksLLM:
    """Two-endpoint handle: a fast router model and a stronger composer.

    The split lets us pay for cheap structured-output classification on
    every turn and reserve the larger model for user-facing prose. Both
    point at Databricks-hosted endpoints, so the "all inference goes
    through Databricks" rule holds.
    """

    def __init__(
        self,
        host: str | None = None,
        token: str | None = None,
        router_endpoint: str | None = None,
        chat_endpoint: str | None = None,
        timeout: float = 30.0,
    ) -> None:
        self.host = (host or os.getenv("DATABRICKS_HOST", "")).rstrip("/")
        self.token = token or os.getenv("DATABRICKS_TOKEN", "")
        self.router_endpoint = router_endpoint or os.getenv(
            "LLM_ROUTER_ENDPOINT", "databricks-gpt-oss-20b"
        )
        self.chat_endpoint = chat_endpoint or os.getenv(
            "LLM_CHAT_ENDPOINT", "databricks-gpt-oss-120b"
        )
        if not self.host or not self.token:
            raise LLMConfigError(
                "DATABRICKS_HOST and DATABRICKS_TOKEN are required for Model Serving."
            )
        self._client = httpx.Client(
            timeout=timeout,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
        )

    def close(self) -> None:
        self._client.close()

    def chat(
        self,
        messages: list[dict[str, str]],
        *,
        endpoint: str | None = None,
        temperature: float = 0.2,
        max_tokens: int = 600,
        response_format: dict[str, Any] | None = None,
    ) -> str:
        """Call a chat endpoint and return the assistant's text content."""
        ep = endpoint or self.chat_endpoint
        url = f"{self.host}/serving-endpoints/{ep}/invocations"
        payload: dict[str, Any] = {
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        if response_format is not None:
            payload["response_format"] = response_format

        try:
            resp = self._client.post(url, content=json.dumps(payload))
        except httpx.HTTPError as exc:
            raise LLMCallError(f"Network error calling {ep}: {exc}") from exc

        if resp.status_code >= 400:
            raise LLMCallError(
                f"Model Serving rejected request ({ep}): {resp.status_code} {resp.text[:500]}"
            )

        try:
            body = resp.json()
        except json.JSONDecodeError as exc:
            raise LLMCallError(f"Invalid JSON from {ep}: {exc}") from exc

        try:
            content = body["choices"][0]["message"]["content"]
        except (KeyError, IndexError, TypeError) as exc:
            raise LLMCallError(f"Unexpected response shape from {ep}: {body}") from exc
        return _coalesce_content(content)

    def chat_json(
        self,
        messages: list[dict[str, str]],
        *,
        endpoint: str | None = None,
        temperature: float = 0.0,
        max_tokens: int = 300,
    ) -> dict[str, Any]:
        """Call a chat endpoint forcing JSON output and parse it.

        Uses ``response_format={"type": "json_object"}`` (OpenAI-compatible)
        which the Databricks Foundation Model APIs accept. If the model
        ignores it (older endpoints), we still try a best-effort parse.
        """
        raw = self.chat(
            messages,
            endpoint=endpoint or self.router_endpoint,
            temperature=temperature,
            max_tokens=max_tokens,
            response_format={"type": "json_object"},
        )
        # Strip code fences the model might still emit despite json_object.
        cleaned = raw.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.strip("`")
            if cleaned.lower().startswith("json"):
                cleaned = cleaned[4:].lstrip()
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError as exc:
            raise LLMCallError(f"Router did not return valid JSON: {raw!r}") from exc
