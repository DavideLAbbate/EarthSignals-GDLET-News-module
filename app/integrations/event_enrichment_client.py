"""Internal event enrichment service client."""

from __future__ import annotations

from typing import TypedDict

import httpx
from pydantic import ValidationError

from app.core.config import get_settings
from app.core.exceptions import ArticleProcessingError
from app.schemas.event_enrichment import EventEnrichmentResponse

_ENRICHMENT_PATH = "enrich"


class ExtractedArticleInput(TypedDict):
    """Deterministic article content sent to the internal enrichment service."""

    title: str | None
    content: str


async def enrich_article_content(
    article: ExtractedArticleInput,
    *,
    http_client: httpx.AsyncClient | None = None,
    base_url: str | None = None,
    timeout_seconds: float | None = None,
) -> EventEnrichmentResponse:
    """Call the internal enrichment service with deterministic extracted article content."""
    request_url, request_timeout = _resolve_client_config(
        base_url=base_url,
        timeout_seconds=timeout_seconds,
    )
    request_payload = {
        "extracted_title": article.get("title"),
        "extracted_content": article["content"],
    }

    if http_client is not None:
        return await _enrich_article_content_with_client(
            http_client,
            request_url,
            request_payload,
            timeout_seconds=request_timeout,
        )

    async with httpx.AsyncClient() as managed_client:
        return await _enrich_article_content_with_client(
            managed_client,
            request_url,
            request_payload,
            timeout_seconds=request_timeout,
        )


async def _enrich_article_content_with_client(
    http_client: httpx.AsyncClient,
    request_url: str,
    request_payload: dict[str, str | None],
    *,
    timeout_seconds: float,
) -> EventEnrichmentResponse:
    """Call the enrichment endpoint with an injected HTTP client."""
    try:
        response = await http_client.post(
            request_url,
            json=request_payload,
            timeout=timeout_seconds,
        )
        response.raise_for_status()
    except httpx.TimeoutException as exc:
        raise ArticleProcessingError(f"timeout calling event enrichment service: {exc}") from exc
    except httpx.HTTPStatusError as exc:
        raise ArticleProcessingError(
            f"HTTP error calling event enrichment service: {exc.response.status_code}"
        ) from exc
    except httpx.RequestError as exc:
        raise ArticleProcessingError(
            f"request error calling event enrichment service: {exc}"
        ) from exc

    try:
        return EventEnrichmentResponse.model_validate(response.json())
    except (ValueError, ValidationError) as exc:
        raise ArticleProcessingError(
            f"event enrichment service response failed schema validation: {exc}"
        ) from exc


def _resolve_client_config(
    *, base_url: str | None, timeout_seconds: float | None
) -> tuple[str, float]:
    """Resolve request URL and timeout from explicit overrides or settings."""
    settings = get_settings()
    resolved_base_url = base_url or str(settings.event_enrichment_service_base_url)
    resolved_timeout_seconds = (
        timeout_seconds
        if timeout_seconds is not None
        else settings.event_enrichment_service_timeout_seconds
    )
    normalized_base_url = resolved_base_url.rstrip("/") + "/"
    return f"{normalized_base_url}{_ENRICHMENT_PATH}", resolved_timeout_seconds
