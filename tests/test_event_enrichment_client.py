"""Tests for the internal event enrichment service client."""

from __future__ import annotations

import json

import httpx
import pytest

from app.core.exceptions import ArticleProcessingError
from app.integrations.event_enrichment_client import enrich_article_content
from app.schemas.event_enrichment import EventEnrichmentResponse


def _valid_enrichment_payload() -> dict[str, object]:
    return {
        "article_title": "Semantic title",
        "article_summary": "Semantic summary",
        "cited_sources": ["Reuters", "AP"],
        "main_topics": ["Diplomacy", "Trade"],
        "keywords": ["summit", "sanctions"],
        "entities": {
            "persons_cited": ["Jane Doe"],
            "organizations_cited": ["United Nations"],
            "locations": ["Geneva"],
            "ethnicities_cited": ["Kurdish"],
            "religions_cited": ["Catholic"],
            "occupations_cited": ["diplomat"],
            "political_affiliations_cited": ["Labour"],
            "industries_cited": ["energy"],
            "products_cited": ["oil futures"],
            "brands_cited": ["Shell"],
        },
    }


@pytest.mark.asyncio
async def test_enrich_article_content_maps_successful_response() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == httpx.URL("https://enrichment.internal/enrich")
        assert request.method == "POST"
        assert request.headers["content-type"].startswith("application/json")
        assert json.loads(request.content) == {
            "extracted_title": "Extracted title",
            "extracted_content": "Extracted body",
        }
        return httpx.Response(
            200,
            json=_valid_enrichment_payload(),
            request=request,
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        result = await enrich_article_content(
            {"title": "Extracted title", "content": "Extracted body"},
            http_client=client,
            base_url="https://enrichment.internal",
            timeout_seconds=7.5,
        )

    assert result == EventEnrichmentResponse(
        article_title="Semantic title",
        article_summary="Semantic summary",
        cited_sources=["Reuters", "AP"],
        main_topics=["Diplomacy", "Trade"],
        keywords=["summit", "sanctions"],
        entities={
            "persons_cited": ["Jane Doe"],
            "organizations_cited": ["United Nations"],
            "locations": ["Geneva"],
            "ethnicities_cited": ["Kurdish"],
            "religions_cited": ["Catholic"],
            "occupations_cited": ["diplomat"],
            "political_affiliations_cited": ["Labour"],
            "industries_cited": ["energy"],
            "products_cited": ["oil futures"],
            "brands_cited": ["Shell"],
        },
    )


@pytest.mark.asyncio
async def test_enrich_article_content_raises_article_processing_error_on_timeout() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(
            ArticleProcessingError, match="timeout calling event enrichment service"
        ):
            await enrich_article_content(
                {"title": "Extracted title", "content": "Extracted body"},
                http_client=client,
                base_url="https://enrichment.internal",
                timeout_seconds=3.0,
            )


@pytest.mark.asyncio
async def test_enrich_article_content_rejects_invalid_response_schema() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        invalid_payload = _valid_enrichment_payload()
        invalid_payload["cited_sources"] = [{"name": "Reuters"}]
        return httpx.Response(
            200,
            json=invalid_payload,
            request=request,
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(ArticleProcessingError, match="failed schema validation"):
            await enrich_article_content(
                {"title": "Extracted title", "content": "Extracted body"},
                http_client=client,
                base_url="https://enrichment.internal",
            )


@pytest.mark.asyncio
async def test_enrich_article_content_rejects_missing_required_keys() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        invalid_payload = _valid_enrichment_payload()
        del invalid_payload["entities"]
        return httpx.Response(
            200,
            json=invalid_payload,
            request=request,
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(ArticleProcessingError, match="failed schema validation"):
            await enrich_article_content(
                {"title": "Extracted title", "content": "Extracted body"},
                http_client=client,
                base_url="https://enrichment.internal",
            )


@pytest.mark.asyncio
async def test_enrich_article_content_rejects_extra_response_keys() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        invalid_payload = _valid_enrichment_payload()
        invalid_payload["confidence"] = 0.92
        return httpx.Response(
            200,
            json=invalid_payload,
            request=request,
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(ArticleProcessingError, match="failed schema validation"):
            await enrich_article_content(
                {"title": "Extracted title", "content": "Extracted body"},
                http_client=client,
                base_url="https://enrichment.internal",
            )


@pytest.mark.asyncio
async def test_enrich_article_content_rejects_invalid_entities_shape() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        invalid_payload = _valid_enrichment_payload()
        invalid_payload["entities"] = {
            "persons_cited": ["Jane Doe"],
            "organizations_cited": ["United Nations"],
            "locations": ["Geneva"],
            "ethnicities_cited": ["Kurdish"],
            "religions_cited": ["Catholic"],
            "occupations_cited": ["diplomat"],
            "political_affiliations_cited": ["Labour"],
            "industries_cited": ["energy"],
            "brands_cited": ["Shell"],
        }
        return httpx.Response(
            200,
            json=invalid_payload,
            request=request,
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(ArticleProcessingError, match="failed schema validation"):
            await enrich_article_content(
                {"title": "Extracted title", "content": "Extracted body"},
                http_client=client,
                base_url="https://enrichment.internal",
            )


@pytest.mark.asyncio
async def test_enrich_article_content_raises_article_processing_error_on_non_2xx_response() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, json={"detail": "temporarily unavailable"}, request=request)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        with pytest.raises(
            ArticleProcessingError, match="HTTP error calling event enrichment service"
        ):
            await enrich_article_content(
                {"title": "Extracted title", "content": "Extracted body"},
                http_client=client,
                base_url="https://enrichment.internal",
            )
