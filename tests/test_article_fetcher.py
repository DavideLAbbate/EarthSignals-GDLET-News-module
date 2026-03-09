"""Tests for deterministic article HTML fetching."""

from __future__ import annotations

import httpx
import pytest
from unittest.mock import AsyncMock, MagicMock

from app.core.exceptions import ArticleProcessingError
from app.integrations.article_fetcher import fetch_article_html


@pytest.mark.asyncio
async def test_fetch_article_html_returns_final_url_and_html() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == httpx.URL("https://example.com/article")
        return httpx.Response(
            200,
            text="<html><body><p>Hello</p></body></html>",
            headers={"content-type": "text/html; charset=utf-8"},
            request=request,
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, follow_redirects=True) as client:
        result = await fetch_article_html("https://example.com/article", http_client=client)

    assert str(result["final_url"]) == "https://example.com/article"
    assert result["html"] == "<html><body><p>Hello</p></body></html>"


@pytest.mark.asyncio
async def test_fetch_article_html_raises_article_processing_error_on_timeout() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, follow_redirects=True) as client:
        with pytest.raises(ArticleProcessingError, match="timed out"):
            await fetch_article_html("https://example.com/article", http_client=client)


@pytest.mark.asyncio
async def test_fetch_article_html_follows_redirects_and_returns_final_url() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url == httpx.URL("https://example.com/article"):
            return httpx.Response(
                302,
                headers={"location": "https://example.com/final"},
                request=request,
            )

        assert request.url == httpx.URL("https://example.com/final")
        return httpx.Response(
            200,
            text="<html><body><p>Redirected</p></body></html>",
            headers={"content-type": "text/html"},
            request=request,
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, follow_redirects=True) as client:
        result = await fetch_article_html("https://example.com/article", http_client=client)

    assert str(result["final_url"]) == "https://example.com/final"
    assert "Redirected" in result["html"]


@pytest.mark.asyncio
async def test_fetch_article_html_overrides_injected_client_redirect_defaults() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        if request.url == httpx.URL("https://example.com/article"):
            return httpx.Response(
                302,
                headers={"location": "https://example.com/final"},
                request=request,
            )

        return httpx.Response(
            200,
            text="<html><body><p>Redirected anyway</p></body></html>",
            headers={"content-type": "text/html"},
            request=request,
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, follow_redirects=False) as client:
        result = await fetch_article_html("https://example.com/article", http_client=client)

    assert str(result["final_url"]) == "https://example.com/final"


@pytest.mark.asyncio
async def test_fetch_article_html_passes_timeout_when_client_is_injected() -> None:
    response = httpx.Response(
        200,
        text="<html><body><p>Hello</p></body></html>",
        headers={"content-type": "text/html"},
        request=httpx.Request("GET", "https://example.com/article"),
    )
    client = MagicMock()
    stream_context = AsyncMock()
    stream_context.__aenter__.return_value = response
    stream_context.__aexit__.return_value = None
    client.stream.return_value = stream_context

    await fetch_article_html(
        "https://example.com/article",
        http_client=client,
        timeout_seconds=7.5,
    )

    client.stream.assert_called_once_with(
        "GET",
        "https://example.com/article",
        follow_redirects=True,
        timeout=7.5,
    )


@pytest.mark.asyncio
async def test_fetch_article_html_rejects_oversized_response_from_content_length() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text="<html><body><p>Hello</p></body></html>",
            headers={"content-type": "text/html", "content-length": "101"},
            request=request,
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, follow_redirects=True) as client:
        with pytest.raises(ArticleProcessingError, match="too large"):
            await fetch_article_html(
                "https://example.com/article",
                http_client=client,
                max_html_bytes=100,
            )


@pytest.mark.asyncio
async def test_fetch_article_html_rejects_non_html_content() -> None:
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            text='{"status":"ok"}',
            headers={"content-type": "application/json"},
            request=request,
        )

    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport, follow_redirects=True) as client:
        with pytest.raises(ArticleProcessingError, match="non-HTML"):
            await fetch_article_html("https://example.com/article", http_client=client)
