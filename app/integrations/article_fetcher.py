"""Deterministic article HTML fetching helpers."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import TypedDict

import httpx

from app.core.exceptions import ArticleProcessingError

_DEFAULT_MAX_HTML_BYTES = 1_000_000


class ArticleHtmlPayload(TypedDict):
    """Fetched article HTML and the resolved final URL."""

    final_url: str
    html: str


async def fetch_article_html(
    source_url: str,
    *,
    http_client: httpx.AsyncClient | None = None,
    timeout_seconds: float = 10.0,
    max_html_bytes: int = _DEFAULT_MAX_HTML_BYTES,
) -> ArticleHtmlPayload:
    """Fetch article HTML with redirects enabled and strict content-type checks."""
    if http_client is not None:
        return await _fetch_article_html_with_client(
            http_client,
            source_url,
            timeout_seconds=timeout_seconds,
            max_html_bytes=max_html_bytes,
        )

    async with httpx.AsyncClient(follow_redirects=True, timeout=timeout_seconds) as managed_client:
        return await _fetch_article_html_with_client(
            managed_client,
            source_url,
            timeout_seconds=timeout_seconds,
            max_html_bytes=max_html_bytes,
        )


async def _fetch_article_html_with_client(
    http_client: httpx.AsyncClient,
    source_url: str,
    *,
    timeout_seconds: float,
    max_html_bytes: int,
) -> ArticleHtmlPayload:
    """Fetch article HTML with an injected HTTP client."""
    try:
        async with http_client.stream(
            "GET",
            source_url,
            follow_redirects=True,
            timeout=timeout_seconds,
        ) as response:
            response.raise_for_status()
            _raise_for_non_html_response(response, source_url)
            _raise_for_declared_size_limit(response, source_url, max_html_bytes)
            html = await _read_html_with_size_limit(
                response.aiter_bytes(), source_url, max_html_bytes
            )
    except httpx.TimeoutException as exc:
        raise ArticleProcessingError(
            f"timeout fetching article HTML for {source_url}: {exc}"
        ) from exc
    except httpx.HTTPStatusError as exc:
        raise ArticleProcessingError(
            f"HTTP error fetching article HTML for {source_url}: {exc.response.status_code}"
        ) from exc
    except httpx.RequestError as exc:
        raise ArticleProcessingError(
            f"request error fetching article HTML for {source_url}: {exc}"
        ) from exc

    return {"final_url": str(response.url), "html": html}


def _raise_for_non_html_response(response: httpx.Response, source_url: str) -> None:
    """Reject non-HTML responses with a deterministic domain error."""
    content_type = response.headers.get("content-type", "").split(";", 1)[0].strip().lower()
    if content_type in {"text/html", "application/xhtml+xml"}:
        return

    rendered_content_type = content_type or "missing content-type"
    raise ArticleProcessingError(f"non-HTML response for {source_url}: {rendered_content_type}")


def _raise_for_declared_size_limit(
    response: httpx.Response,
    source_url: str,
    max_html_bytes: int,
) -> None:
    """Reject responses whose declared size already exceeds the deterministic limit."""
    content_length = response.headers.get("content-length")
    if content_length is None:
        return

    try:
        declared_size = int(content_length)
    except ValueError:
        return

    if declared_size <= max_html_bytes:
        return

    raise ArticleProcessingError(
        f"article HTML too large for {source_url}: {declared_size} bytes exceeds {max_html_bytes}"
    )


async def _read_html_with_size_limit(
    chunks: AsyncIterator[bytes],
    source_url: str,
    max_html_bytes: int,
) -> str:
    """Read streamed HTML bytes while enforcing a deterministic size ceiling."""
    html_chunks: list[bytes] = []
    total_size = 0

    async for chunk in chunks:
        total_size += len(chunk)
        if total_size > max_html_bytes:
            raise ArticleProcessingError(
                f"article HTML too large for {source_url}: exceeded {max_html_bytes} bytes"
            )
        html_chunks.append(chunk)

    return b"".join(html_chunks).decode("utf-8", errors="replace")
