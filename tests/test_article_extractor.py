"""Tests for deterministic article content extraction."""

from __future__ import annotations

import pytest

from app.core.exceptions import ArticleProcessingError
from app.integrations.article_extractor import extract_article_content


def test_extract_article_content_prefers_og_title_and_joins_paragraphs() -> None:
    html = """
    <html>
      <head>
        <title>Page Title</title>
        <meta property="og:title" content="  OG Title  ">
      </head>
      <body>
        <p> First paragraph. </p>
        <p>Second   paragraph.</p>
      </body>
    </html>
    """

    result = extract_article_content(html)

    assert result == {
        "title": "OG Title",
        "content": "First paragraph.\n\nSecond paragraph.",
    }


def test_extract_article_content_falls_back_to_title_and_ignores_empty_paragraphs() -> None:
    html = """
    <html>
      <head>
        <title>
          Example   Title
        </title>
      </head>
      <body>
        <p> </p>
        <p> Line one with extra whitespace. </p>
        <div><p>Line two.</p></div>
      </body>
    </html>
    """

    result = extract_article_content(html)

    assert result == {
        "title": "Example Title",
        "content": "Line one with extra whitespace.\n\nLine two.",
    }


def test_extract_article_content_raises_for_missing_paragraph_content() -> None:
    html = """
    <html>
      <head><title>Example Title</title></head>
      <body>
        <div>No paragraph content here.</div>
        <p>   </p>
      </body>
    </html>
    """

    with pytest.raises(ArticleProcessingError, match="paragraph"):
        extract_article_content(html)
