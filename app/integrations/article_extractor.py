"""Deterministic article content extraction from HTML."""

from __future__ import annotations

import html
import re
from html.parser import HTMLParser
from typing import TypedDict

from app.core.exceptions import ArticleProcessingError

_WHITESPACE_RE = re.compile(r"\s+")


class ExtractedArticlePayload(TypedDict):
    """Deterministic article content extracted from raw HTML."""

    title: str | None
    content: str


class _ArticleContentParser(HTMLParser):
    """Collect title and paragraph text with conservative HTML heuristics."""

    def __init__(self) -> None:
        super().__init__(convert_charrefs=True)
        self._inside_title = False
        self._inside_paragraph = False
        self._title_parts: list[str] = []
        self._paragraph_parts: list[str] = []
        self.paragraphs: list[str] = []
        self.og_title: str | None = None

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_map = {name.lower(): value for name, value in attrs}
        lowered_tag = tag.lower()

        if lowered_tag == "title":
            self._inside_title = True
            return

        if lowered_tag == "meta":
            property_name = (attr_map.get("property") or attr_map.get("name") or "").lower()
            if property_name == "og:title":
                self.og_title = _clean_text(attr_map.get("content"))
            return

        if lowered_tag == "p":
            self._inside_paragraph = True
            self._paragraph_parts = []

    def handle_endtag(self, tag: str) -> None:
        lowered_tag = tag.lower()

        if lowered_tag == "title":
            self._inside_title = False
            return

        if lowered_tag != "p":
            return

        self._inside_paragraph = False
        paragraph = _clean_text("".join(self._paragraph_parts))
        if paragraph:
            self.paragraphs.append(paragraph)
        self._paragraph_parts = []

    def handle_data(self, data: str) -> None:
        if self._inside_title:
            self._title_parts.append(data)
        if self._inside_paragraph:
            self._paragraph_parts.append(data)

    @property
    def title(self) -> str | None:
        return _clean_text("".join(self._title_parts))


def extract_article_content(html_text: str) -> ExtractedArticlePayload:
    """Extract a cleaned title and main text payload from article HTML."""
    if not html_text.strip():
        raise ArticleProcessingError("empty HTML content")

    parser = _ArticleContentParser()
    parser.feed(html_text)
    parser.close()

    cleaned_title = parser.og_title or parser.title
    cleaned_content = "\n\n".join(parser.paragraphs)
    if not cleaned_content:
        raise ArticleProcessingError("article extraction produced no usable paragraph content")

    return {"title": cleaned_title, "content": cleaned_content}


def _clean_text(value: str | None) -> str | None:
    """Normalize whitespace and decode any HTML entities."""
    if value is None:
        return None

    normalized = _WHITESPACE_RE.sub(" ", html.unescape(value)).strip()
    return normalized or None
