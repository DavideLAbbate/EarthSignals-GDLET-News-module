"""Query compiler interfaces for backend-agnostic filtering."""

from __future__ import annotations

from typing import Protocol

from app.schemas.filters import NormalizedFilters


class CompiledQuery(Protocol):
    """Protocol for compiled queries.

    Different backends will implement this differently:
    - PostgreSQL: returns a SQLAlchemy Select statement
    - BigQuery: returns (sql_string, params) tuple
    - ClickHouse (future): returns ClickHouse-specific query
    """

    pass


class EventQueryCompiler(Protocol):
    """
    Protocol for compiling normalized filters to backend-specific queries.

    The compiler transforms backend-agnostic NormalizedFilters into
    queries appropriate for the target database. This allows the filter
    normalization layer to remain independent of the runtime query engine.
    """

    def compile(
        self,
        filters: NormalizedFilters,
    ) -> CompiledQuery:
        """
        Compile normalized filters into a backend-specific query.

        Args:
            filters: Backend-agnostic normalized search filters

        Returns:
            Compiled query appropriate for the target database
        """
        ...

    def compile_count(self, filters: NormalizedFilters) -> CompiledQuery:
        """
        Compile a count query (same filters, just COUNT).

        Args:
            filters: Backend-agnostic normalized search filters

        Returns:
            Compiled count query
        """
        ...
