# Component Candidate Clustering Design

**Date:** 2026-03-24

## Goal

Replace the current URL-centric candidate phase of clustering with a story-centric candidate phase based on connected components in the bipartite graph `event_id <-> mention_identifier`.

## Why change

The current pipeline scores one candidate per `source_url`, so it measures how much depth and repetition exists around a single document URL. That is useful for document prominence, but it is a weak proxy for story breadth. It also makes early candidate admission depend on URL-level aggregation and a single scalar threshold.

The requested redesign moves the semantic center of gravity earlier in the pipeline. A candidate becomes a connected editorial component: a set of events and mentioning documents linked by co-occurrence. This means the candidate phase starts measuring the spread and cohesion of a story rather than the weight of a single URL.

## New candidate definition

A candidate is a connected component in a bipartite graph with two node types:

- `event_id`
- `mention_identifier`

An edge exists when a `gdelt_mentions` row links a `mention_identifier` to a `global_event_id`.

The component boundary is therefore defined by real mention co-occurrence, not by URL canonicalization or by the initial `source_url` grouping.

## New candidate metrics

Each connected component will expose interpretable metrics used for gating and later ranking:

- `event_id_count` — number of distinct events in the component
- `source_url_count` — number of distinct event `source_url` values referenced by the component's events
- `domain_count` — number of distinct domains derived from the component's `source_url` set
- `component_density` — actual edges divided by maximum possible bipartite edges for the component
- `event_time_span_hours` — span between earliest and latest event `date_added` values inside the component

These metrics intentionally separate different notions of importance:

- articulation of the story (`event_id_count`)
- editorial spread (`source_url_count`)
- source diversity (`domain_count`)
- structural cohesion (`component_density`)
- temporal concentration (`event_time_span_hours`)

## Candidate admission model

The old admission rule `topic_score >= threshold` is removed from the candidate phase.

Candidate admission will instead use explicit gates, each independently configurable:

- minimum distinct `event_id_count`
- minimum distinct `source_url_count`
- minimum distinct `domain_count`
- maximum `event_time_span_hours`
- optional minimum `component_density`

This is preferred because candidate components can vary wildly in shape. A single scalar threshold no longer has a stable interpretation once the unit changes from one URL to one connected event-document component.

## Role of topic_score after redesign

`topic_score` is retained, but its meaning changes.

It becomes a descriptive ranking signal for a component that already passed structural gates. It is no longer the primary admission mechanism. The formula should be updated to operate on component-level signals rather than URL-level aggregates.

Initial component-level score inputs should be:

- `event_id_count`
- `source_url_count`
- `domain_count`

Optional future extensions can include density or temporal concentration as bounded terms, but the first iteration should keep gating and ranking conceptually separate.

## Pipeline changes

### Current flow

1. score `source_url` candidates
2. filter by URL-level `topic_score`
3. fetch related events / mentions / GKG
4. build one cluster per `source_url`
5. merge clusters with `ClusterMerger`

### New flow

1. fetch `gdelt_mentions` + related `gdelt_events` within the requested `date_added` window
2. build connected components over `event_id <-> mention_identifier`
3. compute component metrics
4. apply explicit admission gates with reasoned logging
5. build one candidate cluster per component
6. materialise component-derived clusters
7. keep post-build merge only if still justified after observing the new candidate quality

The important design intent is that clustering starts closer to the narrative unit and less downstream of URL-level heuristics.

## Build shape for a component candidate

The first implementation should treat a component as the primary build unit and materialise one cluster row from that component.

The component row should include:

- all distinct event IDs
- all distinct source URLs in the component
- dominant event types / quad classes / countries / locations aggregated from all events
- mention-derived metadata across the full component
- GKG enrichment derived from the component's source URLs, with conservative handling for missing GKG rows

Because the current schema stores a single `source_url`, the first implementation will need an explicit representative field choice. The best short-term option is to keep a representative `source_url` for compatibility while treating it as presentation metadata, not as the candidate identity.

## Candidate identity

The current `cluster_id` is derived solely from `source_url`. That is incompatible with component-based candidates.

The new candidate identity should be derived from stable component content, for example a deterministic hash of sorted event IDs or of sorted event IDs plus sorted source URLs. This prevents the identity from changing merely because one source URL becomes the strongest anchor.

## Logging expectations

Every rejected component should be explainable from logs.

At minimum, each discarded component should log:

- component identifier
- `event_id_count`
- `source_url_count`
- `domain_count`
- `component_density`
- `event_time_span_hours`
- failed gate names

This gives the system a post-hoc debugging surface that the current threshold-based flow lacks.

## Risks and migration notes

- The candidate generation stage will become more graph-heavy and memory-sensitive.
- Existing tests and docs assume `source_url`-centric candidate generation.
- `cluster_id` semantics will change and must be reflected in tests and docs.
- Some current merge responsibilities may become redundant once candidates are already component-level stories.

## Recommended implementation strategy

Implement the redesign in two safe phases:

1. introduce component discovery + gate evaluation behind new internal helpers and tests
2. switch `build_and_materialise()` from URL candidates to component candidates, then re-evaluate what remains necessary in `ClusterMerger`

This keeps the architectural shift controlled while making the new candidate model testable in isolation.
