# Clustering Pipeline — Documentazione Tecnica

> **Progetto:** `web-journal-news-module` (gdelt-news-backend)
> **Data:** 2026-03-13
> **Versione:** 1.0

---

## Indice

1. [Panoramica del sistema](#1-panoramica-del-sistema)
2. [Flusso dati completo](#2-flusso-dati-completo)
3. [Fase 1 — Ingestion GDELT](#3-fase-1--ingestion-gdelt)
4. [Fase 2 — Enrichment degli eventi](#4-fase-2--enrichment-degli-eventi)
5. [Fase 3 — Clusterizzazione](#5-fase-3--clusterizzazione)
6. [Formule di scoring](#6-formule-di-scoring)
7. [Algoritmo di merge dei cluster](#7-algoritmo-di-merge-dei-cluster)
8. [Struttura dei dati prodotti](#8-struttura-dei-dati-prodotti)
9. [Metriche di qualità e milestone di test](#9-metriche-di-qualit%C3%A0-e-milestone-di-test)

---

## 1. Panoramica del sistema

Il sistema scarica automaticamente i dataset pubblici di GDELT 2.0 (Global Database of Events, Language, and Tone), li normalizza, li arricchisce con metadati estratti dagli articoli originali, e li raggruppa in **cluster tematici** (`story_clusters`) che rappresentano storie giornalistiche distinte.

### Componenti principali

| Componente | Ruolo |
|---|---|
| `GdeltHttpClient` | Scarica i file export ZIP da GDELT ogni 15 minuti |
| `IngestionService` | Parsea e inserisce eventi, mention e GKG nel DB |
| `EventEnrichmentService` | Arricchisce ogni evento con titolo, summary, entità estratte dall'articolo originale |
| `ClusterService` | Raggruppa gli eventi in cluster tematici |
| `ClusterMerger` | Fonde cluster sovrapposti con Union-Find |
| `FilterService` + Claude | Normalizza query in linguaggio naturale in filtri CAMEO/FIPS |

---

## 2. Flusso dati completo

```
┌─────────────────────────────────────────────────────────────┐
│                    GDELT 2.0 HTTP Exports                   │
│  data.gdeltproject.org — file ogni 15 minuti                │
│  *.export.CSV.zip  *.mentions.CSV.zip  *.gkg.csv.zip        │
└──────────────────────────┬──────────────────────────────────┘
                           │ download asincrono
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                      IngestionService                        │
│  Bootstrap: finestra [now - retention_days, now]            │
│  Incrementale: ogni ingestion_interval_minutes              │
│  Watermark: date_added dell'ultimo evento inserito          │
└──────────────────────────┬──────────────────────────────────┘
                           │ bulk INSERT ON CONFLICT DO NOTHING
                           ▼
┌───────────────────────────────────────────────────┐
│  PostgreSQL                                        │
│  ├── gdelt_events      (61 campi GDELT, enriched) │
│  ├── gdelt_mentions    (14 campi, join su event)  │
│  └── gdelt_gkg         (temi, persone, org, tone) │
└──────────────────────────┬────────────────────────┘
                           │ ogni 30 min (se abilitato)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                  EventEnrichmentService                      │
│  Seleziona eventi con enrichment_status = 'pending'         │
│  → fetch HTML da source_url                                 │
│  → estrazione titolo + paragrafi                            │
│  → POST /enrich al servizio esterno                         │
│  → salva: title, summary, topics, keywords, entities        │
└──────────────────────────┬──────────────────────────────────┘
                           │ ogni 24h (schedulato)
                           ▼
┌─────────────────────────────────────────────────────────────┐
│                      ClusterService                          │
│  Finestra: ultimi 36 ore di date_added                      │
│  Fase 1: topic scoring per source_url                       │
│  Fase 2: raccolta eventi, mention, GKG per ogni candidato   │
│  Fase 3: build del cluster (aggregazioni + scoring)         │
│  Fase 4: ClusterMerger (Union-Find)                         │
└──────────────────────────┬──────────────────────────────────┘
                           │ INSERT ON CONFLICT DO UPDATE
                           ▼
┌──────────────────────────────────────┐
│  PostgreSQL                          │
│  └── story_clusters                  │
│      cluster_id, topic_score,        │
│      themes, persons, orgs,          │
│      severity, tone, locations ...   │
└──────────────────────────────────────┘
                           │
                           ▼
┌──────────────────────────────────────┐
│  GET /clusters/search                │
│  POST /events/search (+ Claude NLU)  │
└──────────────────────────────────────┘
```

---

## 3. Fase 1 — Ingestion GDELT

### 3.1 Sorgenti dati

GDELT pubblica tre tipi di file export ogni 15 minuti:

| File | Colonne | Contenuto |
|---|---|---|
| `*.export.CSV.zip` | 61 | Eventi geopolitici con codici CAMEO, location, tone, mention counts |
| `*.mentions.CSV.zip` | 14 | Ogni documento che menziona un evento (URL, tono, data) |
| `*.gkg.csv.zip` | 27 | Global Knowledge Graph: temi, persone, organizzazioni per documento |

### 3.2 Campi chiave estratti

**Da `export.CSV`** — campi usati nel clustering:

| Campo GDELT | Campo App | Uso |
|---|---|---|
| `GLOBALEVENTID` | `global_event_id` | Chiave primaria, deduplicazione |
| `SQLDATE` | `sql_date` | Filtro temporale (YYYYMMDD) |
| `EventRootCode` | `event_root_code` | Tipo evento CAMEO (01–20) |
| `QuadClass` | `quad_class` | Classe conflitto (1=CoopVerb, 2=CoopMat, 3=ConflVerb, 4=ConflMat) |
| `GoldsteinScale` | `goldstein_scale` | Stabilità geopolitica (−10 a +10) |
| `NumMentions` | `num_mentions` | Impatto mediatico |
| `NumSources` | `num_sources` | Diversità delle fonti |
| `NumArticles` | `num_articles` | Copertura editoriale |
| `AvgTone` | `avg_tone` | Sentiment medio degli articoli |
| `ActionGeo_FullName` | `action_geo_full_name` | Localizzazione geografica |
| `SOURCEURL` | `source_url` | **Chiave di raggruppamento cluster** |
| `DATEADDED` | `date_added` | Watermark incrementale |

**Da `mentions.CSV`** — campi usati nel clustering:

| Campo GDELT | Campo App | Uso |
|---|---|---|
| `MentionIdentifier` | `mention_identifier` | URL del documento — chiave di merge cluster |
| `MentionSourceName` | `mention_source_name` | Nome testata giornalistica |
| `MentionTimeDate` | `mention_time_date` | Prima/ultima menzione del cluster |

**Da `gkg.csv`** — campi usati nel clustering:

| Campo GDELT | Campo App | Uso |
|---|---|---|
| `DocumentIdentifier` | `document_identifier` | Join con `mention_identifier` |
| `V1Themes` | `themes` | Temi GDELT — chiave Jaccard similarity |
| `V1Persons` | `persons` | Persone citate nell'articolo |
| `V1Organizations` | `organizations` | Organizzazioni citate |
| `V1Locations` | `locations` | Luoghi citati nel testo |
| `V2TONE` (primo valore) | `document_tone` | Tono del documento |

### 3.3 Deduplicazione

- **Eventi**: `ON CONFLICT (global_event_id) DO NOTHING` — GDELT riusa gli stessi ID
- **Mention**: unicità su `(global_event_id, mention_identifier)`
- **GKG**: unicità su `document_identifier`

---

## 4. Fase 2 — Enrichment degli eventi

Ogni evento con `enrichment_status = 'pending'` viene processato da `EventEnrichmentService`:

```
source_url
    → GET HTML (max 1 MB)
    → HTMLParser: estrae <title>, og:title, <p>
    → POST /enrich (servizio esterno)
    → scrive su gdelt_events:
        article_title      — titolo dell'articolo
        article_summary    — riassunto
        main_topics        — lista di argomenti
        keywords           — parole chiave
        cited_sources      — fonti citate nel testo
        entities:
            persons_cited
            organizations_cited
            locations
            ethnicities_cited
            religions_cited
            occupations_cited
            political_affiliations_cited
            industries_cited
            products_cited
            brands_cited
```

**Stati possibili:** `pending` → `processing` → `enriched` / `failed`

---

## 5. Fase 3 — Clusterizzazione

### 5.1 Finestra temporale

Il `ClusterService` opera su una finestra di **36 ore** calcolata su `date_added` dell'evento. Questo garantisce che ogni run giornaliero copra eventi pubblicati nelle ultime ore senza perdere nulla per ritardi di ingestion.

### 5.2 Fase 1: Candidate scoring per `source_url`

Ogni `source_url` distinta nella finestra temporale diventa un candidato cluster. Il sistema aggrega:
- `event_count` — numero di eventi con quella URL
- `num_articles`, `num_mentions`, `num_sources` — somme dei campi GDELT

Calcola il `topic_score` (vedi §6) e **scarta** i candidati con `topic_score < 4.0`.

### 5.3 Fase 2: Raccolta dati

Per ogni candidato qualificato:
1. Fetch di tutti i `GdeltEvent` con quella `source_url`
2. Fetch di tutti i `GdeltMention` per quei `global_event_id`
3. Fetch di tutti i `GdeltGkg` dove `document_identifier` è in `mention_identifier`

### 5.4 Fase 3: Build del cluster

Il `cluster_id` è deterministico:
```
cluster_id = "{YYYYMMDD}_{sha256(source_url)[:12]}"
```

Aggregazioni per ogni cluster:

| Campo output | Logica di aggregazione |
|---|---|
| `dominant_event_types` | Top-5 per frequenza tra tutti gli eventi (etichette italiane CAMEO) |
| `dominant_quad_classes` | Top-5 per frequenza (etichette italiane) |
| `dominant_countries` | Top-5 country codes per frequenza |
| `dominant_locations` | Top-5 `action_geo_full_name` per frequenza |
| `avg_severity_score` | Media di `severity_score` su tutti gli eventi |
| `themes` | Unione di tutti i temi GKG |
| `persons` | Unione di tutti i nomi GKG |
| `organizations` | Unione di tutte le organizzazioni GKG |
| `gkg_locations` | Unione di tutti i luoghi GKG |
| `document_tone_avg` | Media di `document_tone` dai GKG |
| `first_mention_at` | Timestamp minimo tra le mention |
| `last_mention_at` | Timestamp massimo tra le mention |
| `mention_identifiers` | Union ordinata di tutti gli URL mention |
| `distinct_mention_sources` | Union ordinata dei nomi testata |

### 5.5 Fase 4: Merge (ClusterMerger)

Dopo il build, i cluster vengono fusi tramite l'algoritmo Union-Find (§7).

---

## 6. Formule di scoring

### 6.1 Topic Score (rilevanza del cluster)

```
topic_score = ln(event_count + 1)  × 0.4
            + ln(num_articles + 1) × 0.3
            + ln(num_mentions + 1) × 0.2
            + ln(num_sources + 1)  × 0.1
```

**Soglia di ammissione al clustering:** `topic_score ≥ 4.0`

I pesi riflettono la priorità: la molteplicità degli eventi su quella URL è il segnale più forte (0.4), seguita dalla copertura editoriale (0.3), dalla viralità mediatica (0.2) e dalla diversità delle fonti (0.1). Il logaritmo smorza l'effetto di valori estremi.

### 6.2 Severity Score (gravità dell'evento)

```
quad_weight = { 1: 0.0,   # Cooperazione Verbale
                2: 2.0,   # Cooperazione Materiale
                3: 5.0,   # Conflitto Verbale
                4: 10.0 } # Conflitto Materiale

severity = quad_weight[quad_class]
         + |goldstein_scale| × 0.5
         + |avg_tone| × 0.3

severity = min(severity, 20.0)  # cappato a 20
```

Il `avg_severity_score` del cluster è la media di `severity` su tutti gli eventi.

---

## 7. Algoritmo di merge dei cluster

Il `ClusterMerger` usa **Union-Find (disjoint-set)** con due criteri di fusione applicati in sequenza.

### 7.1 Criterio 1 — Mention URL overlap

Due cluster **A** e **B** vengono uniti se condividono **almeno 1 URL mention** in comune:

```
|mention_identifiers(A) ∩ mention_identifiers(B)| ≥ 1
```

Questo cattura storie coperte dallo stesso articolo/documento.

### 7.2 Criterio 2 — Jaccard similarity sui temi GKG

Due cluster non ancora connessi vengono uniti se i loro set di temi GKG hanno similarità Jaccard superiore a 0.30:

```
Jaccard(A, B) = |themes(A) ∩ themes(B)| / |themes(A) ∪ themes(B)| > 0.30
```

Questo cattura storie diverse ma tematicamente correlate (es. due proteste in paesi diversi coperte con lo stesso frame tematico GDELT).

### 7.3 Strategia di fusione

Il cluster con il `topic_score` più alto diventa l'**anchor** del gruppo:

| Campo | Strategia |
|---|---|
| `cluster_id`, `source_url` | Dall'anchor |
| `topic_score` | Massimo del gruppo |
| `event_count`, `num_articles`, `num_mentions`, `num_sources`, `mention_count` | Somma |
| `avg_severity_score`, `document_tone_avg` | Media dei valori non-None |
| `first_mention_at` | Minimo |
| `last_mention_at` | Massimo |
| `mention_identifiers`, `themes`, `persons`, `organizations`, `gkg_locations`, `event_ids`, `distinct_mention_sources` | Unione ordinata |
| `dominant_event_types`, `dominant_quad_classes`, `dominant_countries`, `dominant_locations` | Top-5 per frequenza sul gruppo fuso |

---

## 8. Struttura dei dati prodotti

### Tabella `story_clusters`

```
cluster_id              STRING(100)  — "{YYYYMMDD}_{sha256[:12]}"
source_url              TEXT         — URL anchor del cluster
event_count             INT          — eventi aggregati
num_articles            INT          — articoli totali
num_mentions            INT          — menzioni totali
num_sources             INT          — fonti distinte
topic_score             FLOAT        — rilevanza (≥ 4.0)
avg_severity_score      FLOAT        — gravità media (0–20)
document_tone_avg       FLOAT        — tono medio GKG
event_ids               JSON[]       — lista ID eventi
dominant_event_types    JSON[]       — top-5 tipi CAMEO (italiano)
dominant_quad_classes   JSON[]       — top-5 classi quad (italiano)
dominant_countries      JSON[]       — top-5 country codes
dominant_locations      JSON[]       — top-5 location names
mention_count           INT          — mention totali
distinct_mention_sources JSON[]      — testate giornalistiche
mention_identifiers     JSON[]       — URL documenti mention
first_mention_at        DATETIME     — prima copertura
last_mention_at         DATETIME     — ultima copertura
themes                  JSON[]       — temi GKG unificati
persons                 JSON[]       — persone GKG unificate
organizations           JSON[]       — organizzazioni GKG unificate
gkg_locations           JSON[]       — luoghi GKG unificati
computed_at             DATETIME     — timestamp del run di clustering
```

---

## 9. Metriche di qualità e milestone di test

Le metriche sono organizzate in 4 livelli progressivi. Ogni livello deve essere **superato prima di procedere al successivo**.

---

### Livello 0 — Sanità del dato grezzo (post-ingestion)

Verifiche da eseguire subito dopo che il bootstrap è completato.

| ID | Metrica | Query SQL | Target |
|---|---|---|---|
| M0.1 | Totale eventi nel DB | `SELECT COUNT(*) FROM gdelt_events` | > 100.000 per 30 giorni di data |
| M0.2 | Copertura temporale eventi | `SELECT MIN(sql_date), MAX(sql_date) FROM gdelt_events` | Range ≈ retention_days |
| M0.3 | Completezza source_url | `SELECT COUNT(*) FROM gdelt_events WHERE source_url IS NULL` | = 0 |
| M0.4 | Completezza event_root_code | `SELECT COUNT(*) FROM gdelt_events WHERE event_root_code IS NULL` | < 1% del totale |
| M0.5 | Range GoldsteinScale valido | `SELECT MIN(goldstein_scale), MAX(goldstein_scale) FROM gdelt_events` | Min ≥ −10, Max ≤ 10 |
| M0.6 | Range quad_class valido | `SELECT DISTINCT quad_class FROM gdelt_events` | Solo valori {1, 2, 3, 4} |
| M0.7 | Mention per evento | `SELECT AVG(cnt) FROM (SELECT global_event_id, COUNT(*) cnt FROM gdelt_mentions GROUP BY 1)` | > 1.0 (ogni evento citato almeno una volta in media) |
| M0.8 | GKG linkage rate | `SELECT COUNT(DISTINCT document_identifier) FROM gdelt_gkg` vs `SELECT COUNT(DISTINCT mention_identifier) FROM gdelt_mentions` | Overlap ≥ 30% |
| M0.9 | Duplicati eventi | `SELECT global_event_id, COUNT(*) FROM gdelt_events GROUP BY 1 HAVING COUNT(*) > 1` | = 0 righe |

**Milestone 0 superata:** tutte le metriche M0.x nei target.

---

### Livello 1 — Qualità del clustering (post-materializzazione)

Verifiche da eseguire dopo il primo run completo di `ClusterService`.

| ID | Metrica | Query SQL | Target |
|---|---|---|---|
| M1.1 | Totale cluster prodotti | `SELECT COUNT(*) FROM story_clusters` | > 500 per 48h di finestra | 301 ( 15-16 febbraio)
| M1.2 | Cluster con topic_score ≥ 4.0 | `SELECT COUNT(*) FROM story_clusters WHERE topic_score < 3.6` | = 0 (tutti filtrati) | OK
| M1.3 | Distribuzione topic_score | `SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY topic_score) FROM story_clusters` | Mediana ≥ 5.0 | 3.8385
| M1.4 | Cluster con almeno 2 eventi | `SELECT COUNT(*) FROM story_clusters WHERE event_count < 2` | < 10% del totale | OK -- 0
| M1.5 | Cluster con temi GKG popolati | `SELECT COUNT(*) FROM story_clusters WHERE jsonb_array_length(themes::jsonb) = 0` | < 40% del totale | 7 su 301
| M1.6 | Cluster con persone popolate | `SELECT COUNT(*) FROM story_clusters WHERE jsonb_array_length(persons::jsonb) > 0` | > 20% del totale | 293 su 301 
| M1.7 | Severità nel range valido | `SELECT MIN(avg_severity_score), MAX(avg_severity_score) FROM story_clusters` | Min ≥ 0, Max ≤ 20 | 1.06	17.57
| M1.8 | Cluster con first/last mention | `SELECT COUNT(*) FROM story_clusters WHERE first_mention_at IS NULL` | = 0 | OK
| M1.9 | cluster_id univocità | `SELECT cluster_id, COUNT(*) FROM story_clusters GROUP BY 1 HAVING COUNT(*) > 1` | = 0 righe | OK
| M1.10 | Tone medio nel range plausibile | `SELECT MIN(document_tone_avg), MAX(document_tone_avg) FROM story_clusters WHERE document_tone_avg IS NOT NULL` | Min ≥ −100, Max ≤ 100 |  -12.19	9.5

**Milestone 1 superata:** tutte le metriche M1.x nei target.

---

### Livello 2 — Coerenza semantica del merge (validazione del ClusterMerger)

Verifiche sulla qualità della fusione Union-Find.

| ID | Metrica | Descrizione | Target |
|---|---|---|---|
| M2.1 | Tasso di merge | `(cluster_pre_merge - cluster_post_merge) / cluster_pre_merge` | 10%–50% (merge attivo ma non collasso) |
| M2.2 | Dimensione media post-merge | Media di `event_count` nei cluster fusi | Non > 3× la media pre-merge |
| M2.3 | Cluster "mega" anomali | `SELECT COUNT(*) FROM story_clusters WHERE event_count > 500` | < 1% del totale |
| M2.4 | Overlap mention verificato | Per un campione di 20 cluster fusi: verificare che condividano ≥ 1 mention_identifier | 100% del campione |
| M2.5 | Jaccard verificato | Per un campione di 10 cluster fusi solo per temi: calcolare manualmente Jaccard | Tutti > 0.30 |
| M2.6 | Anchor corretto | L'anchor del cluster fuso ha il `topic_score` più alto tra i fusi | Verificabile su log |
| M2.7 | Consistenza date fuse | `first_mention_at <= last_mention_at` per tutti i cluster | = 0 violazioni |

**Milestone 2 superata:** M2.1 in range, M2.3 < 1%, M2.4 e M2.5 a 100%.

---

### Livello 3 — Rilevanza editoriale (validazione umana campionata)

Queste metriche richiedono ispezione manuale su un campione.

| ID | Metrica | Metodo | Target |
|---|---|---|---|
| M3.1 | Precisione topic principale | Seleziona top-20 cluster per `topic_score`. Per ognuno: il `dominant_event_types[0]` corrisponde al contenuto dell'articolo? | ≥ 80% di corrispondenza |
| M3.2 | Coerenza geografica | Per i top-20 cluster: `dominant_countries[0]` è il paese realmente protagonista della notizia? | ≥ 75% di corrispondenza |
| M3.3 | Temi GKG pertinenti | Per 10 cluster con `themes` popolato: i temi GKG riflettono l'argomento dell'articolo? | ≥ 70% pertinenti |
| M3.4 | Severità percepita | Per 5 cluster con `avg_severity_score > 15`: sono effettivamente notizie di conflitto/crisi grave? | ≥ 80% confermati |
| M3.5 | Falsi positivi di merge | Seleziona 10 cluster fusi (con `event_count` più alto): gli eventi appartengono alla stessa storia? | ≥ 80% coerenti |
| M3.6 | Distribuzione quad_class | `SELECT dominant_quad_classes[1], COUNT(*) FROM story_clusters GROUP BY 1` | Distribuzione plausibile, non > 70% una sola classe |
| M3.7 | Freshness dei cluster | `SELECT COUNT(*) FROM story_clusters WHERE computed_at < NOW() - INTERVAL '25 hours'` | = 0 (run giornaliero rispettato) |

**Milestone 3 superata:** M3.1 ≥ 80%, M3.2 ≥ 75%, M3.5 ≥ 80%.

---

### Dashboard di monitoraggio continuo

Queste query andrebbero eseguite ad ogni run di clusterizzazione per monitorare la deriva nel tempo.

```sql
-- Volume giornaliero cluster prodotti
SELECT DATE(computed_at), COUNT(*) as clusters
FROM story_clusters
GROUP BY 1
ORDER BY 1 DESC;

-- Distribuzione topic_score per quintili
SELECT
  ntile(5) OVER (ORDER BY topic_score) as quintile,
  MIN(topic_score), MAX(topic_score), COUNT(*)
FROM story_clusters
GROUP BY 1;

-- Top-10 cluster per severity oggi
SELECT cluster_id, source_url, avg_severity_score, dominant_countries, dominant_event_types
FROM story_clusters
WHERE DATE(computed_at) = CURRENT_DATE
ORDER BY avg_severity_score DESC
LIMIT 10;

-- Copertura geografica
SELECT
  jsonb_array_elements_text(dominant_countries::jsonb) as country,
  COUNT(*) as cluster_count
FROM story_clusters
GROUP BY 1
ORDER BY 2 DESC
LIMIT 20;

-- Stato enrichment
SELECT enrichment_status, COUNT(*) as n
FROM gdelt_events
GROUP BY 1;

-- Rate di merge (richiede log o tabella di audit)
-- Confrontare count pre-merge (candidati) vs post-merge (story_clusters)
```

---

### Riepilogo milestone

| Milestone | Condizione di superamento | Azione successiva |
|---|---|---|
| **M0** — Dato grezzo | Tutti M0.x OK | Avviare enrichment e clustering |
| **M1** — Cluster prodotti | Tutti M1.x OK | Analizzare merge e semantica |
| **M2** — Merge coerente | M2.1 in range, M2.3 < 1%, M2.4-M2.5 a 100% | Validazione editoriale |
| **M3** — Rilevanza editoriale | M3.1 ≥ 80%, M3.2 ≥ 75%, M3.5 ≥ 80% | Sistema pronto per produzione |

---

*Documento generato il 2026-03-13 — basato sull'analisi del codice sorgente del repository.*
