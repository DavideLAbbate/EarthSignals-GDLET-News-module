# News Cluster Pipeline — GDELT

## Obiettivo

Trasformare il dataset GDELT — rumoroso e frammentato — in **cluster di notizie reali**, utilizzabili in un prodotto stile telegiornale o globo geopolitico.

Il sistema parte dagli eventi GDELT grezzi e costruisce una struttura **document-centric** e **story-centric**, aggregando:

- eventi (`global_event_id`)
- documento sorgente (`source_url`)
- menzioni (`EVENTMENTIONS`)
- conoscenza semantica del documento (`GKG`)

L'output finale non è il singolo evento, ma un **cluster di notizia**.

---

## Principio base

In GDELT, la granularità è questa:

- un articolo può generare **molti eventi** (perché GDELT estrae ogni coppia attore–azione)
- un evento può avere **molte menzioni** (ogni documento che lo riprende)
- una menzione ha un `mention_identifier` che può essere cercato nel **GKG** per arricchimento semantico

Il modello logico è quindi:

```
source_url
  └── cluster documentale iniziale
       └── N × global_event_id
              └── N × mention
                     └── mention_identifier → documento GKG
```

---

## Flusso completo

### Step 1 — Ingest eventi GDELT

Si importano nella tabella `gdelt_events` gli eventi raw scaricati dai file export GDELT v2 (`.export.CSV.zip`, aggiornati ogni 15 minuti da GDELT).

**Campi minimi rilevanti:**

| Campo | Tipo | Note |
|---|---|---|
| `global_event_id` | BigInt | PK dell'evento GDELT |
| `sql_date` | Int (YYYYMMDD) | Data dell'evento |
| `source_url` | Text | URL del documento sorgente |
| `event_code` | String | Codice CAMEO specifico |
| `event_root_code` | String | Codice CAMEO aggregato (es. `14` = Protesta) |
| `quad_class` | Int | Macro-categoria (1–4) |
| `goldstein_scale` | Float | Impatto geopolitico (-10..+10) |
| `avg_tone` | Float | Tono medio del documento |
| `num_mentions` | Int | Quante volte l'evento è menzionato |
| `num_sources` | Int | Quante sorgenti distinte lo menzionano |
| `num_articles` | Int | Quanti articoli lo coprono |
| `action_geo_full_name` | String | Luogo geografico dell'azione |
| `action_geo_country_code` | String | Codice FIPS del paese |
| `actor1_country_code` | String | Codice CAMEO attore 1 |
| `actor2_country_code` | String | Codice CAMEO attore 2 |

> **Nota:** questi campi sono già ingestiti dall'ingestion corrente (`app/integrations/gdelt_http_client.py:parse_gdelt_csv_row`).

---

### Step 2 — Aggregazione primaria per `source_url`

Tutti gli eventi con la stessa `source_url` vengono considerati appartenenti allo stesso **document cluster** iniziale.

**Perché:**
- `global_event_id` non rappresenta una notizia reale univoca — GDELT frammenta un singolo articolo in decine di eventi
- `source_url` è l'ancora documentale più stabile: tutti gli eventi con lo stesso URL provengono dallo stesso pezzo di testo

Questa è la **prima forma di clustering**.

---

### Step 3 — Scoring del cluster documentale

Per ogni `source_url` aggregata, si calcola uno **score di dominanza** della notizia. Serve a identificare le notizie candidate più forti del periodo.

**Formula:**

```
topic_score =
    LN(COUNT(DISTINCT global_event_id) + 1) * 0.4
  + LN(SUM(num_articles)                + 1) * 0.3
  + LN(SUM(num_mentions)                + 1) * 0.2
  + LN(SUM(num_sources)                 + 1) * 0.1
```

> Si usa il logaritmo naturale per smorzare l'effetto degli outlier (un evento con 10.000 menzioni non deve dominare in modo assoluto).

**Query base:**

```sql
SELECT
    source_url,
    COUNT(DISTINCT global_event_id) AS events,
    SUM(num_articles)               AS num_articles,
    SUM(num_mentions)               AS num_mentions,
    SUM(num_sources)                AS num_sources,
    (
        LN(COUNT(DISTINCT global_event_id) + 1) * 0.4 +
        LN(SUM(num_articles)               + 1) * 0.3 +
        LN(SUM(num_mentions)               + 1) * 0.2 +
        LN(SUM(num_sources)                + 1) * 0.1
    ) AS topic_score
FROM gdelt_events
WHERE source_url IS NOT NULL
GROUP BY source_url
ORDER BY topic_score DESC;
```

---

### Step 4 — Costruzione del cluster notizia

Dal risultato della query di scoring si definisce il **cluster notizia**.

Per ogni `source_url` si raccoglie:

- insieme degli `event_id` associati
- statistiche aggregate (mentions, sources, articles, score)
- distribuzione geografica aggregata
- categorie evento dominanti (quali `event_root_code` e `quad_class` compaiono di più)

Questo cluster è la **prima unità editoriale** del sistema.

---

### Step 5 — Enrichment degli eventi associati

Per tutti gli eventi del cluster si calcolano campi derivati che rendono il cluster leggibile e consistente in UI.

**Campi derivati per evento:**

| Campo derivato | Sorgente | Descrizione |
|---|---|---|
| `event_type_label` | `event_root_code` | Label human-readable del tipo evento |
| `quad_class_label` | `quad_class` | Label human-readable della macro-categoria |
| `severity_score` | `quad_class` + `goldstein_scale` + `avg_tone` | Score di gravità aggregato |
| `impact_score` | `num_mentions` + `num_sources` | Score di impatto mediatico |
| `geo_key` | `action_geo_full_name` + `action_geo_country_code` | Chiave geografica normalizzata |
| `actor_countries` | `actor1` + `actor2` | Lista paesi attori |

**Mapping `quad_class`:**

| Valore | Label |
|---|---|
| 1 | Cooperazione diplomatica |
| 2 | Cooperazione concreta |
| 3 | Tensione verbale |
| 4 | Conflitto materiale |

**Mapping `event_root_code` (esempi):**

| Codice | Label |
|---|---|
| 11 | Critica |
| 13 | Minaccia |
| 14 | Protesta |
| 18 | Attacco |
| 19 | Combattimento |
| 20 | Violenza di massa |

**Formula `severity_score`:**

```
severity_score =
    weight(quad_class)          # quad_class 4 → peso massimo
  + abs(goldstein_scale) * 0.5  # più negativo = più grave
  + abs(avg_tone)        * 0.3  # tono negativo amplifica
```

---

### Step 6 — Recupero delle menzioni (EVENTMENTIONS)

Dato l'insieme dei `global_event_id` del cluster, si interrogano le **EVENTMENTIONS** di GDELT (tabella/dataset separato, disponibile su BigQuery o come file separato).

**Relazione:**

```
cluster
  └── N × global_event_id
         └── N × mention (EVENTMENTIONS)
```

Le menzioni arricchiscono il cluster con:
- numero **reale** di menzioni (più preciso del campo `num_mentions` nell'events)
- elenco di `mention_identifier` (URL dei documenti che riprendono la notizia)
- cronologia di propagazione (`first_mention_at` → `last_mention_at`)
- tono di ogni documento mentionante

**Output da mentions:**

```json
{
  "mention_count": 42,
  "distinct_mention_sources": ["fnnews.com", "example.com", "regionalnews.net"],
  "mention_identifiers": [
    "https://www.fnnews.com/news/202603070751359293",
    "https://example.com/iran-special-report"
  ],
  "first_mention_at": "2026-03-07T07:40:00Z",
  "last_mention_at": "2026-03-08T10:15:00Z"
}
```

---

### Step 7 — Recupero dei dati GKG

Dati tutti i `mention_identifier`, si recuperano i record nel dataset **GKG** (Global Knowledge Graph) di GDELT.

**Relazione:**

```
mention_identifier → GKG document
```

Il GKG è il layer semantico di GDELT: per ogni URL di documento calcola temi, persone, organizzazioni, location e tono. Il GKG **non replica** i campi degli EVENTS — aggiunge una lettura document-centrica completamente separata.

**Informazioni utili da GKG:**

| Campo | Tipo | Esempio |
|---|---|---|
| `themes` | `string[]` | `["ARMEDCONFLICT", "IRAN", "MILITARY_ACTION"]` |
| `persons` | `string[]` | `["Mojtaba Khamenei"]` |
| `organizations` | `string[]` | `["Islamic Revolutionary Guard Corps"]` |
| `locations` | `string[]` | `["Tehran, Tehran, Iran", "Bahrain"]` |
| `document_tone` | `float` | `-8.7` |

**Output da GKG:**

```json
{
  "themes": ["ARMEDCONFLICT", "MILITARY_ACTION", "IRAN"],
  "persons": ["Mojtaba Khamenei"],
  "organizations": ["Islamic Revolutionary Guard Corps"],
  "locations": ["Tehran, Tehran, Iran", "Bahrain"],
  "document_tone_avg": -8.7
}
```

---

### Step 8 — Enrichment finale del cluster

A questo punto il cluster viene composto dai 3 layer:

| Layer | Sorgente | Cosa aggiunge |
|---|---|---|
| **Event layer** | `gdelt_events` | Struttura, geografica, gravità, tipo evento |
| **Mentions layer** | `EVENTMENTIONS` | Propagazione mediatica, fonti, cronologia |
| **GKG layer** | `GKG` | Semantica, persone, organizzazioni, temi |

Il cluster finale è la **vera unità da mostrare in UI**.

---

## Architettura logica

```
gdelt_events
  └── GROUP BY source_url → scoring → cluster candidate
         │
         ├── COLLECT global_event_id[]
         │     └── event enrichment (derivati per evento)
         │
         ├── FETCH eventmentions (per event_id[])
         │     └── mentions enrichment
         │
         └── FETCH GKG (per mention_identifier[])
               └── gkg enrichment
                      │
                      └──► final story cluster JSON
```

---

## Strutture dati di riferimento

### 1. Raw event

```json
{
  "event_id": "1292890050",
  "date": "2026-03-07",
  "actor1_country": "IRN",
  "actor2_country": null,
  "event_code": "190",
  "event_base_code": "190",
  "event_root_code": "19",
  "quad_class": 4,
  "goldstein_scale": -10,
  "tone": -9.05,
  "num_mentions": 20,
  "num_sources": 1,
  "num_articles": 10,
  "action_geo_fullname": "Tehran, Tehran, Iran",
  "action_geo_country": "IR",
  "source_name": "fnnews.com",
  "source_url": "https://www.fnnews.com/news/202603070751359293"
}
```

### 2. Enriched event

```json
{
  "event_id": "1292890050",
  "date": "2026-03-07",
  "event_root_code": "19",
  "quad_class": 4,
  "goldstein_scale": -10,
  "tone": -9.05,
  "action_geo_fullname": "Tehran, Tehran, Iran",
  "action_geo_country": "IR",
  "source_url": "https://www.fnnews.com/news/202603070751359293",
  "derived": {
    "event_type_label": "Combattimento",
    "quad_class_label": "Conflitto materiale",
    "severity_score": 9.2,
    "impact_score": 4.8,
    "geo_key": "TEHRAN|IR",
    "actor_countries": ["IRN"],
    "is_conflict": true
  }
}
```

### 3. Final story cluster

```json
{
  "cluster_id": "cluster_20260308_iran_001",
  "source_url": "https://www.thenationalnews.com/news/mena/2026/03/08/...",
  "score": {
    "events": 128,
    "num_articles": 572,
    "num_mentions": 572,
    "num_sources": 128,
    "topic_score": 5.60
  },
  "event_ids": [
    "1292890050",
    "1292890121",
    "1292890203"
  ],
  "event_enrichment": {
    "dominant_event_types": ["Minaccia", "Attacco", "Combattimento"],
    "dominant_quad_classes": ["Conflitto materiale", "Tensione verbale"],
    "avg_severity_score": 8.8,
    "dominant_countries": ["IR", "BH", "AE"],
    "dominant_locations": ["Tehran, Tehran, Iran", "Manama, Bahrain"]
  },
  "mentions_enrichment": {
    "mention_count": 42,
    "distinct_mention_sources": ["thenationalnews.com", "fnnews.com"],
    "first_mention_at": "2026-03-08T04:15:00Z",
    "last_mention_at": "2026-03-09T11:45:00Z"
  },
  "gkg_enrichment": {
    "themes": ["ARMEDCONFLICT", "MIDDLE_EAST", "IRAN"],
    "persons": ["Mojtaba Khamenei"],
    "organizations": ["Arab Foreign Ministers Council"],
    "locations": ["Tehran, Tehran, Iran", "Bahrain", "UAE"],
    "document_tone_avg": -8.1
  }
}
```

---

## Note operative

### Finestra temporale
Il dataset operativo è limitato agli **ultimi 30 giorni** (allineato con la retention window dell'ingestion corrente).

### Logging dell'ingest
Ogni batch di ingest deve registrare:
- range di date importato
- numero di righe ingerite
- distribuzione per `sql_date`

### Separazione dei layer
Tenere sempre distinti, sia a livello di tabelle che di job:

| Layer | Tabella/fonte |
|---|---|
| Raw events | `gdelt_events` |
| Menzioni | `gdelt_mentions` (da aggiungere) |
| GKG | `gdelt_gkg` (da aggiungere) |
| Cluster finali | `story_clusters` (da aggiungere) |
