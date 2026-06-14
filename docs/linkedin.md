# LinkedIn copy — GDELT News Backend

Positioning: **AI / LLM Engineer**. Pick the version that fits where you're posting.
Replace `https://github.com/DavideLAbbate/EarthSignals-GDLET-News-module` with the public GitHub link before publishing.

---

## 🇮🇹 Post LinkedIn (versione lunga)

Ho appena reso pubblico un progetto su cui ho lavorato a lungo: un backend in **Python / FastAPI** che trasforma il feed globale di notizie **GDELT 2.0** in storie consultabili e arricchite da LLM. 🧠📰

La parte che mi ha insegnato di più è il **design a due LLM**, ciascuno per un problema diverso:

🔹 **Anthropic Claude come interprete di filtri** — l'utente cerca in linguaggio naturale ("proteste in Italia", "crisi energetica") e Claude lo traduce in codici di query precisi. Con cache content-addressed (SHA-256), output validato contro schema Pydantic, retry con backoff e un percorso che salta del tutto l'LLM quando i filtri sono già strutturati. L'LLM è uno strumento, non un costo incontrollato.

🔹 **Un LLM locale (Ollama) per la sintesi multi-articolo** — per ogni storia raggruppata, il sistema scarica più articoli da fonti diverse e genera titolo, sommario, topic, keyword ed entità. Gira come microservizio separato, con state machine idempotente (pending → processing → success/failed), recupero degli stati bloccati e dedup delle fetch nel batch.

Sotto c'è la parte di data engineering che rende tutto possibile: ingestion dai file HTTP di GDELT, una pipeline di **clustering basata su grafo** che fonde tre layer (eventi, menzioni, semantica GKG) in storie persistenti cross-run, scheduler in background, architettura a layer (routes → services → repositories), SQLAlchemy async + Alembic, e ~38 file di test che mockano ogni dipendenza esterna.

Cosa mi porto a casa: integrare LLM in produzione non è "chiamare un'API". È caching, validazione, retry, idempotenza, cost-awareness e graceful degradation.

Codice + write-up architetturale completo qui 👉 https://github.com/DavideLAbbate/EarthSignals-GDLET-News-module

#Python #FastAPI #LLM #AI #MachineLearning #Anthropic #Ollama #DataEngineering #BackendDevelopment

---

## 🇮🇹 Sezione "Progetti" del profilo (versione breve)

**GDELT News Backend — Applied LLM / FastAPI**
Backend Python/FastAPI che trasforma il feed news GDELT 2.0 in storie arricchite da LLM. Design a due modelli: Anthropic Claude per la normalizzazione dei filtri in linguaggio naturale (cache, retry, output validato) e un LLM locale (Ollama) per la sintesi multi-articolo (titolo, sommario, entità). Include una pipeline di clustering basata su grafo su tre layer GDELT, scheduler async, PostgreSQL + SQLAlchemy async, Docker, CI e suite di test completa.
🔗 https://github.com/DavideLAbbate/EarthSignals-GDLET-News-module

---

## 🇬🇧 LinkedIn post (long version)

Just open-sourced a project I've been building for a while: a **Python / FastAPI** backend that turns the global **GDELT 2.0** news feed into queryable, LLM-enriched stories. 🧠📰

What taught me the most was the **dual-LLM design** — two models, two very different jobs:

🔹 **Anthropic Claude as a filter interpreter** — users search in plain language ("protests in Italy", "energy crisis") and Claude maps it to precise query codes. With a content-addressed cache (SHA-256), output validated against a strict Pydantic schema, retries with backoff, and a path that skips the LLM entirely when filters are already structured. The model is a tool, not an open-ended bill.

🔹 **A local LLM (Ollama) for multi-article synthesis** — for each clustered story, the system fetches several articles from distinct sources and produces a canonical title, neutral summary, topics, keywords and entities. It runs as a separate microservice with an idempotent state machine (pending → processing → success/failed), stale-state recovery, and per-batch fetch deduplication.

Underneath sits the data engineering that makes it work: ingestion from GDELT's HTTP exports, a **graph-based clustering pipeline** that merges three layers (events, mentions, GKG semantics) into persistent cross-run stories, background scheduling, a clean layered architecture (routes → services → repositories), async SQLAlchemy + Alembic, and ~38 test files that mock every external dependency.

Biggest takeaway: shipping LLMs to production isn't "calling an API." It's caching, validation, retries, idempotency, cost-awareness and graceful degradation.

Code + full architecture write-up 👉 https://github.com/DavideLAbbate/EarthSignals-GDLET-News-module

#Python #FastAPI #LLM #AI #MachineLearning #Anthropic #Ollama #DataEngineering #BackendDevelopment

---

## 🇬🇧 Profile "Projects" section (short version)

**GDELT News Backend — Applied LLM / FastAPI**
Python/FastAPI backend that turns the GDELT 2.0 news feed into LLM-enriched stories. Dual-model design: Anthropic Claude for natural-language filter normalization (caching, retries, validated output) and a local LLM (Ollama) for multi-article synthesis (title, summary, entities). Features a graph-based clustering pipeline across three GDELT layers, async scheduling, PostgreSQL + async SQLAlchemy, Docker, CI and a full test suite.
🔗 https://github.com/DavideLAbbate/EarthSignals-GDLET-News-module
