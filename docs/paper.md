# Cluster Creation Flow — Paper Architetturale

## Obiettivo

Il sistema di clustering trasforma il rumore nativo di GDELT in un insieme di unita' editoriali stabili e interrogabili. GDELT descrive il mondo come una sequenza di eventi atomici, menzioni di documenti e segnali semantici distribuiti. Il backend non espone direttamente questa granularita': costruisce invece cluster che cercano di rappresentare una singola storia giornalistica, oppure un mega-raggruppamento quando la stessa storia cresce oltre una soglia strutturalmente anomala.

L'obiettivo architetturale non e' soltanto aggregare record, ma imporre ordine su tre layer diversi:

- il layer evento, che misura l'azione geopolitica;
- il layer mention, che misura la propagazione mediatica;
- il layer GKG, che misura il contenuto semantico del documento.

Il risultato finale e' una vista materializzata della notizia, progettata per essere stabile nel tempo, ricostruibile in rerun successivi e accessibile via API senza dover ricomputare il grafo ogni volta. La continuita' cross-run non vive piu' nei soli `story_clusters` e `root_clusters`: vive in `cluster_components`, che conserva l'identita' persistente del componente, la sua membership storica e i suoi stati di transizione.

## Principio architetturale

Il principio guida e' component-centric first, story-centric second.

Il sistema non assume piu' che `source_url` definisca da solo un candidato di clustering. Parte invece da un grafo bipartito locale costruito su due tipi di nodo:

- gli `event_ids` della finestra temporale corrente;
- i `mention_identifiers` che collegano quegli eventi ai documenti che li citano o li rilanciano.

L'idea architetturale e' che la prima unita' affidabile non sia il singolo URL sorgente, ma il componente connesso evento-mention che emerge dal layer di propagazione mediatica. Solo dopo aver costruito questi componenti il sistema decide quali meritano di diventare cluster materiali e quali, eventualmente, vadano fusi in storie ancora piu' ampie.

Questo approccio divide il problema in due passaggi diversi:

1. costruire candidati come componenti connessi evento-mention;
2. fondere tra loro solo i cluster che mostrano evidenza forte di appartenere alla stessa storia.

La conseguenza e' importante: il pipeline resta incrementale, spiegabile e idempotente, ma smette di dipendere dall'assunzione ormai deprecata "stesso `source_url` = stesso candidato".

## Entry points del flusso

La creazione dei cluster puo' partire da due ingressi principali.

Il primo e' lo scheduler applicativo. Il job schedulato richiama periodicamente la materializzazione dei cluster su una finestra mobile di 36 ore, ma la finestra non e' ancorata all'orologio di sistema: e' ancorata all'ultimo `date_added` realmente ingerito in `gdelt_events`. Questa finestra e' piu' ampia dell'intervallo di scheduling per assorbire i ritardi fisiologici dell'ingestion GDELT e ridurre i buchi tra una run e la successiva. In questo modello, il clustering e' un processo ricorrente di consolidamento.

Il secondo ingresso e' la CLI manuale. Questo path serve per forzare una materializzazione su richiesta, utile per debug, backfill controllati o verifiche operative. Dal punto di vista architetturale, la CLI non introduce una semantica diversa: riusa lo stesso `ClusterService`, quindi il comportamento resta allineato al job schedulato. C'e' pero' un prerequisito operativo esplicito: il database deve essere migrato fino alla revisione che introduce `cluster_components` e `cluster_component_events`. Se queste tabelle non esistono ancora, il sistema fallisce in modo intenzionale e deve essere riallineato con `alembic upgrade head`.

Questa convergenza su un unico servizio centrale e' una scelta di coerenza: cambia il trigger, non la logica.

### Startup catch-up dell'ingestion

L'ingestion ha un meccanismo analogo all'entry point manuale del clustering: alla partenza del processo il sistema verifica non solo se sia necessario un bootstrap iniziale, ma anche se il watermark dell'ultima ingestion incrementale e' abbastanza indietro nel tempo da giustificare un recupero immediato. La soglia e' quattro ore: se l'ultimo `watermark_dateadded` e' piu' vecchio di quattro ore rispetto all'ora corrente, viene eseguita subito una run incrementale invece di aspettare il primo tick schedulato. Questo copre i casi operativi piu' frequenti — riavvii dopo deploy, finestre di manutenzione, interruzioni di rete — senza richiedere intervento manuale. Riavvii normali (inferiori alla soglia) non triggherano il catch-up e lasciano il primo tick schedulato operare nel modo consueto.

## Dati in ingresso

Il clustering non lavora su dati esterni in tempo reale. Lavora su uno store locale gia' popolato dall'ingestion GDELT. I suoi tre ingressi strutturali sono:

- `gdelt_events`, che contiene gli eventi geopolitici di base;
- `gdelt_mentions`, che contiene i documenti che citano quegli eventi;
- `gdelt_gkg`, che contiene temi, persone, organizzazioni, luoghi e tono dei documenti.

Ogni tabella esprime un tipo di verita' diverso.

`gdelt_events` dice cosa e' successo secondo la codifica GDELT. `gdelt_mentions` dice quanto e dove la notizia si e' propagata. `gdelt_gkg` dice di cosa parla semanticamente il documento. L'architettura del clustering funziona proprio perche' questi layer restano separati fino al momento della composizione finale. Non vengono fusi a monte; vengono orchestrati a valle.

## Fase 1: costruzione dei candidati come componenti

Il primo problema da risolvere e' evitare che ogni evento isolato o ogni URL rumoroso diventi un cluster. Il sistema applica quindi una fase di costruzione e filtro del grafo che ha un ruolo architetturale, non solo di ottimizzazione.

Su tutti gli eventi nella finestra temporale richiesta, il servizio:

1. carica gli eventi dotati di `source_url`;
2. carica le relative mentions;
3. filtra i nodi mention prima della costruzione del grafo;
4. costruisce i componenti connessi tra eventi e `mention_identifiers`.

I filtri vengono applicati a livello di nodo mention, non come post-processing sui cluster gia' formati. Oggi includono:

- esclusione dei domini in `cluster_source_domain_blocklist`;
- esclusione degli URL che somigliano a pagine sezione, category, search o archive;
- rimozione implicita dei candidati singleton, cioe' componenti che non mostrano connettivita' reale tra piu' eventi.

Una volta costruito il componente, il sistema misura esplicitamente alcune proprieta' strutturali:

- numero di eventi;
- numero di `source_url` distinti;
- numero di domini distinti;
- densita' del componente nel grafo evento-mention;
- ampiezza temporale degli eventi.

Questi segnali sostituiscono il vecchio candidate scoring per `source_url`. L'ammissione del candidato non dipende piu' dal `topic_score` storico, ma da gate strutturali configurabili come `cluster_candidate_min_event_ids`, `cluster_candidate_min_source_urls`, `cluster_candidate_min_domains`, `cluster_candidate_min_density` e `cluster_candidate_max_event_span_hours`.

Architetturalmente, questa fase serve a impedire che il sistema costruisca cluster su strutture troppo deboli, troppo isolate o troppo rumorose. Il pipeline non chiede piu' "questo URL e' abbastanza forte?"; chiede invece "questo componente rappresenta davvero una storia condivisa da piu' eventi e piu' fonti?".

## Fase 2: raccolta batch dei layer

Una volta ottenuti i componenti ammessi, il sistema non esegue arricchimenti isolati uno per uno. Esegue raccolte batch per ridurre round-trip e mantenere il flusso sotto controllo.

L'ordine logico e' questo:

1. raccoglie gli eventi della finestra;
2. raccoglie tutte le mentions collegate a quegli eventi;
3. raccoglie i record GKG associati ai `source_url` rappresentati nei componenti ammessi.

Il punto architetturalmente piu' delicato e' il terzo. Il sistema non usa il GKG di documenti esterni al componente; usa solo i GKG dei `source_url` che appartengono al candidato stesso. Questo allarga la base semantica rispetto al vecchio modello single-URL, ma resta confinato al perimetro del componente. In questo modo il cluster puo' ereditare temi, persone, organizzazioni, luoghi e tono da piu' URL editorialmente coinvolti nella stessa storia, senza contaminarsi con documenti esterni che hanno solo citato incidentalmente l'evento.

## Fase 3: costruzione del cluster componente

Per ogni candidato ammesso, il sistema costruisce una rappresentazione completa della storia locale emersa dal componente.

Il `cluster_id` e' deterministico ed e' derivato dall'hash dell'intero insieme ordinato degli `event_ids` del componente. Ma questo identificatore non e' piu' la sorgente di verita' cross-run. La continuita' persistente e' delegata a `component_id`, assegnato alla prima osservazione del componente e conservato in `cluster_components` anche quando il cluster materiale cambia `cluster_id`, cresce, si fonde o cambia tabella di materializzazione. In questo schema, `story_clusters` e `root_clusters` sono proiezioni materializzate del run corrente; `cluster_components` e' la timeline persistente.

Dentro questo cluster confluiscono tre famiglie di attributi.

La prima famiglia e' quantitativa: `event_count`, `num_articles`, `num_mentions`, `num_sources`, `topic_score`. Serve a misurare il peso della storia. In particolare, il `topic_score` del cluster non e' piu' il vecchio score documentale basato su un solo URL, ma un punteggio ricalcolato dai breadth signals del componente: numero di eventi, numero di URL distinti e numero di domini distinti.

La seconda famiglia e' interpretativa sul layer evento: tipi di evento dominanti, classi quad dominanti, severita' media, paesi e location dominanti, insieme degli `event_ids`, e range delle date evento. Serve a dire quale forma geopolitica assume la storia.

La terza famiglia e' narrativa: fonti di mention, finestra temporale delle mention, temi, persone, organizzazioni, luoghi GKG e tono documentale medio. Serve a rendere il cluster leggibile come oggetto quasi editoriale, non solo analitico. Anche `mention_count` viene trattato in modo conservativo: rappresenta il numero di `mention_identifiers` distinti del componente, non la somma grezza delle righe mention, per evitare doppio conteggio quando lo stesso URL e' condiviso da piu' eventi.

Per i campi GKG aggregati — temi, persone, organizzazioni, luoghi — il sistema non produce piu' un semplice insieme deduplicato. Usa un ranking per frequenza documentale: ogni valore viene contato una volta per documento GKG che lo contiene, poi i valori vengono ordinati per frequenza decrescente e troncati ai cap configurati (`cluster_gkg_themes_cap`, `cluster_gkg_persons_cap`, ecc.). Questo approccio privilegia i segnali semantici con consenso editoriale reale rispetto a quelli che compaiono in un solo documento isolato, migliorando la qualita' dei campi GKG esposti sia nel cluster non ancora arricchito sia come input al processo di merge.

Lo stesso principio si applica ai `mention_identifiers`: invece di essere semplicemente ordinati in modo alfanumerico, vengono ordinati per frequenza di corroborazione decrescente — l'URL piu' citato dalle fonti del componente compare prima. Questo ordine ha una conseguenza diretta sull'enrichment: i candidati con piu' corroborazione vengono tentati per primi.

A questo punto il sistema non ha ancora creato una storia globale. Ha creato una buona unita' documentale consistente. Se il componente non ha copertura GKG locale, resta comunque valido: viene materializzato con `has_gkg = false`, senza fallback verso documenti esterni al perimetro del componente, e questa assenza viene resa esplicita nei log applicativi.

## Fase 4: fusione in storie piu' ampie

La fusione e' il passaggio in cui l'architettura smette di essere document-centric e diventa story-centric.

Il componente responsabile e' `ClusterMerger`, che usa un modello Union-Find. Il motivo non e' teorico ma pratico: una volta stabilito che A e' legato a B e B e' legato a C, il sistema deve poter trattare l'intero componente connesso come una sola storia senza rieseguire continuamente confronti globali.

La fusione non avviene su una singola somiglianza generica. E' governata da due criteri principali:

- overlap di `mention_identifiers`, cioe' condivisione di URL che hanno ripreso la stessa storia;
- similarita' Jaccard dei temi GKG, per cogliere cluster diversi ma semanticamente convergenti.

Su questi criteri agiscono due gate di sicurezza:

- prossimita' temporale tra i range delle date evento;
- condivisione di almeno un tipo di evento dominante.

Architetturalmente, questi gate impediscono che due cluster vengano fusi solo perche' condividono segnali deboli o generici. Un tema comune molto frequente o un singolo overlap rumoroso non bastano da soli se il profilo temporale o il tipo di azione raccontata divergono troppo.

Quando piu' cluster vengono fusi, il sistema sceglie ancora un anchor con `topic_score` piu' alto come URL rappresentativo, ma non ne eredita piu' l'identita' logica. L'identita' finale del cluster fuso viene ricalcolata in modo deterministico dall'insieme ordinato degli `event_ids` risultanti. L'anchor orienta la rappresentazione esterna; la struttura completa del componente fuso determina invece l'identita' persistente.

Durante la fusione, i campi GKG del cluster risultante — temi, persone, organizzazioni, luoghi — vengono calcolati con lo stesso algoritmo di frequenza documentale usato nella fase di costruzione. I valori piu' frequenti tra tutti i sub-cluster che partecipano alla fusione vengono conservati; i valori rari o di nicchia vengono eliminati secondo i cap configurati. Questa scelta impedisce che un cluster fuso molto grande accumuli liste GKG illimitate che degraderebbero sia la qualita' esposta via API sia l'efficienza del matching semantico nelle run successive.

## Fase 5: partizione finale tra story e root

Dopo il merge, il sistema applica una decisione architetturale ulteriore: non tutti i cluster fusi restano nella stessa categoria di output.

Se il cluster finale supera la soglia `root_cluster_min_event_count`, viene classificato come root cluster. In caso contrario resta uno story cluster standard. La soglia viene valutata dopo il merge, non prima. Questo e' cruciale, perche' la dimensione vera della storia emerge solo a componente fuso completo.

La separazione produce due viste materializzate distinte:

- `story_clusters` per le storie normali;
- `root_clusters` per i mega-cluster.

La scelta architetturale qui non e' semplicemente dividere per dimensione. E' preservare due semantiche diverse. Uno story cluster e' una notizia coerente e navigabile. Un root cluster e' una macro-struttura che rischierebbe di inquinare la stessa superficie di query se vivesse nello stesso spazio logico.

## Persistenza, idempotenza e riconciliazione

La persistenza non e' append-only. E' idempotente e riconciliativa.

Sia `story_clusters` sia `root_clusters` usano upsert keyed by `cluster_id`. Questo significa che ogni run non crea necessariamente nuove righe: aggiorna la versione corrente del cluster conosciuto. Se un cluster viene ricostruito con contenuto piu' ricco, viene riscritto in place dal punto di vista logico.

La riconciliazione cross-run avviene pero' su un livello piu' profondo. `cluster_components` conserva:

- `component_id` immutabile assegnato alla prima osservazione;
- membership attiva e storica degli `event_ids`;
- anchor originario e insieme degli URL sorgente osservati nel tempo;
- stato del componente (`active`, `merged`, `split`, `stale`);
- soft link verso la proiezione materiale corrente (`current_cluster_id`, `current_table`).

Quando una run corrente incontra componenti storici multipli, il sistema sceglie come canonico il componente piu' vecchio e marca gli altri come `merged`. Quando la membership storica si ramifica e nessun ramo singolo raggiunge la soglia di continuita' configurata, il componente viene marcato `split`. Quando un componente non viene piu' osservato per `cluster_component_stale_after_missing_runs`, viene marcato `stale`.

Un dettaglio importante e' che questa logica di aging non vale solo quando la run produce nuovi cluster. Vale anche nelle finestre a risultato zero. Se una finestra temporale non genera alcuna materializzazione, il sistema esegue comunque la fase di riconciliazione persistente e invecchia i componenti storici non osservati. Questo evita che componenti ormai scomparsi restino `active` indefinitamente solo perche' le run successive non hanno prodotto cluster.

Ma il punto davvero importante e' la riconciliazione tra tabelle opposte. Un cluster puo' cambiare categoria tra una run e la successiva. Se prima era una story e poi supera la soglia root, deve sparire da `story_clusters` e comparire solo in `root_clusters`. Se il fenomeno si ridimensiona, deve accadere il contrario.

Il sistema quindi non si limita a scrivere nella tabella giusta: rimuove l'eventuale proiezione materiale precedente dello stesso `component_id` quando questo cambia `cluster_id` o cambia tabella di destinazione. Questo punto e' architetturalmente cruciale, perche' la pulizia non puo' piu' essere guidata da `source_url`: due cluster distinti possono condividere lo stesso URL rappresentativo senza essere lo stesso oggetto persistente. La mutua esclusione tra `story_clusters` e `root_clusters` viene quindi mantenuta tramite il soft link del componente persistente (`current_cluster_id`, `current_table`), non tramite euristiche sull'anchor URL.

Il sistema esegue controlli di consistenza a inizio di ogni run di materializzazione. Il comportamento originale era fail-fast: qualsiasi anomalia del soft link causava l'interruzione immediata della run con un errore esplicito. Il comportamento attuale e' heal-and-continue: i componenti attivi che presentano un soft link mancante, un puntatore verso una tabella sconosciuta o un riferimento a un cluster materiale inesistente vengono marcati automaticamente come `stale` con un log di warning, e la run prosegue normalmente. Questo self-healing rende il pipeline resiliente rispetto a stati inconsistenti introdotti da migrazioni parziali, rollback o interruzioni anomale, senza richiedere intervento manuale per ogni anomalia diagnosticata.

## Esposizione via API

Le API non calcolano cluster al volo. Espongono viste gia' materializzate.

`/clusters/search` legge solo da `story_clusters`. `/root-clusters/search` legge solo da `root_clusters`. Entrambe le route condividono lo stesso schema pubblico di risposta, perche' la forma dati e' volutamente simmetrica. Cambia la semantica del contenitore, non il contratto del payload.

### Filtri di ricerca

Il set di filtri disponibili e' cresciuto considerevolmente rispetto alla versione originale che esponeva solo `min_score` e `country_code`. Ogni filtro e' opzionale e combinabile con gli altri.

I filtri strutturali e temporali comprendono:

- `min_score`: soglia inferiore sul `topic_score` del cluster;
- `min_event_count`: numero minimo di eventi GDELT nel cluster;
- `min_mentions`: numero minimo di mention distinte;
- `country_code`: codice ISO 3166-1 alpha-2 su `dominant_countries`;
- `date_from` / `date_to`: finestra YYYYMMDD su `event_date_ref_start`;
- `mentioned_after` / `mentioned_before`: finestra ISO-8601 su `first_mention_at` / `last_mention_at`;
- `event_type`: codice radice GDELT presente in `dominant_event_types`;
- `quad_class`: valore GDELT quad class (1-4) presente in `dominant_quad_classes`;
- `theme`: tema GKG presente nel campo `themes` del cluster.

I filtri semantici post-enrichment comprendono:

- `enrichment_status`: stato dell'enrichment LLM (`pending`, `processing`, `success`, `failed`);
- `keyword`: parola chiave presente nel campo LLM `keywords`, efficace solo per cluster con `enrichment_status=success`;
- `topic`: categoria presente nel campo LLM `main_topics`, efficace solo per cluster arricchiti.

### Schema di risposta e blocco enrichment

Lo schema della risposta pubblica e' stato allineato alla dicotomia GKG/LLM. Il blocco enrichment e' mutualmente esclusivo:

- quando `enrichment_status != 'success'`, la risposta include `gkg_enrichment` (layer semantico GDELT) e `llm_enrichment` e' null;
- quando `enrichment_status == 'success'`, la risposta include `llm_enrichment` (output del modello) e `gkg_enrichment` e' null.

`mentions_enrichment` e' sempre presente indipendentemente dallo stato. Il campo `mention_identifiers` all'interno di questo blocco espone la lista ordinata degli URL candidati all'enrichment, utile per diagnostica e per ispezione dei documenti sorgente.

Il campo `event_ids` e' stato rimosso dalla risposta pubblica: era ridondante rispetto alle altre proiezioni del cluster e aumentava il payload senza aggiungere informazione interrogabile lato client.

Questa scelta mantiene il confine tra costruzione e consultazione. Il lavoro pesante avviene nel pipeline. L'API resta un livello sottile di accesso a strutture gia' consolidate.

## Configurazione come strato di governo

Il comportamento del pipeline non e' hardcoded in un solo punto. E' governato da configurazioni che agiscono come leve architetturali.

Tra le piu' importanti:

- la finestra temporale usata dal job schedulato;
- il blocklist dei domini da escludere;
- i segmenti URL che definiscono pagine sezione;
- le soglie minime di `event_ids`, `source_url` e domini per ammettere un componente;
- la densita' minima del componente e il massimo span temporale ammesso;
- il massimo gap temporale ammesso nel merge;
- i parametri del merge per overlap mention, soglia Jaccard, cap dei temi e document frequency massima dei temi;
- la soglia che separa story cluster e root cluster;
- la soglia di continuita' che decide quando una storia storica resta continua vs quando diventa `split`;
- il numero di run mancate prima della transizione a `stale`;
- i cap per i campi GKG (`cluster_gkg_themes_cap`, `cluster_gkg_persons_cap`, `cluster_gkg_orgs_cap`, `cluster_gkg_locations_cap`) che controllano quante voci vengono conservate per ciascun campo dopo il ranking per frequenza documentale;
- `cluster_enrichment_max_articles`: numero massimo di articoli da fonti diverse da combinare prima della chiamata LLM (default 3).

Questo rende il sistema adattabile senza riscrivere il modello logico. In termini architetturali, il codice implementa il pipeline; la configurazione ne modula il comportamento operativo.

## Failure modes e difese del sistema

Il sistema assume che i dati siano rumorosi, incompleti e a volte semanticamente fuorvianti. Per questo incorpora difese distribuite nel flusso.

Scarta nodi mention structuralmente sbagliati gia' in ingresso. Costruisce candidati solo quando esiste connettivita' reale tra eventi e propagazione mediatica. Limita i merge con gate temporali e tipologici. Usa upsert per evitare duplicazioni su rerun. Riconcilia i category flip per mantenere mutua esclusione tra story e root. Esegue aging persistente anche nelle run vuote, cosi' lo stato storico continua a evolvere anche quando la finestra non produce cluster nuovi.

Non elimina ogni possibile errore semantico, ma riduce i due rischi principali del clustering giornalistico:

- esplosione del rumore, quando pagine aggregate o temi generici generano falsi cluster;
- collasso eccessivo, quando storie diverse vengono fuse in un unico mostro narrativo.

L'architettura non promette perfezione ontologica. Promette un equilibrio tra robustezza, costo computazionale, spiegabilita' e utilita' applicativa.

## Retention degli stati terminali

Gli stati terminali come `merged` e `split` restano utili per auditabilita', debug e ricostruzione storica di breve periodo, ma non crescono senza limite. Quando un componente entra in uno stato terminale, il sistema disattiva subito le sue membership attive in `cluster_component_events` e rimuove l'eventuale materializzazione corrente da `story_clusters` o `root_clusters`, cosi' il componente non partecipa piu' alla riconciliazione operativa e non resta esposto come cluster attivo.

La retention applicata e' di tipo delete-after-retention: un job schedulato dedicato elimina periodicamente i componenti in stato `merged` o `split` che hanno superato una finestra operativa configurabile dalla transizione, insieme alle relative righe storiche di membership. Il default e' 7 giorni, coerente con un contesto newsroom in cui il valore operativo residuo di una storia terminale decade rapidamente, ma il parametro puo' essere aumentato se serve piu' buffer per debug o audit operativo.

Il job di retention delle tabelle GDELT e' stato esteso per coprire tutti e tre i layer di ingestion. In origine eliminava solo le righe di `gdelt_events` piu' vecchie della finestra configurata. Ora rimuove in sequenza anche le righe di `gdelt_mentions` e `gdelt_gkg` con `date_added` precedente alla stessa soglia. Questa estensione previene la crescita illimitata delle due tabelle di supporto, che in contesti ad alto volume tendono ad accumularsi piu' rapidamente degli eventi stessi perche' ogni evento puo' generare decine di mention e record GKG. Le tre cancellazioni avvengono in commit separati per ridurre la pressione sulle transazioni e il numero di righe bloccate in un singolo statement.

## Sintesi finale

La creazione dei cluster in questo backend e' un pipeline di materializzazione a piu' layer. Parte da eventi locali gia' ingeriti, costruisce candidati come componenti connessi evento-mention, ammette solo quelli che superano gate strutturali espliciti, li arricchisce con i layer evento/mention/GKG, fonde i casi che mostrano evidenza di appartenere alla stessa storia, poi separa l'output finale in cluster normali e mega-cluster.

La forza architetturale del sistema sta nel fatto che ogni passaggio ha una responsabilita' netta:

- la costruzione del grafo decide quali componenti meritano attenzione;
- la raccolta batch compone i tre layer informativi;
- il cluster build crea l'unita' locale del componente;
- il merger crea l'unita' narrativa piu' ampia;
- la partizione story/root organizza l'output per consumo applicativo;
- l'API espone solo viste gia' consolidate.

In questo senso, il sistema non e' solo un algoritmo di clustering. E' una pipeline editoriale deterministica che trasforma segnali eterogenei in oggetti narrativi stabili.

---

## LLM Enrichment dei cluster

### Motivazione

Il pipeline di clustering produce unita' editoriali strutturalmente solide — componenti connessi, score di copertura, layer evento/mention/GKG — ma non produce linguaggio naturale. Un cluster sa quanti eventi lo compongono, quali paesi dominano, quali temi GKG emergono. Non sa cosa dice l'articolo, chi cita, di cosa parla in modo comprensibile a un consumatore finale.

L'enrichment LLM risolve questo gap aggiungendo al cluster una voce editoriale: titolo canonico, sommario neutro, entita' estratte dal testo, topics e keywords derivati dal contenuto reale del documento, non dai codici GDELT.

### Unita' di enrichment

L'unita' di enrichment e' il cluster, non il singolo evento. Questa scelta e' coerente con il principio architetturale del sistema: il cluster e' la prima unita' stabile e interrogabile; arricchire i singoli eventi sarebbe ridondante e disallineato rispetto a cio' che viene esposto dall'API.

I campi LLM vengono scritti direttamente su `story_clusters` e `root_clusters` come colonne native, senza tabelle satellite. Il cluster e' gia' una vista materializzata pronta per la query; l'enrichment ne completa la rappresentazione senza introdurre join aggiuntivi.

### Multi-article synthesis

Il modello originale usava un singolo URL per arricchire il cluster: il primo `mention_identifier` valido veniva fetchato e il suo contenuto veniva passato all'LLM. Questo approccio era semplice ma soffriva di due limitazioni strutturali: la copertura della storia dipendeva da un solo documento, e se quel documento era opinionistico, parziale o di qualita' bassa, l'intero enrichment ne risentiva.

Il modello attuale e' basato su sintesi multi-articolo. Per ogni cluster l'enrichment raccoglie fino a `cluster_enrichment_max_articles` (default 3) articoli da domini distinti, li concatena separati da un delimitatore esplicito e li passa insieme all'LLM in una sola chiamata. L'ordine di selezione segue la lista `mention_identifiers` pre-ordinata per frequenza di corroborazione: i documenti piu' citati vengono tentati per primi. Il sistema garantisce diversita' di fonte: al massimo un articolo per dominio viene incluso nel batch, cosi' un singolo outlet che rilancia la stessa storia con URL diversi non satura il contesto.

Questa scelta ha due conseguenze architetturali importanti. La prima e' qualitativa: il titolo e il sommario prodotti dal modello sintetizzano il consenso editoriale di piu' fonti indipendenti invece di riflettere la prospettiva di un singolo documento. La seconda e' di robustezza: se un articolo e' irraggiungibile o di contenuto insufficiente, il sistema non fallisce il cluster — usa gli articoli rimanenti fino al raggiungimento del cap configurato.

### URL resolution e retry

I `mention_identifiers` sono gia' ordinati per frequenza di corroborazione nella fase di costruzione del cluster. Quando il job di enrichment processa un cluster, itera questa lista nell'ordine dato e tenta di fetchare ogni URL.

Ogni URL viene tentato fino a `_URL_MAX_RETRIES` (2) volte prima di essere considerato fallito. I tentativi multipli assorbono errori transitori di rete senza abbandonare l'URL dopo un singolo timeout. Se un URL esaurisce i tentativi, viene inserito nella cache come eccezione e saltato immediatamente per tutti gli altri cluster del batch che lo condividono.

Il fallback su `source_url` presente nella versione originale e' stato rimosso: i `mention_identifiers` coprono gia' lo stesso contenuto con diversita' di fonte, rendendo il fallback sull'anchor URL strutturalmente ridondante.

### Deduplicazione intra-batch

Piu' cluster nello stesso batch possono condividere gli stessi `mention_identifier`. La cache URL locale al batch evita fetch ridondanti: il primo cluster che risolve un URL ne copia il contenuto estratto in cache; i successivi riusano il risultato senza nuovi round-trip HTTP. Le failure vengono cacheate allo stesso modo, cosi' URL gia' noti come non raggiungibili vengono saltati immediatamente negli accessi successivi.

### Priorita' e filtro del batch

Il job di enrichment non processa i cluster pending in ordine cronologico di materializzazione. Li ordina per `topic_score` decrescente: le storie con maggiore copertura e piu' alto segnale editoriale vengono arricchite prima. Questo massimizza l'utilita' dei primi batch nei contesti in cui il numero di cluster pending supera la dimensione del batch configurato.

Il batch supporta anche filtri temporali opzionali (`date_from` / `date_to` nel formato YYYYMMDD) applicati a `event_date_ref_start`. I filtri sono disponibili sia nel job schedulato sia nel trigger manuale via `POST /enrich/trigger`, e permettono di concentrare l'enrichment su finestre specifiche — utile per debug, backfill mirati o prioritizzazione di notizie recenti dopo un lungo periodo di inattivita'.

### Recupero degli stati bloccati

Quando il job di enrichment viene interrotto in modo anomalo — crash del processo, timeout del container, riavvio forzato — alcuni cluster possono restare nello stato `processing` indefinitamente. La versione originale non aveva un meccanismo di recupero automatico: questi cluster erano di fatto persi per il job successivo, che li ignorava perche' non piu' in stato `pending`.

Il meccanismo attuale esegue un reset degli stati bloccati all'inizio di ogni run del batch. Qualsiasi cluster in stato `processing` che non ha un `enriched_at` (quindi non ha mai completato) e il cui `computed_at` e' precedente di almeno `_STALE_PROCESSING_MINUTES` (15 minuti) viene riportato automaticamente a `pending`. Questo fa si' che il job successivo li ripicchi normalmente senza perdita di candidati.

### Microservizio Ollama

La chiamata LLM non avviene dentro il processo principale. Esiste un microservizio separato (`enrichment_service`) che gira localmente su porta 8001 e funge da adapter verso Ollama. Il main app lo chiama via `POST /enrich` con il contenuto estratto dall'articolo o dalla combinazione di articoli; il microservizio costruisce il prompt, chiama Ollama, valida la risposta JSON contro lo schema atteso e la restituisce.

Questa separazione mantiene il main app indipendente dal modello locale: il microservizio puo' cambiare modello, prompt o provider LLM senza toccare il pipeline principale.

Il sistema prompt e' stato aggiornato per operare in modalita' multi-articolo: istruisce esplicitamente il modello a sintetizzare il quadro completo della notizia a partire da tutti gli articoli forniti, separati da `---`. Quando gli articoli divergono su un fatto, il modello e' istruito a riflettere il claim piu' corroborato o a segnalare la divergenza nel sommario soltanto se editorialmente rilevante. Il titolo canonico viene derivato dal primo articolo (il piu' corroborato) o sintetizzato se il titolo originale e' fuorviante o troppo ristretto. L'output deve essere sempre e solo JSON valido senza markdown o prosa aggiuntiva.

### Schema dei campi arricchiti

I campi aggiunti a `story_clusters` e `root_clusters` sono:

- `article_title`: titolo canonico estratto o sintetizzato dall'LLM;
- `article_summary`: sommario neutro in 2-4 frasi che sintetizza piu' articoli;
- `cited_sources`: outlet, agenzie o pubblicazioni citate come fonte in qualsiasi degli articoli;
- `main_topics`: 3-8 categorie tematiche ad alto livello;
- `keywords`: 5-15 termini specifici e distintivi derivati dall'insieme degli articoli;
- `entities`: oggetto con 10 bucket (persone, organizzazioni, luoghi, etnie, religioni, occupazioni, affiliazioni politiche, industrie, prodotti, brand); ogni bucket e' deduplicato cross-articolo;
- `enrichment_status`: stato della macchina a stati (`pending`, `processing`, `success`, `failed`);
- `enriched_at`: timestamp UTC dell'ultimo enrichment riuscito;
- `enrichment_error`: messaggio dell'ultimo errore, per diagnostica.

Il valore `success` dello stato (precedentemente `succeeded`) e' allineato al parametro `enrichment_status` dei filtri API per evitare disallineamenti tra il valore scritto nel DB e il valore atteso nelle query.

### Macchina a stati e idempotenza

Ogni cluster nasce con `enrichment_status = pending`. Il pipeline di materializzazione non tocca questi campi: il ricalcolo del cluster su nuovi dati non azzera un enrichment gia' completato. L'upsert su `cluster_id` aggiorna solo le colonne presenti nel dict del pipeline; le colonne LLM vengono modificate esclusivamente dal job di enrichment.

Il job schedulato (`CLUSTER_ENRICHMENT_INTERVAL_MINUTES`, default 30 min) processa un batch configurabile di cluster pending (`CLUSTER_ENRICHMENT_BATCH_SIZE`, default 20) per `story_clusters` e poi per `root_clusters`, con sessioni DB separate. E' disponibile anche un trigger manuale via `POST /enrich/trigger` protetto da API key e cooldown di 1 minuto, con supporto opzionale ai filtri `date_from` / `date_to`.
