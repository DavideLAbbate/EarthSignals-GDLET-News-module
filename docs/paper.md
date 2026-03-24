# Cluster Creation Flow — Paper Architetturale

## Obiettivo

Il sistema di clustering trasforma il rumore nativo di GDELT in un insieme di unita' editoriali stabili e interrogabili. GDELT descrive il mondo come una sequenza di eventi atomici, menzioni di documenti e segnali semantici distribuiti. Il backend non espone direttamente questa granularita': costruisce invece cluster che cercano di rappresentare una singola storia giornalistica, oppure un mega-raggruppamento quando la stessa storia cresce oltre una soglia strutturalmente anomala.

L'obiettivo architetturale non e' soltanto aggregare record, ma imporre ordine su tre layer diversi:

- il layer evento, che misura l'azione geopolitica;
- il layer mention, che misura la propagazione mediatica;
- il layer GKG, che misura il contenuto semantico del documento.

Il risultato finale e' una vista materializzata della notizia, progettata per essere stabile nel tempo, ricostruibile in rerun successivi e accessibile via API senza dover ricomputare il grafo ogni volta.

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

Il primo e' lo scheduler applicativo. Il job schedulato richiama periodicamente la materializzazione dei cluster su una finestra mobile di 36 ore. Questa finestra e' piu' ampia dell'intervallo di scheduling per assorbire i ritardi fisiologici dell'ingestion GDELT e ridurre i buchi tra una run e la successiva. In questo modello, il clustering e' un processo ricorrente di consolidamento.

Il secondo ingresso e' la CLI manuale. Questo path serve per forzare una materializzazione su richiesta, utile per debug, backfill controllati o verifiche operative. Dal punto di vista architetturale, la CLI non introduce una semantica diversa: riusa lo stesso `ClusterService`, quindi il comportamento resta allineato al job schedulato.

Questa convergenza su un unico servizio centrale e' una scelta di coerenza: cambia il trigger, non la logica.

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

Il `cluster_id` e' deterministico ed e' derivato dall'hash dell'intero insieme ordinato degli `event_ids` del componente. Questa decisione e' centrale. Significa che l'identita' del cluster non dipende dall'URL rappresentativo scelto in quella run, ma dalla struttura fattuale del componente. Se in un rerun cambia il `source_url` piu' rappresentativo, il cluster continua comunque a puntare allo stesso identificatore logico finche' il set di eventi resta lo stesso.

Dentro questo cluster confluiscono tre famiglie di attributi.

La prima famiglia e' quantitativa: `event_count`, `num_articles`, `num_mentions`, `num_sources`, `topic_score`. Serve a misurare il peso della storia. In particolare, il `topic_score` del cluster non e' piu' il vecchio score documentale basato su un solo URL, ma un punteggio ricalcolato dai breadth signals del componente: numero di eventi, numero di URL distinti e numero di domini distinti.

La seconda famiglia e' interpretativa sul layer evento: tipi di evento dominanti, classi quad dominanti, severita' media, paesi e location dominanti, insieme degli `event_ids`, e range delle date evento. Serve a dire quale forma geopolitica assume la storia.

La terza famiglia e' narrativa: fonti di mention, finestra temporale delle mention, temi, persone, organizzazioni, luoghi GKG e tono documentale medio. Serve a rendere il cluster leggibile come oggetto quasi editoriale, non solo analitico. Anche `mention_count` viene trattato in modo conservativo: rappresenta il numero di `mention_identifiers` distinti del componente, non la somma grezza delle righe mention, per evitare doppio conteggio quando lo stesso URL e' condiviso da piu' eventi.

A questo punto il sistema non ha ancora creato una storia globale. Ha creato una buona unita' documentale consistente.

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

Ma il punto davvero importante e' la riconciliazione tra tabelle opposte. Un cluster puo' cambiare categoria tra una run e la successiva. Se prima era una story e poi supera la soglia root, deve sparire da `story_clusters` e comparire solo in `root_clusters`. Se il fenomeno si ridimensiona, deve accadere il contrario.

Il sistema quindi non si limita a scrivere nella tabella giusta: elimina il `cluster_id` dalla tabella opposta. Questo garantisce mutua esclusione, evita duplicazioni semantiche e rende ogni cluster univocamente interpretabile.

## Esposizione via API

Le API non calcolano cluster al volo. Espongono viste gia' materializzate.

`/clusters/search` legge solo da `story_clusters`. `/root-clusters/search` legge solo da `root_clusters`. Entrambe le route condividono lo stesso schema pubblico di risposta, perche' la forma dati e' volutamente simmetrica. Cambia la semantica del contenitore, non il contratto del payload.

Questa scelta mantiene il confine tra costruzione e consultazione. Il lavoro pesante avviene nel pipeline. L'API resta un livello sottile di accesso a strutture gia' consolidate, con filtri su score, paese e paginazione.

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
- la soglia che separa story cluster e root cluster.

Questo rende il sistema adattabile senza riscrivere il modello logico. In termini architetturali, il codice implementa il pipeline; la configurazione ne modula il comportamento operativo.

## Failure modes e difese del sistema

Il sistema assume che i dati siano rumorosi, incompleti e a volte semanticamente fuorvianti. Per questo incorpora difese distribuite nel flusso.

Scarta nodi mention structuralmente sbagliati gia' in ingresso. Costruisce candidati solo quando esiste connettivita' reale tra eventi e propagazione mediatica. Limita i merge con gate temporali e tipologici. Usa upsert per evitare duplicazioni su rerun. Riconcilia i category flip per mantenere mutua esclusione tra story e root.

Non elimina ogni possibile errore semantico, ma riduce i due rischi principali del clustering giornalistico:

- esplosione del rumore, quando pagine aggregate o temi generici generano falsi cluster;
- collasso eccessivo, quando storie diverse vengono fuse in un unico mostro narrativo.

L'architettura non promette perfezione ontologica. Promette un equilibrio tra robustezza, costo computazionale, spiegabilita' e utilita' applicativa.

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
