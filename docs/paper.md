# Cluster Creation Flow — Paper Architetturale

## Obiettivo

Il sistema di clustering trasforma il rumore nativo di GDELT in un insieme di unita' editoriali stabili e interrogabili. GDELT descrive il mondo come una sequenza di eventi atomici, menzioni di documenti e segnali semantici distribuiti. Il backend non espone direttamente questa granularita': costruisce invece cluster che cercano di rappresentare una singola storia giornalistica, oppure un mega-raggruppamento quando la stessa storia cresce oltre una soglia strutturalmente anomala.

L'obiettivo architetturale non e' soltanto aggregare record, ma imporre ordine su tre layer diversi:

- il layer evento, che misura l'azione geopolitica;
- il layer mention, che misura la propagazione mediatica;
- il layer GKG, che misura il contenuto semantico del documento.

Il risultato finale e' una vista materializzata della notizia, progettata per essere stabile nel tempo, ricostruibile in rerun successivi e accessibile via API senza dover ricomputare il grafo ogni volta.

## Principio architetturale

Il principio guida e' document-centric first, story-centric second.

Il sistema non prova a costruire storie globali partendo subito da relazioni astratte tra eventi. Parte invece dal segnale piu' concreto e stabile disponibile localmente: `source_url`. Tutti gli eventi che condividono lo stesso URL vengono prima interpretati come facce diverse dello stesso documento. Solo dopo questa fase il sistema valuta se due cluster documentali appartengono in realta' alla stessa storia piu' ampia.

Questo approccio divide il problema in due passaggi architetturalmente diversi:

1. creare cluster documentali coerenti e arricchiti;
2. fondere tra loro solo i cluster che mostrano evidenza forte di essere la stessa storia.

La conseguenza e' importante: il sistema privilegia una costruzione incrementale, spiegabile e idempotente, invece di un clustering opaco eseguito in un solo passo.

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

## Fase 1: selezione dei candidati

Il primo problema da risolvere e' evitare che ogni URL diventi un cluster. Il sistema applica quindi una fase di selezione che ha il ruolo di filtro architetturale, non solo di ottimizzazione.

Su tutti gli eventi nella finestra temporale richiesta, il servizio raggruppa per `source_url` e calcola aggregati base: numero di eventi distinti, volume di articoli, menzioni e fonti. Da questi valori deriva un `topic_score`, cioe' un indice sintetico di rilevanza.

Ma il punteggio da solo non basta. Prima ancora del calcolo finale intervengono tre gate di sanita' del candidato:

- esclusione dei domini noti come aggregatori o content farm;
- esclusione degli URL che assomigliano a pagine sezione, tag, archive o category;
- esclusione dei candidati privi di segnale minimo nel layer mention, se la configurazione lo richiede.

Architetturalmente, questa fase serve a impedire che il sistema costruisca cluster su pagine strutturalmente sbagliate. In altre parole, il pipeline non si limita a prendere i dati piu' forti: prova a scartare i contenitori che non rappresentano una notizia singola.

## Fase 2: raccolta batch dei layer

Una volta ottenuti i candidati, il sistema non esegue arricchimenti isolati uno per uno. Esegue invece raccolte batch per ridurre round-trip e mantenere il flusso sotto controllo.

L'ordine logico e' questo:

1. raccoglie tutti gli eventi per gli URL candidati;
2. raccoglie tutte le mentions per gli event id emersi;
3. raccoglie i record GKG associati ai documenti rilevanti.

Il punto architetturalmente piu' delicato e' il terzo. Il sistema moderno usa solo il GKG del `source_url` del cluster come sorgente semantica primaria del documento. Non usa indiscriminatamente il GKG di tutti i documenti che hanno menzionato quella storia, perche' questo contaminerebbe il cluster con entita' e temi di articoli secondari o completamente laterali.

Questa e' una scelta di purezza semantica: il cluster eredita la semantica del documento sorgente, mentre il layer mention resta un segnale di propagazione, non una fonte da cui assorbire contenuto editoriale indiscriminato.

## Fase 3: costruzione del cluster documentale

Per ogni candidato, il sistema costruisce una rappresentazione completa della notizia documentale.

Il `cluster_id` e' deterministico ed e' derivato soltanto da `source_url`. Questa decisione e' centrale. Significa che la stessa storia documentale, se ricalcolata domani o in un rerun, punta allo stesso identificatore e quindi alla stessa riga logica. Non nascono cluster gemelli solo perche' cambia il giorno di esecuzione.

Dentro questo cluster confluiscono tre famiglie di attributi.

La prima famiglia e' quantitativa: `event_count`, `num_articles`, `num_mentions`, `num_sources`, `topic_score`. Serve a misurare il peso della storia.

La seconda famiglia e' interpretativa sul layer evento: tipi di evento dominanti, classi quad dominanti, severita' media, paesi e location dominanti, insieme degli `event_ids`, e range delle date evento. Serve a dire quale forma geopolitica assume la storia.

La terza famiglia e' narrativa: fonti di mention, finestra temporale delle mention, temi, persone, organizzazioni, luoghi GKG e tono documentale medio. Serve a rendere il cluster leggibile come oggetto quasi editoriale, non solo analitico.

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

Quando piu' cluster vengono fusi, il sistema sceglie come anchor quello con `topic_score` piu' alto. L'anchor fornisce identita' e URL rappresentativo; gli altri cluster contribuiscono invece alle aggregazioni strutturali. Il cluster finale diventa quindi una sintesi orientata dal candidato piu' forte, ma nutrita dall'intero componente.

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
- l'obbligo o meno di avere menzioni per essere candidati;
- il massimo gap temporale ammesso nel merge;
- la soglia che separa story cluster e root cluster.

Questo rende il sistema adattabile senza riscrivere il modello logico. In termini architetturali, il codice implementa il pipeline; la configurazione ne modula il comportamento operativo.

## Failure modes e difese del sistema

Il sistema assume che i dati siano rumorosi, incompleti e a volte semanticamente fuorvianti. Per questo incorpora difese distribuite nel flusso.

Scarta URL structuralmente sbagliati gia' in ingresso. Separa il layer semantico del documento sorgente dal rumore dei documenti che lo citano. Limita i merge con gate temporali e tipologici. Usa upsert per evitare duplicazioni su rerun. Riconcilia i category flip per mantenere mutua esclusione tra story e root.

Non elimina ogni possibile errore semantico, ma riduce i due rischi principali del clustering giornalistico:

- esplosione del rumore, quando pagine aggregate o temi generici generano falsi cluster;
- collasso eccessivo, quando storie diverse vengono fuse in un unico mostro narrativo.

L'architettura non promette perfezione ontologica. Promette un equilibrio tra robustezza, costo computazionale, spiegabilita' e utilita' applicativa.

## Sintesi finale

La creazione dei cluster in questo backend e' un pipeline di materializzazione a piu' layer. Parte da eventi locali gia' ingeriti, filtra candidati documentali, costruisce cluster coerenti attorno a `source_url`, fonde i casi che mostrano evidenza di appartenere alla stessa storia, poi separa l'output finale in cluster normali e mega-cluster.

La forza architetturale del sistema sta nel fatto che ogni passaggio ha una responsabilita' netta:

- il candidate scoring decide cosa merita attenzione;
- la raccolta batch compone i tre layer informativi;
- il cluster build crea l'unita' documentale;
- il merger crea l'unita' narrativa piu' ampia;
- la partizione story/root organizza l'output per consumo applicativo;
- l'API espone solo viste gia' consolidate.

In questo senso, il sistema non e' solo un algoritmo di clustering. E' una pipeline editoriale deterministica che trasforma segnali eterogenei in oggetti narrativi stabili.
