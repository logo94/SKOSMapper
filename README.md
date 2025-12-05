# 📚 SKOSMapper (Authority File Query Tool)

The SKOSMapper is a Python utility designed to simplify and standardize the querying of Authority Files (thesauri, subject headings, classification schemes) based on the SKOS data model.

Its core value proposition is the ability to unify querying across heterogeneous data sources—local files or remote SPARQL endpoints—by leveraging a single, dynamic configuration map. This makes complex queries simple and fast.

## 💡 Core concepts

The class operates on a principle of "Query Once, Use Everywhere" making complex graph queries as simple as calling a Python method.

The class uses `Skosify` to ensure query compatibility across non-strictly SKOS data.

- Local Normalization: When the source is a local static file, it is normalized using Skosify upon loading into a cached .skos.ttl version. This process converts the source file's heterogeneous predicates (e.g., RDFS) into a compatible SKOS standard, optimizing subsequent local queries.
- Shared Configuration Files: The repository is intended to progressively include configuration files (.cfg) necessary for parsing and normalizing well-known Authority Files (e.g., GND, RAMEAU, CDD, etc.). The class uses the exact same Skosify configuration file to execute direct and mapped queries even on remote SPARQL endpoints that do not natively adhere to the SKOS standard.

## 🚀 Unified Access Architecture

Class methods are designed to maximize the simplicity and speed of querying the source graph, adhering to the principle of separating interface from data implementation.

1. Standard SKOS Interface: All public methods (e.g., get_prefLabel, get_broader_concepts) use standardized SKOS field names.
2. Dynamic Translation: The class automatically translates these SKOS requests into the correct underlying predicates (e.g., SKOS, RDFS, or custom properties like GND/CDD) based on the configuration file.
3. Performance: By centralizing logic and generating optimized SPARQL queries (or querying a pre-normalized local graph), it ensures rapid data retrieval.

## ⚙️ Configuration file

The same configuration file is used for two critical, yet distinct, tasks:

### 1. Local Processing (Normalization)
When the source is a local file, the class integrates with Skosify at load time. Skosify uses the .cfg file to physically normalize the source data (e.g., RDFS to SKOS) and saves a standardized .skos.ttl cache.
### 2. Remote Querying (Runtime Mapping)
When the source is a SPARQL endpoint, the class uses the exact same .cfg file to build dynamic SPARQL queries on the fly.

If a user requests prefLabel, the class checks the config:
```
gndo.preferredName = skos:prefLabel
```

It then constructs a dynamic query: 
```
?s gndo:preferredName ?o.
```

This ensures that even if a remote Authority File does not strictly follow SKOS (e.g., uses custom GND predicates), your SKOSMapper queries it successfully and transparently as if it were pure SKOS.

## Class Initialization (__init__)

| Parameter | Type | Description |
| --- | --- | --- |
| source | str | File Path (triggers Skosify normalization and caching) or SPARQL Endpoint URL (triggers runtime dynamic querying) |
| config_file | str | (optional) Path to the Skosify/Mapping CFG file |
| default_lang | str | (optional) Default language code for lookups (e.g., 'it', 'en') |

## 🔍 Key Public Methods

### A. Concept Retrieval (Full Data)

These methods retrieve the complete, aggregated data set for a concept.

|Method |Signature |Focus/Role |
|---|---|---|
|get_concept_by_uri |(uri, lang, raw_response) -> Optional[Dict] |Primary method. Retrieves all mapped fields and any remaining raw triples for a specific concept URI |
|get_all_concepts|(lang, raw_response) -> List[Dict[str, Any]] |Retrieves all concepts defined in the schema, returning their full mapped details |
|search_concepts_by_label_regex|(keyword, lang, raw_response) -> List[Dict] |Rapidly searches both mapped prefLabel and altLabel using regex |
|get_concept_by_notation |(notation, lang, raw_response) -> Optional[Dict]|Retrieves a concept based on its mapped notation (e.g., a CDD code), using a reverse lookup |

### B. Single-Value/List Lookups

These methods perform atomic lookups with built-in language filtering and fallback logic.

| Method | Signature | Output Type | Role |
|---|---|---|---|
|get_prefLabel |(uri, lang) -> Optional[str] |str |Finds the single best-matched label, automatically applying language fallback |
|get_notation |(uri) -> List[str] |List[str] |Finds all notations (e.g., multiple CDD codes or versions), as it is treated as a list-literal field.|
|get_altLabels |(uri, lang) -> List[str] |List[str] |Retrieves all mapped alternative labels, filtered by language |
|get_uri_by_notation |(notation) -> Optional[str] |str |Reverse Lookup: Finds the concept URI given a notation value |

### C. Relationship Navigation

These methods retrieve the URIs of related concepts, then iterate to fetch the full details for each related concept.

| Method | Signature | Role |
| --- | --- | --- |
| get_broader_concepts | (uri, lang) -> List[Dict[str, Any]] | Retrieves all direct broader concepts, returning the full mapped details for each |
| get_narrower_concepts | (uri, lang) -> List[Dict[str, Any]] | Retrieves all direct narrower concepts and their full mapped details |
| get_related_concepts | (uri, lang) -> List[Dict[str, Any]] | Retrieves all direct related concepts and their full mapped details |


## Tested Authorities 


### Thesaurus BNCF (National Central Library of Florence)

LOD Cloud: https://lod-cloud.net/dataset/bncf-ns

SPARQL Endpoint: https://digitale.bncf.firenze.sbn.it/openrdf-sesame/repositories/NS/query

Config file: No

| Type | Tested | Is working | Notes |
| --- | --- | ---| --- |
| Static dump | y | y | |
| SPARQL Endpoint | y | n | SSL Error |


### Gemeinsame Normdatei (GND)

LOD Cloud: https://lod-cloud.net/dataset/dnb-gemeinsame-normdatei

SPARQL Endpoint: https://sparql.dnb.de/api/gnd

Config file: Yes

| Type | Tested | Is working | Notes |
| --- | --- | ---| --- |
| Static dump | n | - | |
| SPARQL Endpoint | y | y |  |


### Art & Architecture Thesaurus (Getty AAT)

LOD Cloud: https://lod-cloud.net/dataset/getty-aat

SPARQL Endpoint: https://vocab.getty.edu/sparql

Config file: -

| Type | Tested | Is working | Notes |
| --- | --- | ---| --- |
| Static dump | n | - | |
| SPARQL Endpoint | n | - |  |


### Bibliothèque nationale de France (BnF)

LOD Cloud: https://lod-cloud.net/dataset/data-bnf-fr

SPARQL Endpoint: http://data.bnf.fr/sparql

Config file: -

| Type | Tested | Is working | Notes |
| --- | --- | ---| --- |
| Static dump | n | - | |
| SPARQL Endpoint | n | - |  |
