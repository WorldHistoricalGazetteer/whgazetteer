### README: ES mappings for WHG
_01 April 2023_

There are two Elasticsearch indexes used in WHG, aliased as **wd** (Wikidata) and **whg** (contributed data).The current version for WHG v2.1 is 7.17 (community). Kibana is used to manage access, test queries, etc.

####WHG
Reindexing is done periodically to a numbered empty index created with a mapping file (includes settings for analysis and field mappings). Lookup fields for name searches are: **title**, **searchy**, and **names.toponym**. Hits to **title** are weighted, producing a hegher _score.

Current numbered WHG indexes are:

* **whg07** (keyword)
  * Name fields are indexed as _keywords_ and normalized (_lowercase_, _asciifolding_)
  * Search screen matches are exact for any word in the name (e.g. 'san' returns 'San Diego' as well as 'San' only)
* **whg08** (text)
  * Name fields are indexed as text; default lowercase normalization only
  * Search screen query for 'san' returns exact matches first, then all records with the token 'san' in their variants, including '**San** Francisco' and 'Kimpoku-**san**' 

The ES query type for names vary by use:
* **Search screen** (**whg** index; single place name, possibly multiple words)
  * single "pass"
    ```
    {"multi_match": {
        "query": qstr,
        "fields": ["title^3", "names.toponym", "searchy"]
    }}
    ```
* **Reconcile to Wikidata** (**wd** index)
  * 3 "passes" [1]
  * The **wd** index has 1 source for name variants 
  * Compares list/array of all WHG _variants_ with single list/array of Wikidata _variants.names_
  * Matches must be exact
    ```
    { ...
    "bool": {
      "must": [
        {"terms": {"variants.names":variants}}
      ], ...
    }
    ```

* **Reconcile to WHG** (**whg** index)
  * 2 passes [1]
  * The **whg** index has 3 sources for name variants
  * Search limited to "parent" records (w/whg_id field)
  * "should" within a "must" == "or"
 
    ```
    {...
    "bool": {
      "must": [
        {"exists": {"field": "whg_id"}},
        {"bool": {
            "should": [
              {"terms": {"names.toponym": variants}},
              {"terms": {"title": variants}},
              {"terms": {"searchy": variants}}
            ]
          }
        }
      ], ...}
    ```
Searches from the search screen use a multi-match 
query:

Searches during reconciliation  




