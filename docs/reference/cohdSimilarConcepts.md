# Get similar concepts from Columbia Open Health Data (COHD) API

Queries the COHD API association/relativeFrequency endpoint to return
concepts that co-occur with the given concept(s), ranked by relative
frequency. Useful for finding clinically related conditions, drugs, or
procedures based on EHR prevalence. When given multiple concept IDs,
returns concepts that co-occur with the input set, ranked by how many
input concepts they co-occur with and by mean relative frequency.

## Usage

``` r
cohdSimilarConcepts(conceptId, datasetId = 1, topN = 50, timeoutSec = 30)
```

## Arguments

- conceptId:

  Integer or character vector. One or more OMOP concept IDs to find
  similar concepts for (e.g. conditions, drugs, or procedures).

- datasetId:

  Integer. COHD dataset ID (1 = 5-year, 2 = lifetime; default 1).

- topN:

  Integer. Maximum number of similar concepts to return (default 50).
  For a single concept, this limits rows by strength; for multiple
  concepts, this limits the aggregated result.

- timeoutSec:

  Numeric. Request timeout in seconds (default 30).

## Value

A data frame with one row per similar concept, or `NULL` if the API is
unavailable or the request fails. When successful:

- **Single concept**: data frame contains `concept_id_1`,
  `concept_id_2`, `concept_count_1`, `concept_count_2`, `concept_count`,
  `relative_frequency`, and `other_concept_id`; rows sorted by
  `relative_frequency` descending.

- **Multiple concepts**: data frame contains `other_concept_id`,
  `n_concepts` (how many input concepts co-occur with this one), and
  `mean_rf` (mean relative frequency); rows sorted by `n_concepts`
  descending then `mean_rf` descending. If no results or an error
  occurs, returns `NULL` and a message is printed.

## References

Ta, Casey N.; Dumontier, Michel; Hripcsak, George; P. Tatonetti,
Nicholas; Weng, Chunhua (2018). Columbia Open Health Data, a database of
EHR prevalence and co-occurrence of conditions, drugs, and procedures.
figshare. Collection. <https://doi.org/10.6084/m9.figshare.c.4151252.v1>

COHD collection:
<https://figshare.com/collections/Columbia_Open_Health_Data_a_database_of_EHR_prevalence_and_co-occurrence_of_conditions_drugs_and_procedures/4151252>

## Examples

``` r
if (FALSE) { # \dontrun{
# Single concept: top 25 similar to concept 201826 (Type 2 diabetes)
cohdSimilarConcepts(201826, datasetId = 1, topN = 25)

# Multiple concepts: concepts likely to co-occur with this set
cohdSimilarConcepts(c(201826, 316866, 255573), datasetId = 1, topN = 50)
} # }
```
