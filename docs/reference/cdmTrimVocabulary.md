# Trim vocabulary tables to the minimum needed for the CDM

Reduces vocabulary tables to only the concepts referenced in the CDM's
clinical data plus their ancestors in the concept_ancestor hierarchy.
Tables like concept_synonym and source_to_concept_map are truncated
(emptied). This makes the CDM much smaller and faster to upload to a
remote test database via
[`copyCdmTo`](https://darwin-eu.github.io/CDMConnector/reference/copyCdmTo.md).

## Usage

``` r
cdmTrimVocabulary(cdm)
```

## Arguments

- cdm:

  A cdm_reference object (DuckDB or local)

## Value

A cdm_reference with trimmed vocabulary tables

## Details

The trimmed vocabulary is "graph-complete": every concept_id referenced
in concept_relationship and concept_ancestor also exists in concept.
Most relationships are trimmed out but the ancestor hierarchy is
preserved for all concepts in the CDM.

Only works on local (DuckDB or data frame based) CDMs since it is
intended for test data preparation.
