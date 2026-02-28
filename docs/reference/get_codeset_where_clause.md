# Get codeset WHERE clause for standard-only codeset (no source concept) Used by DrugEra, ConditionEra, DoseEra to match CirceR/Java output format.

Get codeset WHERE clause for standard-only codeset (no source concept)
Used by DrugEra, ConditionEra, DoseEra to match CirceR/Java output
format.

## Usage

``` r
get_codeset_where_clause(codeset_id, column_name)
```

## Arguments

- codeset_id:

  Integer or NULL

- column_name:

  Column name

## Value

"WHERE col in (...)" or ""
