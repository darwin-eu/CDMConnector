# Run a dplyr query and add the result set to an existing

Run a dplyr query and add the result set to an existing

## Usage

``` r
appendPermanent(x, name, schema = NULL)
```

## Arguments

- x:

  A dplyr query

- name:

  Name of the table to be appended. If it does not already exist it will
  be created.

- schema:

  Schema where the table exists. Can be a length 1 or 2 vector. (e.g.
  schema = "my_schema", schema = c("my_schema", "dbo"))

## Value

A dplyr reference to the newly created table

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)

con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
concept <- dplyr::tbl(con, "concept")

# create a table
rxnorm_count <- concept %>%
  dplyr::filter(domain_id == "Drug") %>%
  dplyr::mutate(isRxnorm = (vocabulary_id == "RxNorm")) %>%
  dplyr::count(domain_id, isRxnorm) %>%
  compute("rxnorm_count")

# append to an existing table
rxnorm_count <- concept %>%
  dplyr::filter(domain_id == "Procedure") %>%
  dplyr::mutate(isRxnorm = (vocabulary_id == "RxNorm")) %>%
  dplyr::count(domain_id, isRxnorm) %>%
  appendPermanent("rxnorm_count")

DBI::dbDisconnect(con, shutdown = TRUE)

} # }
```
