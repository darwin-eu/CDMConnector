# Execute dplyr query and save result in remote database

This function is a wrapper around
[`dplyr::compute`](https://dplyr.tidyverse.org/reference/compute.html)
that is tested on several database systems. It is needed to handle edge
cases where
[`dplyr::compute`](https://dplyr.tidyverse.org/reference/compute.html)
does not produce correct SQL.

## Usage

``` r
computeQuery(
  x,
  name = uniqueTableName(),
  temporary = TRUE,
  schema = NULL,
  overwrite = TRUE,
  ...
)
```

## Arguments

- x:

  A dplyr query

- name:

  The name of the table to create.

- temporary:

  Should the table be temporary: TRUE (default) or FALSE

- schema:

  The schema where the table should be created. Ignored if temporary =
  TRUE.

- overwrite:

  Should the table be overwritten if it already exists: TRUE (default)
  or FALSE Ignored if temporary = TRUE.

- ...:

  Further arguments passed on the
  [`dplyr::compute`](https://dplyr.tidyverse.org/reference/compute.html)

## Value

A [`dplyr::tbl()`](https://dplyr.tidyverse.org/reference/tbl.html)
reference to the newly created table.

## Examples

``` r
if (FALSE) { # \dontrun{
library(CDMConnector)

con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomiaDir())
cdm <- cdmFromCon(con, "main")

# create a temporary table in the remote database from a dplyr query
drugCount <- cdm$concept %>%
  dplyr::count(domain_id == "Drug") %>%
  computeQuery()

# create a permanent table in the remote database from a dplyr query
drugCount <- cdm$concept %>%
  dplyr::count(domain_id == "Drug") %>%
  computeQuery("tmp_table", temporary = FALSE, schema = "main")

DBI::dbDisconnect(con, shutdown = TRUE)
} # }
```
