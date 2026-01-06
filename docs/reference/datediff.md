# Compute the difference between two days

This function must be "unquoted" using the "bang bang" operator (!!).
See example.

## Usage

``` r
datediff(start, end, interval = "day")
```

## Arguments

- start:

  The name of the start date column in the database as a string.

- end:

  The name of the end date column in the database as a string.

- interval:

  The units to use for difference calculation. Must be either "day"
  (default) or "year".

## Value

Platform specific SQL that can be used in a dplyr query.

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb())
date_tbl <- dplyr::copy_to(con, data.frame(date1 = as.Date("1999-01-01")),
                           name = "tmpdate", overwrite = TRUE, temporary = TRUE)

df <- date_tbl %>%
  dplyr::mutate(date2 = !!dateadd("date1", 1, interval = "year")) %>%
  dplyr::mutate(dif_years = !!datediff("date1", "date2", interval = "year")) %>%
  dplyr::collect()

DBI::dbDisconnect(con, shutdown = TRUE)
} # }
```
