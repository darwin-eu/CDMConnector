# Add days or years to a date in a dplyr query

This function must be "unquoted" using the "bang bang" operator (!!).
See example.

## Usage

``` r
dateadd(date, number, interval = "day")
```

## Arguments

- date:

  The name of a date column in the database table as a character string

- number:

  The number of units to add. Can be a positive or negative whole
  number.

- interval:

  The units to add. Must be either "day" (default) or "year"

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
  dplyr::collect()

DBI::dbDisconnect(con, shutdown = TRUE)
} # }
```
