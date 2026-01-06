# Extract the day, month or year of a date in a dplyr pipeline

Extract the day, month or year of a date in a dplyr pipeline

## Usage

``` r
datepart(date, interval = "year", dbms = NULL)
```

## Arguments

- date:

  Character string that represents to a date column.

- interval:

  Interval to extract from a date. Valid options are "year", "month", or
  "day".

- dbms:

  Database system, if NULL it is auto detected.

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb(), ":memory:")
date_tbl <- dplyr::copy_to(con,
                           data.frame(birth_date = as.Date("1993-04-19")),
                           name = "tmp",
                           temporary = TRUE)
df <- date_tbl %>%
  dplyr::mutate(year = !!datepart("birth_date", "year")) %>%
  dplyr::mutate(month = !!datepart("birth_date", "month")) %>%
  dplyr::mutate(day = !!datepart("birth_date", "day")) %>%
  dplyr::collect()
DBI::dbDisconnect(con, shutdown = TRUE)
} # }
```
