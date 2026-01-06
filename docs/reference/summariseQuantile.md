# Quantile calculation using dbplyr

This function provides DBMS independent syntax for quantiles estimation.
Can be used by itself or in combination with
[`mutate()`](https://dplyr.tidyverse.org/reference/mutate.html) when
calculating other aggregate metrics (min, max, mean).

`summarise_quantile()`, `summarize_quantile()`, `summariseQuantile()`
and `summarizeQuantile()` are synonyms.

## Usage

``` r
summariseQuantile(.data, x = NULL, probs, nameSuffix = "value")
```

## Arguments

- .data:

  lazy data frame backed by a database query.

- x:

  column name whose sample quantiles are wanted.

- probs:

  numeric vector of probabilities with values in \[0,1\].

- nameSuffix:

  character; is appended to numerical quantile value as a column name
  part.

## Value

An object of the same type as '.data'

## Details

Implemented quantiles estimation algorithm returns values analogous to
`quantile{stats}` with argument `type = 1`. See discussion in Hyndman
and Fan (1996). Results differ from `PERCENTILE_CONT` natively
implemented in various DBMS, where returned values are equal to
`quantile{stats}` with default argument `type = 7`

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(duckdb::duckdb())
mtcars_tbl <- dplyr::copy_to(con, mtcars, name = "tmp", overwrite = TRUE, temporary = TRUE)

df <- mtcars_tbl %>%
 dplyr::group_by(cyl) %>%
 dplyr::mutate(mean = mean(mpg, na.rm = TRUE)) %>%
 summariseQuantile(mpg, probs = c(0, 0.2, 0.4, 0.6, 0.8, 1),
                   nameSuffix = "quant") %>%
 dplyr::collect()

DBI::dbDisconnect(con, shutdown = TRUE)
} # }
```
