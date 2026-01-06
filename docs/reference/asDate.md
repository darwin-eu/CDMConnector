# as.Date dbplyr translation wrapper

This is a workaround for using as.Date inside dplyr verbs against a
database backend. This function should only be used inside dplyr verbs
where the first argument is a database table reference. `asDate` must be
unquoted with !! inside dplyr verbs (see example).

## Usage

``` r
asDate(x)
```

## Arguments

- x:

  an R expression

## Examples

``` r
if (FALSE) { # \dontrun{
con <- DBI::dbConnect(odbc::odbc(), "Oracle")
date_tbl <- dplyr::copy_to(con,
                           data.frame(y = 2000L, m = 10L, d = 10L),
                           name = "tmp",
                           temporary = TRUE)

df <- date_tbl %>%
  dplyr::mutate(date_from_parts = !!asDate(paste0(
    .data$y, "/",
    .data$m, "/",
    .data$d
  ))) %>%
  dplyr::collect()
} # }
```
