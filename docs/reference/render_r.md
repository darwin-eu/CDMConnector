# Render parameterized SQL (Pure R implementation — fallback)

Supports @param substitution and conditional `{cond}?{then}:{else}`
syntax.

## Usage

``` r
render_r(sql, warnOnMissingParameters = TRUE, ...)
```

## Arguments

- sql:

  SQL string.

- warnOnMissingParameters:

  Whether to warn on missing parameters.

- ...:

  Named parameters for substitution.
