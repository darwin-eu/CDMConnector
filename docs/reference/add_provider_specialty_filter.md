# Add ProviderSpecialty filter support to a domain builder

Add ProviderSpecialty filter support to a domain builder

## Usage

``` r
add_provider_specialty_filter(criteria, cdm_schema, alias_prefix)
```

## Arguments

- criteria:

  Criteria list (e.g. with ProviderSpecialty).

- cdm_schema:

  CDM schema name.

- alias_prefix:

  Table alias prefix.

## Value

list with select_col (to add to inner SELECT), join_sql, where_parts
