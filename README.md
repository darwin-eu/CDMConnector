
<!-- README.md is generated from README.Rmd. Please edit that file -->

# [CDMConnector](https://darwin-eu.github.io/CDMConnector/)

<!-- badges: start -->

[![CRAN
status](https://www.r-pkg.org/badges/version/CDMConnector)](https://CRAN.R-project.org/package=CDMConnector)
[![codecov.io](https://codecov.io/gh/darwin-eu/CDMConnector/coverage.svg?branch=main)](https://app.codecov.io/gh/darwin-eu/CDMConnector?branch=main)
[![Build
Status](https://github.com/darwin-eu/CDMConnector/workflows/R-CMD-check/badge.svg)](https://github.com/darwin-eu/CDMConnector/actions?query=workflow%3AR-CMD-check)
[![duckdb
status](https://github.com/darwin-eu/CDMConnector/workflows/duckdb-test/badge.svg)](https://github.com/darwin-eu/CDMConnector/actions?query=workflow%3Aduckdb-test)
[![Postgres
status](https://github.com/darwin-eu/CDMConnector/workflows/postgres-test/badge.svg)](https://github.com/darwin-eu/CDMConnector/actions?query=workflow%3Apostgres-test)
[![SQL Server odbc
status](https://github.com/darwin-eu/CDMConnector/workflows/sqlserver-odbc-test/badge.svg)](https://github.com/darwin-eu/CDMConnector/actions?query=workflow%3Asqlserver-odbc-test)
[![Redshift
status](https://github.com/darwin-eu/CDMConnector/workflows/redshift-test/badge.svg)](https://github.com/darwin-eu/CDMConnector/actions?query=workflow%3Aredshift-test)
[![Snowflake
status](https://github.com/darwin-eu/CDMConnector/workflows/snowflake-odbc-test/badge.svg)](https://github.com/darwin-eu/CDMConnector/actions?query=workflow%3Asnowflake-odbc-test)
<!-- badges: end -->

> Are you using the [tidyverse](https://www.tidyverse.org/) with an OMOP
> Common Data Model?
>
> Interact with your CDM in a pipe-friendly way with CDMConnector.
>
> - Quickly connect to your CDM and start exploring.
> - Build data analysis pipelines using familiar dplyr verbs.
> - Easily extract subsets of CDM data from a database.

## Overview

CDMConnector introduces a single R object that represents an OMOP CDM
relational database inspired by the [dm](https://dm.cynkra.com/),
[DatabaseConnector](http://ohdsi.github.io/DatabaseConnector/), and
[Andromeda](https://ohdsi.github.io/Andromeda/) packages. The cdm
objects encapsulate references to [OMOP CDM
tables](https://ohdsi.github.io/CommonDataModel/) in a remote RDBMS as
well as metadata necessary for interacting with a CDM, allowing for
dplyr style data analysis pipelines and interactive data exploration.

[![OMOP CDM
v5.4](https://ohdsi.github.io/CommonDataModel/images/cdm54.png)](https://ohdsi.github.io/CommonDataModel/)

## Features

CDMConnector is meant to be the entry point for composable tidyverse
style data analysis operations on an OMOP CDM. A `cdm_reference` object
behaves like a named list of tables.

- Quickly create a list of references to a subset of CDM tables
- Store connection information for later use inside functions
- Use any DBI driver back-end with the OMOP CDM

See Getting started for more details.

## Installation

CDMConnector can be installed from CRAN:

``` r
install.packages("CDMConnector")
```

The development version can be installed from GitHub:

``` r
# install.packages("devtools")
devtools::install_github("darwin-eu/CDMConnector")
```

## Usage

Create a cdm reference from any DBI connection to a database containing
OMOP CDM tables. Use the cdm_schema argument to point to a particular
schema in your database that contains your OMOP CDM tables and the
write_schema to specify the schema where results tables can be created,
and use cdm_name to provide a name for the database.

``` r
library(CDMConnector)

con <- DBI::dbConnect(duckdb::duckdb(dbdir = eunomia_dir()))

cdm <- cdm_from_con(con = con, 
                    cdm_schema = "main", 
                    write_schema = "main", 
                    cdm_name = "my_duckdb_database")
```

A `cdm_reference` is a named list of table references:

``` r
library(dplyr)
names(cdm)
```

    ##  [1] "person"                "observation_period"    "visit_occurrence"     
    ##  [4] "visit_detail"          "condition_occurrence"  "drug_exposure"        
    ##  [7] "procedure_occurrence"  "device_exposure"       "measurement"          
    ## [10] "observation"           "death"                 "note"                 
    ## [13] "note_nlp"              "specimen"              "fact_relationship"    
    ## [16] "location"              "care_site"             "provider"             
    ## [19] "payer_plan_period"     "cost"                  "drug_era"             
    ## [22] "dose_era"              "condition_era"         "metadata"             
    ## [25] "cdm_source"            "concept"               "vocabulary"           
    ## [28] "domain"                "concept_class"         "concept_relationship" 
    ## [31] "relationship"          "concept_synonym"       "concept_ancestor"     
    ## [34] "source_to_concept_map" "drug_strength"

Use dplyr verbs with the table references.

``` r
cdm$person %>% 
  tally()
```

    ## # Source:   SQL [1 x 1]
    ## # Database: DuckDB v1.0.1-dev5 [root@Darwin 23.0.0:R 4.3.1//private/var/folders/xx/01v98b6546ldnm1rg1_bvk000000gn/T/Rtmp9UjDOa/file25a7230b0dc2.duckdb]
    ##       n
    ##   <dbl>
    ## 1  2694

Compose operations with the pipe.

``` r
cdm$condition_era %>%
  left_join(cdm$concept, by = c("condition_concept_id" = "concept_id")) %>% 
  count(top_conditions = concept_name, sort = TRUE)
```

    ## # Source:     SQL [?? x 2]
    ## # Database:   DuckDB v1.0.1-dev5 [root@Darwin 23.0.0:R 4.3.1//private/var/folders/xx/01v98b6546ldnm1rg1_bvk000000gn/T/Rtmp9UjDOa/file25a7230b0dc2.duckdb]
    ## # Ordered by: desc(n)
    ##    top_conditions                               n
    ##    <chr>                                    <dbl>
    ##  1 Viral sinusitis                          17268
    ##  2 Acute viral pharyngitis                  10217
    ##  3 Acute bronchitis                          8184
    ##  4 Otitis media                              3561
    ##  5 Osteoarthritis                            2694
    ##  6 Streptococcal sore throat                 2656
    ##  7 Sprain of ankle                           1915
    ##  8 Concussion with no loss of consciousness  1013
    ##  9 Sinusitis                                 1001
    ## 10 Acute bacterial sinusitis                  939
    ## # ℹ more rows

And much more besides. See vignettes for further explanations on how to
create database connections, make a cdm reference, and start analysing
your data.

## Getting help

If you encounter a clear bug, please file an issue with a minimal
[reproducible example](https://reprex.tidyverse.org/) on
[GitHub](https://github.com/darwin-eu/CDMConnector/issues).

## Citation

    ## To cite package 'CDMConnector' in publications use:
    ## 
    ##   Black A, Gorbachev A, Burn E, Catala Sabate M (????). _CDMConnector:
    ##   Connect to an OMOP Common Data Model_.
    ##   https://darwin-eu.github.io/CDMConnector/,
    ##   https://github.com/darwin-eu/CDMConnector.
    ## 
    ## A BibTeX entry for LaTeX users is
    ## 
    ##   @Manual{,
    ##     title = {CDMConnector: Connect to an OMOP Common Data Model},
    ##     author = {Adam Black and Artem Gorbachev and Edward Burn and Marti {Catala Sabate}},
    ##     note = {https://darwin-eu.github.io/CDMConnector/, https://github.com/darwin-eu/CDMConnector},
    ##   }

------------------------------------------------------------------------

License: Apache 2.0
