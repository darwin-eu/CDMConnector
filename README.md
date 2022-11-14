<!-- README.md is generated from README.Rmd. Please edit that file -->

# [CDMConnector](https://odyosg.github.io/CDMConnector/)

<!-- badges: start -->

[![CRAN
status](https://www.r-pkg.org/badges/version/CDMConnector)](https://CRAN.R-project.org/package=CDMConnector)
<!-- badges: end -->

> Are you using the [tidyverse](https://www.tidyverse.org/) with an OMOP
> Common Data Model?
>
> Interact with your CDM in a pipe-friendly way with CDMConnector.
>
> -   Quickly connect to your CDM and start exploring.
> -   Build data analysis pipelines using familiar dplyr verbs.
> -   Easily extract subsets of CDM data from a database.

## Overview

CDMConnector introduces a single R object that represents an OMOP CDM
relational database inspired by the [dm](https://cynkra.github.io/dm/),
[DatabaseConnector](http://ohdsi.github.io/DatabaseConnector/), and
[Andromeda](https://ohdsi.github.io/Andromeda/) packages. The cdm object
can be used in dplyr style data analysis pipelines and facilitates
interactive data exploration. cdm objects encapsulate references to
[OMOP CDM tables](https://ohdsi.github.io/CommonDataModel/) in a remote
RDBMS as well as metadata necessary for interacting with a CDM.

[![OMOP CDM
v5.4](https://ohdsi.github.io/CommonDataModel/images/cdm54.png)](https://ohdsi.github.io/CommonDataModel/)

## Features

CDMConnector is meant to be the entry point for composable tidyverse
style data analysis operations on an OMOP CDM. A `cdm_reference` object
behaves like a named list of tables.

-   Quickly create a list of references to a subset of CDM tables
-   Store connection information for later use inside functions
-   Use any DBI driver back-end with the OMOP CDM

See Getting started for more details.

## Installation

CDMConnector can be installed from CRAN:

    install.packages("CDMConnector")

The development version can be installed from GitHub:

    # install.packages("devtools")
    devtools::install_github("darwin-eu/CDMConnector")

## Usage

Create a `cdm_reference` object from any DBI connection. Use the
\`cdm\_schema argument to point to a particular schema in your database.

    library(CDMConnector)

    con <- DBI::dbConnect(duckdb::duckdb(), dbdir = eunomia_dir())
    cdm <- cdm_from_con(con, cdm_schema = "main")
    cdm

    ## # OMOP CDM reference (tbl_duckdb_connection)
    ## 
    ## Tables: person, observation_period, visit_occurrence, visit_detail, condition_occurrence, drug_exposure, procedure_occurrence, measurement, observation, death, location, care_site, provider, drug_era, dose_era, condition_era, concept, vocabulary, concept_relationship, concept_ancestor, drug_strength

A `cdm_reference` is a named list of table references:

    library(dplyr, warn.conflicts = FALSE)
    names(cdm)

    ##  [1] "person"               "observation_period"   "visit_occurrence"    
    ##  [4] "visit_detail"         "condition_occurrence" "drug_exposure"       
    ##  [7] "procedure_occurrence" "measurement"          "observation"         
    ## [10] "death"                "location"             "care_site"           
    ## [13] "provider"             "drug_era"             "dose_era"            
    ## [16] "condition_era"        "concept"              "vocabulary"          
    ## [19] "concept_relationship" "concept_ancestor"     "drug_strength"

Use dplyr verbs with the table references.

    tally(cdm$person)

    ## # Source:   SQL [1 x 1]
    ## # Database: DuckDB 0.5.1 [root@Darwin 21.6.0:R 4.2.0//var/folders/xx/01v98b6546ldnm1rg1_bvk000000gn/T//RtmpFda1KI/ymwnttkr/cdm.duckdb]
    ##       n
    ##   <dbl>
    ## 1  2694

Compose operations with the pipe.

    cdm$condition_era %>%
      left_join(cdm$concept, by = c("condition_concept_id" = "concept_id")) %>% 
      count(top_conditions = concept_name, sort = TRUE)

    ## # Source:     SQL [?? x 2]
    ## # Database:   DuckDB 0.5.1 [root@Darwin 21.6.0:R 4.2.0//var/folders/xx/01v98b6546ldnm1rg1_bvk000000gn/T//RtmpFda1KI/ymwnttkr/cdm.duckdb]
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
    ## # … with more rows

Run a simple quality check on a cdm.

    cdm <- cdm_from_con(con, cdm_tables = c("person", "observation_period"))
    validate_cdm(cdm)

    ## ── CDM v5.4 validation (checking 2 tables) ─────────────────────────────────────
    ## ✔ cdm table names
    ## ✔ all row counts > 0

## DBI Drivers

CDMConnector is tested using the following DBI driver backends:

-   [RPostgres](https://rpostgres.r-dbi.org/reference/postgres) on
    Postgres and Redshift
-   [odbc](https://solutions.rstudio.com/db/r-packages/odbc/) on
    Microsoft SQL Server
-   [duckdb](https://duckdb.org/docs/api/r)

## Getting help

If you encounter a clear bug, please file an issue with a minimal
[reproducible example](https://reprex.tidyverse.org/) on
[GitHub](https://github.com/OdyOSG/CDMConnector/issues).

------------------------------------------------------------------------

License: Apache 2.0
