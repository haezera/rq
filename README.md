# rq

`rq` is a lightweight job scheduler for R. You define your work as a collection of R scripts, declare what depends on what, and `rq` runs them in the right order — in parallel where it can, respecting dependencies where it must.

It's designed for the kind of workflows that come up often in data work: pull from a few sources, transform, aggregate, write somewhere. Nothing exotic, but enough moving parts that getting the order right manually is annoying.

## Installation

```r
devtools::install_github("haezera/rq")
```

## Quick example

```r
library(rq)

a <- JobNode$new("fetch data",   script_path = "scripts/fetch.R")
b <- JobNode$new("clean data",   script_path = "scripts/clean.R",    upstream = list(a))
c <- JobNode$new("write to db",  script_path = "scripts/write_db.R", upstream = list(b))

jo <- JobOrchestrator$new()
jo$add_job(a)
jo$add_job(b)
jo$add_job(c)

jo$run()
```

Jobs that share no dependencies run concurrently, up to `max_workers` (defaults to the number of available cores minus one).

## User guide

For a full walkthrough of the API — including visualising workflows, handling failures, and controlling concurrency — see the [user guide](vignettes/user-guide.Rmd).
