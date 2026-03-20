## rq

`rq` is a lightweight job scheduler for R. You define your work as a collection of R scripts, declare what depends on what, and `rq` runs them in the right order — in parallel where it can, respecting dependencies where it must.

It was designed very specifically for the simple workflows that come up in places like data science and quantitative research. You'll pull some data from source `X`, `Y` and `Z`, aggregate it in another script or function, and then finally save it. In many workflows, some steps have jobs that can run concurrently, and have different runtimes, whilst having child jobs that need to wait for it to be completed.

`rq` is a simple drop-in replacement to these sorts of workflows; all you need to provide is the path to the scripts, names that describes these scripts well, and the dependencies for each job.

### Features

- [X] DAG-like workflow orchestration
- [X] Concurrent job execution
- [X] Retries for non-deterministic jobs
- [ ] Job timeouts
- [X] Graph visualisation using `visNetwork`
- [ ] Scheduling of workflows using `cron`, etc.
- [ ] Execution logging
- [ ] Workflow/jobs run history

### Installation

```r
devtools::install_github("haezera/rq")
```

### Quick example

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

### User guide

For a full walkthrough of the API — including visualising workflows, handling failures, and controlling concurrency — see the [user guide](vignettes/user-guide.html).
