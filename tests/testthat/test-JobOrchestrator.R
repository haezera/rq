library(testthat)
library(rq)

make_script <- function() {
  path <- tempfile(fileext = ".R")
  writeLines("1 + 1", path)
  path
}

make_job <- function(name = "job", ...) {
  JobNode$new(name = name, script_path = make_script(), ...)
}

test_that("add_job() rejects non-JobNode objects", {
  orc <- JobOrchestrator$new()
  expect_error(orc$add_job("not a job"))
})

test_that("add_job() rejects duplicate job names", {
  orc <- JobOrchestrator$new()
  orc$add_job(make_job("a"))
  expect_error(orc$add_job(make_job("a")))
})

test_that("add_job() detects a direct cycle", {
  a <- make_job("a")
  b <- make_job("b", upstream = list(a))
  # Close the cycle manually on the node level before registering
  a$upstream <- list(b)

  orc <- JobOrchestrator$new()
  orc$add_job(a)
  expect_error(orc$add_job(b), "cycle")
})

test_that("jobs() returns all registered jobs", {
  orc <- JobOrchestrator$new()
  orc$add_job(make_job("a"))
  orc$add_job(make_job("b"))

  expect_equal(length(orc$jobs()), 2)
  expect_true(all(c("a", "b") %in% names(orc$jobs())))
})

test_that("status() returns a data frame with correct columns", {
  orc <- JobOrchestrator$new()
  orc$add_job(make_job("a"))

  df <- orc$status()
  expect_s3_class(df, "data.frame")
  expect_named(df, c("name", "status"))
})

test_that("max_workers defaults to detectCores() - 1", {
  orc <- JobOrchestrator$new()
  expect_equal(orc$max_workers, parallel::detectCores() - 1L)
})

test_that("max_workers can be set manually", {
  orc <- JobOrchestrator$new(max_workers = 2)
  expect_equal(orc$max_workers, 2)
})

test_that("reset_all() resets all jobs to pending", {
  orc <- JobOrchestrator$new()
  a <- make_job("a")
  b <- make_job("b")
  orc$add_job(a)
  orc$add_job(b)
  a$status <- "success"
  b$status <- "failed"

  orc$reset_all()
  expect_equal(a$status, "pending")
  expect_equal(b$status, "pending")
})

test_that("reset_all() restores retries_remaining", {
  orc <- JobOrchestrator$new()
  job <- make_job("a")
  orc$add_job(job, retries = 3)
  job$retries_remaining <- 0L

  orc$reset_all()
  expect_equal(job$retries_remaining, 3L)
})

test_that("add_job() sets retries_remaining on the job", {
  orc <- JobOrchestrator$new()
  job <- make_job("a")
  orc$add_job(job, retries = 3)
  expect_equal(job$retries_remaining, 3L)
})

test_that("add_job() defaults retries to 0", {
  orc <- JobOrchestrator$new()
  job <- make_job("a")
  orc$add_job(job)
  expect_equal(job$retries_remaining, 0L)
})

test_that("add_job() rejects negative retries", {
  orc <- JobOrchestrator$new()
  expect_error(orc$add_job(make_job("a"), retries = -1))
})

test_that("remove_job() errors on unknown job name", {
  orc <- JobOrchestrator$new()
  expect_error(orc$remove_job("ghost"), "No job named")
})

test_that("remove_job() removes the job from the registry", {
  orc <- JobOrchestrator$new()
  orc$add_job(make_job("a"))
  orc$remove_job("a")
  expect_equal(length(orc$jobs()), 0)
})

test_that("remove_job() cleans up downstream edges on upstream jobs", {
  a <- make_job("a")
  b <- make_job("b", upstream = list(a))
  orc <- JobOrchestrator$new()
  orc$add_job(a)
  orc$add_job(b)

  orc$remove_job("b")
  expect_equal(length(a$downstream), 0)
})

test_that("remove_job() cleans up upstream edges on downstream jobs", {
  a <- make_job("a")
  b <- make_job("b", upstream = list(a))
  orc <- JobOrchestrator$new()
  orc$add_job(a)
  orc$add_job(b)

  orc$remove_job("a")
  expect_equal(length(b$upstream), 0)
})

test_that("remove_job() detaches downstream jobs without removing them", {
  a <- make_job("a")
  b <- make_job("b", upstream = list(a))
  orc <- JobOrchestrator$new()
  orc$add_job(a)
  orc$add_job(b)

  orc$remove_job("a")
  expect_true("b" %in% names(orc$jobs()))
  expect_equal(length(b$upstream), 0)
})

test_that("add_upstream() wires a dependency between two registered jobs", {
  orc <- JobOrchestrator$new()
  orc$add_job(make_job("a"))
  orc$add_job(make_job("b"))
  orc$add_upstream("b", "a")

  a <- orc$jobs()[["a"]]
  b <- orc$jobs()[["b"]]
  expect_equal(length(b$upstream), 1)
  expect_equal(b$upstream[[1]]$name, "a")
  expect_equal(length(a$downstream), 1)
  expect_equal(a$downstream[[1]]$name, "b")
})

test_that("add_upstream() errors on unknown job names", {
  orc <- JobOrchestrator$new()
  orc$add_job(make_job("a"))
  expect_error(orc$add_upstream("a", "ghost"), "No job named")
  expect_error(orc$add_upstream("ghost", "a"), "No job named")
})

test_that("add_upstream() errors on duplicate dependency", {
  a <- make_job("a")
  b <- make_job("b", upstream = list(a))
  orc <- JobOrchestrator$new()
  orc$add_job(a)
  orc$add_job(b)
  expect_error(orc$add_upstream("b", "a"), "already an upstream")
})

test_that("add_upstream() errors when it would introduce a cycle", {
  orc <- JobOrchestrator$new()
  orc$add_job(make_job("a"))
  orc$add_job(make_job("b"))
  orc$add_upstream("b", "a")
  expect_error(orc$add_upstream("a", "b"), "cycle")
})

test_that("remove_upstream() detaches a dependency", {
  a <- make_job("a")
  b <- make_job("b", upstream = list(a))
  orc <- JobOrchestrator$new()
  orc$add_job(a)
  orc$add_job(b)
  orc$remove_upstream("b", "a")

  expect_equal(length(b$upstream), 0)
  expect_equal(length(a$downstream), 0)
})

test_that("remove_upstream() errors on unknown job names", {
  orc <- JobOrchestrator$new()
  orc$add_job(make_job("a"))
  expect_error(orc$remove_upstream("a", "ghost"), "No job named")
  expect_error(orc$remove_upstream("ghost", "a"), "No job named")
})

test_that("remove_upstream() errors when dependency does not exist", {
  orc <- JobOrchestrator$new()
  orc$add_job(make_job("a"))
  orc$add_job(make_job("b"))
  expect_error(orc$remove_upstream("b", "a"), "not an upstream")
})

test_that("validate() returns TRUE for a valid orchestrator", {
  orc <- JobOrchestrator$new()
  orc$add_job(make_job("a"))
  expect_true(orc$validate())
})

test_that("validate() returns FALSE and warns when a script is missing", {
  orc <- JobOrchestrator$new()
  job <- make_job("a")
  orc$add_job(job)
  job$script_path <- "/nonexistent/path.R"

  expect_false(suppressWarnings(orc$validate()))
  expect_warning(orc$validate(), "script not found")
})
