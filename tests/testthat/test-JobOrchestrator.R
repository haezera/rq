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
