library(testthat)
library(rq)

make_script <- function() {
  path <- tempfile(fileext = ".R")
  writeLines("1 + 1", path)
  path
}

make_job <- function(name = "job", upstream = list(), ...) {
  JobNode$new(name = name, script_path = make_script(), upstream = upstream, ...)
}

test_that("JobNode initializes with expected defaults", {
  job <- make_job()

  expect_equal(job$name, "job")
  expect_equal(job$status, "pending")
  expect_equal(job$on_fail, "skip")
  expect_null(job$last_run)
  expect_equal(job$upstream, list())
})

test_that("JobNode rejects non-existent script paths", {
  expect_error(JobNode$new(name = "job", script_path = "/does/not/exist.R"))
})

test_that("JobNode rejects invalid on_fail values", {
  expect_error(make_job(on_fail = "explode"))
})

test_that("is_ready() is TRUE when there are no upstream jobs", {
  expect_true(make_job()$is_ready())
})

test_that("is_ready() is FALSE when an upstream job has not succeeded", {
  upstream <- make_job("upstream")
  job <- make_job("job", upstream = list(upstream))

  expect_false(job$is_ready())

  upstream$status <- "success"
  expect_true(job$is_ready())
})

test_that("reset() clears status and last_run", {
  job <- make_job()
  job$status <- "success"
  job$last_run <- Sys.time()

  job$reset()

  expect_equal(job$status, "pending")
  expect_null(job$last_run)
})

test_that("JobNode defaults retries_remaining to 0", {
  expect_equal(make_job()$retries_remaining, 0L)
})
