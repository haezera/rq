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

make_orc <- function() {
  orc <- JobOrchestrator$new()
  orc$add_job(make_job("a"))
  orc$add_job(make_job("b", upstream = list(orc$jobs()[["a"]])))
  orc
}

# ── Construction ──────────────────────────────────────────────────────────────

test_that("Scheduler rejects non-JobOrchestrator input", {
  expect_error(Scheduler$new("not an orchestrator"), "JobOrchestrator")
})

test_that("Scheduler initialises with a valid orchestrator", {
  expect_no_error(Scheduler$new(make_orc()))
})

# ── Runner script generation ──────────────────────────────────────────────────

test_that("write_runner() produces a file on disk", {
  s      <- Scheduler$new(make_orc())
  script <- s$.__enclos_env__$private$write_runner()

  expect_true(file.exists(script))
})

test_that("write_runner() script contains required rq boilerplate", {
  s       <- Scheduler$new(make_orc())
  script  <- s$.__enclos_env__$private$write_runner()
  content <- readLines(script)

  expect_true(any(grepl("library(rq)",        content, fixed = TRUE)))
  expect_true(any(grepl("JobOrchestrator",    content, fixed = TRUE)))
  expect_true(any(grepl("orc$run()",          content, fixed = TRUE)))
})

test_that("write_runner() script contains all registered jobs", {
  s       <- Scheduler$new(make_orc())
  script  <- s$.__enclos_env__$private$write_runner()
  content <- paste(readLines(script), collapse = "\n")

  expect_true(grepl('"a"', content, fixed = TRUE))
  expect_true(grepl('"b"', content, fixed = TRUE))
})

test_that("write_runner() script includes add_upstream() calls for dependent jobs", {
  s       <- Scheduler$new(make_orc())
  script  <- s$.__enclos_env__$private$write_runner()
  content <- readLines(script)

  expect_true(any(grepl("add_upstream", content, fixed = TRUE)))
})

test_that("write_runner() script with no edges omits add_upstream()", {
  orc <- JobOrchestrator$new()
  orc$add_job(make_job("a"))
  s       <- Scheduler$new(orc)
  script  <- s$.__enclos_env__$private$write_runner()
  content <- readLines(script)

  expect_false(any(grepl("add_upstream", content, fixed = TRUE)))
})

# ── OS detection ──────────────────────────────────────────────────────────────

test_that("os() returns 'unix' or 'windows'", {
  s  <- Scheduler$new(make_orc())
  os <- s$.__enclos_env__$private$os()
  expect_true(os %in% c("unix", "windows"))
})

# ── Package check ─────────────────────────────────────────────────────────────

test_that("check_pkg() errors informatively for a missing package", {
  s <- Scheduler$new(make_orc())
  expect_error(
    s$.__enclos_env__$private$check_pkg("__no_such_pkg__"),
    "__no_such_pkg__"
  )
})
