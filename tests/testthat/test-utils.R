library(testthat)
library(rq)

test_that("prettylog() outputs a timestamped prefixed message", {
  out <- capture.output(prettylog("myjob", "hello world"))
  expect_match(out, "^\\[\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}\\] \\[myjob\\] hello world$")
})
