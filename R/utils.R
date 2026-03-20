#' @title prettylog
#' @description Emit a timestamped, prefixed log line to the console.
#'
#' @param prefix A short label shown in brackets, e.g. a job name.
#' @param ... Character strings passed to \code{paste0} to form the message.
#' @export
prettylog <- function(prefix, ...) {
  ts   <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
  msg  <- paste0(...)
  lines <- strsplit(msg, "\n", fixed = TRUE)[[1]]
  for (line in lines) {
    cat(sprintf("[%s] [%s] %s\n", ts, prefix, line))
  }
}
