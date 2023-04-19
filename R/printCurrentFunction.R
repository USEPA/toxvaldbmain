#--------------------------------------------------------------------------------------
#' Print the name of the current function
#' @param comment.string An optional string to be printed
#--------------------------------------------------------------------------------------
printCurrentFunction <- function(comment.string=NA) {
  cat("=========================================\n")
  curcall <- sys.call(sys.parent(n=1))[[1]]
  cat(curcall,"\n")
  if(!is.na(comment.string))	cat(comment.string,"\n")
  cat("=========================================\n")
  flush.console()
}
