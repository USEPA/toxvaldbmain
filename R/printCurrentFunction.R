#--------------------------------------------------------------------------------------
#' @description Print the name of the current function
#' @param comment.string An optional string to be printed
#' @title printCurrentFunction
#' @return None
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[utils]{flush.console}}
#' @rdname printCurrentFunction
#' @export
#' @importFrom utils flush.console
#--------------------------------------------------------------------------------------
printCurrentFunction <- function(comment.string=NA) {
  cat("=========================================\n")
  curcall <- sys.call(sys.parent(n=1))[[1]]
  cat(curcall,"\n")
  if(!is.null(comment.string) && !is.na(comment.string))	cat(comment.string,"(", format(Sys.time(), usetz = TRUE),")\n")
  cat("=========================================\n")
  utils::flush.console()
}
