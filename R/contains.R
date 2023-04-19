#--------------------------------------------------------------------------------------
#' Find out if one string contains another
#' @param x The string to be searched in
#' @param query the second string
#' @param verbose if TRUE, the two strings are printed
#' @return if x contains query, return TRUE, FALSE otherwise
#'
#--------------------------------------------------------------------------------------
contains <- function(x,query,verbose=F) {
  if(verbose) {
    print(x)
    print(query)
  }
  if(is.null(x)) return(FALSE)
  if(is.null(query)) return(FALSE)
  if(is.na(x)) return(FALSE)
  if(is.na(query)) return(FALSE)
  x <- tolower(x)
  query <- tolower(query)

  val <- sum(grep(query,x,fixed=T))
  if(val>0) return(TRUE)
  else return(FALSE)
}
