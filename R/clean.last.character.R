#--------------------------------------------------------------------------------------
#' @description Clean unneeded characters from the end of a string
#' @param x String to be cleaned
#' @return The cleaned string
#'
#' @title clean.last.character
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  #EXAMPLE1
#'  }
#' }
#' @seealso
#'  \code{\link[stringr]{str_trim}}
#' @rdname clean.last.character
#' @export
#' @importFrom stringr str_trim
#--------------------------------------------------------------------------------------
clean.last.character <- function(x) {
  # Remove any of ; / . _ at the end of strings
  ylist = c(";","/",".", "_")
  x = stringr::str_trim(x)
  for(i in 1:3) {
    for(y in ylist) {
      if(substr(x,nchar(x),nchar(x))==y) {
        x = substr(x,1,(nchar(x)-1))
      }
    }
    x = stringr::str_trim(x)
  }
  return(x)
}
