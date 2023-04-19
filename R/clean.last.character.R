#--------------------------------------------------------------------------------------
#' Clean unneeded characters from the end of a string
#' @param x String to be cleaned
#' @return The cleaned string
#'
#--------------------------------------------------------------------------------------
clean.last.character <- function(x) {
  ylist = c(";","/",".")
  x = str_trim(x)
  for(i in 1:3) {
    for(y in ylist) {
      if(substr(x,nchar(x),nchar(x))==y) {
        x = substr(x,1,(nchar(x)-1))
      }
    }
    x = str_trim(x)
  }
  return(x)
}
