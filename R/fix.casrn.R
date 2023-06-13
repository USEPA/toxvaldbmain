#--------------------------------------------------------------------------------------
#' @description Fix a CASRN that has one of several problems
#'
#' @param casrn Input CASRN to be fixed
#' @param cname An optional chemical name
#' @param verbose if TRUE, print the input values
#' @return the fixed CASRN
#' @title fix.casrn
#' @details DETAILS
#' @examples
#' \dontrun{
#' if(interactive()){
#'  fix.casrn("107028")
#'  # Expected output "107-02-8"
#'  }
#' }
#' @seealso
#'  \code{\link[tidyr]{reexports}}
#' @rdname fix.casrn
#' @export
#
#--------------------------------------------------------------------------------------
fix.casrn <- function(casrn, cname="", verbose=FALSE) {
  # verbose print input
  if(verbose) cat("input: ",cname,":",casrn,"\n")

  # Set known values first
  if(!is.na(cname)) {
    if(cname=="epsilon-Hexachlorocyclohexane (epsilon-HC)") casrn <- "6108-10-7"
    if(cname=="Captafol") casrn <- "2425-06-1"
    if(cname=="Hydrogen sulfide") casrn <- "7783-06-4"
    if(cname=="Picloram") casrn <- "1918-02-1"
    if(cname=="Dodine") casrn <- "2439-10-3"
    if(cname=="Mancozeb") casrn <- "8018-01-7"
  }

  # Ignore if input contains non-numerics
  if(!grepl("^[0-9]*$", casrn)) return(casrn)

  # Account for padded string with leading 0's
  casrn <- sub("^0+", "", casrn)

  # Convert to cas format (e.g., 107028 into 107-02-8)
  nc <- nchar(casrn)
  ctemp <- casrn
  right <- substr(ctemp,nc,nc)
  mid <- substr(ctemp,nc-2,nc-1)
  left <- substr(ctemp,1,nc-3)
  casrn <- paste(left,"-",mid,"-",right,sep="")

  # verbose print output
  if(verbose) cat("output: ",cname,":",casrn,"\n")
  return(casrn)
}
