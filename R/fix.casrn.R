#--------------------------------------------------------------------------------------
#' Fix a CASRN that has one of several problems
#'
#' @param casrn Input CASRN to be fixed
#' @param cname An optional chemical name
#' @param verbose if TRUE, print hte input values
#' @return the fixed CASRN
#
#--------------------------------------------------------------------------------------
fix.casrn <- function(casrn,cname="",verbose=F) {
  if(verbose) cat("input: ",cname,":",casrn,"\n")
  if(contains(casrn,"NOCAS")) return(casrn)
  doit <- T
  while(doit) {
    if(substr(casrn,1,1)=="0") casrn <- substr(casrn,2,nchar(casrn))
    else doit <- F
  }

  if(!contains(casrn,"-")) {
    nc <- nchar(casrn)
    ctemp <- casrn
    right <- substr(ctemp,nc,nc)
    mid <- substr(ctemp,nc-2,nc-1)
    left <- substr(ctemp,1,nc-3)
    casrn <- paste(left,"-",mid,"-",right,sep="")
  }
  if(!is.na(cname)) {
    if(cname=="epsilon-Hexachlorocyclohexane (epsilon-HC)") casrn <- "6108-10-7"
    if(cname=="Captafol") casrn <- "2425-06-1"
    if(cname=="Hydrogen sulfide") casrn <- "7783-06-4"
    if(cname=="Picloram") casrn <- "1918-02-1"
    if(cname=="Dodine") casrn <- "2439-10-3"
    if(cname=="Mancozeb") casrn <- "8018-01-7"
  }
  if(verbose) cat("output: ",cname,":",casrn,"\n")
  return(casrn)
}
