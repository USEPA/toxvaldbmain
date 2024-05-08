#--------------------------------------------------------------------------------------
#' Check the chemicals from a file
#' Names with special characters are cleaned and trimmed
#' CASRN are fixed (dashes put in, trimmed) and check sums are calculated
#' The output is sent to a file called chemcheck.xlsx in the source data file
#' One option for using this is to edit the source file until no errors are found
#'
#' @param res0  The data frame in which chemicals names and CASRN will be replaced
#' @param name.col The column name that contains the chemical names
#' @param casrn.col The column name that contains the CARN values
#' @param source The source to be processed. If source=NULL, process all sources
#' @param verbose If TRUE, print diagnostic messages
#' @return Return a list with fixed CASRN and name and flags indicating if fixes were made:
#' res0=res0,name.OK=name.OK,casrn.OK=casrn.OK,checksum.OK=checksum.OK
#'
#--------------------------------------------------------------------------------------
chem.check <- function(res0,
                       name.col="name",
                       casrn.col="casrn",
                       source=NULL,
                       verbose=F) {
  printCurrentFunction(source)
  ccheck = NULL
  row = as.data.frame(matrix(nrow=1,ncol=4))
  names(row)= c("original","escaped","cleaned","checksum")
  name.OK = T
  casrn.OK = T
  checksum.OK = T
  cat(">>>Deal with name\n")
  for(i in 1:nrow(res0)) {
    n0 = res0[i,name.col]
    n1 = iconv(n0,from="UTF-8",to="ASCII//TRANSLIT")
    n2 = stri_escape_unicode(n1)
    n2 = str_replace_all(n2,"\\\\'","\'")
    n2 = str_replace_all(n2,"\r"," ")
    n2 = str_replace_all(n2,"\n"," ")
    n2 = str_replace_all(n2,"  "," ")
    n2 = str_trim(n2)
    if(source %in% c("Alaska DEC",
                           "California DPH",
                           "EPA AEGL",
                           "Mass. Drinking Water Standards",
                           "OSHA Air contaminants",
                           "OW Drinking Water Standards",
                           "Pennsylvania DEP MCLs",
                           "USGS HBSL",
                           "WHO IPCS",
                           "ATSDR MRLs",
                           "Cal OEHHA",
                           "COSMOS",
                           "DOD ERED",
                           "DOE Wildlife Benchmarks",
                           "DOE Protective Action Criteria",
                           "IRIS",
                           "EPA OPP",
                           "Pennsylvania DEP ToxValues",
                           "EnviroTox_v2",
                           "HEAST")) {
      if(contains(n2,";")) {
        start = gregexpr(";",n2)[[1]][1]
        n3 = str_trim(substr(n2,1,start-1))
        string = paste0(source," [",n2,"] [",n3,"]")
        #cat(string,"\n")
        n2 = n3
      }
      if(contains(n2," (")) {
        start = gregexpr(" \\(",n2)[[1]][1]
        n3 = str_trim(substr(n2,1,start-1))
        string = paste0(source," [",n2,"] [",n3,"]")
        #cat(string,"\n")
        n2 = n3
      }
    }
    n2 = clean.last.character(n2)
    if(verbose) cat("1>>> ",n0,n1,n2,"\n")
    if(is.na(n0)) browser()
    if(n2!=n0) {
      row[1,1] = n0
      row[1,2] = n1
      row[1,3] = n2
      ccheck = rbind(ccheck,row)
      res0[i,name.col] = n2
      name.OK = F
    }
    if(i%%1000==0) cat(" chemcheck name: finished ",i," out of ",nrow(res0),"\n")
  }
  cat("\n>>> Deal with CASRN\n")
  for(i in 1:nrow(res0)) {
    n0 = res0[i,casrn.col]
    if(!is.na(n0)) {
      n1 = iconv(n0,from="UTF-8",to="ASCII//TRANSLIT")
      n2 = stri_escape_unicode(n1)
      n2 = fix.casrn(n2)
      cs = cas_checkSum(n2)
      if(is.na(cs)) cs = 0
      if(verbose) cat("2>>> ",n0,n1,n2,cs,"\n")
      if(n2!=n0) {
        res0[i,casrn.col] = n2
        row[1,1] = n0
        row[1,2] = n1
        row[1,3] = n2
        row[1,4] = cs
        ccheck = rbind(ccheck,row)
        casrn.OK = F
      }
      if(!cs) {
        row[1,1] = n0
        row[1,2] = n1
        row[1,3] = NA
        row[1,4] = cs
        ccheck = rbind(ccheck,row)
        checksum.OK = F
        cat("bad checksum:",n0,n1,"\n")
      }
    }
    if(i%%1000==0) cat(" chemcheck casrn: finished ",i," out of ",nrow(res0),"\n")
  }
  ccheck = unique(ccheck)
  indir = paste0(toxval.config()$datapath,"chemcheck/")
  if(is.null(source)) file = paste0(indir,"chemcheck no source.xlsx")
  else file = paste0(indir,"chemcheck ",source,".xlsx")
  if(!is.null(ccheck)) if(nrow(ccheck)>0) openxlsx::write.xlsx(ccheck,file)

  if(!name.OK) cat("Some names fixed\n")
  else cat("All names OK\n")
  if(!casrn.OK) cat("Some casrn fixed\n")
  else cat("All casrn OK\n")
  if(!checksum.OK) cat("Some casrn have bad checksums\n")
  else cat("All checksums OK\n")
  return(list(res0=res0,name.OK=name.OK,casrn.OK=casrn.OK,checksum.OK=checksum.OK))
}
