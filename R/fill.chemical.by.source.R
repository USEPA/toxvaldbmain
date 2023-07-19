#-------------------------------------------------------------------------------------
#'
#' Fill the chemical table
#'
#' @param toxval.db The version of toxvaldb to use.
#' @param source The source to be used
#' @param verbose If TRUE, print out extra diagnostic messages
#-------------------------------------------------------------------------------------
fill.chemical.by.source <- function(toxval.db, source, verbose=T) {
  printCurrentFunction(paste(toxval.db,":", source))

  #runQuery(paste0("delete from chemical where dtxsid in (select dtxsid from source_chemical where source = '",source,"')"),toxval.db)
  res = runQuery(paste0("select dtxsid,casrn,name from source_chemical where source = '",source,"'") ,toxval.db)
  res = res[!is.element(res$dtxsid,"-"),]
  cat("Rows in source_chemical:",nrow(res),"\n")
  res = unique(res)
  cat("Unique rows in source_chemical:",nrow(res),"\n")
  x = unique(res$dtxsid)
  cat("Unique dtxsid values:",length(x),"\n")
  res = res[order(res$dtxsid),]
  res2 = res
  res2 = res2[!is.na(res2$dtxsid),]
  if(!nrow(res2)){
    message("No curated DTXSID values available for source to add to 'chemicals' tables")
    return()
  }
  res2$duplicate <- 0
  for(i in 2:nrow(res2)) {
    if(res2[i,"dtxsid"]==res2[i-1,"dtxsid"]) {
      res2[i,"duplicate"] <- 1
    }
    name = res2[i,"name"]
    if(!validUTF8(name)) name = "invalid name"
    name2 = name
    tryCatch({
       name2 = stri_escape_unicode(stri_enc_toutf8(name))
    }, warning = function(w) {
    }, error = function(e) {
      cat("Error messge: ",paste0(e, collapse=" | "), "\n")
      cat("bad name\n  ",name,"\n  ",name2,"\n")
      browser()
    })
    res2[i,"name"] = name2
  }

  res2 = res2[res2$duplicate==0,]
  dlist = runQuery("select distinct dtxsid from chemical",toxval.db)
  # Only add new dtxsid entries
  res2 = res2[!res2$dtxsid %in% dlist$dtxsid,]

  cat("Unique rows in res2 to be added:",nrow(res2),"\n")
  if(nrow(res2)){
    res2 = res2[,c("dtxsid","casrn","name")]
    runInsertTable(res2, "chemical", toxval.db, verbose)
  }
}
