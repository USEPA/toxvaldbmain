#-------------------------------------------------------------------------------------
#'
#' Fill the chemical table
#'
#' @param toxval.db The version of toxvaldb to use.
#' @param source The source to be used
#' @param verbose If TRUE, print out extra diagnostic messages
#-------------------------------------------------------------------------------------
fill.chemical.by.source <- function(toxval.db, source, verbose=TRUE) {
  printCurrentFunction(paste(toxval.db,":", source))

  #runQuery(paste0("delete from chemical where dtxsid in (select dtxsid from source_chemical where source = '",source,"')"),toxval.db)
  res = runQuery(paste0("select dtxsid,casrn,name from source_chemical where source = '",source,
                        "' and dtxsid != '-'") ,toxval.db)
  cat("Rows in source_chemical:",nrow(res),"\n")
  res = dplyr::distinct(res)
  cat("Unique rows in source_chemical:",nrow(res),"\n")
  x = unique(res$dtxsid)
  cat("Unique dtxsid values:",length(x),"\n")

  # Filter out NA and duplicate values (only keeps first in duplicate set)
  res2 = res %>%
    dplyr::arrange(dtxsid) %>%
    dplyr::filter(!is.na(dtxsid),
                  !duplicated(dtxsid))
  # Fix names
  for(i in  seq_len(nrow(res2))) {
    name = res2$name[i]
    if(!validUTF8(name)) name = "invalid name"
    name2 = name
    name2 = tryCatch({
       stringi::stri_escape_unicode(stringi::stri_enc_toutf8(name))
    }, error = function(e) {
      cat("Error messge: ",paste0(e, collapse=" | "), "\n")
      cat("bad name\n  ",name,"\n  ",name2,"\n")
      browser()
    })
    res2[i,"name"] = name2
  }

  dlist = runQuery("select distinct dtxsid from chemical",toxval.db)
  # Only add new dtxsid entries
  res2 = res2[!res2$dtxsid %in% dlist$dtxsid,]

  cat("Unique rows in res2 to be added:",nrow(res2),"\n")
  if(nrow(res2)){
    res2 = res2[,c("dtxsid","casrn","name")]
    runInsertTable(res2, "chemical", toxval.db, verbose)
  }
}
