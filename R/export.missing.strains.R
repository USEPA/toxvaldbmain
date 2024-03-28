#--------------------------------------------------------------------------------------
#'
#' Set the strain information in toxval
#'
#' @param toxval.db The version of the database to use
#' @param date_string The date of the latest dictionary version
#' @export
#--------------------------------------------------------------------------------------
export.missing.strains <- function(toxval.db,date_string="2024-02-27") {
  printCurrentFunction()
  file = paste0(toxval.config()$datapath,"species/strain_dictionary_",date_string,".xlsx")
  dict = read.xlsx(file)

  cat("original:",nrow(dict),"\n")
  missing = runQuery("select distinct strain_original from toxval",toxval.db)[,1]
  missing = missing[!is.element(missing,dict$strain_original)]
  cat("missing:",length(missing),"\n")
  if(length(missing)>0) {
    temp = as.data.frame(matrix(nrow=length(missing),ncol=ncol(dict)))
    names(temp) = names(dict)
    temp[] = "-"
    temp[,1] = missing
    for(i in 1:nrow(temp)) {
      s0 = temp[i,1]
      s0 = stri_escape_unicode(s0)
      query = paste0("select distinct species_original from toxval where strain_original='",s0,"'")
      temp[i,"species_original"] = runQuery(query,toxval.db)[1,1]
    }
    dict = rbind(dict,temp)
    file = paste0(toxval.config()$datapath,"species/strain_dictionary_",Sys.Date(),".xlsx")
    write.xlsx(dict,file)
  }
}
