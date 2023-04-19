#-------------------------------------------------------------------------------------
#' Update the exposure route, method and form from a dictionary
#' @param toxval.db The database version to use
#' @param source The source to process
#-------------------------------------------------------------------------------------
fix.exposure_method.and.form.by.source <- function(toxval.db, source){
  printCurrentFunction(paste(toxval.db,":", source))

  conv = read.xlsx(paste0(toxval.config()$datapath,"dictionary/exposure_method_temp_fix.xlsx"))
  for (i in 1: nrow(conv)){
    query = paste0("
                   update toxval
                   set exposure_method = '",conv[i,2],"',
                   exposure_form = '",conv[i,3],"'
                   where exposure_method='",conv[i,1],"' and source = '",source,"'")
    runInsert(query,toxval.db,T,F,T)
  }
}
