#--------------------------------------------------------------------------------------
#'
#' Set the strain information in toxval
#'
#' @param toxval.db The version of the database to use
#' @param source The source to be fixed. If NULL, fix for all sources
#' @param date_string The date of the latest dictionary version
#' @export
#--------------------------------------------------------------------------------------
fix.strain.v2 <- function(toxval.db,source=NULL,date_string="2023-04-03") {
  printCurrentFunction()
  file = paste0(toxval.config()$datapath,"species/strain_dictionary_",date_string,".xlsx")
  dict = read.xlsx(file)

  if(is.null(source)) slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  else slist = source
  for(source in slist) {
    cat("fix strain:",source,"\n")
    so = runQuery(paste0("select distinct strain_original from toxval where source='",source,"'"),toxval.db)[,1]
    count.good = 0
    if(length(so)>0) {
      for(i in 1:length(so)) {
        tag = stri_escape_unicode(so[i])
        tag = str_replace_all(tag,"\\'","")
        if(is.element(tag,dict$strain_original)) {
          strain = dict[is.element(dict$strain_original,tag),"strain"]
          strain = stri_escape_unicode(strain)
          strain = str_replace_all(strain,"\\'","")
          sg = dict[is.element(dict$strain_original,tag),"strain_group"]
          query = paste0("update toxval set strain='",strain,"', strain_group='",sg,"' where source='",source,"' and strain_original='",tag,"'")
          runQuery(query,toxval.db)
        }
        if(i%%100==0) cat("finished",i,"out of",length(so),":",count.good,"\n")
      }
    }
  }
}
