#--------------------------------------------------------------------------------------
#'
#' Set the strain information in toxval
#'
#' @param toxval.db The version of the database to use
#' @param source The source to be fixed. If NULL, fix for all sources
#' @param subsource The subsource to be fixed (NULL default)
#' @param date_string The date of the latest dictionary version
#' @export
#--------------------------------------------------------------------------------------
fix.strain.v2 <- function(toxval.db,source=NULL,subsource=NULL,date_string="2023-04-03",reset=FALSE) {
  printCurrentFunction()
  if(reset) runQuery("update toxval set strain='-', strain_group='-'",db5)

  fix.species.duplicates(toxval.db)

  file = paste0(toxval.config()$datapath,"species/strain_dictionary_",date_string,".xlsx")
  dict = read.xlsx(file)
  dict = distinct(dict)
  dict$common_name = tolower(dict$common_name)
  if(is.null(source)) {
    slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  } else {
    slist = source
  }

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }
          
  for(source in slist) {
    cat("fix strain:",source,subsource"\n")
    query = paste0("select a.species_original, a.strain_original, b.species_id, b.common_name ",
                   "from toxval a, species b ",
                   "where a.species_id=b.species_id ",
                   # "and a.human_eco='human health' ",
                   "and a.source='",source,"'",query_addition)
    t1 = runQuery(query,toxval.db)
    t1 = distinct(t1)
    if(nrow(t1)) {
      spolist = sort(unique(t1$species_original))
      for(spo in spolist) {
        t2 = t1[is.element(t1$species_original,spo),]
        if(nrow(t2)) {
          if(is.element(spo,dict$species_original)) {
            cat("  ",spo,"\n")
            sid = unique(t2[t2$species_original==spo,"species_id"])
            cn = unique(t2[t2$species_original==spo,"common_name"])
            if(length(cn)>1) {
              message("Multiple common names identified...how to proceed?")
              browser()
            }
            t3 = t2[t2$common_name==cn,"strain_original"]
            for(so in t3) {
              so = stringr::str_replace_all(so,"\\'","")
              d1 = dict[is.element(dict$species_original,spo),]
              d2 = d1[is.element(d1$strain_original,so),]
              if(nrow(d2)) {
                if(nrow(d2)>1) {
                  message("Multiple dictionary fixes identified...how to proceed?")
                  browser()
                }
                query = paste0("update toxval set strain='",d2[1,"strain"],"', strain_group='",d2[1,"strain_group"],"' where source='",source,"' and strain_original='",so,"' and species_id=",sid,query_addition)
                # if(d2[1,"strain_group"]=="Bird") browser()
                runQuery(query,toxval.db)
              }
            }
          }
        }
      }
    }
  }
}
