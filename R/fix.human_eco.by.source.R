#-------------------------------------------------------------------------------------
#' Fix the human_eco flag
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be fixed. If NULL, fix all sources
#' @param reset If TRUE, reset all values to 'not specified' before processing all records in the source
#' @return The database will be altered
#' @export
#--------------------------------------------------------------------------------------
fix.human_eco.by.source <- function(toxval.db,source=NULL, reset=T){
  printCurrentFunction(paste(toxval.db,":", source))

  if(is.null(source)) slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  else slist = source
  for(source in slist) {
    cat("fix human_eco:",source,"\n")
    if(reset) runQuery(paste0("update toxval set human_eco='not specified' where source like '",source,"'"),toxval.db)

    human.list = c("Alaska DEC","ATSDR MRLs 2020","ATSDR MRLs 2022","NIOSH","DOE Protective Action Criteria","USGS HBSL",
                   "Mass. Drinking Water Standards","DOD","DOE Protective Action Criteria",
                   "OSHA Air contaminants","OW Drinking Water Standards","Pennsylvania DEP MCLs","Pennsylvania DEP ToxValues")
    if(is.element(source,human.list)) {
      query = paste0("update toxval set species_original='Human (RA)',species_id=5000000 where source='",source,"'")
      runQuery(query,toxval.db)
    }

    human.list = c("Cal OEHHA","California DPH",
                   "EPA AEGL","EPA OPP","FDA CEDI","IRIS",
                   "PFAS Summary PODs","PPRTV (CPHEA)","PPRTV (NCEA)","PPRTV (ORNL)",
                   "RSL","Wignall")
    if(is.element(source,human.list)) {
      query = paste0("update toxval set species_original='Human (RA)' where species_id=1000000 and source='",source,"'")
      runQuery(query,toxval.db)
      query = paste0("update toxval set species_id=5000000 where species_id=1000000 and source='",source,"'")
      runQuery(query,toxval.db)
    }
    if(is.element(source,c("TEST","WHO IPCS","EPA OPPT","HESS","NIOSH","PFAS 150 SEM","Alaska DEC"))) {
      query = paste0("update toxval set human_eco='human health' where source='",source,"'")
      runQuery(query,toxval.db)
    }
    else {
      query = paste0("update toxval set human_eco='not specified' where source='",source,"'")
      runQuery(query,toxval.db)
      query = paste0("update toxval set human_eco='human health' where species_id in (select species_id from species where ecotox_group='Mammals') and source='",source,"'")
      runQuery(query,toxval.db)
      query = paste0("update toxval set human_eco='eco' where species_id in
                     (select species_id from species where ecotox_group not in ('Not specified','None','-','Mammals')) and source='",source,"'")
      runQuery(query,toxval.db)
     }
  }
}

