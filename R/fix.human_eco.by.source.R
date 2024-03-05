#-------------------------------------------------------------------------------------
#' Fix the human_eco flag
#' @param toxval.db The version of toxval in which the data is altered.
#' @param source The source to be fixed. If NULL, fix all sources
#' @param subsource The subsource to be fixed (NULL default)
#' @param reset If TRUE, reset all values to 'not specified' before processing all records in the source
#' @return The database will be altered
#' @export
#--------------------------------------------------------------------------------------
fix.human_eco.by.source <- function(toxval.db,source=NULL,subsource=NULL,reset=TRUE){
  printCurrentFunction(paste(toxval.db,":", source,subsource))

  human.list = c("Alaska DEC","ATSDR MRLs",
                 "ATSDR PFAS 2021","Cal OEHHA",
                 "California DPH","ChemIDplus","Chiu",
                 "Copper Manufacturers","COSMOS","DOD",
                 "DOE Protective Action Criteria",
                 "ECHA IUCLID","EFSA",
                 "EPA AEGL",
                 "EPA OPP","EPA OPPT","FDA CEDI",
                 "HAWC PFAS 150","HAWC PFAS 430","HAWC Project",
                 "Health Canada","HEAST","HESS",
                 "HPVIS","IRIS","Mass. Drinking Water Standards",
                 "NIOSH","OSHA Air contaminants","OW Drinking Water Standards",
                 "Pennsylvania DEP MCLs","Pennsylvania DEP ToxValues","PFAS 150 SEM v2",
                 "PPRTV (CPHEA)","PPRTV (NCEA)","RSL",
                 "TEST","ToxRefDB",
                 "Uterotrophic Hershberger DB","WHO IPCS",
                 "USGS HBSL")

  hra.list = c("Alaska DEC","ATSDR MRLs",
               "ATSDR PFAS 2021","Cal OEHHA",
               "California DPH","EPA AEGL","FDA CEDI",
               "Health Canada","HEAST",
               "IRIS","Mass. Drinking Water Standards",
               "NIOSH","OSHA Air contaminants","OW Drinking Water Standards",
               "Pennsylvania DEP MCLs","Pennsylvania DEP ToxValues",
               "PPRTV (CPHEA)","PPRTV (NCEA)","RSL")

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  if(is.null(source)) {
    slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  } else {
    slist = source
  }
  for(source in slist) {
    cat("fix human_eco:",source,"\n")
    if(reset) runQuery(paste0("update toxval set human_eco='not specified' where source like '",source,"'",query_addition),
                       toxval.db)

    if(is.element(source,human.list)) {
      query = paste0("update toxval set target_species='Human' where source='",source,"'",query_addition)
      runQuery(query,toxval.db)
      query = paste0("update toxval set human_eco='human health' where source='",source,"'",query_addition)
      runQuery(query,toxval.db)
      if(is.element(source,hra.list)) {
        query = paste0("update toxval set human_ra='Y' where source='",source,"'",query_addition)
        runQuery(query,toxval.db)
      }
      # some of these human sources have some ECO records
      query = paste0("update toxval set human_eco='eco' where species_id in
                     (select species_id from species where ecotox_group not in
                     ('Miscellaneous','Unspecified','Not specified','None','-','Mammals')) and source='",source,"'",query_addition)
      runQuery(query,toxval.db)

    } else {
      query = paste0("update toxval set human_eco='human health' where species_id in (select species_id from species where ecotox_group='Mammals') and source='",source,"'",query_addition)
      runQuery(query,toxval.db)
      query = paste0("update toxval set human_eco='eco' where species_id in
                     (select species_id from species where ecotox_group not in ('Not specified','None','-','Mammals')) and source='",source,"'",query_addition)
      runQuery(query,toxval.db)
    }
    # special cases
    query = paste0("update toxval set human_eco='microorganisms' where species_id in
                     (select species_id from species where ecotox_group in
                     ('Fungi','Virus','Bacteria','Microorganisms','Yeast','Bacteriophage')) and source='",source,"'",query_addition)
    runQuery(query,toxval.db)
    runQuery(paste0("update toxval set human_eco='eco' where species_id=23840 and source='",source,"'",query_addition),toxval.db)
    runQuery(paste0("update toxval set target_species='-' where human_eco != 'human health' and source='",source,"'",query_addition),toxval.db)

    tvtlist = c("8-hr TWA","adequate intake","aesthetic objective",
                "ALD","critical value","discriminating dose","drinking water quality guideline",
                "cancer slope factor","cancer unit risk",
                "RDA","RfC","RfD","RfD (screening chronic)","RfD (screening subchronic)","STEL (15 min)",
                "TDI","TDLo","threshold dose","ERCL (10)",
                "tolerable concentration in air","tolerable upper intake","UL","optimal intake value")
    stvtlist = runQuery(paste0("select distinct toxval_type from toxval where source='",source,"'",query_addition),toxval.db)[,1]
    tvtlist = tvtlist[tvtlist %in% stvtlist]
    if(length(tvtlist)){
      query = paste0("update toxval set species_id=1000000, target_species='Human' where source='",source,
                     "' and toxval_type in ('",paste0(tvtlist, collapse="', '"),"')",query_addition)
      runQuery(query,toxval.db)
    }
   # "BMC","BMDL","BMDL (01)","BMDL (05)","BMDL (10)","HNEL","LC0",
   # "LD0","LD100","LD50","LEC","LEL","LOAEL", "LOEL","NOAEC","NOAEL","NOEL","POD"
  }
}

