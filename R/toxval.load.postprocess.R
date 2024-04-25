#--------------------------------------------------------------------------------------
#' Do all of the post-processing steps for a source
#' @param toxval.db The database version to use
#' @param sourcedb The source database name
#' @param source The source name
#' @param do.convert.units If TRUE, convert units, mainly from ppm to mg/kg-day. This code is not debugged
#' @param chem_source Used only for source=ECHA IUCLID
#' @param remove_null_dtxsid If TRUE, delete source records without curated DTXSID value
#--------------------------------------------------------------------------------------
toxval.load.postprocess <- function(toxval.db,
                                    source.db,
                                    source,
                                    do.convert.units=FALSE,
                                    chem_source,
                                    subsource=NULL,
                                    remove_null_dtxsid=TRUE){
  printCurrentFunction(toxval.db)

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  do.convert.units = TRUE # override default because it is not specified in all toxval load functions
  if(source=="ECOTOX") do.convert.units = FALSE
  #####################################################################
  cat("check that the dictionaries are loaded\n")
  #####################################################################
  n1 = runQuery("select count(*) from toxval_dictionary",toxval.db)[1,1]
  n2 = runQuery("select count(*) from toxval_type_dictionary",toxval.db)[1,1]
  if(n1==0 || n2==0) import.dictionary(toxval.db)

  #####################################################################
  cat("load chemical info to source_chemical\n")
  #####################################################################
  if(source=="ECHA IUCLID") {
    toxval.load.source_chemical(toxval.db,source.db,source=chem_source,verbose=TRUE, remove_null_dtxsid=remove_null_dtxsid)
  } else {
    toxval.load.source_chemical(toxval.db,source.db,source=source,verbose=TRUE, remove_null_dtxsid=remove_null_dtxsid)
  }

  # Check if any data left after NULL DTXSID removal
  n_check = runQuery(paste0("SELECT count(*) as n FROM toxval where source = '", source,"'",query_addition),
                     toxval.db)
  if(!n_check$n){
    message("No rows to postprocess...returning...")
    return()
  }

  #####################################################################
  cat("fill chemical by source\n")
  #####################################################################
  if(source=="ECHA IUCLID") {
    fill.chemical.by.source(toxval.db, chem_source)
  } else {
    fill.chemical.by.source(toxval.db, source)
  }

  #####################################################################
  cat("fix species by source\n")
  #####################################################################
  fix.species.v2(toxval.db, source, subsource)

  #####################################################################
  cat("fix human_eco by source\n")
  #####################################################################
  fix.human_eco.by.source(toxval.db, source, subsource, reset = TRUE)

  #####################################################################
  cat("fix all.parameters (exposure_method, exposure_route, sex,strain,
    study_duration_class, study_duration_units, study_type,toxval_type,
    exposure_form, media, toxval_subtype) by source\n")
  #####################################################################
  fix.all.param.by.source(toxval.db,source,subsource,fill.toxval_fix=TRUE)

  #####################################################################
  cat("get MW if needed for unit conversion\n")
  #####################################################################
  if(do.convert.units) toxval.set.mw(toxval.db, source, subsource)

  #####################################################################
  cat("fix units by source\n")
  #####################################################################
  fix.units.by.source(toxval.db, source,subsource,do.convert.units)

  #####################################################################
  cat("fix strain by source\n")
  #####################################################################
  fix.strain.v2(toxval.db, source, subsource)

  #####################################################################
  cat("fix priority_id by source\n")
  #####################################################################
  fix.priority_id.by.source(toxval.db, source, subsource)

  #####################################################################
  cat("fix critical_effect by source\n")
  #####################################################################
  doit = TRUE
  if(is.element(source,c("ToxRefDB","ECOTOX"))) doit = FALSE
  if(doit) fix.critical_effect.icf.by.source(toxval.db, source)

  #####################################################################
  cat("add the manual study_type fixes\n")
  #####################################################################
  fix.study_type.by.source(toxval.db, mode="import", source=source,
                           subsource=subsource)

  #####################################################################
  cat("fix empty cells to hyphen by source\n")
  #####################################################################
  fix.empty.by.source(toxval.db, source, subsource)

  #####################################################################
  cat("fix empty cells in record source to hyphen by source\n")
  #####################################################################
  fix.empty.record_source.by.source(toxval.db, source)

  #####################################################################
  cat("set toxval defaults globally by source\n")
  #####################################################################
  fill.toxval.defaults.global.by.source(toxval.db, source, subsource)

  #####################################################################
  cat("special case for toxval_numeric_qualifier\n")
  #####################################################################
  runQuery(paste0("update toxval set toxval_numeric_qualifier='' where toxval_numeric_qualifier='-' and source='",source,"'",query_addition),toxval.db)

  #####################################################################
  cat("special case for study_duration_value\n")
  #####################################################################
  runQuery(paste0("update toxval set study_duration_value=-999 where study_duration_value is NULL and source='",source,"'",query_addition),toxval.db)

  #####################################################################
  cat("fix study group by source\n")
  #####################################################################
  fix.study_group(toxval.db, source, subsource)

  #####################################################################
  cat("fix risk assessment class by source\n")
  #####################################################################
  fix.risk_assessment_class.by.source(toxval.db, source, subsource)

  #####################################################################
  cat("fix qa status by source\n")
  #####################################################################
  fix.qc_status.by.source(toxval.db, source, subsource)

  #####################################################################
  #cat("set hash toxval by source\n")
  # currently not set - may not be needed for future dashboard releases
  #####################################################################
  #set.hash.toxval.by.source(toxval.db, source)
  #set.hash.record_source.by.source(toxval.db, source)
  #map.hash.record_source.by.source(toxval.db, source )

  #####################################################################
  cat("export by source\n")
  #####################################################################
  export.all.by.source(toxval.db, source, subsource)
}
