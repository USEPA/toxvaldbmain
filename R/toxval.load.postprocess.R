#--------------------------------------------------------------------------------------
#' Do all of the post-processing steps for a source
#' @param toxval.db The database version to use
#' @param sourcedb The source database name
#' @param source The source name
#' @param do.convert.units If TRUE, convert units, mainly from ppm to mg/kg-day. This code is not debugged
#' @param chem_source Used only for source=ECHA IUCLID
#--------------------------------------------------------------------------------------
toxval.load.postprocess <- function(toxval.db, source.db,source, do.convert.units=F,chem_source,subsource=NULL){
  printCurrentFunction(toxval.db)

  do.convert.units = T # override default because it is not specified in all toxval load functions
  if(source=="ECOTOX") do.convert.units = F
  #####################################################################
  cat("check that the dictionaries are loaded\n")
  #####################################################################
  n1 = runQuery("select count(*) from toxval_dictionary",toxval.db)[1,1]
  n2 = runQuery("select count(*) from toxval_type_dictionary",toxval.db)[1,1]
  if(n1==0 || n2==0) import.dictionary(toxval.db)

  #####################################################################
  cat("load chemical info to source_chemical\n")
  #####################################################################
  if(is.element(source,c("ECHA IUCLID"))) toxval.load.source_chemical(toxval.db,source.db,chem_source,verbose=T)
  else toxval.load.source_chemical(toxval.db,source.db,source,verbose=T)

  #####################################################################
  cat("fill chemical by source\n")
  #####################################################################
  if(source=="ECHA IUCLID") fill.chemical.by.source(toxval.db, chem_source)
  else fill.chemical.by.source(toxval.db, source)

  #####################################################################
  cat("fix species by source\n")
  #####################################################################
  fix.species.v2(toxval.db, source)

  #####################################################################
  cat("fix human_eco by source\n")
  #####################################################################
  fix.human_eco.by.source(toxval.db, source, reset = T)

  #####################################################################
  cat("fix all.parameters(exposure_method, exposure_route, sex,strain,
    study_duration_class, study_duration_units, study_type,toxval_type,
    exposure_form, media, toxval_subtype) by source\n")
  #####################################################################
  fix.all.param.by.source(toxval.db, source,subsource,fill.toxval_fix=T)

  #####################################################################
  cat("get MW if needed for unit conversion\n")
  #####################################################################
  if(do.convert.units) toxval.set.mw(toxval.db, source)

  #####################################################################
  cat("fix units by source\n")
  #####################################################################
  fix.units.by.source(toxval.db, source,subsource,do.convert.units)

  #####################################################################
  cat("fix strain by source\n")
  #####################################################################
  fix.strain.v2(toxval.db, source)

  #####################################################################
  cat("fix priority_id by source\n")
  #####################################################################
  fix.priority_id.by.source(toxval.db, source)

  #####################################################################
  cat("fix critical_effect by source\n")
  #####################################################################
  doit = T
  if(is.element(source,c("ToxRefDB","ECOTOX"))) doit = F
  if(doit) fix.critical_effect.icf.by.source(toxval.db, source)

  #####################################################################
  cat("add the manual study_type fixes\n")
  #####################################################################
  fix.study_type.manual(toxval.db, source)

  #####################################################################
  cat("fix risk assessment class by source\n")
  #####################################################################
  fix.risk_assessment_class.by.source(toxval.db, source)

  #####################################################################
  cat("fix empty cells to hyphen by source\n")
  #####################################################################
  fix.empty.by.source(toxval.db, source)

  #####################################################################
  cat("fix empty cells in record source to hyphen by source\n")
  #####################################################################
  fix.empty.record_source.by.source(toxval.db, source)

  #####################################################################
  cat("set toxval defaults globally by source\n")
  #####################################################################
  fill.toxval.defaults.global.by.source(toxval.db, source)

  #####################################################################
  cat("special case for toxval_numeric_qualifier\n")
  #####################################################################
  runQuery(paste0("update toxval set toxval_numeric_qualifier='' where toxval_numeric_qualifier='-' and source='",source,"'"),toxval.db)

  #####################################################################
  cat("special case for study_duration_value\n")
  #####################################################################
  runQuery(paste0("update toxval set study_duration_value=-999 where study_duration_value is NULL and source='",source,"'"),toxval.db)

  #####################################################################
  cat("fix qa status by source\n")
  #####################################################################
  fix.qc_status.by.source(toxval.db, source)

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
  export.all.by.source(toxval.db, source)
}
