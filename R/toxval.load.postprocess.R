#--------------------------------------------------------------------------------------
#' Do all of the post-processing steps for a source
#' @param toxval.db The database version to use
#' @param source.db The source database name
#' @param source The source name
#' @param subsource The specific subsource to process, if desired (Default: NULL)
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
  cat("reset initial QC status for", source, "\n")
  #####################################################################
  set.initial.qc_status(toxval.db, source.db, source, subsource)

  #####################################################################
  cat("fix deduping hierarchy by source\n")
  #####################################################################
  # Source specific criteria
  if(source %in% c("USGS HBSL", "EPA OPP")){
    criteria = c("dtxsid", "toxval_type")
  } else {
    criteria = c("dtxsid")
  }

  fix.dedup.hierarchy.by.source(toxval.db=toxval.db,
                                source=source, subsource=subsource,
                                criteria=criteria)

  #####################################################################
  cat("set redundant source_url to '-'\n")
  #####################################################################
  query = paste0("UPDATE toxval SET subsource_url='-' ",
                 "WHERE subsource_url=source_url AND source_url!='-' AND source='", source, "'",
                 query_addition)
  runQuery(query, toxval.db)

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
  cat("get MW if needed for unit conversion\n")
  #####################################################################
  if(do.convert.units) toxval.set.mw(toxval.db, source, subsource)

  #####################################################################
  cat("fix derived toxval_type by source\n")
  #####################################################################
  fix.derived.toxval_type.by.source(toxval.db, source=source, subsource=subsource)

  #####################################################################
  cat("fix escaped quotes in toxval_type\n")
  #####################################################################
  runQuery(paste0("update toxval SET toxval_type"," = ", "REPLACE",
                  "( toxval_type",  ",\'\"\',", " \"'\" ) WHERE toxval_type",
                  " LIKE \'%\"%\' and source = '",source,"'",query_addition),
           toxval.db)

  #####################################################################
  cat("check that the dictionaries are loaded\n")
  #####################################################################
  import.dictionary(toxval.db)

  #####################################################################
  cat("fix priority_id by source\n")
  #####################################################################
  fix.priority_id.by.source(toxval.db, source, subsource)

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
  cat("fix critical_effect by source\n")
  #####################################################################
  doit = TRUE
  if(is.element(source,c("ToxRefDB","ECOTOX"))) doit = FALSE
  if(doit) fix.critical_effect.icf.by.source(toxval.db, source)

  #####################################################################
  cat("fix species by source\n")
  #####################################################################
  fix.species.v2(toxval.db, source, subsource)

  #####################################################################
  cat("fix human_eco by source\n")
  #####################################################################
  fix.human_eco.by.source(toxval.db, source, subsource)

  #####################################################################
  cat("fix strain by source\n")
  #####################################################################
  fix.strain.v2(toxval.db, source, subsource)

  #####################################################################
  cat("fix all.parameters (exposure_method, exposure_route, sex,strain,
    study_duration_class, study_duration_units, study_type, toxval_type,
    exposure_form, media, toxval_subtype) by source\n")
  #####################################################################
  fix.all.param.by.source(toxval.db,source,subsource,fill.toxval_fix=TRUE)

  #####################################################################
  cat("special case for NULL study_duration_value and units\n")
  #####################################################################
  runQuery(paste0("update toxval set study_duration_value=-999 ",
                  "where (study_duration_value is NULL OR study_duration_units='-') ",
                  "and source='",source,"'",query_addition),
           toxval.db)
  runQuery(paste0("update toxval set study_duration_units='-' ",
                  "where study_duration_value=-999 ",
                  "and source='",source,"'",query_addition),
           toxval.db)

  #####################################################################
  cat("set qc_status='fail' for entries without toxval_type_supercategory\n")
  #####################################################################
  query = paste0("UPDATE toxval a LEFT JOIN toxval_type_dictionary b ON a.toxval_type=b.toxval_type ",
                 "SET a.qc_status = CASE ",
                 "WHEN a.qc_status like '%Out of scope, no toxval_type supercategory assignment%' THEN a.qc_status ",
                 "WHEN a.qc_status like '%fail%' THEN CONCAT(a.qc_status, '; Out of scope, no toxval_type supercategory assignment') ",
                 "ELSE 'fail:Out of scope, no toxval_type supercategory assignment'",
                 "END ",
                 "WHERE (b.toxval_type_supercategory IS NULL OR b.toxval_type_supercategory IN ('-', '')) ",
                 "AND a.source='", source, "'", query_addition %>% gsub("subsource", "a.subsource", .))
  runQuery(query, toxval.db)

  #####################################################################
  cat("add the manual study_type fixes\n")
  #####################################################################
  fix.study_type.by.source(toxval.db, mode="import", source=source,
                           subsource=subsource)

  #####################################################################
  cat("set blank study_duration_class for developmental study_type\n")
  #####################################################################
  query = paste0("UPDATE toxval SET study_duration_class='-' ",
                 "WHERE source='", source, "' AND study_type='developmental'",
                 query_addition)
  runQuery(query, toxval.db)

  #####################################################################
  cat("fix units by source\n")
  #####################################################################
  fix.units.by.source(toxval.db, source,subsource,do.convert.units)

  #####################################################################
  cat("set exposure_route='oral' for select toxval_type and mg/kg-day\n")
  #####################################################################
  toxval_type_list = c("BMD", 'NEL', 'LEL', 'LOEL', 'NOEL', 'NOAEL', 'LOAEL')
  query = paste0("UPDATE toxval ",
                 "SET exposure_route = 'oral' ",
                 "WHERE (exposure_route = '-' OR exposure_route_original = '-') ",
                 "AND toxval_units = 'mg/kg-day' ",
                 "AND (", paste0(paste0("toxval_type LIKE '", toxval_type_list, "%'"), collapse = " OR "), ") ",
                 "AND source = '",source,"'",query_addition)
  runQuery(query, toxval.db)

  #####################################################################
  cat("fix study group by source\n")
  #####################################################################
  fix.study_group(toxval.db, source, subsource)

  #####################################################################
  cat("set study_type based on study_group\n")
  #####################################################################
  set.study_type.by.study_group(toxval.db, source, subsource)

  #####################################################################
  cat("fix risk assessment class by source\n")
  #####################################################################
  fix.risk_assessment_class.by.source(toxval.db, source, subsource)

  #####################################################################
  cat("fix QC status by source\n")
  #####################################################################
  fix.qc_status.by.source(toxval.db, source.db, source, subsource)

  #####################################################################
  cat("export report to check toxval_type, exposure_route, units\n")
  #####################################################################
  check.toxval_type.route.units(toxval.db, source, subsource)

  #####################################################################
  cat("set export_source_name and supersource by source\n")
  #####################################################################
  set.supersource.export.names(toxval.db, source)

  cat("set experimental_record tag\n")
  set.experimental_record.by.source(toxval.db, source)

  #####################################################################
  cat("generate export for entries with 'Not Specified' species\n")
  #####################################################################
  query = paste0("SELECT a.source_hash, a.species_original, ",
                 "a.toxval_type_original, a.study_type_original ",
                 "FROM toxval a INNER JOIN species b ON a.species_id=b.species_id ",
                 "WHERE b.common_name LIKE '%Not Specified%' ",
                 "AND a.source='", source, "'",
                 query_addition %>% gsub("subsource", "a.subsource", .))
  not_specified = runQuery(query, toxval.db)

  if(nrow(not_specified)) {
    out_file = paste0("Repo/dictionary/missing/missing_species_", source, "_", subsource, ".xlsx") %>%
      gsub("_\\.xlsx", ".xlsx", .)
    cat(nrow(not_specified), " entries have 'Not Specified' species")
    writexl::write_xlsx(not_specified, paste0("Repo/dictionary/missing/missing"))
  }


  #####################################################################
  cat("export by source\n")
  #####################################################################
  export.all.by.source(toxval.db, source, subsource)
}
