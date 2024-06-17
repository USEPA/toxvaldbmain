#-------------------------------------------------------------------------------------
#' Export values related to toxval_type, toxval_type_supercategory, study_type, and RAC by source
#' @param toxval.db The current version of toxval
#' @param source_name The source to be reported (single name or list). If source=NULL, report all sources
#' @return A list of tibbles containing relevant values
#' @export
#-------------------------------------------------------------------------------------
export.toxval_type.study_type.rac.by.source <- function(toxval.db, source_name=NULL) {
  printCurrentFunction(toxval.db)

  # Determine sources to include
  source_addition = ""
  if(is.null(source_name)) {
    cat("Exporting values for all sources\n")
  } else {
    cat("Exporting values for ", slist, "\n")
    source_addition = paste0(" WHERE a.source='", source, "'")
  }

  # Get data with all fields included
  query = paste0(
    "SELECT a.supersource, a.source, a.subsource, ",
    "a.toxval_type_original, a.toxval_type, b.toxval_type_supercategory, ",
    "a.study_type, a.study_type_original, ",
    "a.risk_assessment_class, ",
    "c.dtxsid, c.casrn, c.name, ",
    "a.toxval_subtype, a.toxval_units, ",
    "a.study_duration_value, a.study_duration_units, ",
    "d.common_name, a.generation, a.lifestage, ",
    "a.exposure_route, a.exposure_method, a.critical_effect, ",
    "e.long_ref, e.title, ",
    "a.source_hash ",
    "FROM toxval a ",
    "LEFT JOIN toxval_type_dictionary b ON a.toxval_type=b.toxval_type ",
    "LEFT JOIN source_chemical c on a.chemical_id=c.chemical_id ",
    "LEFT JOIN species d on a.species_id=d.species_id ",
    "LEFT JOIN record_source e on a.toxval_id=e.toxval_id ",
    source_addition
  )
  full_data = runQuery(query, toxval.db) %>%
    dplyr::distinct() %>%
    dplyr::mutate(
      dplyr::across(dplyr::where(is.character), ~stringr::str_trunc(., 32000))
    )

  # Get subsets of data for different sheets
  main_data = full_data %>%
    dplyr::select(toxval_type_supercategory, toxval_type_original, toxval_type,
                  study_type_original, study_type, risk_assessment_class) %>%
    dplyr::distinct()

  toxval_type_data = full_data %>%
    dplyr::select(supersource, source, subsource,
                  toxval_type_original, toxval_type, toxval_type_supercategory) %>%
    dplyr::distinct()

  study_type_data = full_data %>%
    dplyr::select(supersource, source, subsource,
                  study_type_original, study_type,
                  dtxsid, casrn, name, risk_assessment_class, toxval_type, toxval_subtype,
                  toxval_units, study_duration_value, study_duration_units,
                  common_name, generation, lifestage, exposure_route, exposure_method, critical_effect,
                  long_ref, title, source_hash) %>%
    dplyr::distinct()

  rac_data = full_data %>%
    dplyr::select(supersource, source, subsource,
                  risk_assessment_class,
                  study_duration_units, study_type, study_type_original, toxval_subtype, toxval_type) %>%
    dplyr::distinct()

  # Combine output into single report
  values_export = list(main_data, toxval_type_data, study_type_data, rac_data)
  names(values_export) = c("main_values", "toxval_type_values", "study_type_values", "rac_values")

  # Write export to file
  fname = paste0(toxval.config()$datapath, "QC Reports/toxval_type.study_type.rac ",
                 Sys.Date(), ".xlsx")
  if(!is.null(source_name)) {
    fname = paste0(toxval.config()$datapath, "QC Reports/toxval_type.study_type.rac ",
                   source_name, " ", Sys.Date(), ".xlsx")
  }

  openxlsx::write.xlsx(values_export, fname)
  return(values_export)
}
