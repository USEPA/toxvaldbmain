#' @title release_qc_report
#' @description Generate report to check field values before release.
#' @param toxval.db The version of toxval into which the tables are loaded.
#' @return DataFrame list of report metrics. An XLSX file is also saved.
release_qc_report <- function(toxval.db){

  list(
    species = runQuery(paste0(
      "SELECT distinct a.species_id, a.species_original, b.common_name, b.latin_name, a.qc_status FROM ",
      "toxval a LEFT JOIN species b ON a.species_id = b.species_id"
    ),
    toxval.db),

    study_type = runQuery(paste0(
      "SELECT DISTINCT ",
      "study_type, study_duration_class_original, study_duration_value_original, study_duration_units_original, ",
      "study_duration_class, study_duration_value, study_duration_units, qc_status ",
      "FROM toxval"
    ),
    toxval.db),

    toxval_type = runQuery(paste0(
      "SELECT DISTINCT a.toxval_type_original, a.toxval_type, b.toxval_type_supercategory, a.qc_status ",
      "FROM toxval a LEFT JOIN toxval_type_dictionary b ON ",
      "a.toxval_type = b.toxval_type"
    ),
    toxval.db),

    risk_assessment_class = runQuery(paste0(
      "SELECT distinct risk_assessment_class, toxval_type, qc_status ",
      "FROM toxval"
    ),
    toxval.db),

    critical_effect_species = runQuery(paste0(
      "SELECT DISTINCT source_hash, source, toxval_type_original, ",
      "species_original, sex_original, generation_original, ",
      "critical_effect_original, critical_effect, qc_status ",
      "FROM res_toxval_v95.toxval WHERE source in (",
      "'ATSDR MRLs', 'Copper Manufacturers', 'EPA HHTV', ",
      "'Health Canada', 'HEAST', 'IRIS', 'PPRTV (CPHEA)') ",
      "and (toxval_type_original like '%RfC%' or ",
      "toxval_type_original like '%unit risk%' or ",
      "toxval_type_original like '%slope factor%' or ",
      "toxval_type_original like '%MRL%' or ",
      "toxval_type_original like '%SF%' or ",
      "toxval_type_original like '%UR%' or ",
      "toxval_type_original like '%RfD%') ",
      "and critical_effect_original != '-'"
    ),
    toxval.db)) %T>%
    # Export report
    {
      writexl::write_xlsx(., paste0("Repo/QC Reports/release_fields_qc_", Sys.Date(),".xlsx"))
    } %>%
    # Return report
    return()
}
