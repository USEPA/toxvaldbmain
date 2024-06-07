
#-----------------------------------------------------------------------------------
#' Produce a view of the ToxValDB Data
#'
#' `toxvaldb.view` Produces a view for ToxValDB by merging specified tables
#'
#' @param toxval.db Database version
#' @param user The username for the MySQL database. The database instance is
#' hard-coded in the function setDBConn().
#' @param password The user's MySQL database password.
#' @param count If count>0, only select this number of records from each source, used for debugging
#' @return Write a file with the results: data/view/ToxValDB View {toxval.db} {Sys.Date()}.xlsx
#' @export
#-----------------------------------------------------------------------------------
toxvaldb.view <- function(toxval.db="res_toxval_v95", user="_dataminer", password="pass",
                          count=10) {
  printCurrentFunction(toxval.db)
  dir = paste0(toxval.config()$datapath, "data/view/")
  setDBConn(user=user,password=password)

  # Get main Hazard View from toxval tables
  slist = runQuery("select distinct source from toxval",toxval.db)[,1]
  hazard_view = data.frame()
  for(src in slist) {
    n = runQuery(paste0("select count(*) from toxval where source='",src,"'"),toxval.db)[1,1]
    cat(src,":",n,"\n")
    query = paste0("SELECT ",
                   #! used to map
                   "e.toxval_type_supercategory as Supercategory, ",

                   # Main UI View columns
                   "b.source as `Source`, ",
                   "b.subsource as `Subsource`, ",
                   "b.toxval_type as `Type`, ",
                   "b.toxval_subtype as `Subtype`, ",
                   "b.toxval_numeric_qualifier as Qualifier, ",
                   "b.toxval_numeric as `Value`, ",
                   "b.toxval_units as `Units`, ",
                   "b.study_type as `Study Type`, ",
                   "b.risk_assessment_class as `Risk Assessment`, ",
                   "b.exposure_route as `Exposure Route`, ",
                   "b.critical_effect as `Critical Effect`, ",
                   "d.common_name as Species, ",
                   "f.year as Year, ",
                   "b.experimental_record, as `Experimental`, ",
                   "b.subsource_url as `Subsource URL`, ",
                   "f.clowder_doc_id as `Stored Source Record`, ",
                   "b.qc_category as `QC Level`, ",

                   # Chemical Identifiers #! used to map
                   "a.dtxsid as DTXSID, ",

                   # Database Identifiers
                   "b.toxval_id, ",
                   "b.source_hash, ",
                   # Additional study fields
                   "b.study_duration_value, ",
                   "b.study_duration_units, ",
                   "b.study_duration_class, ",

                   "b.exposure_method, ",
                   "b.exposure_form, ",
                   "b.media, ",

                   "b.sex, ",
                   "b.generation, ",
                   "b.lifestage, ",
                   "b.population, ",
                   "b.strain, ",
                   "b.strain_group, ",

                   "b.toxval_type_original, ",
                   "b.toxval_subtype_original, ",
                   "b.toxval_numeric_qualifier_original, ",
                   "b.toxval_numeric_original, ",
                   "b.toxval_units_original, ",
                   "b.study_type_original, ",
                   "b.exposure_route_original, ",
                   "b.critical_effect_original, ",
                   "b.species_original, ",
                   "b.study_duration_value_original, ",
                   "b.study_duration_units_original, ",
                   "b.study_duration_class_original, ",
                   "b.year_original, ",
                   "b.media_original, ",
                   "b.sex_original, ",
                   "b.generation_original, ",
                   "b.lifestage_original, ",
                   "b.population_original, ",
                   "b.strain_original, ",
                   "d.latin_name, ",
                   "d.ecotox_group, ",

                   "b.human_eco, ",


                   # Reference Information
                   "b.source_url, ",

                   "f.long_ref, ",
                   "f.url, ",
                   "f.title, ",
                   "f.author, ",
                   "f.journal, ",
                   "f.volume, ",
                   "f.issue, ",
                   "f.page, ",
                   "f.doi, ",
                   "f.pmid, ",
                   # Additional metdata and flags
                   "b.study_group, ",
                   "f.guideline, ",
                   "f.glp, ",
                   "f.quality, ",
                   "b.key_finding, ",
                   "f.record_source_level, ",
                   "f.record_source_type, ",

                   "f.clowder_doc_metadata, ",
                   "b.qc_status, ",

                   "FROM ",
                   "toxval b ",
                   "INNER JOIN source_chemical a on a.chemical_id=b.chemical_id ",
                   "LEFT JOIN species d on b.species_id=d.species_id ",
                   "INNER JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type ",
                   "INNER JOIN record_source f on b.toxval_id=f.toxval_id ",
                   "WHERE ",
                   "b.source='",src,"'and f.priority=1 and b.qc_status = 'pass'",
                   "ORDER BY DTXSID, Source")
    # Add limiter if provided
    if(is.numeric(count) && count > 0) {
      query = paste(query," limit ",count)
    }
    mat = runQuery(query,toxval.db,T,F)

    hazard_view = hazard_view %>%
      dplyr::bind_rows(mat)
  }

  # Cancer View
  cancer_view = paste0("SELECT dtxsid as DTXSID, ",
                       "source as Source, ",
                       "exposure_route as `Exposure Route`, ",
                       "cancer_call as `Cancer Call`, ",

                       "url as source_url ",
                       "FROM cancer_summary ",
                       "ORDER BY DTXSID, Source") %>%
    runQuery(db = toxval.db)

  # Genotoxicity View
  genetox_summary = paste0("SELECT ",
                           "dtxsid as DTXSID, ",
                           "reports_pos as `Reports Positive`, ",
                           "reports_neg as `Reports Negative`, ",
                           "reports_other as `Reports Other`, ",
                           "genetox_call as `Genotox Call`, ",
                           "ames as AMES, ",
                           "micronucleus as Micronucleus, ",

                           "genetox_summary_id , ",
                           "chemical_id , ",
                           "clowder_doc_id ",
                           "FROM genetox_summary ",
                           "ORDER BY DTXSID") %>%
    runQuery(db = toxval.db)

  genetox_details = paste0("SELECT ",
                           "dtxsid as DTXSID, ",
                           "source as Source, ",
                           "assay_category as `Assay Category`, ",
                           "assay_type as `Assay Type`, ",
                           "metabolic_activation as `Metabolic Activation`, ",
                           "species as Species, ",
                           "strain as Strain, ",
                           "assay_result as `Assay Result`, ",
                           "year as Year, ",

                           "genetox_details_id, ",
                           "chemical_id, ",
                           "genetox_details_uuid, ",
                           "genetox_details_hash, ",
                           "smiles_2d_qsar, ",
                           "aggregate_study_type, ",
                           "assay_code, ",
                           "assay_type_standard, ",
                           "assay_type_simple_aggregate, ",
                           "dose_response, ",
                           "duration, ",
                           "sex, ",
                           "panel_report, ",
                           "assay_outcome, ",
                           "assay_potency, ",
                           "assay_result_std, ",
                           "genetox_results, ",
                           "genetox_note, ",
                           "comment, ",
                           "cytotoxicity, ",
                           "data_quality, ",
                           "document_number, ",
                           "document_source, ",
                           "reference, ",
                           "reference_url, ",
                           "title, ",
                           "protocol_era, ",
                           "clowder_doc_id ",
                           "FROM genetox_details ",
                           "ORDER BY DTXSID, Source") %>%
    runQuery(db = toxval.db)

  # Skin/Eye View
  skineye_view = paste0("SELECT ",
                        "dtxsid as DTXSID, ",
                        "source as Source, ",
                        "study_type as `Study Type`, ",
                        "species as Species, ",
                        "strain as Strain, ",
                        "reliability as Reliability, ",
                        "endpoint as Endpoint, ",
                        "score as Score, ",
                        "year as Year, ",
                        "guideline as Guideline, ",
                        "classification as Classification, ",
                        "result_text as `Result Text`, ",

                        "skin_eye_id, ",
                        "chemical_id, ",
                        "skin_eye_hash, ",
                        "skin_eye_uuid, ",
                        "record_url, ",
                        "glp, ",
                        "authority ",
                        "FROM skin_eye ",
                        "ORDER BY DTXSID, Source") %>%
    runQuery(db = toxval.db)

  sty = createStyle(halign="center", valign="center", textRotation=90, textDecoration = "bold")
  file = paste0(dir, "ToxValDB Views_", toxval.db, "_", Sys.Date(),".xlsx")
  openxlsx::write.xlsx(list(Hazard=hazard_view,
                            Cancer=cancer_view,
                            `Genotoxicity Summary` = genetox_summary,
                            `Genotoxicity Details` = genetox_details,
                            SkinEye = skineye_view), file, firstRow=TRUE, headerStyle=sty)
}
