#-----------------------------------------------------------------------------------
#' Build a data frame of the data from toxval and export by source as a
#' series of xlsx files
#'
#' @param toxval.db Database version
#' @param source The source to be updated
#' @param subsource The subsource to be updated
#' @param include.qc.status Boolean whether to include teh qc_status field, or filter out "fail" records. Default is TRUE.
#' #' @return for each source writes an Excel file with the name
#'  ../export/export_by_source_{data}/toxval_all_{toxval.db}_{source}.xlsx
#'
#-----------------------------------------------------------------------------------
export.all.by.source <- function(toxval.db, source=NULL, subsource=NULL, include.qc.status = TRUE) {
  printCurrentFunction(toxval.db)

  dir = paste0(toxval.config()$datapath,"export/export_by_source_",toxval.db,"_",Sys.Date())
  if(!dir.exists(dir)) dir.create(dir)

  slist = sort(runQuery("select distinct source from toxval",toxval.db)[,1])

  if(is.null(source)) {
    nlist = c("source","notes","dtxsid","casrn","name","risk_assessment_class","human_eco","toxval_type",
              "b.toxval_numeric","toxval_units","study_type","common_name","strain","sex",
              "generation","exposure_route","exposure_method","toxicological_effect"
    )
    qc = as.data.frame(matrix(nrow=length(slist),ncol=length(nlist)))
    names(qc) = nlist
    qc$source = slist
    file = paste0(dir,"/ToxValDB release QA ",toxval.db," ",Sys.Date(),".xlsx")
    sty = openxlsx::createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
    openxlsx::write.xlsx(qc,file,firstRow=T,headerStyle=sty)
  }

  if(!is.null(source)) slist = source

  # Handle addition of subsource for queries
  query_addition = ""
  if(!is.null(subsource)) {
    query_addition = paste0(" and subsource='", subsource, "'")
  }

  for(src in slist) {
    query = paste0("SELECT ",
                   "a.dtxsid as DTXSID, ",
                   "a.casrn as CASRN, ",
                   "a.name as NAME, ",
                   "b.source as SOURCE, ",
                   "b.subsource as SUB_SOURCE, ",
                   "b.toxval_type as TOXVAL_TYPE, ",
                   "b.toxval_subtype as TOXVAL_SUBTYPE, ",
                   "e.toxval_type_supercategory as TOXVAL_TYPE_SUPERCATEGORY, ",
                   "b.toxval_numeric_qualifier as QUALIFIER, ",
                   "b.toxval_numeric as TOXVAL_NUMERIC, ",
                   "b.toxval_units as TOXVAL_UNITS, ",
                   "b.risk_assessment_class as RISK_ASSESSMENT_CLASS, ",
                   "b.study_type as STUDY_TYPE, ",
                   "b.study_duration_class as STUDY_DURATION_CLASS, ",
                   "b.study_duration_value as STUDY_DURATION_VALUE, ",
                   "b.study_duration_units as STUDY_DURATION_UNITS, ",
                   "d.common_name as SPECIES_COMMON, ",
                   "b.strain as STRAIN, ",
                   "d.latin_name as LATIN_NAME, ",
                   "d.ecotox_group as SPECIES_SUPERCATEGORY, ",
                   "b.sex as SEX, ",
                   "b.generation as GENERATION, ",
                   "b.lifestage as LIFESTAGE, ",
                   "b.exposure_route as EXPOSURE_ROUTE, ",
                   "b.exposure_method as EXPOSURE_METHOD, ",
                   "b.exposure_form as EXPOSURE_FORM, ",
                   "b.media as MEDIA, ",
                   "b.toxicological_effect as TOXICOLOGICAL_EFFECT, ",
                   "b.experimental_record as EXPERIMENTAL_RECORD, ",
                   "b.study_group as STUDY_GROUP, ",
                   "f.long_ref as LONG_REF, ",
                   "f.doi as DOI, ",
                   "f.title as TITLE, ",
                   "f.author as AUTHOR, ",
                   "b.year as YEAR, ",
                   "f.guideline as GUIDELINE, ",
                   "f.quality as QUALITY, ",
                   "b.qc_category as QC_CATEGORY, ",
                   "b.qc_status as QC_STATUS, ",
                   "b.source_hash as SOURCE_HASH, ",
                   "f.external_source_id as EXTERNAL_SOURCE_ID, ",
                   "f.external_source_id_desc as EXTERNAL_SOURCE_ID_DESC, ",
                   "b.source_url as SOURCE_URL, ",
                   "b.subsource_url as SUBSOURCE_URL, ",
                   "f.clowder_doc_id as STORED_SOURCE_RECORD, ",
                   "b.toxval_type_original as TOXVAL_TYPE_ORIGINAL, ",
                   "b.toxval_subtype_original as TOXVAL_SUBTYPE_ORIGINAL, ",
                   "b.toxval_numeric_original as TOXVAL_NUMERIC_ORIGINAL, ",
                   "b.toxval_units_original as TOXVAL_UNITS_ORIGINAL, ",
                   "b.study_type_original as STUDY_TYPE_ORIGINAL, ",
                   "b.study_duration_class_original as STUDY_DURATION_CLASS_ORIGINAL, ",
                   "b.study_duration_value_original as STUDY_DURATION_VALUE_ORIGINAL, ",
                   "b.study_duration_units_original as STUDY_DURATION_UNITS_ORIGINAL, ",
                   "b.species_original as SPECIES_ORIGINAL, ",
                   "b.strain_original as STRAIN_ORIGINAL, ",
                   "b.sex_original as SEX_ORIGINAL, ",
                   "b.generation_original as GENERATION_ORIGINAL, ",
                   "b.lifestage_original as LIFESTAGE_ORIGINAL, ",
                   "b.exposure_route_original as EXPOSURE_ROUTE_ORIGINAL, ",
                   "b.exposure_method_original as EXPOSURE_METHOD_ORIGINAL, ",
                   "b.exposure_form_original as EXPOSURE_FORM_ORIGINAL, ",
                   "b.media_original as MEDIA_ORIGINAL, ",
                   "b.toxicological_effect_original as TOXICOLOGICAL_EFFECT_ORIGINAL, ",
                   "b.year_original as ORIGINAL_YEAR ",
                   "FROM ",
                   "toxval b ",
                   "LEFT JOIN source_chemical a on a.chemical_id=b.chemical_id ",
                   "LEFT JOIN species d on b.species_id=d.species_id ",
                   "LEFT JOIN toxval_type_dictionary e on b.toxval_type=e.toxval_type ",
                   "LEFT JOIN record_source f on b.toxval_id=f.toxval_id ",
                   "WHERE ",
                   "b.source='",src,"'")

    if(!is.null(subsource)) {
      query = paste0(query, " and b.subsource='",subsource,"'")
    }

    mat = runQuery(query, toxval.db, TRUE, FALSE) %>%
      dplyr::distinct()

    # If not including qc_status, filter "fail" out and remove columns
    if(!include.qc.status){
      mat = mat %>%
        dplyr::filter(!grepl("$fail", QC_STATUS)) %>%
        dplyr::select(-QC_STATUS)
    }

    cat(src, nrow(mat),"\n")
    file = paste0(dir,"/toxval_all_",toxval.db,"_",src, " ", subsource, ".xlsx") %>%
      gsub(" \\.xlsx", ".xlsx", .)
    sty = openxlsx::createStyle(halign="center",valign="center",textRotation=90,textDecoration = "bold")
    openxlsx::write.xlsx(mat,file,firstRow=T,headerStyle=sty)
  }
}
